#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import os
import re
import logging
import random
import traceback
from datetime import datetime
from bs4 import BeautifulSoup

# 引入 Crawl4AI 相关组件
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CrawlResult
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from crawl4ai.content_filter_strategy import PruningContentFilter
from markdownify import markdownify as md

# ================= 核心配置区 =================
BASE_URL = "http://oa.htek.com:8090"
OUTPUT_DIR = r"e:\Htek\output"
LOG_DIR = r"e:\Htek\log"

# 登录凭证
USERNAME = "leo.lu"
PASSWORD = "Lzh654321"

# 手动定义的默认 Page ID 列表。
# 当不使用命令行传参直接运行脚本时，将默认爬取这里的页面。
DEFAULT_PAGE_IDS = [
    #"257003697",
    #"257004139",
    #"257004172",
    "257003250",
    "257004322",
    "257003791",
    "257003795",
    "257003799",
    "257003802",
    "257003812",
    "257004447",
    "257004451",
    "257004467",
    "257003705",
    "257004332",
    "257004522"
    # "在这里可以继续添加其他的 ID",
]
# ============================================

def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    log_filename = os.path.join(LOG_DIR, f"confluence_crawl4ai_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def sanitize_filename(title):
    invalid_chars = r'[\\/:*?"<>|]'
    return re.sub(invalid_chars, '_', title)

def fix_relative_paths(html):
    soup = BeautifulSoup(html, 'html.parser')
    for img in soup.find_all('img', src=True):
        if img['src'].startswith('/'):
            img['src'] = BASE_URL + img['src']
    for link in soup.find_all('a', href=True):
        if link['href'].startswith('/'):
            link['href'] = BASE_URL + link['href']
    return str(soup)

def handle_complex_tables(html):
    soup = BeautifulSoup(html, 'html.parser')
    tables = soup.find_all('table')
    for table in tables:
        has_colspan = any(th.get('colspan') for th in table.find_all(['th', 'td']))
        has_rowspan = any(th.get('rowspan') for th in table.find_all(['th', 'td']))
        if has_colspan or has_rowspan:
            table['data-preserve-html'] = 'true'
    return str(soup)

def custom_markdownify(html):
    soup = BeautifulSoup(html, 'html.parser')
    preserved_tables = []
    
    # 提取需要保留 HTML 格式的复杂表格
    for i, table in enumerate(soup.find_all('table', {'data-preserve-html': 'true'})):
        placeholder = f"<!-- PRESERVED_TABLE_{i} -->"
        preserved_tables.append(str(table))
        table.replace_with(placeholder)
    
    # 执行 Markdown 转换
    markdown_content = md(str(soup), heading_style="ATX")
    
    # 还原复杂表格的 HTML
    for i, table_html in enumerate(preserved_tables):
        placeholder = f"<!-- PRESERVED_TABLE_{i} -->"
        markdown_content = markdown_content.replace(placeholder, "\n\n" + table_html + "\n\n")
    
    return markdown_content

# ================= 自动化登录钩子函数 =================
async def on_page_context_created(page, context, **kwargs):
    """
    当页面和上下文创建后，立即执行登录操作。
    此时 Crawl4AI 已经注入了初始化的 page 对象。
    """
    try:
        logging.info("正在执行自动化登录流程...")
        await page.goto(f"{BASE_URL}/login.action", wait_until="networkidle")
        
        await page.fill("#os_username", USERNAME)
        await page.fill("#os_password", PASSWORD)
        await page.click("#loginButton")
        
        # 等待网络空闲
        await page.wait_for_load_state("networkidle")
        
        # 额外硬编码 3 秒等待，确保 Cookie 完全写入和 SSO 重定向完成
        logging.info("登录完成，等待 3 秒确保会话稳定...")
        await asyncio.sleep(3)
        
        logging.info("自动化登录执行完毕")
    except Exception as e:
        logging.error(f"登录失败: {e}")
        # 【改进 4】打印完整堆栈
        logging.error(traceback.format_exc())
    
    # 必须返回 page 对象，以便主爬虫流程继续使用
    return page

# ====================================================

async def fetch_confluence_page(crawler, page_id, logger):
    """
    接受已有的 crawler 对象作为参数，不再每次创建新的
    """
    url = f"{BASE_URL}/pages/viewpage.action?pageId={page_id}"
    
    run_config = CrawlerRunConfig(
        markdown_generator=DefaultMarkdownGenerator(
            content_filter=PruningContentFilter()
        ),
        wait_for="css:#main-content",
        magic=True
    )
    
    try:
        logger.info(f"正在抓取页面: page_id={page_id}")
        result: CrawlResult = await crawler.arun(url=url, config=run_config)
        
        if not result.success:
            logger.error(f"抓取失败: {result.error_message}")
            return None, None

        title = result.metadata.get('title', f"page_{page_id}")
        html_content = result.html
        
        # 执行预处理与格式转换逻辑
        html_content = fix_relative_paths(html_content)
        html_content = handle_complex_tables(html_content)
        markdown_content = custom_markdownify(html_content)
        
        return title, markdown_content
        
    except Exception as e:
        logger.error(f"处理页面异常: {e}, page_id={page_id}")
        # 打印完整堆栈
        logger.error(traceback.format_exc())
        raise

def save_markdown(title, content, logger):
    safe_title = sanitize_filename(title)
    filepath = os.path.join(OUTPUT_DIR, f"{safe_title}.md")
    
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        logger.info(f"成功保存 Markdown 文件: {filepath}")
        return filepath
    except Exception as e:
        logger.error(f"保存文件失败: {e}, filepath={filepath}")
        # 打印完整堆栈
        logger.error(traceback.format_exc())
        raise

async def main():
    logger = setup_logging()
    logger.info("Confluence 自动化爬取工具启动 (基于 Crawl4AI Hooks - 优化版)")
    
    import sys
    # 动态参数判定逻辑
    if len(sys.argv) >= 2:
        page_ids = sys.argv[1:]
        logger.info(f"检测到命令行参数，将爬取以下 page_id: {page_ids}")
    else:
        page_ids = DEFAULT_PAGE_IDS
        logger.info(f"未检测到命令行参数，将使用默认配置的 page_id: {page_ids}")
    
    if not page_ids:
        logger.warning("未提供任何 page_id，任务结束。")
        return
    
    # 全局会话复用 - 将浏览器上下文管理器移到 main 函数
    browser_config = BrowserConfig(
        headless=True,
        verbose=True,
        ignore_https_errors=True,
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
    
    async with AsyncWebCrawler(config=browser_config) as crawler:
        # 只设置一次钩子，浏览器会话会被复用
        crawler.crawler_strategy.set_hook("on_page_context_created", on_page_context_created)
        
        success_count = 0
        fail_count = 0
        
        for i, page_id in enumerate(page_ids):
            try:
                title, markdown_content = await fetch_confluence_page(crawler, page_id, logger)
                if title and markdown_content:
                    save_markdown(title, markdown_content, logger)
                    success_count += 1
            except Exception as e:
                logger.error(f"任务中断: page_id={page_id}, error={e}")
                # 打印完整堆栈
                logger.error(traceback.format_exc())
                fail_count += 1
                continue
            
            # 增加随机间隔，保护服务器不被请求过快
            if i < len(page_ids) - 1:  # 最后一个页面不需要等待
                sleep_time = random.uniform(2, 5)
                logger.info(f"等待 {sleep_time:.2f} 秒后继续下一个页面...")
                await asyncio.sleep(sleep_time)
        
        logger.info(f"所有爬取任务处理完成: 成功 {success_count} 个, 失败 {fail_count} 个")

if __name__ == "__main__":
    asyncio.run(main())
