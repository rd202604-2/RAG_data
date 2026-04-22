#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
【代码 B】读取 page_tree.json，按树状结构创建嵌套文件夹，
使用 Crawl4AI（与 confluence_crawl4ai.py 相同策略）导出 Markdown。
"""

from __future__ import annotations

import argparse
import asyncio
import glob
import json
import logging
import os
import random
import re
import sys
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CrawlResult
from crawl4ai.content_filter_strategy import PruningContentFilter
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from markdownify import markdownify as md

# 与 confluence_crawl4ai.py 的 OUTPUT_DIR 对齐：项目下 output（如 E:\Htek\output）
_WORKSPACE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_OUTPUT_BASE = os.path.join(_WORKSPACE_DIR, "output")

# 依次尝试：先严格等 #main-content，再放宽选择器，最后不设 wait_for（依赖 dom 加载）
_FETCH_WAIT_PROFILES: Tuple[Dict[str, Union[str, int, None]], ...] = (
    {"wait_for": "css:#main-content", "page_timeout": 120_000, "wait_for_timeout": 120_000},
    {"wait_for": "css:#main-content", "page_timeout": 180_000, "wait_for_timeout": 180_000},
    {"wait_for": "css:.wiki-content", "page_timeout": 120_000, "wait_for_timeout": 120_000},
    {"wait_for": "css:#wiki-content", "page_timeout": 120_000, "wait_for_timeout": 120_000},
    {"wait_for": None, "page_timeout": 150_000, "wait_for_timeout": None},
)


def resolve_tree_json_path(cli_path: Optional[str], logger: logging.Logger) -> Optional[str]:
    """
    命令行未传路径时：优先 output/AI项目_page_tree.json；
    否则在 output 下取修改时间最新的 *page_tree*.json。
    """
    if cli_path:
        return os.path.abspath(cli_path)

    preferred = os.path.join(DEFAULT_OUTPUT_BASE, "AI项目_page_tree.json")
    if os.path.isfile(preferred):
        logger.info("未指定 tree_json，使用: %s", preferred)
        return os.path.abspath(preferred)

    if not os.path.isdir(DEFAULT_OUTPUT_BASE):
        logger.error(
            "未指定 tree_json，且默认目录不存在: %s",
            DEFAULT_OUTPUT_BASE,
        )
        return None

    pattern = os.path.join(DEFAULT_OUTPUT_BASE, "*page_tree*.json")
    matches = [p for p in glob.glob(pattern) if os.path.isfile(p)]
    if not matches:
        logger.error(
            "未指定 tree_json，且在 %s 下未找到 *page_tree*.json；请传入 JSON 路径。",
            DEFAULT_OUTPUT_BASE,
        )
        return None

    matches.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    chosen = matches[0]
    logger.info("未指定 tree_json，使用最近修改的: %s", chosen)
    return os.path.abspath(chosen)


def setup_logging(log_dir: str, verbose: bool) -> logging.Logger:
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(
        log_dir, f"markdown_tree_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )
    level = logging.DEBUG if verbose else logging.INFO
    logger = logging.getLogger("markdown_tree_export")
    logger.setLevel(level)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


def sanitize_filename(title: str) -> str:
    invalid_chars = r'[\\/:*?"<>|]'
    return re.sub(invalid_chars, "_", title or "").strip() or "untitled"


def sanitize_segment(node: Dict[str, Any]) -> str:
    slug = (node.get("slug") or "").strip()
    if slug:
        return sanitize_filename(slug)
    return sanitize_filename(node.get("title") or f"page_{node.get('id', '')}")


def fix_relative_paths(html: str, base_url: str) -> str:
    base = base_url.rstrip("/")
    soup = BeautifulSoup(html, "html.parser")
    for img in soup.find_all("img", src=True):
        if img["src"].startswith("/"):
            img["src"] = base + img["src"]
    for link in soup.find_all("a", href=True):
        if link["href"].startswith("/"):
            link["href"] = base + link["href"]
    return str(soup)


def handle_complex_tables(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for table in soup.find_all("table"):
        has_colspan = any(th.get("colspan") for th in table.find_all(["th", "td"]))
        has_rowspan = any(th.get("rowspan") for th in table.find_all(["th", "td"]))
        if has_colspan or has_rowspan:
            table["data-preserve-html"] = "true"
    return str(soup)


def custom_markdownify(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    preserved_tables: List[str] = []

    for i, table in enumerate(soup.find_all("table", {"data-preserve-html": "true"})):
        placeholder = f"<!-- PRESERVED_TABLE_{i} -->"
        preserved_tables.append(str(table))
        table.replace_with(placeholder)

    markdown_content = md(str(soup), heading_style="ATX")

    for i, table_html in enumerate(preserved_tables):
        placeholder = f"<!-- PRESERVED_TABLE_{i} -->"
        markdown_content = markdown_content.replace(placeholder, "\n\n" + table_html + "\n\n")

    return markdown_content


def narrow_confluence_main_html(html: str) -> str:
    """尽量只保留正文区域再转 Markdown，降低侧栏/页眉混入概率。"""
    soup = BeautifulSoup(html, "html.parser")
    for sel in ("#main-content", "#wiki-content", ".wiki-content", "#content"):
        el = soup.select_one(sel)
        if el:
            return str(el)
    return html


def _login_env() -> Tuple[str, str, str]:
    base = os.environ.get("CONFLUENCE_BASE_URL", "").rstrip("/")
    user = os.environ.get("CONFLUENCE_USERNAME", "")
    password = os.environ.get("CONFLUENCE_PASSWORD", "")
    return base, user, password


async def on_page_context_created(page, context, **kwargs):
    """
    打开登录页：若存在标准 Confluence 表单则填写；若无（已登录 / 企业 SSO 自定义页），
    则跳过填表，避免对 #os_username 长时间超时导致后续整批失败。
    """
    logger = logging.getLogger("markdown_tree_export")
    base_url, username, password = _login_env()
    login_url = f"{base_url}/login.action"
    try:
        logger.info("正在检测登录页: %s", login_url)
        await page.goto(login_url, wait_until="domcontentloaded", timeout=90_000)
        try:
            await page.wait_for_selector("#os_username", timeout=20_000)
        except Exception:
            logger.info(
                "未检测到 #os_username（可能已登录或为企业 SSO 页），跳过表单登录，继续后续抓取"
            )
            return page

        logger.info("检测到标准登录表单，正在填写…")
        await page.fill("#os_username", username, timeout=15_000)
        await page.fill("#os_password", password, timeout=15_000)
        await page.click("#loginButton")
        try:
            await page.wait_for_load_state("domcontentloaded", timeout=90_000)
        except Exception:
            logger.warning("登录提交后 domcontentloaded 等待超时，继续执行")
        logger.info("登录流程结束，等待 3 秒稳定会话…")
        await asyncio.sleep(3)
    except Exception as exc:  # noqa: BLE001
        logger.error("登录过程异常: %s", exc)
        logger.error(traceback.format_exc())
    return page


async def fetch_confluence_page(
    crawler: AsyncWebCrawler,
    page_id: str,
    base_url: str,
    logger: logging.Logger,
    max_strategies: int,
) -> Tuple[Optional[str], Optional[str], str]:
    """
    多策略依次尝试同一 URL，尽量提高成功率。
    返回 (title, markdown, error_message)；成功时 error_message 为空字符串。
    """
    url = f"{base_url}/pages/viewpage.action?pageId={page_id}"
    profiles = list(_FETCH_WAIT_PROFILES[: max(1, max_strategies)])

    last_err = ""
    for idx, prof in enumerate(profiles):
        cfg_kwargs: Dict[str, Any] = {
            "markdown_generator": DefaultMarkdownGenerator(
                content_filter=PruningContentFilter()
            ),
            "magic": True,
            "page_timeout": int(prof["page_timeout"]),
        }
        wf = prof.get("wait_for")
        if wf:
            cfg_kwargs["wait_for"] = wf
        wft = prof.get("wait_for_timeout")
        if wft is not None:
            cfg_kwargs["wait_for_timeout"] = int(wft)

        run_config = CrawlerRunConfig(**cfg_kwargs)
        try:
            logger.info(
                "抓取 page_id=%s [%s/%s] wait_for=%s page_timeout=%sms",
                page_id,
                idx + 1,
                len(profiles),
                wf or "(无，仅依赖页面加载)",
                cfg_kwargs["page_timeout"],
            )
            result: CrawlResult = await crawler.arun(url=url, config=run_config)
            if result.success and result.html:
                title = result.metadata.get("title", f"page_{page_id}")
                html_content = narrow_confluence_main_html(result.html)
                html_content = fix_relative_paths(html_content, base_url)
                html_content = handle_complex_tables(html_content)
                markdown_content = custom_markdownify(html_content)
                if markdown_content and markdown_content.strip():
                    return title, markdown_content, ""
                last_err = "HTML 转 Markdown 后为空"
                logger.warning("%s — 将尝试下一策略", last_err)
            else:
                last_err = (result.error_message or "result.success 为 False 或无 html").strip()
                logger.warning("本策略失败: %s", last_err[:800])
        except Exception as exc:  # noqa: BLE001
            last_err = str(exc).strip()
            logger.warning("本策略异常: %s", last_err[:800])
            logger.debug(traceback.format_exc())

        if idx < len(profiles) - 1:
            await asyncio.sleep(1.5 + idx * 0.5)

    return None, None, last_err or "所有抓取策略均失败"


def record_failure(
    failures: List[Dict[str, str]],
    page_id: str,
    title: str,
    rel_parts: List[str],
    error: str,
) -> None:
    rel = "/".join(rel_parts) if rel_parts else ""
    failures.append(
        {
            "page_id": page_id,
            "title": title,
            "relative_path": rel,
            "error": error[:2000],
        }
    )


def write_and_log_failure_report(
    failures: List[Dict[str, str]],
    out_root: str,
    logger: logging.Logger,
) -> None:
    if not failures:
        logger.info("未抓取列表: 无（全部成功或已跳过 resume）")
        return

    report_path = os.path.join(out_root, "_export_failures.json")
    try:
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(failures, f, ensure_ascii=False, indent=2)
        logger.info("失败明细已写入: %s", os.path.abspath(report_path))
    except OSError as exc:
        logger.error("无法写入失败报告 JSON: %s", exc)

    sep = "=" * 60
    lines = [
        "",
        sep,
        f"未抓取成功的文章（共 {len(failures)} 篇）",
        sep,
    ]
    for i, item in enumerate(failures, 1):
        lines.append(
            f"{i}. page_id={item['page_id']} | {item.get('title', '')!s}\n"
            f"   路径: {item.get('relative_path', '')}\n"
            f"   原因: {item.get('error', '')[:500]}"
        )
    lines.append(sep)
    block = "\n".join(lines)
    print(block)
    logger.warning(block)


def unique_folder_name(segment: str, page_id: str, used: Set[str]) -> str:
    name = segment or f"page_{page_id}"
    candidate = name
    if candidate not in used:
        used.add(candidate)
        return candidate
    candidate = f"{name}_{page_id}"
    used.add(candidate)
    return candidate


def load_tree(path: str) -> Dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


async def export_subtree(
    crawler: AsyncWebCrawler,
    node: Dict[str, Any],
    base_dir: str,
    rel_parts: List[str],
    base_url: str,
    logger: logging.Logger,
    resume: bool,
    throttle_min: float,
    throttle_max: float,
    stats: Dict[str, Any],
    failures: List[Dict[str, str]],
    max_strategies: int,
) -> None:
    """
    在 base_dir 下按 rel_parts 拼出当前页目录，写入 index.md，再递归子节点。
    子目录名：json 的 slug（经 sanitize），同层重名则附加 _{page_id}。
    """
    if stats["sequence"] > 0:
        delay = random.uniform(throttle_min, throttle_max)
        logger.debug("页面间隔 sleep %.2fs", delay)
        await asyncio.sleep(delay)
    stats["sequence"] += 1

    page_id = str(node.get("id", ""))
    current_dir = os.path.join(base_dir, *rel_parts)
    os.makedirs(current_dir, exist_ok=True)
    md_path = os.path.join(current_dir, "index.md")

    if resume and os.path.isfile(md_path) and os.path.getsize(md_path) > 0:
        logger.info("跳过（已存在）: %s", md_path)
        stats["skipped"] += 1
    else:
        title, markdown_content, err_msg = await fetch_confluence_page(
            crawler, page_id, base_url, logger, max_strategies
        )
        if title and markdown_content:
            with open(md_path, "w", encoding="utf-8") as f:
                f.write(markdown_content)
            logger.info("已保存: %s", md_path)
            stats["ok"] += 1
        else:
            logger.error("未写入: page_id=%s path=%s err=%s", page_id, md_path, err_msg[:300])
            stats["fail"] += 1
            record_failure(
                failures,
                page_id,
                str(node.get("title") or title or ""),
                rel_parts,
                err_msg,
            )

    children: List[Dict[str, Any]] = node.get("children") or []
    sibling_used: Set[str] = set()
    for child in children:
        ch_seg = sanitize_segment(child)
        ch_id = str(child.get("id", ""))
        folder = unique_folder_name(ch_seg, ch_id, sibling_used)
        child_rel = rel_parts + [folder]
        await export_subtree(
            crawler,
            child,
            base_dir,
            child_rel,
            base_url,
            logger,
            resume,
            throttle_min,
            throttle_max,
            stats,
            failures,
            max_strategies,
        )


async def async_main() -> int:
    parser = argparse.ArgumentParser(description="根据 page_tree.json 导出嵌套 Markdown")
    parser.add_argument(
        "tree_json",
        nargs="?",
        default=None,
        help=(
            "page_tree.json 路径；省略时在项目 output 下自动选择 "
            "（优先 AI项目_page_tree.json，否则取最新的 *page_tree*.json）"
        ),
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        default="",
        help=(
            "导出根目录；省略时为「项目/output/<json 文件名去后缀>_md」"
            f"（当前即 {DEFAULT_OUTPUT_BASE} 下）"
        ),
    )
    parser.add_argument(
        "--log-dir",
        default="",
        help="日志目录，默认为 workspace 下 log",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="若某页 index.md 已存在且非空则跳过抓取，仍递归子目录",
    )
    parser.add_argument(
        "--throttle-min",
        type=float,
        default=2.0,
        help="页面之间随机 sleep 下限（秒）",
    )
    parser.add_argument(
        "--throttle-max",
        type=float,
        default=5.0,
        help="页面之间随机 sleep 上限（秒）",
    )
    parser.add_argument(
        "--max-fetch-strategies",
        type=int,
        default=len(_FETCH_WAIT_PROFILES),
        metavar="N",
        help=(
            "单页最多尝试的抓取策略数（1–%s），越大越不易失败但耗时更长"
            % len(_FETCH_WAIT_PROFILES)
        ),
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    log_dir = args.log_dir or os.path.join(_WORKSPACE_DIR, "log")
    logger = setup_logging(log_dir, args.verbose)

    base_url, username, password = _login_env()
    if not base_url or not username or not password:
        logger.error("请设置环境变量 CONFLUENCE_BASE_URL、CONFLUENCE_USERNAME、CONFLUENCE_PASSWORD")
        return 2

    tree_path = resolve_tree_json_path(args.tree_json, logger)
    if not tree_path or not os.path.isfile(tree_path):
        logger.error("找不到可用的 page_tree JSON: %s", tree_path)
        return 3

    if args.output_dir:
        out_root = os.path.abspath(args.output_dir)
    else:
        stem = os.path.splitext(os.path.basename(tree_path))[0]
        out_root = os.path.join(DEFAULT_OUTPUT_BASE, f"{stem}_md")

    os.makedirs(out_root, exist_ok=True)
    logger.info("JSON: %s", tree_path)
    logger.info("导出根目录: %s", out_root)

    tree = load_tree(tree_path)
    stats: Dict[str, Any] = {"ok": 0, "fail": 0, "skipped": 0, "sequence": 0}
    failures: List[Dict[str, str]] = []
    max_strategies = max(1, min(int(args.max_fetch_strategies), len(_FETCH_WAIT_PROFILES)))

    browser_config = BrowserConfig(
        headless=True,
        verbose=args.verbose,
        ignore_https_errors=True,
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
    )

    async with AsyncWebCrawler(config=browser_config) as crawler:
        crawler.crawler_strategy.set_hook("on_page_context_created", on_page_context_created)
        page_id = str(tree.get("id", ""))
        segment = sanitize_segment(tree)
        root_folder = unique_folder_name(segment, page_id, set())
        await export_subtree(
            crawler,
            tree,
            out_root,
            [root_folder],
            base_url,
            logger,
            args.resume,
            args.throttle_min,
            args.throttle_max,
            stats,
            failures,
            max_strategies,
        )

    logger.info(
        "完成: 成功 %s, 失败 %s, 跳过 %s",
        stats["ok"],
        stats["fail"],
        stats["skipped"],
    )
    write_and_log_failure_report(failures, out_root, logger)
    return 0 if stats["fail"] == 0 else 1


def main() -> None:
    try:
        raise SystemExit(asyncio.run(async_main()))
    except KeyboardInterrupt:
        raise SystemExit(130)


if __name__ == "__main__":
    main()
