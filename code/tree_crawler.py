#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Confluence Server 页面树抓取（代码 A）
依赖：crawl4ai、playwright（与现有项目一致）
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import random
import re
import sys
import traceback
from typing import Any, Dict, List, Optional

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig

# 与浏览器 Hook 之间传递 Playwright BrowserContext
_CTX_HOLDER: Dict[str, Any] = {}


def setup_logging(verbose: bool) -> logging.Logger:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    return logging.getLogger("tree_crawler")


def sanitize_segment(title: str) -> str:
    """供代码 B 参考的目录名片段（Windows 非法字符替换）。"""
    invalid = r'[\\/:*?"<>|]'
    s = re.sub(invalid, "_", title or "").strip()
    return s or "untitled"


async def throttle(logger: logging.Logger) -> None:
    delay = random.uniform(1, 3)
    logger.debug("throttle sleep %.2fs", delay)
    await asyncio.sleep(delay)


async def on_page_context_created(page, context, **kwargs):
    """
    与 confluence_crawl4ai.py 同策略：用真实页面完成 SSO/表单登录，
    并把 BrowserContext 存起来供 context.request 调用 REST API。
    """
    logger = logging.getLogger("tree_crawler")
    base_url = os.environ.get("CONFLUENCE_BASE_URL", "").rstrip("/")
    username = os.environ.get("CONFLUENCE_USERNAME", "")
    password = os.environ.get("CONFLUENCE_PASSWORD", "")

    try:
        logger.info("Hook: 开始登录流程")
        await page.goto(f"{base_url}/login.action", wait_until="networkidle")

        await page.fill("#os_username", username)
        await page.fill("#os_password", password)
        await page.click("#loginButton")

        await page.wait_for_load_state("networkidle")
        logger.info("Hook: 登录提交完成，等待会话稳定…")
        await asyncio.sleep(3)

        _CTX_HOLDER["context"] = context
        _CTX_HOLDER["page"] = page
        logger.info("Hook: 已缓存 BrowserContext，可用于 REST API")
    except Exception as exc:  # noqa: BLE001
        logger.error("Hook 登录失败: %s", exc)
        logger.error(traceback.format_exc())

    return page


async def api_json(context, url: str, logger: logging.Logger) -> Dict[str, Any]:
    await throttle(logger)
    resp = await context.request.get(
        url,
        headers={"Accept": "application/json"},
    )
    if not resp.ok:
        text = await resp.text()
        raise RuntimeError(f"HTTP {resp.status} for {url}: {text[:500]}")
    return await resp.json()


async def fetch_page_meta(
    context, base_url: str, page_id: str, logger: logging.Logger
) -> Dict[str, Any]:
    url = f"{base_url}/rest/api/content/{page_id}"
    return await api_json(context, url, logger)


async def fetch_child_pages_all(
    context, base_url: str, parent_id: str, logger: logging.Logger, limit: int = 50
) -> List[Dict[str, Any]]:
    """分页拉取子页面，合并为列表。"""
    aggregated: List[Dict[str, Any]] = []
    start = 0
    while True:
        url = (
            f"{base_url}/rest/api/content/{parent_id}/child/page"
            f"?limit={limit}&start={start}"
        )
        data = await api_json(context, url, logger)
        batch = data.get("results") or []
        aggregated.extend(batch)
        if len(batch) < limit:
            break
        start += limit
    return aggregated


async def build_subtree(
    context,
    base_url: str,
    node_id: str,
    logger: logging.Logger,
    depth: int,
    max_depth: Optional[int],
) -> Dict[str, Any]:
    meta = await fetch_page_meta(context, base_url, node_id, logger)
    title = meta.get("title") or f"page_{node_id}"
    node: Dict[str, Any] = {
        "id": str(meta.get("id", node_id)),
        "title": title,
        "type": meta.get("type", "page"),
        "slug": sanitize_segment(title),
        "children": [],
    }

    if max_depth is not None and depth >= max_depth:
        logger.warning("达到 max_depth=%s，跳过子节点: %s", max_depth, title)
        return node

    children_meta = await fetch_child_pages_all(context, base_url, node_id, logger)
    for ch in children_meta:
        cid = str(ch["id"])
        subtree = await build_subtree(
            context, base_url, cid, logger, depth + 1, max_depth
        )
        node["children"].append(subtree)
    return node


def print_tree_console(node: Dict[str, Any], prefix: str = "", is_last: bool = True) -> None:
    """终端树形打印：├── └──"""
    connector = "└── " if is_last else "├── "
    title = node.get("title") or ""
    pid = node.get("id") or ""
    line = f"{prefix}{connector}{title}  (id={pid})"
    print(line.encode("utf-8", errors="replace").decode("utf-8", errors="replace"))

    children = node.get("children") or []
    child_prefix = prefix + ("    " if is_last else "│   ")
    for idx, ch in enumerate(children):
        print_tree_console(ch, child_prefix, idx == len(children) - 1)


async def warmup_crawler_session(crawler, base_url: str, logger: logging.Logger) -> None:
    """
    触发一次 arun，使 _crawl_web 创建 page/context 并执行 on_page_context_created。
    URL 可用首页或任意登录后可访问的 Confluence 页面。
    """
    landing = os.environ.get("CONFLUENCE_TREE_LANDING_URL", f"{base_url}/")
    cfg = CrawlerRunConfig(
        wait_until="domcontentloaded",
        magic=True,
        cache_mode=None,  # 若你项目 crawl4ai 版本要求显式传 CacheMode，可改为 CacheMode.BYPASS
    )
    logger.info("Warmup arun: %s", landing)
    result = await crawler.arun(url=landing, config=cfg)
    if not result.success:
        raise RuntimeError(f"Warmup arun 失败: {result.error_message}")


async def async_main() -> int:
    parser = argparse.ArgumentParser(description="Confluence 页面树导出为 page_tree.json")
    parser.add_argument("root_page_id", help="根页面 page id")
    parser.add_argument(
        "-o",
        "--output",
        default="page_tree.json",
        help="输出 JSON 路径",
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        default=None,
        help="可选：限制递归深度（根为 0）",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logger = setup_logging(args.verbose)
    base_url = os.environ.get("CONFLUENCE_BASE_URL", "").rstrip("/")
    if not base_url:
        logger.error("请设置环境变量 CONFLUENCE_BASE_URL")
        return 2

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
        await warmup_crawler_session(crawler, base_url, logger)

        context = _CTX_HOLDER.get("context")
        if context is None:
            logger.error("未获取到 BrowserContext，请检查 Hook 登录是否成功")
            return 3

        root = await build_subtree(
            context,
            base_url,
            str(args.root_page_id),
            logger,
            depth=0,
            max_depth=args.max_depth,
        )

    out_path = args.output
    os.makedirs(os.path.dirname(os.path.abspath(out_path)) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(root, f, ensure_ascii=False, indent=2)

    logger.info("已写入 %s", os.path.abspath(out_path))
    print("\n========== 页面树（预览） ==========\n")
    print_tree_console(root)
    return 0


def main() -> None:
    try:
        raise SystemExit(asyncio.run(async_main()))
    except KeyboardInterrupt:
        raise SystemExit(130)


if __name__ == "__main__":
    main()