#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Confluence Server 页面树抓取（代码 A）
依赖：crawl4ai、playwright（与现有项目一致）

断点续爬（手动 page_id）：使用 --resume-from-page-id，从该页作为「子树根」开始递归
抓取整棵子树并写入 -o；无需自动 checkpoint。若同时写了位置参数 root_page_id，将忽略
root_page_id。
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
from urllib.parse import parse_qs, urlparse
from typing import Any, Dict, List, Optional

from crawl4ai import AsyncWebCrawler, BrowserConfig, CacheMode, CrawlerRunConfig

# 与浏览器 Hook 之间传递 Playwright BrowserContext
_CTX_HOLDER: Dict[str, Any] = {}
# 运行期进度计数：每成功读取一个页面结构 +1
_PROGRESS: Dict[str, int] = {"pages": 0}
# 运行期 checkpoint 状态
_CHECKPOINT_STATE: Dict[str, Any] = {"path": None, "root": None}


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


def write_checkpoint(
    checkpoint_path: Optional[str],
    root: Optional[Dict[str, Any]],
    logger: logging.Logger,
    reason: str,
) -> None:
    """把当前已构建的树快照写入 checkpoint 文件。"""
    if not checkpoint_path or root is None:
        return
    os.makedirs(os.path.dirname(os.path.abspath(checkpoint_path)) or ".", exist_ok=True)
    tmp_path = f"{checkpoint_path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(root, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, checkpoint_path)
    logger.info("Checkpoint 已保存(%s): %s", reason, os.path.abspath(checkpoint_path))


def write_checkpoint_on_abort(logger: logging.Logger, reason: str) -> None:
    """异常/中断时尽量保存当前树快照。"""
    root = _CHECKPOINT_STATE.get("root")
    checkpoint_path = _CHECKPOINT_STATE.get("path")
    if root is None:
        logger.warning("中断时无可保存的根节点快照，跳过 checkpoint")
        return
    write_checkpoint(checkpoint_path, root, logger, reason)


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
        links = data.get("_links") or {}
        next_link = links.get("next")
        if not next_link:
            if len(batch) == limit:
                logger.debug(
                    "child/page 返回满页但无 next，按当前批次结束: parent_id=%s start=%d limit=%d",
                    parent_id,
                    start,
                    limit,
                )
            break
        parsed = urlparse(next_link)
        next_start_raw = parse_qs(parsed.query).get("start", [None])[0]
        if next_start_raw is None:
            start += limit
        else:
            try:
                start = int(next_start_raw)
            except ValueError:
                logger.warning(
                    "next 链接的 start 非法(%s)，改为按 limit 递增: parent_id=%s",
                    next_start_raw,
                    parent_id,
                )
                start += limit
    return aggregated


async def build_subtree(
    context,
    base_url: str,
    node_id: str,
    logger: logging.Logger,
    depth: int,
    max_depth: Optional[int],
    checkpoint_every: int,
    checkpoint_path: Optional[str],
    existing_node: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    meta = await fetch_page_meta(context, base_url, node_id, logger)
    title = meta.get("title") or f"page_{node_id}"
    if existing_node is None:
        node: Dict[str, Any] = {}
    else:
        node = existing_node
    # 就地写入节点字段，保证 checkpoint 中已挂载的占位节点会实时变“完整”。
    node["id"] = str(meta.get("id", node_id))
    node["title"] = title
    node["type"] = meta.get("type", "page")
    node["slug"] = sanitize_segment(title)
    if "children" not in node:
        node["children"] = []
    if depth == 0:
        _CHECKPOINT_STATE["root"] = node
    _PROGRESS["pages"] += 1
    logger.info(
        "进度: 已读取页面结构 #%d | depth=%d | id=%s | title=%s",
        _PROGRESS["pages"],
        depth,
        node["id"],
        title,
    )
    if checkpoint_every > 0 and _PROGRESS["pages"] % checkpoint_every == 0:
        write_checkpoint(
            checkpoint_path,
            _CHECKPOINT_STATE.get("root") or node,
            logger,
            f"pages={_PROGRESS['pages']}",
        )

    if max_depth is not None and depth >= max_depth:
        logger.warning("达到 max_depth=%s，跳过子节点: %s", max_depth, title)
        return node

    children_meta = await fetch_child_pages_all(context, base_url, node_id, logger)
    for ch in children_meta:
        cid = str(ch["id"])
        child_title = ch.get("title") or f"page_{cid}"
        # 先挂占位子节点，保证深度递归期间 checkpoint 能看到已发现结构。
        node["children"].append(
            {
                "id": cid,
                "title": child_title,
                "type": ch.get("type", "page"),
                "slug": sanitize_segment(child_title),
                "children": [],
            }
        )
        subtree = await build_subtree(
            context,
            base_url,
            cid,
            logger,
            depth + 1,
            max_depth,
            checkpoint_every,
            checkpoint_path,
            existing_node=node["children"][-1],
        )
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
    # cache_mode=None 会在 crawl4ai 的 arun 内被改成 ENABLED，命中缓存时不会走浏览器，
    # on_page_context_created 不会执行，导致拿不到 BrowserContext。
    cfg = CrawlerRunConfig(
        wait_until="domcontentloaded",
        magic=True,
        cache_mode=CacheMode.BYPASS,
    )
    logger.info("Warmup arun: %s", landing)
    result = await crawler.arun(url=landing, config=cfg)
    if not result.success:
        raise RuntimeError(f"Warmup arun 失败: {result.error_message}")


async def async_main() -> int:
    parser = argparse.ArgumentParser(description="Confluence 页面树导出为 page_tree.json")
    parser.add_argument(
        "root_page_id",
        nargs="?",
        default=None,
        help="根页面 page id；与 --resume-from-page-id 二选一即可（同时给出时以断点 id 为准）",
    )
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
    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=20,
        help="每读取 N 个页面结构保存一次 checkpoint（<=0 表示关闭）",
    )
    parser.add_argument(
        "--checkpoint-path",
        default=None,
        help="checkpoint 文件路径（默认: <output>.checkpoint.json）",
    )
    parser.add_argument(
        "--resume-from-page-id",
        default=None,
        metavar="PAGE_ID",
        help=(
            "从手动指定的 page_id 作为子树根开始抓取（输出为该页的整棵子树 JSON）；"
            "可不写位置参数 root_page_id。与位置参数同时给出时忽略 root_page_id"
        ),
    )
    args = parser.parse_args()

    logger = setup_logging(args.verbose)
    _PROGRESS["pages"] = 0
    _CHECKPOINT_STATE["root"] = None
    base_url = os.environ.get("CONFLUENCE_BASE_URL", "").rstrip("/")
    if not base_url:
        logger.error("请设置环境变量 CONFLUENCE_BASE_URL")
        return 2

    resume_id = (args.resume_from_page_id or "").strip()
    root_id = (args.root_page_id or "").strip()
    if resume_id:
        start_page_id = resume_id
        if root_id:
            logger.info(
                "已指定 --resume-from-page-id=%s，将从此页作为子树根抓取；忽略位置参数 root_page_id=%s",
                resume_id,
                root_id,
            )
    elif root_id:
        start_page_id = root_id
    else:
        logger.error("请提供 root_page_id，或使用 --resume-from-page-id PAGE_ID")
        return 2

    checkpoint_path = (
        args.checkpoint_path if args.checkpoint_path else f"{args.output}.checkpoint.json"
    )
    _CHECKPOINT_STATE["path"] = checkpoint_path
    if args.checkpoint_every > 0:
        logger.info(
            "已启用 checkpoint: every=%d, path=%s",
            args.checkpoint_every,
            os.path.abspath(checkpoint_path),
        )
    else:
        logger.info("checkpoint 已关闭 (--checkpoint-every <= 0)")

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
    root: Optional[Dict[str, Any]] = None
    try:
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
                start_page_id,
                logger,
                depth=0,
                max_depth=args.max_depth,
                checkpoint_every=args.checkpoint_every,
                checkpoint_path=checkpoint_path,
            )
    except Exception as exc:  # noqa: BLE001
        logger.error("抓取流程异常中断: %s", exc)
        try:
            write_checkpoint_on_abort(logger, f"abort:{type(exc).__name__}")
        except Exception as ckpt_exc:  # noqa: BLE001
            logger.error("中断后保存 checkpoint 失败: %s", ckpt_exc)
        raise

    if root is None:
        logger.error("未生成页面树结果")
        return 4

    out_path = args.output
    os.makedirs(os.path.dirname(os.path.abspath(out_path)) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(root, f, ensure_ascii=False, indent=2)

    logger.info("已写入 %s", os.path.abspath(out_path))
    if args.checkpoint_every > 0:
        write_checkpoint(checkpoint_path, root, logger, "final")
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