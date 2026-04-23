#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
【代码 B】读取 page_tree.json，按树递归创建子文件夹，用 Crawl4AI 抓取内网 Wiki 并导出 Markdown。

未在命令行指定 JSON 路径时，默认扫描整个「output/页面树集合」目录下全部 *.json（排除
以下划线开头的文件），在同一浏览器会话内依次导出多棵树；失败明细合并写入
「output/页面树集合/_export_failures_merged.json」。若该目录下无可用 JSON，则回退为
output 根目录下优先 AI项目_page_tree.json，否则取最新的 *page_tree*.json。

运行过程中默认将断点写入「output/_markdown_tree_export_checkpoint.json」（可用
--checkpoint / --no-checkpoint 调整）。Ctrl+C 或异常中断时会更新该文件状态并追加
「log/markdown_tree_export_interrupt.log」一行说明。下次使用相同页面树列表并加上
--resume：若检查点中 tree_order 与本次一致，则跳过已整棵完成的树；单页仍依据已生成的
.md 跳过（与原有 --resume 行为一致）。

默认导出根：单棵页面树为项目「output」目录本身（其下按树根 slug/title 建子文件夹）；
多棵页面树时为「output/<各 page_tree.json 文件名去后缀>/」以免不同树同名冲突。

抓取与 HTML→Markdown 流程与 code/confluence_crawl4ai.py 对齐：登录钩子使用 networkidle、
整页 HTML 经 fix_relative_paths / handle_complex_tables / custom_markdownify（不裁切
#main-content，尽量保留正文与评论等全部 DOM）。每页写入「节点标题.md」，resume 仍识别
index.md。环境变量：CONFLUENCE_BASE_URL、CONFLUENCE_USERNAME、CONFLUENCE_PASSWORD。
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
from dataclasses import dataclass
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
# 默认批量：该目录下每个 *.json 视为一棵页面树（排除 _*.json，避免误读报告类文件）
DEFAULT_PAGE_TREE_COLLECTION_DIR = os.path.join(DEFAULT_OUTPUT_BASE, "页面树集合")
MERGED_FAILURE_REPORT_BASENAME = "_export_failures_merged.json"
DEFAULT_CHECKPOINT_BASENAME = "_markdown_tree_export_checkpoint.json"
INTERRUPT_LOG_BASENAME = "markdown_tree_export_interrupt.log"
CHECKPOINT_VERSION = 1

# 与 confluence_crawl4ai.py 一致首选 wait_for=#main-content；失败时再尝试不设 wait_for
_FETCH_WAIT_PROFILES: Tuple[Dict[str, Union[str, int, None]], ...] = (
    {"wait_for": "css:#main-content", "page_timeout": 120_000, "wait_for_timeout": 120_000},
    {"wait_for": None, "page_timeout": 180_000, "wait_for_timeout": None},
)


def _list_page_tree_json_in_dir(dir_path: str) -> List[str]:
    """目录内全部 *.json 的绝对路径，按 basename 排序；排除以下划线开头的文件名。"""
    if not os.path.isdir(dir_path):
        return []
    out: List[str] = []
    pattern = os.path.join(dir_path, "*.json")
    for p in sorted(glob.glob(pattern), key=lambda x: os.path.basename(x).lower()):
        if not os.path.isfile(p):
            continue
        base = os.path.basename(p)
        if base.startswith("_"):
            continue
        out.append(os.path.abspath(p))
    return out


def resolve_tree_json_paths(cli_path: Optional[str], logger: logging.Logger) -> List[str]:
    """
    解析本次要处理的 page_tree.json 路径列表（有序）。

    - 命令行传入**文件**路径：仅含该文件的列表。
    - 命令行传入**目录**路径：该目录下全部 *.json（排除 _*.json），按文件名排序。
    - 未传路径：若存在 output/页面树集合 且其中有可用 *.json，则**全部**纳入批量；
      否则回退为 output/AI项目_page_tree.json，再否则为 output 下修改时间最新的 *page_tree*.json。
    """
    if cli_path:
        abs_in = os.path.abspath(cli_path)
        if os.path.isfile(abs_in):
            return [abs_in]
        if os.path.isdir(abs_in):
            found = _list_page_tree_json_in_dir(abs_in)
            if found:
                logger.info("目录模式: 共 %s 个 JSON — %s", len(found), abs_in)
                return found
            logger.error("目录下未找到可用的 *.json: %s", abs_in)
            return []

    coll = DEFAULT_PAGE_TREE_COLLECTION_DIR
    batch = _list_page_tree_json_in_dir(coll)
    if batch:
        logger.info("未指定 tree_json，默认批量扫描页面树集合: 共 %s 个文件 — %s", len(batch), coll)
        return batch

    preferred = os.path.join(DEFAULT_OUTPUT_BASE, "AI项目_page_tree.json")
    if os.path.isfile(preferred):
        logger.info("页面树集合为空，未指定 tree_json，使用: %s", preferred)
        return [os.path.abspath(preferred)]

    if not os.path.isdir(DEFAULT_OUTPUT_BASE):
        logger.error(
            "未指定 tree_json，且默认目录不存在: %s",
            DEFAULT_OUTPUT_BASE,
        )
        return []

    pattern = os.path.join(DEFAULT_OUTPUT_BASE, "*page_tree*.json")
    matches = [p for p in glob.glob(pattern) if os.path.isfile(p)]
    if not matches:
        logger.error(
            "未指定 tree_json，页面树集合 %s 无 JSON，且在 %s 下未找到 *page_tree*.json；请传入路径。",
            coll,
            DEFAULT_OUTPUT_BASE,
        )
        return []

    matches.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    chosen = matches[0]
    logger.info("页面树集合为空，未指定 tree_json，使用最近修改的: %s", chosen)
    return [os.path.abspath(chosen)]


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


def strip_browser_title_suffix(title: str) -> str:
    """
    去掉 Crawl4AI / 浏览器 <title> 常见后缀：「页面名 - 空间 - Htek wiki」。
    若无法匹配则原样返回（去首尾空白）。
    """
    t = (title or "").strip()
    if not t:
        return ""
    m = re.match(r"^(.+?)\s+-\s+.+\s+-\s*Htek\s+wiki\s*$", t, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return t


def markdown_basename_for_page(
    node: Dict[str, Any],
    page_id: str,
    fetched_title: Optional[str],
) -> str:
    """
    导出用 .md 主文件名（不含扩展名）。
    优先使用与 page_tree.json 一致的节点 title，便于与 rel_parts 目录语义对齐且 resume 稳定；
    节点无 title 时再用抓取标题（去掉「- 空间 - Htek wiki」类后缀），最后回退 slug / page_id。
    """
    nt = (node.get("title") or "").strip()
    if nt:
        return sanitize_filename(nt) or "untitled"
    cleaned = strip_browser_title_suffix(fetched_title or "")
    if cleaned:
        return sanitize_filename(cleaned) or "untitled"
    base = sanitize_segment(node) or f"page_{page_id}"
    return sanitize_filename(base) or "untitled"


def resolve_markdown_output_path(
    current_dir: str,
    node: Dict[str, Any],
    page_id: str,
    fetched_title: Optional[str],
) -> str:
    """当前页应写入的 .md 绝对路径。"""
    stem = markdown_basename_for_page(node, page_id, fetched_title)
    return os.path.join(current_dir, f"{stem}.md")


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

    # 纯字母数字占位符，避免 markdownify 对下划线等转义导致 replace 失败（旧版 HTML 注释占位会变为 PRESERVED\_TABLE\_0）
    for i, table in enumerate(soup.find_all("table", {"data-preserve-html": "true"})):
        placeholder = f"@@PRESERVEDTABLE{i}@@"
        preserved_tables.append(str(table))
        table.replace_with(placeholder)

    markdown_content = md(str(soup), heading_style="ATX")

    for i, table_html in enumerate(preserved_tables):
        placeholder = f"@@PRESERVEDTABLE{i}@@"
        markdown_content = markdown_content.replace(placeholder, "\n\n" + table_html + "\n\n")

    return markdown_content


def crawler_html_to_markdown(html: str, base_url: str) -> str:
    """
    与 confluence_crawl4ai.py 中 fetch 成功后的处理一致：不裁切 #main-content，
    保留整页 HTML（含评论区等），再 fix_relative_paths → handle_complex_tables → custom_markdownify。
    """
    html_content = fix_relative_paths(html, base_url)
    html_content = handle_complex_tables(html_content)
    return custom_markdownify(html_content)


def _login_env() -> Tuple[str, str, str]:
    base = os.environ.get("CONFLUENCE_BASE_URL", "").rstrip("/")
    user = os.environ.get("CONFLUENCE_USERNAME", "")
    password = os.environ.get("CONFLUENCE_PASSWORD", "")
    return base, user, password


async def on_page_context_created(page, context, **kwargs):
    """
    与 confluence_crawl4ai.py 一致：进入 login.action → networkidle → 填写 os 表单 → 点击登录
    → networkidle → sleep(3)。失败仅记录日志并返回 page，由后续 arun 继续尝试目标页。
    """
    logger = logging.getLogger("markdown_tree_export")
    base_url, username, password = _login_env()
    if not base_url or not username or not password:
        logger.warning("登录钩子: 缺少 CONFLUENCE_BASE_URL / USERNAME / PASSWORD，跳过自动登录")
        return page

    login_url = f"{base_url.rstrip('/')}/login.action"
    try:
        logger.info("正在执行自动化登录流程: %s", login_url)
        await page.goto(login_url, wait_until="networkidle", timeout=90_000)
        await page.fill("#os_username", username)
        await page.fill("#os_password", password)
        await page.click("#loginButton")
        await page.wait_for_load_state("networkidle")
        logger.info("登录完成，等待 3 秒确保会话稳定…")
        await asyncio.sleep(3)
        logger.info("自动化登录执行完毕")
    except Exception as exc:  # noqa: BLE001
        logger.error("登录失败: %s", exc)
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
                markdown_content = crawler_html_to_markdown(result.html, base_url)
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
    *,
    source_tree_json: str = "",
    markdown_export_root: str = "",
) -> None:
    rel = "/".join(rel_parts) if rel_parts else ""
    row: Dict[str, str] = {
        "page_id": page_id,
        "title": title,
        "relative_path": rel,
        "error": error[:2000],
    }
    if source_tree_json:
        row["source_tree_json"] = source_tree_json
    if markdown_export_root:
        row["markdown_export_root"] = markdown_export_root
    failures.append(row)


def write_and_log_failure_report(
    failures: List[Dict[str, str]],
    report_json_path: str,
    logger: logging.Logger,
) -> None:
    if not failures:
        logger.info("未抓取列表: 无（全部成功或已跳过 resume）")
        return

    report_dir = os.path.dirname(os.path.abspath(report_json_path))
    if report_dir:
        os.makedirs(report_dir, exist_ok=True)
    try:
        with open(report_json_path, "w", encoding="utf-8") as f:
            json.dump(failures, f, ensure_ascii=False, indent=2)
        logger.info("失败明细已写入: %s", os.path.abspath(report_json_path))
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
        src = item.get("source_tree_json", "")
        root = item.get("markdown_export_root", "")
        extra = ""
        if src or root:
            extra = (
                f"\n   树 JSON: {src}\n"
                f"   导出根: {root}"
            )
        lines.append(
            f"{i}. page_id={item['page_id']} | {item.get('title', '')!s}\n"
            f"   路径: {item.get('relative_path', '')}{extra}\n"
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


def default_checkpoint_path() -> str:
    return os.path.join(DEFAULT_OUTPUT_BASE, DEFAULT_CHECKPOINT_BASENAME)


def normalize_path(path: str) -> str:
    """Windows 友好的路径标准化：绝对路径 + 规范大小写。"""
    return os.path.normcase(os.path.abspath(path))


def append_interrupt_log(log_dir: str, line: str, logger: logging.Logger) -> None:
    """单行追加 UTF-8 文本，供人工查看最近一次中断上下文。"""
    try:
        os.makedirs(log_dir, exist_ok=True)
        path = os.path.join(log_dir, INTERRUPT_LOG_BASENAME)
        stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(path, "a", encoding="utf-8") as f:
            f.write(f"[{stamp}] {line}\n")
        logger.info("中断记录已追加: %s", os.path.abspath(path))
    except OSError as exc:
        logger.error("无法写入中断日志: %s", exc)


class RunCheckpoint:
    """
    断点状态：tree_order 与本次任务一致时，trees_completed 表示已整棵导出完毕的树 JSON 绝对路径；
    last_page 记录最近一次成功写入的 .md，便于中断后人工对照。文件以临时文件 + os.replace 原子更新。
    """

    def __init__(
        self,
        path: str,
        tree_paths: List[str],
        resume: bool,
        logger: logging.Logger,
    ) -> None:
        self.path = os.path.abspath(path)
        self.logger = logger
        self.tree_order = [normalize_path(p) for p in tree_paths]
        self.resume_mode = resume
        self.trees_completed: Set[str] = set()
        self.last_page: Dict[str, str] = {}
        self.loaded_status = ""

        if resume and os.path.isfile(self.path):
            self._load_or_reset()
        else:
            self.trees_completed = set()
            self.last_page = {}
            self._save_atomic("running", reason="init")

    def _load_or_reset(self) -> None:
        try:
            with open(self.path, encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError) as exc:
            self.logger.warning("断点文件损坏或不可读，将重建: %s — %s", self.path, exc)
            self.trees_completed = set()
            self.last_page = {}
            self._save_atomic("running", reason="reset_corrupt")
            return

        self.last_page = {}
        self.loaded_status = str(data.get("status") or "")
        lp = data.get("last_page")
        if isinstance(lp, dict):
            self.last_page = {str(k): str(v) for k, v in lp.items()}
        if data.get("version") != CHECKPOINT_VERSION:
            self.logger.warning(
                "断点版本不匹配 (文件=%s, 期望=%s)，将忽略旧 trees_completed",
                data.get("version"),
                CHECKPOINT_VERSION,
            )
            self.trees_completed = set()
        else:
            prev_order_raw = data.get("tree_order") or []
            prev_order = [normalize_path(str(p)) for p in prev_order_raw]
            if prev_order != self.tree_order:
                self.logger.warning(
                    "断点中的 tree_order 与本次解析结果不一致（页面树集合或参数已变），"
                    "将不跳过整树；但仍尝试使用 last_page 从中断位置继续。"
                )
                self.trees_completed = set()
            else:
                self.trees_completed = set(
                    normalize_path(str(p)) for p in (data.get("trees_completed") or [])
                )
                self.logger.info(
                    "已载入断点: 已完成整树 %s 棵，最后成功页: %s",
                    len(self.trees_completed),
                    self.last_page.get("page_id", ""),
                )
        self._save_atomic("running", reason="resume_start")

    def interrupted_page_for_tree(self, tree_path: str) -> str:
        """
        返回某棵树在最近一次中断时记录的 last_page.page_id。
        仅在 --resume 且断点文件状态确为 interrupted 时生效。
        """
        if not self.resume_mode or self.loaded_status != "interrupted":
            return ""
        cp_tree = normalize_path(str(self.last_page.get("tree_json") or ""))
        if cp_tree != normalize_path(tree_path):
            return ""
        return str(self.last_page.get("page_id") or "").strip()

    def is_tree_completed(self, tree_path: str) -> bool:
        return normalize_path(tree_path) in self.trees_completed

    def mark_tree_completed(self, tree_path: str) -> None:
        self.trees_completed.add(normalize_path(tree_path))
        self._save_atomic("running", reason="tree_done")

    def note_page_saved(self, tree_path: str, page_id: str, md_path: str) -> None:
        self.last_page = {
            "tree_json": os.path.abspath(tree_path),
            "page_id": str(page_id),
            "markdown_path": os.path.abspath(md_path),
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }
        self._save_atomic("running", reason="page_saved")

    def mark_completed(self, stats: Dict[str, Any]) -> None:
        self._save_atomic("completed", reason="run_finished", stats=stats)

    def mark_interrupted(
        self,
        stats: Dict[str, Any],
        interrupt_log_dir: str,
        exc: BaseException,
    ) -> None:
        self._save_atomic("interrupted", reason="exception", stats=stats, exc=str(exc))
        msg = (
            f"status=interrupted exc={type(exc).__name__}: {exc!s} | "
            f"trees_done={len(self.trees_completed)} ok={stats.get('ok')} "
            f"fail={stats.get('fail')} skipped={stats.get('skipped')} | "
            f"last_page_id={self.last_page.get('page_id', '')} | "
            f"checkpoint={self.path}"
        )
        append_interrupt_log(interrupt_log_dir, msg, self.logger)

    def _save_atomic(
        self,
        status: str,
        *,
        reason: str = "",
        stats: Optional[Dict[str, Any]] = None,
        exc: str = "",
    ) -> None:
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        payload: Dict[str, Any] = {
            "version": CHECKPOINT_VERSION,
            "tree_order": self.tree_order,
            "trees_completed": sorted(self.trees_completed),
            "last_page": self.last_page,
            "status": status,
            "reason": reason,
            "saved_at": datetime.now().isoformat(timespec="seconds"),
        }
        if stats is not None:
            payload["stats"] = {
                "ok": int(stats.get("ok", 0)),
                "fail": int(stats.get("fail", 0)),
                "skipped": int(stats.get("skipped", 0)),
            }
        if exc:
            payload["exception"] = exc
        tmp = f"{self.path}.{os.getpid()}.tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            os.replace(tmp, self.path)
        except OSError as e:
            self.logger.error("写入断点失败: %s", e)
            try:
                if os.path.isfile(tmp):
                    os.remove(tmp)
            except OSError:
                pass


def _should_skip_resume_export(
    current_dir: str,
    node: Dict[str, Any],
    page_id: str,
    resume: bool,
) -> bool:
    """
    resume 时：若目标标题 .md 已存在且非空则跳过；兼容旧版同目录 index.md。
    探测路径按「尚无抓取标题」时的预期文件名（与树节点 title/slug 一致），避免误跳过。
    """
    if not resume:
        return False
    primary = resolve_markdown_output_path(current_dir, node, page_id, None)
    legacy = os.path.join(current_dir, "index.md")
    for p in (primary, legacy):
        if os.path.isfile(p) and os.path.getsize(p) > 0:
            return True
    return False


def tree_contains_page_id(node: Dict[str, Any], page_id: str) -> bool:
    """判断某 page_id 是否存在于当前树（含子孙）。"""
    if str(node.get("id", "")) == page_id:
        return True
    children: List[Dict[str, Any]] = node.get("children") or []
    for child in children:
        if tree_contains_page_id(child, page_id):
            return True
    return False


@dataclass
class ResumeCursor:
    """
    断点定位游标：active=True 时表示仍在“寻找 last_page”阶段，
    命中 target_page_id 后切换为 active=False，后续节点恢复正常抓取。
    """

    target_page_id: str
    active: bool = True
    reached_target: bool = False


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
    failure_source_tree_json: str = "",
    failure_markdown_export_root: str = "",
    checkpoint: Optional[RunCheckpoint] = None,
    checkpoint_tree_json: str = "",
    resume_cursor: Optional[ResumeCursor] = None,
) -> None:
    """
    在 base_dir 下按 rel_parts 拼出当前页目录，写入「标题.md」，再递归子节点。
    子目录结构仍由 page_tree.json（如 AI项目_page_tree.json）的 children 与 slug/title 决定，
    同层文件夹重名则 unique_folder_name 附加 _{page_id}。
    若抓取失败，保留文件夹并添加 "_空" 后缀。
    """
    page_id = str(node.get("id", ""))
    current_dir = os.path.join(base_dir, *rel_parts)
    os.makedirs(current_dir, exist_ok=True)
    
    is_empty = False

    if resume_cursor is not None and resume_cursor.active:
        stats["skipped"] += 1
        if page_id == resume_cursor.target_page_id:
            resume_cursor.active = False
            resume_cursor.reached_target = True
            logger.info("断点续爬定位到中断页 page_id=%s，后续节点继续抓取", page_id)
        else:
            logger.debug(
                "断点续爬预跳过 page_id=%s（等待命中中断页 page_id=%s）",
                page_id,
                resume_cursor.target_page_id,
            )
    elif _should_skip_resume_export(current_dir, node, page_id, resume):
        primary = resolve_markdown_output_path(current_dir, node, page_id, None)
        legacy = os.path.join(current_dir, "index.md")
        hit = primary if os.path.isfile(primary) and os.path.getsize(primary) > 0 else legacy
        logger.info("跳过（已存在）: %s", hit)
        stats["skipped"] += 1
    else:
        if stats["sequence"] > 0:
            delay = random.uniform(throttle_min, throttle_max)
            logger.debug("页面间隔 sleep %.2fs", delay)
            await asyncio.sleep(delay)
        stats["sequence"] += 1

        title, markdown_content, err_msg = await fetch_confluence_page(
            crawler, page_id, base_url, logger, max_strategies
        )

        md_path = resolve_markdown_output_path(current_dir, node, page_id, title)

        if title and markdown_content:
            with open(md_path, "w", encoding="utf-8") as f:
                f.write(markdown_content)
            logger.info("已保存: %s", md_path)
            stats["ok"] += 1
            if checkpoint is not None and checkpoint_tree_json:
                checkpoint.note_page_saved(checkpoint_tree_json, page_id, md_path)
        else:
            logger.error("未写入: page_id=%s path=%s err=%s", page_id, md_path, err_msg[:300])
            stats["fail"] += 1
            record_failure(
                failures,
                page_id,
                str(node.get("title") or title or ""),
                rel_parts,
                err_msg,
                source_tree_json=failure_source_tree_json,
                markdown_export_root=failure_markdown_export_root,
            )
            is_empty = True

    # 重命名文件夹添加 "_空" 后缀（如果抓取失败）
    if is_empty and len(rel_parts) > 0:
        original_folder_name = rel_parts[-1]
        empty_folder_name = f"{original_folder_name}_空"
        parent_dir = os.path.dirname(current_dir)
        empty_dir = os.path.join(parent_dir, empty_folder_name)
        
        try:
            # 确保目标文件夹不存在
            if os.path.exists(empty_dir):
                import shutil
                shutil.rmtree(empty_dir)
            
            os.rename(current_dir, empty_dir)
            logger.info("文件夹重命名（抓取失败）: %s -> %s", current_dir, empty_dir)
            # 更新 rel_parts，以便子节点使用新的目录名
            rel_parts[-1] = empty_folder_name
            current_dir = empty_dir
        except Exception as e:
            logger.warning("文件夹重命名失败: %s", e)

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
            failure_source_tree_json=failure_source_tree_json,
            failure_markdown_export_root=failure_markdown_export_root,
            checkpoint=checkpoint,
            checkpoint_tree_json=checkpoint_tree_json,
            resume_cursor=resume_cursor,
        )


async def async_main() -> int:
    parser = argparse.ArgumentParser(description="根据 page_tree.json 导出嵌套 Markdown")
    parser.add_argument(
        "tree_json",
        nargs="?",
        default=None,
        help=(
            "page_tree.json 文件路径，或包含多个 *.json 的目录；省略时默认扫描整个 "
            f"「{DEFAULT_PAGE_TREE_COLLECTION_DIR}」下全部页面树 JSON（排除 _*.json）；"
            "若该目录无 JSON 则回退 output 下 AI项目_page_tree.json 或最新 *page_tree*.json"
        ),
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        default="",
        help=(
            "导出目录：处理单个 JSON 时为该树的导出根目录（其下再按页面树建子文件夹）；"
            "一次处理多棵页面树时为父目录，每棵树写入其下「<json 文件名去后缀>」子目录。"
            "省略时单树为项目 output 根目录，多树为 output 下各 json 同名子目录。"
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
        help=(
            "断点续爬：若某页「标题.md」（或旧版 index.md）已存在且非空则跳过抓取，仍递归子目录；"
            "且当 output/_markdown_tree_export_checkpoint.json 中 tree_order 与本次一致时，"
            "跳过已在检查点中标记为整棵完成的树 JSON"
        ),
    )
    parser.add_argument(
        "--checkpoint",
        default="",
        metavar="PATH",
        help=(
            "断点 JSON 路径，默认 output/_markdown_tree_export_checkpoint.json；"
            "与 --no-checkpoint 互斥"
        ),
    )
    parser.add_argument(
        "--no-checkpoint",
        action="store_true",
        help="不读写断点文件与整树跳过逻辑（仍可用 --resume 按已存在 .md 跳过）",
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
            "单页最多尝试的 wait 策略数（1–%s）：先 #main-content，再不设 wait_for"
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

    tree_paths = resolve_tree_json_paths(args.tree_json, logger)
    if not tree_paths:
        logger.error("找不到可用的 page_tree JSON")
        return 3

    multi_tree = len(tree_paths) > 1
    merged_failure_path = os.path.join(
        DEFAULT_PAGE_TREE_COLLECTION_DIR,
        MERGED_FAILURE_REPORT_BASENAME,
    )

    stats: Dict[str, Any] = {"ok": 0, "fail": 0, "skipped": 0, "sequence": 0}
    failures: List[Dict[str, str]] = []
    max_strategies = max(1, min(int(args.max_fetch_strategies), len(_FETCH_WAIT_PROFILES)))

    checkpoint: Optional[RunCheckpoint] = None
    if args.no_checkpoint and args.checkpoint.strip():
        logger.error("--no-checkpoint 与 --checkpoint 不能同时使用")
        return 4
    if not args.no_checkpoint:
        cp_path = os.path.abspath(args.checkpoint.strip()) if args.checkpoint.strip() else default_checkpoint_path()
        checkpoint = RunCheckpoint(cp_path, tree_paths, args.resume, logger)

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

    last_out_root = ""
    exit_code = 1
    try:
        async with AsyncWebCrawler(config=browser_config) as crawler:
            crawler.crawler_strategy.set_hook("on_page_context_created", on_page_context_created)
            for idx, tree_path in enumerate(tree_paths, 1):
                abs_tree = os.path.abspath(tree_path)
                stem = os.path.splitext(os.path.basename(tree_path))[0]
                if args.output_dir:
                    parent = os.path.abspath(args.output_dir)
                    if multi_tree:
                        out_root = os.path.join(parent, stem)
                    else:
                        out_root = parent
                else:
                    if multi_tree:
                        out_root = os.path.join(DEFAULT_OUTPUT_BASE, stem)
                    else:
                        out_root = DEFAULT_OUTPUT_BASE
                last_out_root = out_root

                if checkpoint is not None and args.resume and checkpoint.is_tree_completed(abs_tree):
                    logger.info("断点续爬: 跳过已在检查点中完成的整棵树 [%s/%s] %s", idx, len(tree_paths), abs_tree)
                    continue

                os.makedirs(out_root, exist_ok=True)
                logger.info(
                    "======== 树 [%s/%s] JSON: %s ========",
                    idx,
                    len(tree_paths),
                    tree_path,
                )
                logger.info("导出根目录: %s", out_root)

                tree = load_tree(tree_path)
                resume_cursor: Optional[ResumeCursor] = None
                if checkpoint is not None and args.resume:
                    interrupted_page_id = checkpoint.interrupted_page_for_tree(abs_tree)
                    if interrupted_page_id:
                        if tree_contains_page_id(tree, interrupted_page_id):
                            resume_cursor = ResumeCursor(target_page_id=interrupted_page_id)
                            logger.info(
                                "断点续爬: 当前树将从中断页 page_id=%s 之后继续",
                                interrupted_page_id,
                            )
                        else:
                            logger.warning(
                                "断点 last_page.page_id=%s 不在当前树中，将退回到按 .md 的 --resume 逻辑",
                                interrupted_page_id,
                            )
                page_id = str(tree.get("id", ""))
                segment = sanitize_segment(tree)
                root_folder = unique_folder_name(segment, page_id, set())
                failures_before_tree = len(failures)
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
                    failure_source_tree_json=abs_tree,
                    failure_markdown_export_root=os.path.abspath(out_root),
                    checkpoint=checkpoint,
                    checkpoint_tree_json=abs_tree,
                    resume_cursor=resume_cursor,
                )
                if resume_cursor is not None and not resume_cursor.reached_target:
                    logger.warning(
                        "断点续爬未命中中断页 page_id=%s，本树本次未执行抓取；"
                        "建议检查检查点文件与树 JSON 是否对应",
                        resume_cursor.target_page_id,
                    )
                if checkpoint is not None and len(failures) == failures_before_tree:
                    checkpoint.mark_tree_completed(abs_tree)
                elif checkpoint is not None and len(failures) > failures_before_tree:
                    logger.info(
                        "本树存在抓取失败条目，检查点不标记整树完成，下次 --resume 将重新遍历该树: %s",
                        abs_tree,
                    )

        logger.info(
            "完成（全部树累计）: 成功 %s, 失败 %s, 跳过 %s",
            stats["ok"],
            stats["fail"],
            stats["skipped"],
        )
        if multi_tree:
            write_and_log_failure_report(failures, merged_failure_path, logger)
        elif last_out_root:
            write_and_log_failure_report(
                failures,
                os.path.join(last_out_root, "_export_failures.json"),
                logger,
            )
        exit_code = 0 if stats["fail"] == 0 else 1
    except BaseException as exc:
        if checkpoint is not None and not isinstance(exc, SystemExit):
            checkpoint.mark_interrupted(stats, log_dir, exc)
        raise
    else:
        if checkpoint is not None:
            checkpoint.mark_completed(stats)
    return exit_code


def main() -> None:
    try:
        raise SystemExit(asyncio.run(async_main()))
    except KeyboardInterrupt:
        raise SystemExit(130)


if __name__ == "__main__":
    main()
