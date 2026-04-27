#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
递归清洗 Confluence 导出的 Markdown：提取元数据为 YAML、以「转至元数据起始」切掉文首壳层、
再截断尾部 Confluence/评论区噪声，清理 UI/锚点/面包屑等，输出到新目录不覆盖源文件。

可选：在清洗时依据正文中的 pageId 调用 Confluence REST 列举附件并下载，同时拉取
MD 中已有的 download/attachments 直链，去重后保存到与输出 .md 同目录。

默认输入、输出见 DEFAULT_* 常量。
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import unquote, urlparse

try:
    from tqdm import tqdm
except ImportError as e:  # pragma: no cover
    raise SystemExit(
        "请先安装 tqdm：pip install tqdm\n"
        "或在 code 目录执行：pip install -r requirements.txt"
    ) from e

try:
    import requests
except ImportError as e:  # pragma: no cover
    raise SystemExit(
        "请安装 requests：pip install requests\n"
        "或在 code 目录执行：pip install -r requirements.txt"
    ) from e

from confluence_env_defaults import (  # noqa: E402 与 run_tree_crawler_resume.bat 默认一致
    confluence_base_url,
    confluence_password,
    confluence_username,
)


# ---------------------------------------------------------------------------
# 路径默认值（相对本文件所在仓库根目录的 output）
# ---------------------------------------------------------------------------
_REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_INPUT_DIR = _REPO_ROOT / "output" / "人力资源空间"
DEFAULT_OUTPUT_DIR = _REPO_ROOT / "output" / "clean_md" / "人力资源空间"
DEFAULT_LOG_DIR = _REPO_ROOT / "log"


# ---------------------------------------------------------------------------
# 1. 元数据提取
# ---------------------------------------------------------------------------
# 单行整段：由 [A](一行的 url) 创建, 最后修改于 [B](一行的 url)
_METADATA_AUTHOR_DATE = re.compile(
    r"由\s*\[([^\]]+)\]\([^)]*\)\s*创建\s*,\s*最后修改于\s*\[([^\]]+)\]\([^)]*\)",
    re.MULTILINE,
)
# 作者 [name](url) 的 url 被 Confluence 折到下一行时：在「)」与「创建,」之间含换行，需 DOTALL
_METADATA_AUTHOR_DATE_ML = re.compile(
    r"由\s*\[([^\]]+)\](?:.|\n)*?创建\s*,\s*最后修改于\s*\[([^\]]+)\]",
    re.DOTALL,
)
# 老页：)创建于[日期]…（无「最后修改于」）
_METADATA_CREATED_ONLY_ML = re.compile(
    r"由\s*\[([^\]]+)\](?:.|\n)*?创建\s*于\s*\[([^\]]+)\]",
    re.DOTALL,
)
# 壳内 viewpageattachments 须在文首切刀前从 raw 取 URL 后拼进正文
_VIEWPAGE_ATTACHMENTS_URL = re.compile(
    r"https?://[^)\s\"']+/pages/viewpageattachments\.action\?pageId=\d+",
    re.IGNORECASE,
)
# URL 前一小段中解析「附件(N) / t(N)」等
_ATTACH_COUNT_BEFORE_URL = re.compile(
    r"附件[（(](?P<na>\d+)[）)]|"
    r"\[t[（(](?P<nb>\d+)[）)]\]|"
    r"t[（(](?P<nc>\d+)[）)]\s*附件|"
    r"t附件[（(](?P<nd>\d+)[）)]",
    re.IGNORECASE,
)
# 整行/段移除：允许行首可选 *、空白，匹配到行尾
_METADATA_LINE_REMOVE = re.compile(
    r"^\s*\*?\s*由\s*\[[^\]]+\]\([^)]*\)\s*创建\s*,\s*最后修改于\s*\[[^\]]+\]\([^)]*\)[^\n]*\s*$",
    re.MULTILINE,
)
# 宽松：多行时「由…创建」与「最后修改于…」分两行
_METADATA_SPLIT = re.compile(
    r"^\s*\*?\s*由\s*\[([^\]]+)\]\([^)]*\)\s*创建\s*,?\s*$"
    r"|^\s*最后修改于\s*\[([^\]]+)\]\([^)]*\)[^\n]*\s*$",
    re.MULTILINE,
)


def _is_confluence_time_line(line: str) -> bool:
    """
    判断是否为「最后修改于 [时间](https://...」的完整行（行尾为 )，可含链内 title 引号）。
    """
    t = line.rstrip()
    if "最后修改于" not in t or "http" not in t or "(" not in t:
        return False
    return t.endswith(")")


def _strip_confluence_byline_block(text: str) -> Tuple[str, str, str]:
    """
    按行删除 Confluence 页内「由 [作者](…可折行)创建, 最后修改于 [时间](http…)」元数据行块。
    与示例「AI辅助编程工具对比」一致：作者链接触可拆成多行，时间链接在块末行。
    返回 (新文本, author, last_modified)。
    """
    lines = text.splitlines(keepends=True)
    out: List[str] = []
    i = 0
    n = len(lines)
    author, last_modified = "", ""
    byline_names = re.compile(
        r"由\s*\[([^\]]+)\](?:.|\n)*?创建\s*,\s*最后修改于\s*\[([^\]]+)\]",
        re.DOTALL,
    )
    start_pat = re.compile(r"^\s*([*+])?\s*由\s+\[")
    while i < n:
        line = lines[i]
        if not start_pat.search(line):
            out.append(line)
            i += 1
            continue
        max_j = min(n, i + 20)
        found = False
        for j in range(i, max_j):
            block = "".join(lines[i : j + 1])
            if "创建" not in block or "最后修改于" not in block:
                continue
            m = byline_names.search(block)
            if not m:
                continue
            last_ln = lines[j]
            if not _is_confluence_time_line(last_ln) or "最后修改" not in last_ln:
                continue
            author = (m.group(1) or "").strip()
            last_modified = (m.group(2) or "").strip()
            i = j + 1
            found = True
            break
        if not found:
            out.append(line)
            i += 1
    return ("".join(out), author, last_modified)


def _strip_confluence_created_byline_block(text: str) -> Tuple[str, str, str]:
    """
    删除老版导出的「由 [作者]…(折行)创建于[日期](http)」多行块（无「最后修改于」）；
    如产品提升建议流程 示例；组 1 为作者、组 2 为日期的方括号内展示文本，时间写入 last_modified 字段。
    """
    lines = text.splitlines(keepends=True)
    out: List[str] = []
    i, n = 0, len(lines)
    author, when = "", ""
    names = re.compile(
        r"由\s*\[([^\]]+)\](?:.|\n)*?创建\s*于\s*\[([^\]]+)\]",
        re.DOTALL,
    )
    start = re.compile(r"^\s*([*+])?\s*由\s+\[")
    while i < n:
        line = lines[i]
        if not start.search(line):
            out.append(line)
            i += 1
            continue
        if "最后修改于" in line:
            out.append(line)
            i += 1
            continue
        found = False
        for j in range(i, min(n, i + 20)):
            block = "".join(lines[i : j + 1])
            if "最后修改于" in block:
                break
            if not re.search(r"创建\s*于", block) or re.search(
                r"创建\s*,\s*最后修改", block
            ):
                continue
            m = names.search(block)
            if not m or "最后修改" in block:
                continue
            if not re.search(
                r"\)创建\s*于|创建于\s*\[", block, re.IGNORECASE
            ):
                continue
            l2 = lines[j]
            if "http" not in l2 or not l2.rstrip().endswith(")"):
                continue
            author, when = m.group(1).strip(), m.group(2).strip()
            i = j + 1
            found = True
            break
        if not found:
            out.append(line)
            i += 1
    return ("".join(out), author, when)


def extract_metadata(text: str) -> Tuple[str, Dict[str, str]]:
    """
    从正文提取作者、最后修改时间，删除匹配到的元数据原文，返回 (剩余文本, meta)。
    meta 键：author, last_modified（可能为空字符串）。

    Confluence 导出中作者 [name](url) 的 url 常被硬折行；先按行整段删除
    _strip_confluence_byline_block，再回退单行 `_METADATA_AUTHOR_DATE` 与分行 `_METADATA_SPLIT`。
    """
    author, last_modified = "", ""

    # 1) 多行/折行作者链接触（如 AI辅助编程工具对比 示例）——有「最后修改于」
    cleaned, a2, t2 = _strip_confluence_byline_block(text)
    if a2 and t2:
        author, last_modified = a2, t2

    # 1b) 老页「由 … 创建于[日期]」无「最后修改于」（如 产品提升建议流程）
    cleaned, ac, tc = _strip_confluence_created_byline_block(cleaned)
    if ac and tc and not (author and last_modified):
        author, last_modified = ac, tc

    # 2) 整行在一条的「由 [x](...)创建, 最后修改于 [y](...)」
    if not (author and last_modified):
        m = _METADATA_AUTHOR_DATE.search(cleaned)
        if m:
            author = (m.group(1) or "").strip()
            last_modified = (m.group(2) or "").strip()

    cleaned = _METADATA_LINE_REMOVE.sub("", cleaned)

    if not (author and last_modified):
        m2 = _METADATA_AUTHOR_DATE.search(cleaned)
        if m2:
            author = author or (m2.group(1) or "").strip()
            last_modified = last_modified or (m2.group(2) or "").strip()

    # 3) 「由」行与「最后」行分行的旧格式
    if not (author and last_modified):
        m_author = re.search(
            r"(?m)^\s*\*?\s*由\s*\[([^\]]+)\]\([^)]*\)\s*创建\s*,?\s*$",
            cleaned,
        )
        m_time = re.search(
            r"(?m)^\s*最后修改于\s*\[([^\]]+)\]\([^)]*\)[^\n]*\s*$",
            cleaned,
        )
        if m_author:
            author = (m_author.group(1) or "").strip()
        if m_time:
            last_modified = (m_time.group(1) or "").strip()

    cleaned = _METADATA_SPLIT.sub("", cleaned)
    if not (author and last_modified):
        cleaned, a3, t3 = _strip_confluence_byline_block(cleaned)
        if a3 and t3:
            author, last_modified = a3, t3

    meta = {"author": author, "last_modified": last_modified}
    return cleaned, meta


def prepend_yaml_front_matter(text: str, meta: Dict[str, str]) -> str:
    """在全文最顶部插入标准 YAML Front Matter（若两项皆空则仍插入占位，便于下游统一）。"""
    author = meta.get("author") or ""
    lm = meta.get("last_modified") or ""
    # YAML 1.1 简单标量：含逗号、冒号、引号等时用双引号包裹
    def _escape_scalar(s: str) -> str:
        if not s:
            return '""'
        if any(c in s for c in "\n:#{}[]&*!|>'\",") or s.strip() != s:
            esc = s.replace("\\", "\\\\").replace('"', '\\"')
            return f'"{esc}"'
        return s

    fm = (
        "---\n"
        f"author: {_escape_scalar(author)}\n"
        f"last_modified: {_escape_scalar(lm)}\n"
        "---\n\n"
    )
    body = text.lstrip("\ufeff").lstrip("\n")
    return fm + body


# ---------------------------------------------------------------------------
# 1b. 文首切刀：以「[转至元数据起始](#…)」为界（其后的内容即正文，之前为侧栏/壳层）
# ---------------------------------------------------------------------------
_CUTOFF_LINE_METADATA_START = re.compile(
    r"(?m)^\s*\[转至元数据起始\]\s*\(([^)]*)\)\s*(?:\r?\n)?",
)


def _cut_body_after_confluence_metadata_start(text: str) -> str:
    """
    丢弃自文件开头至并包含「[转至元数据起始](…」整行之前的全部内容。
    与 Confluence 导出中「H1(当前页) → 转至元数据尾 → 由/时间 → 转至元数据起 → 真正文」结构一致；无此锚时原样返回。
    """
    m = _CUTOFF_LINE_METADATA_START.search(text)
    if not m:
        return text
    return text[m.end() :].lstrip("\n")


# ---------------------------------------------------------------------------
# 2. 尾部截断
# ---------------------------------------------------------------------------
# 不依赖「赞成为第一个赞同者」：有赞同者/社交文案变动时易漏截或误截，优先用 ## 评论、无标签 等块。
_TAIL_MARKERS: Tuple[str, ...] = (
    "## 评论",
    "无标签",
    "写评论...",
    "概览\n\n内容工具",
    "基于 Atlassian Confluence",
    '{"serverDuration":',
    "search\n\nrecentlyviewed",
    "更新恢复页面保留草稿取消",
)


def truncate_tail(text: str) -> str:
    """在全文自前向后查找，取最早命中的尾标记所在行，从该行起丢弃到文末（优先 ## 评论 等结构块，已不含「赞成第一个」唯一点）。"""
    cut_pos: Optional[int] = None
    for marker in _TAIL_MARKERS:
        idx = text.find(marker)
        if idx == -1:
            continue
        line_start = text.rfind("\n", 0, idx) + 1
        if cut_pos is None or line_start < cut_pos:
            cut_pos = line_start
    if cut_pos is None:
        return text
    return text[:cut_pos].rstrip() + ("\n" if text[:cut_pos].strip() else "")


# ---------------------------------------------------------------------------
# 2b. 附件/图片：壳层中 viewpageattachments 与裸资源 URL
# ---------------------------------------------------------------------------
def _extract_confluence_attachments_index_md(raw: str) -> str:
    """
    在「文首切刀」会丢弃的壳层里常有 t 附件(1) / viewpageattachments 链；从 raw 先提取
    再拼到切刀后的「## 附件与文件」段。无则返回空串。
    """
    seen: set = set()
    out: List[str] = []
    for m in _VIEWPAGE_ATTACHMENTS_URL.finditer(raw):
        url = m.group(0)
        if url in seen:
            continue
        pre = raw[max(0, m.start() - 220) : m.start()]
        n: Optional[str] = None
        for m2 in _ATTACH_COUNT_BEFORE_URL.finditer(pre):
            g2 = m2.groupdict()
            n = g2.get("na") or g2.get("nb") or g2.get("nc") or g2.get("nd")
        label = f"共 {n} 个" if n else "本页"
        seen.add(url)
        out.append(f"* [附件与文件管理（{label}）]({url})")
    if not out:
        return ""
    return "## 附件与文件\n" + "\n".join(out) + "\n\n"


def _normalize_bare_confluence_file_lines(text: str) -> str:
    """
    将整行仅为 Confluence 资源 URL（download/attachments/…）的行转为可点击 Markdown。
    图片类扩展名用 ![](url)，否则用 * [文件名](url)。
    """
    _durl = r"https?://[^)\s\"']+/download/attachments/[^/]+/[^)\s\"']+"
    img_re = re.compile(
        _durl + r"\.(?:png|jpe?g|gif|webp|svg)(?:\?[^)\s\"']*)?$",
        re.IGNORECASE,
    )
    bare_line = re.compile(rf"^({_durl})$", re.IGNORECASE)
    out: List[str] = []
    for line in text.splitlines(keepends=True):
        st = line.rstrip("\n\r").strip()
        if not st:
            out.append(line)
            continue
        if st[0] in ("#", "-", "*", ">", "|", "`", "!") or st.startswith("["):
            out.append(line)
            continue
        m = bare_line.match(st)
        if m and "download/attachments" in st:
            u = m.group(1)
            path = urlparse(u).path
            name = unquote(path.rsplit("/", 1)[-1].split("?")[0]) or u
            if img_re.match(u) or re.search(
                r"image\d|thumbnail", u, re.IGNORECASE
            ):
                out.append(f"![{name}]({u})\n")
            else:
                out.append(f"* [{name}]({u})\n")
        else:
            out.append(line)
    return "".join(out)


# ---------------------------------------------------------------------------
# 3. UI 组件清理
# ---------------------------------------------------------------------------
_SKIP_MENU_ITEM = re.compile(
    r"(?:转至内容|转至导航栏|转至主菜单|转至动作菜单|转至快速搜索)",
)
_RELATED_HINTS = ("已链接应用程序", "Htek CRM", "Htek JIRA", "Htek wiki")
_NAV_KEYWORDS = (
    "空间",
    "人员",
    "日程表",
    "创建空白页",
    "在线帮助",
    "快捷键",
    "注销",
)
_OP_BUTTON_LINE = re.compile(
    r"(?m)^\s*[*+]\s*\[(?:E编辑|F收藏|S分享|导出为PDF|Doc文件导入)\][^\n]*\s*$",
)


def _split_front_matter(text: str) -> Tuple[str, str]:
    """返回 (front_matter 含结尾换行 或空串, body)。"""
    if not text.startswith("---\n"):
        return "", text
    end = text.find("\n---\n", 4)
    if end == -1:
        return "", text
    fm = text[: end + len("\n---\n")]
    body = text[end + len("\n---\n") :]
    return fm, body


def _join_front_matter(fm: str, body: str) -> str:
    if not fm:
        return body
    return fm + body


def _remove_consecutive_list_block(
    lines: List[str],
    start_idx: int,
    line_predicate,
) -> int:
    """
    从 start_idx 起若当前行为列表项，则向后删除连续同类列表行，返回删除行数。
    line_predicate(line: str) -> bool 表示该行是否属于该块。
    """
    if start_idx >= len(lines):
        return 0
    line = lines[start_idx]
    if not re.match(r"^\s*[-*+]\s", line):
        return 0
    if not line_predicate(line):
        return 0
    j = start_idx + 1
    while j < len(lines) and re.match(r"^\s*[-*+]\s", lines[j]) and line_predicate(lines[j]):
        j += 1
    del lines[start_idx:j]
    return j - start_idx


def _scan_and_remove_ui_blocks_in_head(body: str, max_lines: int = 120) -> str:
    """
    在正文「前 max_lines 行」内扫描并删除快捷菜单、关联应用、主导航等列表块。
    """
    lines = body.splitlines(keepends=True)
    head_limit = min(len(lines), max_lines)
    i = 0
    while i < head_limit:
        line = lines[i]

        def pred_skip_menu(ln: str) -> bool:
            return bool(_SKIP_MENU_ITEM.search(ln))

        def pred_related(ln: str) -> bool:
            return any(h in ln for h in _RELATED_HINTS)

        def pred_nav_cluster(ln: str) -> bool:
            return any(k in ln for k in _NAV_KEYWORDS)

        removed = _remove_consecutive_list_block(lines, i, pred_skip_menu)
        if removed:
            head_limit = min(len(lines), max_lines)
            continue
        removed = _remove_consecutive_list_block(lines, i, pred_related)
        if removed:
            head_limit = min(len(lines), max_lines)
            continue
        # 主导航：连续列表行中至少命中 3 个不同导航关键词
        if re.match(r"^\s*[-*+]\s", line) and any(k in line for k in _NAV_KEYWORDS):
            j = i
            hits: set[str] = set()
            while j < len(lines) and re.match(r"^\s*[-*+]\s", lines[j]):
                for k in _NAV_KEYWORDS:
                    if k in lines[j]:
                        hits.add(k)
                j += 1
                if len(hits) >= 3:
                    del lines[i:j]
                    head_limit = min(len(lines), max_lines)
                    break
            else:
                i += 1
            continue
        i += 1
    return "".join(lines)


def _remove_page_tree_sidebar(body: str) -> str:
    marker = "##### 页面树结构"
    idx = body.find(marker)
    if idx == -1:
        return body
    rest = body[idx:]
    end_candidates: List[int] = []

    m_row = re.search(r"(?m)^.*重排页面.*$", rest)
    if m_row:
        end_candidates.append(m_row.end())

    m_h1 = re.search(r"(?m)^#\s+.+$", rest)
    if m_h1:
        end_candidates.append(m_h1.start())

    if not end_candidates:
        return body[:idx].rstrip() + "\n"
    end_rel = min(end_candidates)
    return body[:idx] + rest[end_rel:]


def _remove_op_button_lines(text: str) -> str:
    prev = None
    cur = text
    while prev != cur:
        prev = cur
        cur = _OP_BUTTON_LINE.sub("", cur)
    return cur


def clean_ui_noise(text: str) -> str:
    """删除快捷菜单、关联应用、主导航、页面树侧边栏、操作按钮等。"""
    fm, body = _split_front_matter(text)
    body = _scan_and_remove_ui_blocks_in_head(body)
    body = _remove_page_tree_sidebar(body)
    body = _remove_op_button_lines(body)
    return _join_front_matter(fm, body)


# ---------------------------------------------------------------------------
# 4. 锚点、面包屑、图标、编辑器残留
# ---------------------------------------------------------------------------
_ANCHOR_LINKS = (
    "跳到banner的尾部",
    "回到标题开始",
    "转至元数据结尾",
    "转至元数据起始",
)
_ANCHOR_REMOVE = re.compile(
    r"\[(?:"
    + "|".join(re.escape(s) for s in _ANCHOR_LINKS)
    + r")\]\([^)]*\)",
)
_AVATAR_PLACEHOLDER = re.compile(
    r"\[!\[用户图标:\s*添加头像\]\([^)]*\)\]\([^)]*\)\s*",
)
_TOOLBAR_HASH_LINK = re.compile(r"(?m)^\s*[*+-]\s*\[[^\]]+\]\(#\)\s*$")
# 文首连续多行「数字. [文字](url)」面包屑
_BREADCRUMB_BLOCK_START = re.compile(
    r"\A(?:\d+\.\s+\[[^\]]+\]\([^)]*\)\s*(?:\r?\n|$))+",
    re.MULTILINE,
)


def clean_anchors_breadcrumbs_icons(text: str) -> str:
    fm, body = _split_front_matter(text)
    body = _ANCHOR_REMOVE.sub("", body)
    body = _AVATAR_PLACEHOLDER.sub("", body)
    body = _TOOLBAR_HASH_LINK.sub("", body)
    # 面包屑：文首连续编号链接行，循环剥离
    prev_b = None
    while prev_b != body:
        prev_b = body
        body_stripped = body.lstrip("\n")
        m = _BREADCRUMB_BLOCK_START.match(body_stripped)
        if m:
            body = body_stripped[m.end() :].lstrip("\n")
    return _join_front_matter(fm, body)


# ---------------------------------------------------------------------------
# 5. 标题去重与空行压缩
# ---------------------------------------------------------------------------
_FRONT_MATTER_BLOCK = re.compile(r"\A---\r?\n.*?\r?\n---\r?\n", re.DOTALL)


def _normalize_title_line(s: str) -> str:
    s = s.strip()
    s = re.sub(r"^#{1,6}\s+", "", s)
    return re.sub(r"\s+", " ", s).casefold()


def dedupe_opening_titles(text: str) -> str:
    """
    在 YAML 之后合并文首连续、相同或高度相似的标题/重复 H1，仅保留一条「# 标题」。
    """
    m = _FRONT_MATTER_BLOCK.match(text)
    if not m:
        fm, body = "", text
    else:
        fm = m.group(0)
        body = text[m.end() :]

    lines = body.splitlines()
    while lines and not lines[0].strip():
        lines.pop(0)
    if not lines:
        return text

    # 文首连续「标题候选」块：最多 8 行，每行为 H1~H6 或短纯文本（非列表）
    block_end = 0
    max_scan = min(len(lines), 8)
    for i in range(max_scan):
        s = lines[i].strip()
        if not s:
            break
        if re.match(r"^#{1,6}\s+\S", s):
            block_end = i + 1
            continue
        if (
            len(s) < 200
            and not s.startswith(("-", "*", "+"))
            and not s.startswith("#")
        ):
            block_end = i + 1
            continue
        break

    if block_end < 2:
        merged_body = "\n".join(lines)
        return fm + merged_body if fm else merged_body

    block = lines[:block_end]
    rest = lines[block_end:]

    canonical = ""
    for s in block:
        st = s.strip()
        if re.match(r"^#{1,6}\s+\S", st):
            canonical = st
            break
    if not canonical:
        for s in block:
            st = s.strip()
            if st and not st.startswith("#"):
                canonical = "# " + st
                break
    if not canonical:
        st = block[0].strip()
        canonical = st if st.startswith("#") else "# " + st

    norms = [_normalize_title_line(x.strip()) for x in block]
    similar_pair = False
    for i in range(len(norms)):
        for j in range(i + 1, len(norms)):
            a, b = norms[i], norms[j]
            if a and b and (a == b or a in b or b in a):
                similar_pair = True
                break
        if similar_pair:
            break
    if not similar_pair:
        merged_body = "\n".join(lines)
        return fm + merged_body if fm else merged_body

    # 统一为一级标题：取首个 # 级标题的正文，否则用首条纯文本
    canonical_fixed = canonical
    mh = re.match(r"^(#{1,6})\s+(.+)$", canonical_fixed.strip())
    if mh:
        canonical_fixed = "# " + mh.group(2).strip()
    elif not canonical_fixed.strip().startswith("#"):
        canonical_fixed = "# " + canonical_fixed.strip()

    norm_can = _normalize_title_line(canonical_fixed)
    tail_from_block: List[str] = []
    for raw in block:
        st = raw.strip()
        ns = _normalize_title_line(st)
        if ns == norm_can or (
            ns and norm_can and (ns in norm_can or norm_can in ns)
        ):
            continue
        tail_from_block.append(raw)

    out_lines = [canonical_fixed] + tail_from_block + rest
    merged_body = "\n".join(out_lines).lstrip("\n")
    return fm + merged_body


def compress_blank_lines(text: str) -> str:
    """将连续 3 个及以上换行压缩为 2 个换行（保留一个空段）。"""
    return re.sub(r"(?:\r?\n){3,}", "\n\n", text)


# ---------------------------------------------------------------------------
# 2c. Confluence REST + 直链：清洗后同目录下载附件
# ---------------------------------------------------------------------------
# 从 MD 中收集 pageId（与 shell 中 viewpage / viewpageattachments 等一致）
_PAGEID_IN_URL_RE = re.compile(r"[\?&]pageId=(\d+)", re.IGNORECASE)
# 正文中已有直链（与 _normalize_bare 中逻辑同系：避免吞到引号后字符）
_DIRECT_ATTACH_URL_RE = re.compile(
    r"https?://[^)\s\"']+/download/attachments/[^)\s\"']+",
    re.IGNORECASE,
)


@dataclass
class AttachmentDownloadConfig:
    """与 --download-attachments 配套；认证可二选一或组合。"""

    enabled: bool = False
    user: str = ""
    password: str = ""
    cookie: str = ""
    request_delay: float = 0.5
    rest_limit: int = 50


def _safe_win_filename(name: str, max_len: int = 180) -> str:
    for c in '<>:"/\\|?*':
        name = name.replace(c, "_")
    name = name.strip() or "attachment"
    return name[:max_len]


def _confluence_site_bases(blob: str) -> List[str]:
    """从正文中推断 Confluence 根 URL（scheme://host:port），优先带 /pages/ 或 /download/ 的。"""
    bases: List[str] = []
    seen: Set[str] = set()
    for pat in (
        r"(https?://[^\s\"'`()<>\]]+)(?=/pages/)",
        r"(https?://[^\s\"'`()<>\]]+)(?=/download/attachments/)",
    ):
        for m in re.finditer(pat, blob, re.IGNORECASE):
            b = m.group(1).rstrip("/")
            if b not in seen and "://" in b:
                seen.add(b)
                bases.append(b)
    if bases:
        return bases
    m = re.search(
        r"(https?://[a-z0-9.:-]+)(?=/pages/[^ \n)\"']*[\?&]pageId=)",
        blob,
        re.IGNORECASE,
    )
    if m:
        return [m.group(1).rstrip("/")]
    m2 = re.search(r"https?://[a-z0-9.:-]+(?=/)", blob, re.IGNORECASE)
    if m2:
        return [m2.group(0).rstrip("/")]
    b = confluence_base_url()
    return [b] if b else []


def _link_download_url(
    site_base: str, download_field: str
) -> str:
    t = (download_field or "").strip()
    if t.lower().startswith("http://") or t.lower().startswith("https://"):
        return t
    if not t:
        return ""
    base = site_base.rstrip("/")
    if t.startswith("/"):
        return f"{base}{t}"
    return f"{base}/{t}"


def _confluence_get_json(
    session: requests.Session,
    url: str,
    request_delay: float,
    logger: logging.Logger,
) -> Optional[Dict[str, Any]]:
    try:
        time.sleep(request_delay)
        r = session.get(
            url,
            timeout=120,
            headers={"Accept": "application/json"},
        )
    except OSError as e:  # pragma: no cover
        logger.error("Confluence 请求失败 %s: %s", url, e)
        return None
    if r.status_code == 404:
        logger.debug("Confluence 404（探测站点/页面时属正常）: %s", url)
        return None
    if r.status_code in (401, 403):
        logger.error("Confluence 拒绝访问 %s：HTTP %s", url, r.status_code)
        return None
    if not r.ok:
        logger.error("Confluence %s：HTTP %s", url, r.status_code)
        return None
    try:
        return r.json()
    except ValueError:
        logger.error("Confluence 非 JSON: %s", url)
        return None


def _list_attachments_for_page(
    session: requests.Session,
    site_base: str,
    page_id: str,
    rest_limit: int,
    request_delay: float,
    logger: logging.Logger,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    start = 0
    rest_base = f"{site_base.rstrip('/')}/rest/api"
    while True:
        u = f"{rest_base}/content/{page_id}/child/attachment?start={start}&limit={rest_limit}"
        data = _confluence_get_json(session, u, request_delay, logger)
        if not data or "results" not in data:
            break
        part = data["results"]
        if not part:
            break
        out.extend(part)
        if len(part) < rest_limit:
            break
        start += len(part)
    return out


def _download_url_to(
    session: requests.Session,
    file_url: str,
    dest: Path,
    request_delay: float,
    logger: logging.Logger,
) -> bool:
    if dest.is_file() and dest.stat().st_size > 0:
        return True
    try:
        time.sleep(request_delay)
        r = session.get(file_url, stream=True, timeout=300, allow_redirects=True)
    except OSError as e:  # pragma: no cover
        logger.error("下载失败 %s: %s", file_url, e)
        return False
    if r.status_code in (401, 403, 404):
        logger.error("下载被拒绝或不存在 %s：HTTP %s", file_url, r.status_code)
        return False
    if r.status_code >= 400:
        logger.error("下载失败 %s：HTTP %s", file_url, r.status_code)
        return False
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    try:
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(65536):
                if chunk:
                    f.write(chunk)
        tmp.replace(dest)
        return True
    except OSError as e:  # pragma: no cover
        logger.error("写出失败 %s: %s", dest, e)
        if tmp.is_file():
            try:
                tmp.unlink()
            except OSError:
                pass
        return False


def _filename_from_url(url: str) -> str:
    p = urlparse(url)
    seg = p.path.rsplit("/", 1)
    if len(seg) >= 2 and seg[1]:
        return _safe_win_filename(unquote(seg[1].split("?")[0]))
    return "attachment.bin"


def _dedupe_dest_for_url(out_dir: Path, url: str, base_name: str) -> Path:
    p = out_dir / base_name
    if not p.is_file():
        return p
    d = hashlib.md5(url.encode("utf-8", errors="replace")).hexdigest()[:8]
    return out_dir / f"{d}_{base_name}"


def _build_confluence_session(cfg: AttachmentDownloadConfig) -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = "Htek/clean_md_files (Confluence attach)"
    if cfg.user and cfg.password:
        s.auth = (cfg.user, cfg.password)
    if cfg.cookie:
        s.headers["Cookie"] = cfg.cookie
    return s


def download_confluence_attachments_by_markdown(
    raw: str,
    cleaned: str,
    out_dir: Path,
    cfg: AttachmentDownloadConfig,
    session: requests.Session,
    logger: logging.Logger,
) -> Tuple[int, int]:
    """
    合并 raw 与清洗结果：用 pageId 调 Confluence REST 子附件的 download 链，再合入正文中
    出现的 /download/attachments/ 直链，按绝对 URL 去重后保存到 out_dir。

    返回 (成功数, 失败数)。
    """
    if not cfg.enabled:
        return 0, 0
    blob = f"{raw}\n{cleaned}"
    success, failed = 0, 0
    done_urls: Set[str] = set()
    bases = _confluence_site_bases(blob)

    page_ids = set(_PAGEID_IN_URL_RE.findall(blob))
    direct_urls: Set[str] = {
        m.group(0) for m in _DIRECT_ATTACH_URL_RE.finditer(blob)
    }

    # 1) REST：每个 pageId 在推断出的各 site 上试，直到某一站点返回非空子附件
    for pid in sorted(page_ids, key=int):
        atts: List[Dict[str, Any]] = []
        site_ok = ""
        for site in bases or []:
            part = _list_attachments_for_page(
                session,
                site,
                pid,
                cfg.rest_limit,
                cfg.request_delay,
                logger,
            )
            if part:
                atts = part
                site_ok = site
                break
        if not atts or not site_ok:
            continue
        for att in atts:
            durl = _link_download_url(
                site_ok,
                str((att.get("_links") or {}).get("download") or ""),
            )
            if not durl or durl in done_urls:
                continue
            done_urls.add(durl)
            title = (att.get("title") or "").strip() or _filename_from_url(durl)
            name = _safe_win_filename(unquote(title.split("?")[0])) or "file"
            att_id = str(att.get("id") or "")
            fn = f"{att_id[:20]}_{name}" if att_id else name
            out_p = out_dir / fn
            if _download_url_to(
                session, durl, out_p, cfg.request_delay, logger
            ):
                success += 1
            else:
                failed += 1

    # 2) 直链：与 REST 已下载 URL 去重
    for durl in sorted(direct_urls):
        if durl in done_urls:
            continue
        done_urls.add(durl)
        name = _filename_from_url(durl)
        if name in ("attachment.bin", "attachment", "file"):
            name = "file.bin"
        out_p = _dedupe_dest_for_url(out_dir, durl, name)
        if _download_url_to(
            session, durl, out_p, cfg.request_delay, logger
        ):
            success += 1
        else:
            failed += 1
    return success, failed


def clean_markdown_pipeline(raw: str) -> str:
    """按用户指定顺序链式调用各清洗步骤。"""
    att = _extract_confluence_attachments_index_md(raw)
    text, meta = extract_metadata(raw)
    text = _cut_body_after_confluence_metadata_start(text)
    if att:
        text = att + text
    text = prepend_yaml_front_matter(text, meta)
    text = truncate_tail(text)
    text = clean_ui_noise(text)
    text = clean_anchors_breadcrumbs_icons(text)
    text = dedupe_opening_titles(text)
    text = _normalize_bare_confluence_file_lines(text)
    text = compress_blank_lines(text)
    return text.rstrip() + "\n"


def iter_markdown_files(root: Path) -> List[Path]:
    return sorted(root.rglob("*.md"))


def process_all(
    input_dir: Path,
    output_dir: Path,
    logger: logging.Logger,
    attach_cfg: Optional[AttachmentDownloadConfig] = None,
) -> Tuple[int, int, int, int]:
    """
    遍历 input_dir 下全部 .md，写入 output_dir 保持相对路径；可选在写盘后对每篇
    合并 raw+正文，按 REST 与直链下载附件到**输出 .md 同目录**。

    返回 (md 成功数, md 失败数, 附件成功数, 附件失败/跳过不计入失败时可忽略)。
    附件 “失败数” 仅统计 _download_url_to 返回 False 与下载阶段异常。
    """
    ac = attach_cfg or AttachmentDownloadConfig(False)
    session: Optional[requests.Session] = (
        _build_confluence_session(ac) if ac.enabled else None
    )
    files = iter_markdown_files(input_dir)
    ok, fail, att_ok, att_fail = 0, 0, 0, 0
    for path in tqdm(files, desc="清洗 Markdown", unit="file"):
        rel = path.relative_to(input_dir)
        out_path = output_dir / rel
        try:
            raw = path.read_text(encoding="utf-8", errors="replace")
            cleaned = clean_markdown_pipeline(raw)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(cleaned, encoding="utf-8", newline="\n")
            ok += 1
            if session and ac.enabled:
                try:
                    a, f = download_confluence_attachments_by_markdown(
                        raw,
                        cleaned,
                        out_path.parent,
                        ac,
                        session,
                        logger,
                    )
                    att_ok += a
                    att_fail += f
                except Exception:  # pragma: no cover
                    logger.exception("附件下载过程异常: %s", out_path)
                    att_fail += 1
        except Exception:
            fail += 1
            logger.exception("处理失败: %s", path)
    return ok, fail, att_ok, att_fail


def setup_logger(log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "clean_md_files.log"
    logger = logging.getLogger("clean_md_files")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.ERROR)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="清洗 Confluence 导出的 Markdown，输出到新目录。")
    p.add_argument(
        "-i",
        "--input",
        type=Path,
        default=DEFAULT_INPUT_DIR,
        help=f"输入根目录（默认：{DEFAULT_INPUT_DIR}）",
    )
    p.add_argument(
        "-o",
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"输出根目录（默认：{DEFAULT_OUTPUT_DIR}）",
    )
    p.add_argument(
        "--log-dir",
        type=Path,
        default=DEFAULT_LOG_DIR,
        help=f"日志目录（默认：{DEFAULT_LOG_DIR}）",
    )
    p.add_argument(
        "--download-attachments",
        action="store_true",
        help="清洗后从正文合并 raw 解析 pageId 与直链，调用 Confluence REST 并下载到输出 .md 同目录。",
    )
    p.add_argument(
        "--confluence-user",
        default="",
        help="Basic 认证用户名；环境变量 CONFLUENCE_USERNAME / CONFLUENCE_USER；"
        "未设置时与 run_tree_crawler_resume.bat 中默认一致。",
    )
    p.add_argument(
        "--confluence-password",
        default="",
        help="Basic 认证密码；环境变量 CONFLUENCE_PASSWORD；"
        "未设置时与 run_tree_crawler_resume.bat 中默认一致。",
    )
    p.add_argument(
        "--confluence-cookie",
        default="",
        help='浏览器 Cookie 串，例如 "JSESSIONID=..."。',
    )
    p.add_argument(
        "--confluence-cookie-file",
        type=Path,
        default=None,
        help="从文件读取 Cookie（整文件为一个 Cookie 头内容或一行）。",
    )
    p.add_argument(
        "--request-delay",
        type=float,
        default=0.5,
        metavar="SEC",
        help="两次 HTTP 之间的最小间隔（秒，默认 0.5，减轻 Confluence 压力）。",
    )
    p.add_argument(
        "--rest-attachment-page-size",
        type=int,
        default=50,
        metavar="N",
        help="每页拉取子附件的 REST limit（默认 50）。",
    )
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    input_dir: Path = args.input.resolve()
    output_dir: Path = args.output.resolve()
    log_dir: Path = args.log_dir.resolve()

    if not input_dir.is_dir():
        print(f"错误：输入目录不存在或不是文件夹：{input_dir}", file=sys.stderr)
        return 2

    logger = setup_logger(log_dir)
    print(f"输入：{input_dir}")
    print(f"输出：{output_dir}")
    print(f"错误日志：{log_dir / 'clean_md_files.log'}")

    cookie = (args.confluence_cookie or "").strip()
    if getattr(args, "confluence_cookie_file", None) is not None:
        cfp: Path = args.confluence_cookie_file
        cfp = cfp.expanduser().resolve()
        if cfp.is_file():
            cookie = cfp.read_text(encoding="utf-8", errors="replace").strip()
        elif args.confluence_cookie_file:  # 显式传了但不存在
            print(
                f"警告：--confluence-cookie-file 不存在，已忽略：{cfp}",
                file=sys.stderr,
            )
    ac = AttachmentDownloadConfig(
        enabled=bool(getattr(args, "download_attachments", False)),
        user=(getattr(args, "confluence_user", "") or "").strip()
        or confluence_username(),
        password=(getattr(args, "confluence_password", "") or "").strip()
        or confluence_password(),
        cookie=cookie,
        request_delay=float(
            getattr(args, "request_delay", 0.5) or 0.0
        )
        or 0.0,
        rest_limit=max(1, int(getattr(args, "rest_attachment_page_size", 50) or 50)),
    )
    if ac.enabled and not (ac.user and ac.password) and not ac.cookie:
        print(
            "提示：已启用 --download-attachments，但未提供 "
            "用户名+密码 或 Cookie；若 Confluence 需登录，REST/下载将返回 401/403。",
            file=sys.stderr,
        )

    ok, fail, aok, afail = process_all(
        input_dir, output_dir, logger, ac
    )
    print(
        f"完成：Markdown 成功 {ok}，失败 {fail}；"
        f"附件下载成功 {aok}，失败/拒绝 {afail}（需开启 --download-attachments）。"
    )
    return 1 if fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
