#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
递归清洗 Confluence 导出的 Markdown：提取元数据为 YAML、截断尾部噪声、
清理 UI/锚点/面包屑等，输出到新目录不覆盖源文件。

默认输入：项目 output/AI项目_page_tree_md
默认输出：项目 output/clean_md
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

try:
    from tqdm import tqdm
except ImportError as e:  # pragma: no cover
    raise SystemExit(
        "请先安装 tqdm：pip install tqdm\n"
        "或在 code 目录执行：pip install -r requirements.txt"
    ) from e


# ---------------------------------------------------------------------------
# 路径默认值（相对本文件所在仓库根目录的 output）
# ---------------------------------------------------------------------------
_REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_INPUT_DIR = _REPO_ROOT / "output" / "AI项目_page_tree_md"
DEFAULT_OUTPUT_DIR = _REPO_ROOT / "output" / "clean_md"
DEFAULT_LOG_DIR = _REPO_ROOT / "log"


# ---------------------------------------------------------------------------
# 1. 元数据提取
# ---------------------------------------------------------------------------
_METADATA_AUTHOR_DATE = re.compile(
    r"由\s*\[([^\]]+)\]\([^)]*\)\s*创建\s*,\s*最后修改于\s*\[([^\]]+)\]\([^)]*\)",
    re.MULTILINE,
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


def extract_metadata(text: str) -> Tuple[str, Dict[str, str]]:
    """
    从正文提取作者、最后修改时间，删除匹配到的元数据原文，返回 (剩余文本, meta)。
    meta 键：author, last_modified（可能为空字符串）。
    """
    author = ""
    last_modified = ""

    m = _METADATA_AUTHOR_DATE.search(text)
    if m:
        author = (m.group(1) or "").strip()
        last_modified = (m.group(2) or "").strip()

    cleaned = _METADATA_LINE_REMOVE.sub("", text)

    if not m:
        # 尝试分行匹配
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
# 2. 尾部截断
# ---------------------------------------------------------------------------
_TAIL_MARKERS: Tuple[str, ...] = (
    "赞成为第一个赞同者",
    "无标签",
    "写评论...",
    "概览\n\n内容工具",
    "基于 Atlassian Confluence",
    "## 评论",
    '{"serverDuration":',
    "search\n\nrecentlyviewed",
    "更新恢复页面保留草稿取消",
)


def truncate_tail(text: str) -> str:
    """自首次出现任一特征子串所在行之首起，丢弃该行及之后全部内容。"""
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
# 管道与文件遍历
# ---------------------------------------------------------------------------
def clean_markdown_pipeline(raw: str) -> str:
    """按用户指定顺序链式调用各清洗步骤。"""
    text = raw
    text, meta = extract_metadata(text)
    text = prepend_yaml_front_matter(text, meta)
    text = truncate_tail(text)
    text = clean_ui_noise(text)
    text = clean_anchors_breadcrumbs_icons(text)
    text = dedupe_opening_titles(text)
    text = compress_blank_lines(text)
    return text.rstrip() + "\n"


def iter_markdown_files(root: Path) -> List[Path]:
    return sorted(root.rglob("*.md"))


def process_all(
    input_dir: Path,
    output_dir: Path,
    logger: logging.Logger,
) -> Tuple[int, int]:
    """
    遍历 input_dir 下全部 .md，写入 output_dir 保持相对路径。
    返回 (成功数, 失败数)。
    """
    files = iter_markdown_files(input_dir)
    ok, fail = 0, 0
    for path in tqdm(files, desc="清洗 Markdown", unit="file"):
        rel = path.relative_to(input_dir)
        out_path = output_dir / rel
        try:
            raw = path.read_text(encoding="utf-8", errors="replace")
            cleaned = clean_markdown_pipeline(raw)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(cleaned, encoding="utf-8", newline="\n")
            ok += 1
        except Exception:
            fail += 1
            logger.exception("处理失败: %s", path)
    return ok, fail


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

    ok, fail = process_all(input_dir, output_dir, logger)
    print(f"完成：成功 {ok}，失败 {fail}")
    return 1 if fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
