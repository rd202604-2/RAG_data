# -*- coding: utf-8 -*-
"""
与 run_tree_crawler_resume.bat 中 set 的 Confluence 项一致，可被环境变量覆盖。

- CONFLUENCE_BASE_URL
- CONFLUENCE_USERNAME（或兼容 clean_md 曾用的 CONFLUENCE_USER）
- CONFLUENCE_PASSWORD
"""

from __future__ import annotations

import os

# 与 run_tree_crawler_resume.bat 第 12–15 行对应
_DEFAULT_CONFLUENCE_BASE_URL = "http://oa.htek.com:8090"
_DEFAULT_CONFLUENCE_USERNAME = "leo.lu"
_DEFAULT_CONFLUENCE_PASSWORD = "Lzh654321"


def confluence_base_url() -> str:
    v = (os.environ.get("CONFLUENCE_BASE_URL") or "").strip()
    return (v or _DEFAULT_CONFLUENCE_BASE_URL).rstrip("/")


def confluence_username() -> str:
    u = (os.environ.get("CONFLUENCE_USERNAME") or "").strip()
    if u:
        return u
    u = (os.environ.get("CONFLUENCE_USER") or "").strip()
    if u:
        return u
    return _DEFAULT_CONFLUENCE_USERNAME


def confluence_password() -> str:
    return (os.environ.get("CONFLUENCE_PASSWORD") or _DEFAULT_CONFLUENCE_PASSWORD)
