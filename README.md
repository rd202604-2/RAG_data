# 企业WIKI 爬取与 Markdown 导出

本文说明仓库内两个脚本的分工、依赖与用法。三者均基于 **Crawl4AI**（Playwright 无头浏览器）访问 Confluence。

---

## 依赖与环境

在 `code` 目录下安装 Python 依赖（见同目录 `requirements.txt`）：

```powershell
pip install -r requirements.txt
python -m playwright install chromium
```

按需执行 Crawl4AI 自带的环境检查（若已安装 CLI）：

```powershell
python -m crawl4ai setup
```

## 1. `tree_crawler.py`（页面树 → `page_tree.json`）

### 功能

- 同样通过 Crawl4AI 钩子完成 **Confluence 表单登录**，并把 **`BrowserContext`** 缓存下来。
- 先做一次 **warmup**（`arun` 打开落地页，默认 `CONFLUENCE_TREE_LANDING_URL` 或站点根），确保 Hook 跑通。
- 使用 **已登录会话** 调用 Confluence **REST API**（`rest/api/content/{id}`、`rest/api/content/{id}/child/page`）递归拉取子页面元数据，构建嵌套 JSON 树（含 `id`、`title`、`slug`、`children`）。
- 在终端打印树形预览（`├──` / `└──`）。

### 环境变量（必填 / 可选）

| 变量 | 说明 |
|------|------|
| `CONFLUENCE_BASE_URL` | 站点根 URL，例如 `http://oa.example.com:8090`（勿尾斜杠多余也可，脚本会 `rstrip`） |
| `CONFLUENCE_USERNAME` | 登录用户名 |
| `CONFLUENCE_PASSWORD` | 登录密码 |
| `CONFLUENCE_TREE_LANDING_URL` | 可选；warmup 时打开的 URL，未设置则用 `{BASE_URL}/` |

### 用法

```powershell
cd E:\Htek\code
$env:CONFLUENCE_BASE_URL = "http://oa.example.com:8090"
$env:CONFLUENCE_USERNAME = "your.user"
$env:CONFLUENCE_PASSWORD = "your.password"

# 从根页面 ID 递归整棵树，输出到默认 page_tree.json
python tree_crawler.py 257003250

# 指定输出路径、最大深度、调试日志
python tree_crawler.py 257003250 -o E:\Htek\output\AI项目_page_tree.json --max-depth 5 --verbose
```

### 参数说明

| 参数 | 含义 |
|------|------|
| `root_page_id` | 根页面的 Confluence `pageId`（位置参数） |
| `-o` / `--output` | 输出 JSON 路径，默认 `page_tree.json` |
| `--max-depth` | 可选；限制递归深度（根深度为 0） |
| `--verbose` | 更详细的日志 |

### 输出

- 一个 **JSON 文件**，供 `markdown_tree_export.py` 做树状目录导出。建议放在 `E:\Htek\output\` 下，与导出脚本的默认查找规则一致。

---

## 2. `markdown_tree_export.py`（`page_tree.json` → 嵌套文件夹 + `index.md`）

### 功能

- 读取 **`page_tree.json`**（由 `tree_crawler.py` 生成），在导出根目录下按树结构创建**嵌套文件夹**；每个页面对应目录内写 **`index.md`**。
- 通过 **环境变量** 配置站点与登录信息；Hook 中若检测到标准 Confluence 登录表单则填写，否则认为已登录或为企业 SSO 页并跳过填表，避免长时间卡在 `#os_username`。
- 单页抓取采用**多策略重试**（不同 `wait_for` / 超时组合），失败会尝试下一档策略。
- 支持 **`--resume`**：若某页 `index.md` 已存在且非空则跳过抓取，仍递归子节点。
- 失败页汇总为 **`_export_failures.json`**，并在控制台与日志中输出摘要。

### 环境变量（必填）

| 变量 | 说明 |
|------|------|
| `CONFLUENCE_BASE_URL` | 同 `tree_crawler.py` |
| `CONFLUENCE_USERNAME` | 登录用户名 |
| `CONFLUENCE_PASSWORD` | 登录密码 |

### 用法

```powershell
cd E:\Htek\code
$env:CONFLUENCE_BASE_URL = "http://oa.example.com:8090"
$env:CONFLUENCE_USERNAME = "your.user"
$env:CONFLUENCE_PASSWORD = "your.password"

# 省略 JSON 路径时：优先 output\AI项目_page_tree.json；否则选 output 下最新的 *page_tree*.json
python markdown_tree_export.py

# 显式指定 JSON 与导出根目录
python markdown_tree_export.py E:\Htek\output\AI项目_page_tree.json -o E:\Htek\output\AI项目_md

# 断点续跑（已有 index.md 则跳过）
python markdown_tree_export.py --resume --verbose
```

### 主要参数

| 参数 | 含义 |
|------|------|
| `tree_json` | 可选位置参数；`page_tree.json` 路径 |
| `-o` / `--output-dir` | 导出根目录；省略则为 `项目/output/<json 文件名去后缀>_md` |
| `--log-dir` | 日志目录，默认项目下 `log` |
| `--resume` | 跳过已存在且非空的 `index.md` |
| `--throttle-min` / `--throttle-max` | 页面间隔随机休眠（秒），默认约 2～5 |
| `--max-fetch-strategies` | 单页最多尝试的策略档数（上限为脚本内定义档数） |
| `--verbose` | 调试日志 |

### 输出

- 嵌套目录：默认在 **`E:\Htek\output\<stem>_md\`**（`<stem>` 为 JSON 主文件名不含扩展名），或 `-o` 指定目录。
- 每页：`.../<slug 或标题清洗>/index.md`（同层重名会加 `_pageId` 后缀）。
- 日志：`log/markdown_tree_export_*.log`。
- 失败列表：导出根下的 **`_export_failures.json`**（若有失败）。

### 与另外一个脚本的关系

```text
tree_crawler.py          markdown_tree_export.py
     │                            │
     │  page_tree.json            │ 读取同一 JSON
     └────────────────────────────┘
confluence_crawl4ai.py：不依赖 JSON，按 ID 列表直接导出到 output 根目录（扁平 .md）
```

---

## 小结

| 脚本 | 输入 | 输出 |
|------|------|------|
| `tree_crawler.py` | 环境变量 + 根 pageId | 嵌套结构的 `page_tree.json` |
| `markdown_tree_export.py` | 环境变量 + `page_tree.json` | 嵌套目录 + 每页 `index.md` |

若仅需少量固定页面，用 **`confluence_crawl4ai.py`** 最快；若需整站/整空间按树备份，用 **`tree_crawler.py` → `markdown_tree_export.py`** 流水线。
