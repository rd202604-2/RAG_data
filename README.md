# 企业 WIKI 爬取与 Markdown 导出工具

本项目用于自动化爬取 Confluence (v7.7.4) 企业内部 Wiki，并将其结构化导出为干净的本地 Markdown 文件。

## ⚙️ 依赖与环境

在项目根目录下依次执行以下命令，安装所需的 Python 依赖及浏览器内核：

```powershell
# 1. 安装核心依赖包
pip install -r requirements.txt

# 2. 安装 Playwright 所需的 Chromium 内核
python -m playwright install chromium

# 3. 初始化 Crawl4AI 环境配置
python -m crawl4ai setup
```

---

## 🛠️ 代码功能说明

本项目包含两种不同的爬取模式（流水线模式 vs 极简模式），主要由以下三个脚本构成：

1. **`tree_crawler.py`（获取目录结构）**
   - **功能**：基于提供的根节点 Page ID，递归获取整棵页面树的层级结构。
   - **输出**：生成一个包含页面父子关系的 `page_tree.json` 文件。

2. **`markdown_tree_export.py`（按目录树导出）**
   - **功能**：读取上述的 `page_tree.json`，在本地动态创建与 Wiki 完全一致的嵌套文件夹结构，并将每个页面清洗后保存为 `index.md`。支持断点续传。

3. **`confluence_crawl4ai.py`（扁平化单篇爬取）**
   - **功能**：不依赖树状结构，直接根据代码里写死的 Page ID 列表进行单页抓取，所有 Markdown 文件平铺保存在同一级目录下。

---

## 🚀 使用方法

### 步骤一：配置环境变量
在运行任何脚本前，需要在终端（以 PowerShell 为例）配置你的企业内网 URL 与登录账密：

```powershell
$env:CONFLUENCE_BASE_URL = "[http://oa.example.com:8090](http://oa.example.com:8090)"
$env:CONFLUENCE_USERNAME = "你的用户名"
$env:CONFLUENCE_PASSWORD = "你的密码"
```

### 步骤二：运行爬虫流水线
如果你想完整备份一个空间或某个目录下的所有子页面，按顺序执行以下两步：

```powershell
# 1. 爬取树状目录 (替换末尾数字为你需要的根页面 ID)
python tree_crawler.py 257003250 

# 2. 根据生成的 JSON 开始批量下载 Markdown 文档
python markdown_tree_export.py
```

*附加命令提示：*
- 如果中途网络中断，再次运行 `python markdown_tree_export.py --resume` 即可跳过已下载好的文件，实现断点续传。
```
