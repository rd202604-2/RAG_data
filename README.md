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
