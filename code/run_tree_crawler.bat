@echo off
REM 设置 Confluence 环境变量
set CONFLUENCE_BASE_URL=http://oa.htek.com:8090
set CONFLUENCE_USERNAME=leo.lu
set CONFLUENCE_PASSWORD=Lzh654321

REM 运行页面树爬虫，根页面 ID 自行设置
python markdown_tree_export.py --resume --checkpoint output/_markdown_tree_export_checkpoint.json
#从断点页开始爬取
#python code/tree_crawler.py --resume-from-page-id 1617677
pause
