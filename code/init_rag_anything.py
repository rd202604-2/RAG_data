import asyncio
import os
import sys
from functools import partial
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from raganything.parser import Parser, register_parser
from raganything import RAGAnything, RAGAnythingConfig
from lightrag.llm.openai import openai_complete_if_cache, openai_embed
from lightrag.utils import EmbeddingFunc


class PlaintextRagParser(Parser):
    """直读 UTF-8 文本，不调用 MinerU（避免 .md 走「转 PDF + hybrid」导致 NumPy/torch 依赖问题）。"""

    def check_installation(self) -> bool:
        return True

    def parse_document(
        self,
        file_path: Union[str, Path],
        method: str = "auto",
        output_dir: Optional[str] = None,
        lang: Optional[str] = None,
        **kwargs: Any,
    ) -> List[Dict[str, Any]]:
        del method, output_dir, lang, kwargs
        path = Path(file_path).resolve()
        if not path.is_file():
            raise FileNotFoundError(f"文件不存在或不是文件: {path}")
        ext = path.suffix.lower()
        if ext not in self.TEXT_FORMATS:
            raise ValueError(
                f"当前解析器为「plaintext」，仅支持扩展名 {sorted(self.TEXT_FORMATS)}，收到: {ext!r}。\n"
                "若需解析 PDF / Office，请设置环境变量 RAGANYTHING_PARSER=mineru，并安装：\n"
                '  pip install "numpy>=1.26,<2" "mineru[pipeline]" --upgrade\n'
                "或预先将内容导出为 .md / .txt 后再用 plaintext。"
            )
        text = path.read_text(encoding="utf-8", errors="replace")
        return [{"type": "text", "text": text, "page_idx": 0}]


register_parser("plaintext", PlaintextRagParser)

# 需求中 Base URL 若写死为 ""，LightRAG 会构造 AsyncOpenAI(base_url="")，通常不可用。
# 此处：环境变量优先，否则使用官方 OpenAI 兼容网关（可在 run_rag_anything.bat 中覆盖）。
DASHSCOPE_OPENAI_BASE = (
    os.getenv("DASHSCOPE_OPENAI_BASE", "").strip()
    or "https://dashscope.aliyuncs.com/compatible-mode/v1"
)
SILICONFLOW_OPENAI_BASE = (
    os.getenv("SILICONFLOW_OPENAI_BASE", "").strip()
    or "https://api.siliconflow.cn/v1"
)

# 默认 plaintext：适合 clean_md 等以 .md/.txt 为主的目录。PDF 多模态请改为 mineru 并装好依赖。
PARSER_NAME = os.getenv("RAGANYTHING_PARSER", "plaintext").strip().lower()

LLM_MODEL = "qwen3.5-plus-2026-02-15"
EMBED_MODEL = "BAAI/bge-m3"
INPUT_FOLDER = r"E:\Htek\output\clean_md"
OUTPUT_DIR = "./rag_output"
FILE_EXTENSIONS = [".md", ".pdf", ".docx", ".xlsx", ".txt"]

PLACEHOLDER_KEYS = (
    "your_dashscope_key_here",
    "your_siliconflow_key_here",
)


def _die(msg: str) -> None:
    print(msg, file=sys.stderr)
    raise SystemExit(1)


def _require_api_key(name: str) -> str:
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        _die(
            f"错误：未检测到环境变量 {name}。\n"
            f"请先在 run_rag_anything.bat 中填写真实 Key，或在当前终端中 export/set 该变量后再运行。"
        )
    val = str(raw).strip()
    if val.lower() in PLACEHOLDER_KEYS:
        _die(f"错误：{name} 仍为占位符，请替换为真实密钥后再运行。")
    return val


async def llm_model_func(
    prompt: str,
    system_prompt: str | None = None,
    history_messages: list | None = None,
    **kwargs,
):
    """LightRAG 约定首参为 prompt；内部用 openai_complete_if_cache 调百炼兼容接口。"""
    api_key = _require_api_key("DASHSCOPE_API_KEY")
    return await openai_complete_if_cache(
        LLM_MODEL,
        prompt,
        system_prompt=system_prompt,
        history_messages=history_messages,
        base_url=DASHSCOPE_OPENAI_BASE,
        api_key=api_key,
        **kwargs,
    )


def build_embedding_func() -> EmbeddingFunc:
    api_key = _require_api_key("SILICONFLOW_API_KEY")
    return EmbeddingFunc(
        embedding_dim=1024,
        max_token_size=8192,
        model_name=EMBED_MODEL,
        func=partial(
            openai_embed.func,
            model=EMBED_MODEL,
            base_url=SILICONFLOW_OPENAI_BASE,
            api_key=api_key,
        ),
    )


async def main() -> None:
    print("正在校验环境变量 DASHSCOPE_API_KEY / SILICONFLOW_API_KEY …")
    _require_api_key("DASHSCOPE_API_KEY")
    _require_api_key("SILICONFLOW_API_KEY")

    print(f"文档解析器: {PARSER_NAME}（可用环境变量 RAGANYTHING_PARSER 覆盖，例如 mineru）")
    if PARSER_NAME == "plaintext":
        print(
            "提示：plaintext 仅处理 .md / .txt；目录中的 .pdf/.docx 等将报错跳过。"
            "需要 MinerU 时请先 pip install \"numpy>=1.26,<2\" \"mineru[pipeline]\"，再设 RAGANYTHING_PARSER=mineru。"
        )

    print("正在构造 RAGAnythingConfig …")
    config = RAGAnythingConfig(
        working_dir="./rag_workspace",
        parser=PARSER_NAME,
        enable_image_processing=True,
        enable_table_processing=True,
        enable_equation_processing=True,
    )

    print("正在构造 RAGAnything（将懒加载 LightRAG 与解析管线）…")
    rag = RAGAnything(
        config=config,
        llm_model_func=llm_model_func,
        embedding_func=build_embedding_func(),
    )

    try:
        print(f"开始批量处理文件夹：{INPUT_FOLDER}")
        print(f"解析与中间输出目录：{OUTPUT_DIR}")
        print(f"扩展名过滤：{FILE_EXTENSIONS}，recursive=True")
        await rag.process_folder_complete(
            folder_path=INPUT_FOLDER,
            output_dir=OUTPUT_DIR,
            file_extensions=FILE_EXTENSIONS,
            recursive=True,
        )
        print("批量处理流程已结束（若上游有告警请查看日志）。")
    finally:
        print("正在收尾存储（finalize_storages）…")
        await rag.finalize_storages()
        print("收尾完成。")


if __name__ == "__main__":
    asyncio.run(main())
