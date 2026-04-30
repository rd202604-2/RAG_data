import argparse
import concurrent.futures
import datetime as dt
import functools
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, List, Optional, Sequence

from markitdown import MarkItDown
from openai import OpenAI


# -----------------------------
# 固定路径配置（按需求约定）
# -----------------------------
INPUT_DIR = Path(r"E:\Htek\output\人力资源空间\文件列表")
OUTPUT_DIR = Path(r"E:\Htek\output\clean_md\人力资源空间\文件列表目录")
LOG_DIR = Path(r"E:\Htek\log")
FAIL_LOG_PATH = LOG_DIR / "pipeline_failures.jsonl"
RUN_LOG_PATH = LOG_DIR / "pipeline_markitdown.log"

DASHSCOPE_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"

# 需求指定模型池（轮询负载均衡）
QWEN_MODELS: List[str] = [
    "qwen3.6-plus-2026-04-02",
    "qwen3.5-plus-2026-04-20",
    "qwen3.5-flash-2026-02-23",
]

MODERN_EXTS = {".docx", ".pptx", ".xlsx", ".pdf", ".png"}
LEGACY_EXTS = {".doc", ".xls", ".ppt"}
# 音视频不参与转换：耗时过长且易报错，仅记录跳过（不调用 ffmpeg / MarkItDown 音频链路）
VIDEO_EXTS = {".avi", ".mp4", ".mov", ".mkv", ".webm", ".wmv", ".flv", ".m4v"}
AUDIO_EXTS = {
    ".mp3",
    ".wav",
    ".m4a",
    ".aac",
    ".flac",
    ".ogg",
    ".opus",
    ".wma",
    ".aiff",
    ".au",
}
SKIP_MEDIA_EXTS = VIDEO_EXTS | AUDIO_EXTS
# XMind 不支持自动转换：跳过并记 INFO，不记入失败
SKIP_XMIND_EXTS = {".xmind"}

# 兜底放行一些常见可直接转文本格式
DIRECT_EXTS = MODERN_EXTS | {".md", ".txt", ".jpg", ".jpeg"}


@dataclass
class PreparedInput:
    source_file: Path
    prepared_file: Path
    file_type: str
    preprocess_trace: str
    temp_dir: Optional[tempfile.TemporaryDirectory]


@dataclass
class ConvertResult:
    source_file: Path
    output_file: Optional[Path]
    used_llm_model: str
    status: str
    error: str = ""
    preprocess_trace: str = ""
    elapsed_sec: float = 0.0


class ModelDispatcher:
    """
    多模型负载均衡调度器（线程安全 Round-Robin）。

    设计要点：
    1) 线程池并发场景下，多个线程会同时请求模型；
    2) 使用 Lock 保证 index 自增的原子性，避免同一时刻重复分配；
    3) 每次 get_next_model() 都返回下一个模型，形成公平轮询。
    """

    def __init__(self, models: Sequence[str]) -> None:
        if not models:
            raise ValueError("ModelDispatcher 初始化失败：models 不能为空。")
        self._models = list(models)
        self._idx = 0
        self._lock = threading.Lock()

    def get_next_model(self) -> str:
        with self._lock:
            model = self._models[self._idx % len(self._models)]
            self._idx += 1
            return model


def build_dashscope_client() -> OpenAI:
    api_key = (os.getenv("DASHSCOPE_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError(
            "未检测到环境变量 DASHSCOPE_API_KEY，请先 set DASHSCOPE_API_KEY=你的密钥。"
        )
    return OpenAI(api_key=api_key, base_url=DASHSCOPE_BASE_URL)


def validate_dashscope_api_key(logger: logging.Logger) -> None:
    """
    启动前校验百炼 API Key，避免线程池内才发现未配置。
    Windows 下一键配置说明写入日志与 stderr，便于对照 bat / 系统环境变量。
    """
    key = (os.getenv("DASHSCOPE_API_KEY") or "").strip()
    if key:
        return
    msg = (
        "未设置环境变量 DASHSCOPE_API_KEY。\n\n"
        "Windows 配置方式（任选其一）：\n"
        "  1) 当前 PowerShell 会话：  $env:DASHSCOPE_API_KEY=\"sk-你的密钥\"\n"
        "  2) 当前 CMD 会话：        set DASHSCOPE_API_KEY=sk-你的密钥\n"
        "  3) 用户级永久（新开终端生效）： setx DASHSCOPE_API_KEY \"sk-你的密钥\"\n"
        "  4) 编辑本仓库 code\\run_markitdown_pipeline.bat，在「配置区」设置 set DASHSCOPE_API_KEY=...\n\n"
        "密钥获取：阿里云控制台 -> 百炼 -> API-KEY。\n"
    )
    logger.error(msg.strip())
    print(msg, file=sys.stderr)
    raise SystemExit(3)


@functools.lru_cache(maxsize=1)
def resolve_soffice_executable() -> str:
    """
    解析 LibreOffice soffice.exe 绝对路径，避免仅依赖 PATH 导致 WinError 2。
    优先级：LIBREOFFICE_SOFFICE_PATH / SOFFICE_PATH -> 常见安装目录。
    """
    candidates: List[Path] = []
    for env_name in ("LIBREOFFICE_SOFFICE_PATH", "SOFFICE_PATH"):
        raw = (os.getenv(env_name) or "").strip().strip('"')
        if raw:
            candidates.append(Path(raw))
    candidates.extend(
        [
            Path(r"D:\Program Files\LibreOffice\program\soffice.exe"),
            Path(r"C:\Program Files\LibreOffice\program\soffice.exe"),
            Path(r"C:\Program Files (x86)\LibreOffice\program\soffice.exe"),
        ]
    )
    for p in candidates:
        if p.is_file():
            return str(p.resolve())
    raise FileNotFoundError(
        "未找到 LibreOffice 的 soffice.exe（WinError 2）。\n"
        "请安装 LibreOffice，或设置环境变量 LIBREOFFICE_SOFFICE_PATH 为 soffice.exe 绝对路径，例如：\n"
        r'  set LIBREOFFICE_SOFFICE_PATH="D:\Program Files\LibreOffice\program\soffice.exe"'
        "\n"
        r'  （或 "C:\Program Files\LibreOffice\program\soffice.exe"）'
    )


def setup_logger() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("pipeline_markitdown")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh = logging.FileHandler(RUN_LOG_PATH, encoding="utf-8")
    fh.setFormatter(formatter)
    fh.setLevel(logging.INFO)
    logger.addHandler(fh)

    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    sh.setLevel(logging.INFO)
    logger.addHandler(sh)
    return logger


def run_subprocess(command: Sequence[str], logger: logging.Logger) -> None:
    proc = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"子进程失败: {' '.join(command)}\n"
            f"stdout:\n{proc.stdout}\n"
            f"stderr:\n{proc.stderr}"
        )
    logger.info("预处理命令成功: %s", " ".join(command))


def soffice_convert_legacy(src: Path, logger: logging.Logger) -> PreparedInput:
    ext = src.suffix.lower()
    mapping = {".doc": "docx", ".xls": "xlsx", ".ppt": "pptx"}
    if ext not in mapping:
        raise ValueError(f"不支持的 legacy 扩展名: {ext}")

    soffice_exe = resolve_soffice_executable()
    logger.info("Legacy 转换使用 LibreOffice: %s", soffice_exe)

    tmp = tempfile.TemporaryDirectory(prefix="legacy_to_modern_")
    out_dir = Path(tmp.name)
    target_ext = mapping[ext]
    command = [
        soffice_exe,
        "--headless",
        "--convert-to",
        target_ext,
        "--outdir",
        str(out_dir),
        str(src),
    ]
    run_subprocess(command, logger)
    converted = out_dir / f"{src.stem}.{target_ext}"
    if not converted.exists():
        tmp.cleanup()
        raise FileNotFoundError(f"LibreOffice 转换成功但输出文件不存在: {converted}")
    return PreparedInput(
        source_file=src,
        prepared_file=converted,
        file_type=target_ext,
        preprocess_trace=f"legacy_convert:{ext}->{target_ext}",
        temp_dir=tmp,
    )


def route_and_prepare(src: Path, logger: logging.Logger) -> PreparedInput:
    ext = src.suffix.lower()
    if ext in LEGACY_EXTS:
        return soffice_convert_legacy(src, logger)
    if ext in DIRECT_EXTS:
        return PreparedInput(
            source_file=src,
            prepared_file=src,
            file_type=ext.lstrip(".") or "unknown",
            preprocess_trace="direct",
            temp_dir=None,
        )
    raise ValueError(f"不支持的文件类型: {ext} ({src})")


def stream_reasoning_response(
    client: OpenAI,
    model: str,
    messages: List[dict[str, Any]],
) -> tuple[str, str]:
    """
    按阿里云流式接口拼接正文与 reasoning_content。
    当前 Pipeline 主流程主要调用 MarkItDown.convert；
    这里保留一个可复用工具函数，便于后续对图像描述等自定义调用进行兜底。
    """
    content_parts: List[str] = []
    reasoning_parts: List[str] = []
    stream = client.chat.completions.create(
        model=model,
        messages=messages,
        extra_body={"enable_thinking": True},
        stream=True,
    )
    for chunk in stream:
        choices = getattr(chunk, "choices", None) or []
        if not choices:
            continue
        delta = getattr(choices[0], "delta", None)
        if not delta:
            continue
        reasoning = getattr(delta, "reasoning_content", None)
        if reasoning:
            reasoning_parts.append(str(reasoning))
        content = getattr(delta, "content", None)
        if content:
            content_parts.append(str(content))
    return "".join(content_parts), "".join(reasoning_parts)


def normalize_markitdown_result(result: Any) -> str:
    """
    兼容 MarkItDown 不同版本返回对象：
    - 可能直接返回 str
    - 可能返回包含 text_content/content/text 的对象
    """
    if isinstance(result, str):
        return result
    for attr in ("text_content", "content", "text", "markdown"):
        val = getattr(result, attr, None)
        if isinstance(val, str) and val.strip():
            return val
    # 最后兜底：字符串化
    return str(result)


def compress_blank_lines(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text


def normalize_table_lines(text: str) -> str:
    lines = text.splitlines()
    fixed: List[str] = []
    for line in lines:
        stripped = line.strip()
        if "|" in stripped and not stripped.startswith("```"):
            cells = [cell.strip() for cell in stripped.strip("|").split("|")]
            maybe_rule = all(re.fullmatch(r"[:\-\s]+", c or "") for c in cells)
            if maybe_rule:
                rule_cells = []
                for c in cells:
                    c = c.replace(" ", "")
                    rule_cells.append(c if c else "---")
                fixed.append("| " + " | ".join(rule_cells) + " |")
                continue
            fixed.append("| " + " | ".join(cells) + " |")
            continue
        fixed.append(line.rstrip())
    return "\n".join(fixed)


def yaml_quote(v: str) -> str:
    safe = v.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{safe}"'


def inject_frontmatter(
    markdown: str,
    source_file: Path,
    file_type: str,
    conversion_date: str,
    used_llm_model: str,
) -> str:
    fm = (
        "---\n"
        f"source_file: {yaml_quote(str(source_file))}\n"
        f"file_type: {yaml_quote(file_type)}\n"
        f"conversion_date: {yaml_quote(conversion_date)}\n"
        f"used_llm_model: {yaml_quote(used_llm_model)}\n"
        "---\n\n"
    )
    body = markdown.lstrip("\ufeff").lstrip("\n")
    return fm + body


def clean_markdown(
    markdown: str,
    source_file: Path,
    file_type: str,
    used_llm_model: str,
) -> str:
    cleaned = compress_blank_lines(markdown)
    cleaned = normalize_table_lines(cleaned)
    cleaned = compress_blank_lines(cleaned)
    date_str = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cleaned = inject_frontmatter(
        markdown=cleaned,
        source_file=source_file,
        file_type=file_type,
        conversion_date=date_str,
        used_llm_model=used_llm_model,
    )
    return cleaned.rstrip() + "\n"


def scan_input_files(root: Path) -> List[Path]:
    files: List[Path] = []
    for p in root.rglob("*"):
        if p.is_file():
            files.append(p)
    return sorted(files)


def write_failure_log(result: ConvertResult) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "source_file": str(result.source_file),
        "used_llm_model": result.used_llm_model,
        "status": result.status,
        "error": result.error,
        "preprocess_trace": result.preprocess_trace,
        "elapsed_sec": round(result.elapsed_sec, 3),
        "timestamp": dt.datetime.now().isoformat(timespec="seconds"),
    }
    with FAIL_LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def convert_one_file(
    src: Path,
    input_root: Path,
    output_root: Path,
    dispatcher: ModelDispatcher,
    logger: logging.Logger,
) -> ConvertResult:
    started = time.perf_counter()
    ext = src.suffix.lower()
    if ext in SKIP_MEDIA_EXTS:
        elapsed = time.perf_counter() - started
        logger.info(
            "跳过(音频/视频，不转换) | %s | ext=%s",
            src,
            ext or "(无扩展名)",
        )
        return ConvertResult(
            source_file=src,
            output_file=None,
            used_llm_model="",
            status="skipped",
            error="",
            preprocess_trace="skip_audio_video",
            elapsed_sec=elapsed,
        )

    if ext in SKIP_XMIND_EXTS:
        elapsed = time.perf_counter() - started
        logger.info("跳过(.xmind 不支持自动转换) | %s", src)
        return ConvertResult(
            source_file=src,
            output_file=None,
            used_llm_model="",
            status="skipped",
            error="",
            preprocess_trace="skip_xmind",
            elapsed_sec=elapsed,
        )

    model_name = dispatcher.get_next_model()
    prepared: Optional[PreparedInput] = None
    try:
        prepared = route_and_prepare(src, logger)
        llm_client = build_dashscope_client()
        md = MarkItDown(llm_client=llm_client, llm_model=model_name)
        raw_result = md.convert(str(prepared.prepared_file))
        markdown = normalize_markitdown_result(raw_result)
        cleaned = clean_markdown(
            markdown=markdown,
            source_file=src,
            file_type=prepared.file_type,
            used_llm_model=model_name,
        )

        rel = src.relative_to(input_root)
        out_path = output_root / rel.with_suffix(".md")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(cleaned, encoding="utf-8", newline="\n")
        elapsed = time.perf_counter() - started
        logger.info("转换成功 | %s | model=%s | %.2fs", src, model_name, elapsed)
        return ConvertResult(
            source_file=src,
            output_file=out_path,
            used_llm_model=model_name,
            status="ok",
            preprocess_trace=prepared.preprocess_trace if prepared else "",
            elapsed_sec=elapsed,
        )
    except Exception as exc:
        elapsed = time.perf_counter() - started
        err = str(exc)
        logger.error("转换失败 | %s | model=%s | %s", src, model_name, err)
        result = ConvertResult(
            source_file=src,
            output_file=None,
            used_llm_model=model_name,
            status="failed",
            error=err,
            preprocess_trace=prepared.preprocess_trace if prepared else "",
            elapsed_sec=elapsed,
        )
        write_failure_log(result)
        return result
    finally:
        if prepared and prepared.temp_dir is not None:
            prepared.temp_dir.cleanup()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="MarkItDown 多模型转换 Pipeline")
    p.add_argument("--input-dir", type=Path, default=INPUT_DIR, help=f"输入目录，默认: {INPUT_DIR}")
    p.add_argument(
        "--output-dir",
        type=Path,
        default=OUTPUT_DIR,
        help=f"输出目录，默认: {OUTPUT_DIR}",
    )
    p.add_argument("--workers", type=int, default=max(2, (os.cpu_count() or 4) // 2))
    return p.parse_args()


def summarize(results: Iterable[ConvertResult], logger: logging.Logger) -> int:
    items = list(results)
    ok = sum(1 for r in items if r.status == "ok")
    skipped = sum(1 for r in items if r.status == "skipped")
    fail = sum(1 for r in items if r.status == "failed")
    logger.info(
        "总文件数: %d | 成功: %d | 跳过: %d | 失败: %d",
        len(items),
        ok,
        skipped,
        fail,
    )
    logger.info("运行日志: %s", RUN_LOG_PATH)
    logger.info("失败明细: %s", FAIL_LOG_PATH)
    return 1 if fail else 0


def main() -> int:
    args = parse_args()
    input_dir: Path = args.input_dir.resolve()
    output_dir: Path = args.output_dir.resolve()
    workers: int = max(1, int(args.workers))
    logger = setup_logger()
    validate_dashscope_api_key(logger)

    if not input_dir.is_dir():
        logger.error("输入目录不存在或不是文件夹: %s", input_dir)
        return 2

    files = scan_input_files(input_dir)
    if not files:
        logger.warning("输入目录没有可处理文件: %s", input_dir)
        return 0

    dispatcher = ModelDispatcher(QWEN_MODELS)
    logger.info("开始执行 Pipeline")
    logger.info("输入目录: %s", input_dir)
    logger.info("输出目录: %s", output_dir)
    logger.info("日志目录: %s", LOG_DIR)
    logger.info("线程数: %d", workers)
    logger.info("模型池(轮询): %s", ", ".join(QWEN_MODELS))

    results: List[ConvertResult] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [
            ex.submit(convert_one_file, f, input_dir, output_dir, dispatcher, logger)
            for f in files
        ]
        for fut in concurrent.futures.as_completed(futures):
            results.append(fut.result())

    return summarize(results, logger)


if __name__ == "__main__":
    raise SystemExit(main())
