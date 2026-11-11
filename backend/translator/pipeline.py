from __future__ import annotations

import asyncio
import hashlib
import json
import re
import time
from collections import defaultdict
from pathlib import Path
from typing import Any
from uuid import uuid4

from backend.config import build_runtime_config, reload_config
from backend.translator.chunker import build_chunks
from backend.translator.context_builder import build_messages, build_single_segment_messages, format_examples
from backend.translator.corpus_manager import CorpusManager
from backend.translator.exporter import export_outputs
from backend.translator.hy_mt2_client import HyMT2Client
from backend.translator.storage import FileStorage
from backend.translator.text_loader import load_text_file
from backend.translator.segmenter import segment_document
from backend.translator.validator import (
    AUTH_FAILURE_MESSAGE,
    ChunkRecord,
    JobConfigSnapshot,
    JobRecord,
    LogEntry,
    SegmentRecord,
    TranslationParseResult,
    TranslationAuthError,
    TranslationTimeoutError,
    ensure_list,
    utc_now_iso,
)


SEGMENT_TRANSLATION_RE = re.compile(
    r"<segment\b[^>]*\bid\s*=\s*(['\"])(.*?)\1[^>]*>\s*(.*?)\s*</segment\s*>",
    re.DOTALL | re.IGNORECASE,
)
FENCED_BLOCK_RE = re.compile(r"```(?:xml|html)?\s*(.*?)```", re.DOTALL | re.IGNORECASE)
REFUSAL_PATTERNS = [
    "无法给到相关内容",
    "无法提供相关内容",
    "无法协助",
    "不能协助",
    "不能提供",
    "抱歉",
    "对不起",
    "sorry",
    "i can't",
    "i cannot",
    "i’m sorry",
    "i am sorry",
    "unable to help",
    "cannot help with that",
]


class IdempotencyConflictError(ValueError):
    pass


class EmptyDocumentError(ValueError):
    pass


class JobAlreadyRunningError(RuntimeError):
    pass


class EventBroker:
    def __init__(self) -> None:
        self.subscribers: dict[str, list[asyncio.Queue[dict[str, Any]]]] = defaultdict(list)

    def subscribe(self, job_id: str) -> asyncio.Queue[dict[str, Any]]:
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.subscribers[job_id].append(queue)
        return queue

    def unsubscribe(self, job_id: str, queue: asyncio.Queue[dict[str, Any]]) -> None:
        if job_id not in self.subscribers:
            return
        self.subscribers[job_id] = [item for item in self.subscribers[job_id] if item is not queue]
        if not self.subscribers[job_id]:
            self.subscribers.pop(job_id, None)

    async def publish(self, job_id: str, event: str, data: dict[str, Any]) -> None:
        payload = {"event": event, "data": data, "timestamp": utc_now_iso()}
        for queue in self.subscribers.get(job_id, []):
            await queue.put(payload)


class TranslationPipeline:
    def __init__(self, storage: FileStorage, broker: EventBroker):
        self.storage = storage
        self.broker = broker
        self.tasks: dict[str, asyncio.Task[None]] = {}
        self._submission_lock = asyncio.Lock()
        self._lifecycle_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self.corpus_manager = CorpusManager(storage.base_dir / "corpus")

    async def create_job_from_file(
        self,
        source_path: str | Path,
        file_name: str,
        target_language: str | None = None,
        translation_mode: str = "忠实翻译",
        stream: bool | None = None,
        chunk_size_chars: int | None = None,
        corpus_id: str = "default",
        use_corpus: bool = True,
        use_glossary: bool = True,
        use_style_examples: bool = True,
        use_domain_prompt: bool = True,
        translate_mode: str = "faithful",
        translation_level: int = 3,
        speed_mode: str = "stable",
        client_request_id: str = "",
        source_sha256: str = "",
        request_fingerprint: str = "",
    ) -> JobRecord:
        source_path = str(Path(source_path).resolve())
        config = build_runtime_config(
            {
                "default_target_language": target_language,
                "stream": stream,
                "chunk_size_chars": chunk_size_chars,
            }
        )
        text = load_text_file(source_path)
        segment_limit = max(800, min(2200, config.chunk_size_chars - 800))
        segments = segment_document(text, max_segment_chars=segment_limit)
        if not segments:
            raise EmptyDocumentError("文件中没有可翻译的文字")
        chunks = build_chunks(segments, config.chunk_size_chars)
        avg_segment_chars = round(sum(len(segment.source_text) for segment in segments) / max(len(segments), 1), 2)
        max_segment_chars_value = max((len(segment.source_text) for segment in segments), default=0)

        job = JobRecord(
            job_id=f"job_{uuid4().hex[:12]}",
            file_name=file_name,
            source_path=source_path,
            target_language=target_language or config.default_target_language,
            client_request_id=client_request_id,
            source_sha256=source_sha256,
            request_fingerprint=request_fingerprint,
            total_segments=len(segments),
            total_chunks=len(chunks),
            avg_segment_chars=avg_segment_chars,
            max_segment_chars=max_segment_chars_value,
            config=JobConfigSnapshot(
                model=config.model,
                temperature=config.temperature,
                stream=config.stream,
                chunk_size_chars=config.chunk_size_chars,
                max_retries=config.max_retries,
                request_timeout_seconds=config.request_timeout_seconds,
                stream_timeout_seconds=config.stream_timeout_seconds,
                stream_fallback=config.stream_fallback,
                max_segments_per_chunk=config.max_segments_per_chunk,
                min_chars_for_standalone_chunk=config.min_chars_for_standalone_chunk,
                merge_tiny_chapter_segments=config.merge_tiny_chapter_segments,
                speed_mode=speed_mode,
                target_language=target_language or config.default_target_language,
                translation_mode=translation_mode,
                corpus_id=corpus_id,
                use_corpus=use_corpus,
                use_glossary=use_glossary,
                use_style_examples=use_style_examples,
                use_domain_prompt=use_domain_prompt,
                translate_mode=translate_mode,
                translation_level=translation_level,
            ),
        )
        await self.storage.create_job(job, segments, chunks)
        return job

    async def create_job_from_upload(
        self,
        file_name: str,
        content: bytes,
        target_language: str | None = None,
        translation_mode: str = "忠实翻译",
        stream: bool | None = None,
        chunk_size_chars: int | None = None,
        corpus_id: str = "default",
        use_corpus: bool = True,
        use_glossary: bool = True,
        use_style_examples: bool = True,
        use_domain_prompt: bool = True,
        translate_mode: str = "faithful",
        translation_level: int = 3,
        speed_mode: str = "stable",
        client_request_id: str = "",
    ) -> tuple[JobRecord, bool]:
        source_sha256 = hashlib.sha256(content).hexdigest()
        fingerprint_payload = {
            "source_sha256": source_sha256,
            "target_language": target_language,
            "translation_mode": translation_mode,
            "stream": stream,
            "chunk_size_chars": chunk_size_chars,
            "corpus_id": corpus_id,
            "use_corpus": use_corpus,
            "use_glossary": use_glossary,
            "use_style_examples": use_style_examples,
            "use_domain_prompt": use_domain_prompt,
            "translate_mode": translate_mode,
            "translation_level": translation_level,
            "speed_mode": speed_mode,
        }
        request_fingerprint = hashlib.sha256(
            json.dumps(fingerprint_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        client_request_id = client_request_id.strip()

        async with self._submission_lock:
            if client_request_id:
                existing = await self.storage.find_job_by_client_request_id(client_request_id)
                if existing:
                    if existing.request_fingerprint != request_fingerprint:
                        raise IdempotencyConflictError("client_request_id 已用于不同的文件或翻译参数")
                    return existing, True

            temp_job_id = f"upload_{uuid4().hex[:12]}"
            upload_path = await self.storage.save_uploaded_file(file_name, content, temp_job_id)
            try:
                job = await self.create_job_from_file(
                    upload_path,
                    file_name=file_name,
                    target_language=target_language,
                    translation_mode=translation_mode,
                    stream=stream,
                    chunk_size_chars=chunk_size_chars,
                    corpus_id=corpus_id,
                    use_corpus=use_corpus,
                    use_glossary=use_glossary,
                    use_style_examples=use_style_examples,
                    use_domain_prompt=use_domain_prompt,
                    translate_mode=translate_mode,
                    translation_level=translation_level,
                    speed_mode=speed_mode,
                    client_request_id=client_request_id,
                    source_sha256=source_sha256,
                    request_fingerprint=request_fingerprint,
                )
            except Exception:
                upload_path.unlink(missing_ok=True)
                upload_path.with_suffix(f"{upload_path.suffix}.name.txt").unlink(missing_ok=True)
                raise
            return job, False

    async def get_job(self, job_id: str) -> JobRecord:
        return await self.storage.load_job(job_id)

    async def get_segments(self, job_id: str) -> list[SegmentRecord]:
        return await self.storage.load_segments(job_id)

    async def request_pause(self, job_id: str) -> JobRecord:
        job = await self.storage.load_job(job_id)
        if job.status == "completed":
            return job
        if job.status in {"running", "pausing"}:
            job.status = "pausing"
            job.pause_requested = True
            job.updated_at = utc_now_iso()
            await self.storage.save_job(job)
            await self.broker.publish(job_id, "job_pausing", {"job_id": job_id, "status": job.status})
        return job

    async def request_cancel(self, job_id: str) -> JobRecord:
        job = await self.storage.load_job(job_id)
        if job.status in {"pending", "paused", "running", "pausing", "failed"}:
            job.status = "cancelled"
            job.cancel_requested = True
            job.updated_at = utc_now_iso()
            await self.storage.save_job(job)
            await self.broker.publish(job_id, "job_cancelled", {"job_id": job_id, "status": job.status})
        return job

    async def start_job(self, job_id: str) -> JobRecord:
        async with self._lifecycle_locks[job_id]:
            return await self._start_job_locked(job_id)

    async def _start_job_locked(self, job_id: str) -> JobRecord:
        job = await self.storage.load_job(job_id)
        if job.status in {"completed", "cancelled"}:
            return job
        if not self._cleanup_finished_task(job_id):
            return job
        config = reload_config()
        if not config.tokenhub_api_key.strip():
            raise ValueError("模型接口的 API Key 未配置，请检查页面配置、.env 或 backend/config.local.json")
        segments = await self.storage.load_segments(job_id)
        chunks = await self.storage.load_chunks(job_id)
        await self.storage.clear_result_manifest(job_id)
        job.config.model = config.model
        changed = self.normalize_interrupted_segments(segments, chunks)
        if changed:
            await self.storage.save_segments(job_id, segments)
            for chunk in chunks:
                await self.storage.save_chunk(job_id, chunk)
        job.status = "running"
        job.pause_requested = False
        job.cancel_requested = False
        job.last_error = ""
        await self._recompute_job_progress(job, segments)
        job.updated_at = utc_now_iso()
        await self.storage.save_job(job)
        self._create_background_task(job_id)
        return job

    async def resume_job(self, job_id: str) -> JobRecord:
        async with self._lifecycle_locks[job_id]:
            return await self._resume_job_locked(job_id)

    async def _resume_job_locked(self, job_id: str) -> JobRecord:
        job = await self.storage.load_job(job_id)
        old_status = job.status
        print(f"[resume] job_id={job_id} old_status={old_status}")
        if job.status == "completed":
            job.pause_requested = False
            job.cancel_requested = False
            job.last_error = ""
            job.current_segment_id = ""
            job.current_chapter = ""
            segments = await self.storage.load_segments(job_id)
            chunks = await self.storage.load_chunks(job_id)
            await self._recompute_job_progress(job, segments)
            await export_outputs(self.storage, job, segments, chunks)
            await self.storage.save_job(job)
            return job
        if job.status == "cancelled":
            return job
        if not self._cleanup_finished_task(job_id):
            return job
        segments = await self.storage.load_segments(job_id)
        chunks = await self.storage.load_chunks(job_id)
        await self.storage.clear_result_manifest(job_id)
        job.config.model = reload_config().model
        if all(segment.status == "success" for segment in segments):
            job.status = "completed"
            job.current_segment_id = ""
            job.current_chapter = ""
            job.last_error = ""
            await self._recompute_job_progress(job, segments)
            await export_outputs(self.storage, job, segments, chunks)
            await self.storage.save_job(job)
            return job
        self.normalize_interrupted_segments(segments, chunks)
        self._reset_failed_segments_for_retry(segments, chunks)
        await self.storage.save_segments(job_id, segments)
        for chunk in chunks:
            await self.storage.save_chunk(job_id, chunk)
        job.status = "running"
        job.pause_requested = False
        job.cancel_requested = False
        job.last_error = ""
        await self._recompute_job_progress(job, segments)
        job.updated_at = utc_now_iso()
        await self.storage.save_job(job)
        unfinished = [segment for segment in segments if segment.status != "success"]
        print(f"[resume] success_count={job.completed_segments} unfinished_count={len(unfinished)}")
        print(f"[resume] next_segment={unfinished[0].segment_id if unfinished else ''}")
        print("[resume] create background task")
        self._create_background_task(job_id)
        return job

    async def set_speed_mode(self, job_id: str, speed_mode: str) -> JobRecord:
        job = await self.storage.load_job(job_id)
        valid = {"stable", "balanced", "fast"}
        if speed_mode not in valid:
            raise ValueError(f"speed_mode must be one of {valid}")
        job.config.speed_mode = speed_mode
        job.updated_at = utc_now_iso()
        await self.storage.save_job(job)
        await self._emit_job_snapshot(job)
        print(f"[speed-mode] job_id={job_id} speed_mode={speed_mode}")
        return job

    def _create_background_task(self, job_id: str) -> None:
        task = asyncio.create_task(self._run_job(job_id))
        task.add_done_callback(lambda item, current_job_id=job_id: self._on_task_done(current_job_id, item))
        self.tasks[job_id] = task

    def has_active_task(self, job_id: str) -> bool:
        task = self.tasks.get(job_id)
        return bool(task and not task.done())

    def _on_task_done(self, job_id: str, task: asyncio.Task[None]) -> None:
        try:
            exc = task.exception()
        except asyncio.CancelledError:
            return
        if exc:
            print(f"[run-exit] reason=exception job_id={job_id} error={exc}")
            asyncio.create_task(self._record_task_exception(job_id, exc))

    async def _record_task_exception(self, job_id: str, exc: BaseException) -> None:
        try:
            job = await self.storage.load_job(job_id)
            job.status = "failed"
            job.last_error = str(exc)
            job.updated_at = utc_now_iso()
            await self.storage.save_job(job)
            await self.broker.publish(job_id, "job_failed", job.model_dump())
        except Exception as record_exc:
            print(f"[run-exit] reason=exception_record_failed job_id={job_id} error={record_exc}")

    def _cleanup_finished_task(self, job_id: str) -> bool:
        existing = self.tasks.get(job_id)
        if not existing:
            return True
        if not existing.done():
            return False
        self.tasks.pop(job_id, None)
        try:
            exc = existing.exception()
            if exc:
                print(f"[resume] previous task error job_id={job_id} error={exc}")
        except asyncio.CancelledError:
            print(f"[resume] previous task cancelled job_id={job_id}")
        return True

    async def repair_stuck_job(self, job_id: str) -> JobRecord:
        async with self._lifecycle_locks[job_id]:
            return await self._repair_stuck_job_locked(job_id)

    async def _repair_stuck_job_locked(self, job_id: str) -> JobRecord:
        job = await self.storage.load_job(job_id)
        if not self._cleanup_finished_task(job_id):
            raise JobAlreadyRunningError("任务仍在运行，不能执行卡住修复")
        if job.status == "completed":
            job.pause_requested = False
            job.cancel_requested = False
            job.last_error = ""
            job.current_segment_id = ""
            await self.storage.save_job(job)
            return job
        segments = await self.storage.load_segments(job_id)
        chunks = await self.storage.load_chunks(job_id)
        self.normalize_interrupted_segments(segments, chunks)
        job.status = "paused" if any(segment.status != "success" for segment in segments) else "completed"
        job.pause_requested = False
        job.cancel_requested = False
        job.last_error = ""
        await self._recompute_job_progress(job, segments)
        await self.storage.save_segments(job_id, segments)
        for chunk in chunks:
            await self.storage.save_chunk(job_id, chunk)
        await self.storage.save_job(job)
        print(f"[repair] job_id={job_id} status={job.status} completed={job.completed_segments}")
        return job

    def normalize_interrupted_segments(self, segments: list[SegmentRecord], chunks: list[ChunkRecord]) -> bool:
        changed = False
        segment_map = {segment.segment_id: segment for segment in segments}
        for segment in segments:
            if segment.status == "running":
                if segment.translated_text.strip():
                    segment.translated_text = self._clean_translation_text(segment.translated_text)
                    segment.status = "success"
                else:
                    segment.status = "pending"
                segment.error = ""
                changed = True

        for chunk in chunks:
            chunk_segments = [segment_map[segment_id] for segment_id in chunk.segment_ids if segment_id in segment_map]
            if chunk.status == "running":
                chunk.status = "pending"
                changed = True
            if chunk_segments and all(segment.status == "success" for segment in chunk_segments):
                if chunk.status != "completed":
                    chunk.status = "completed"
                    changed = True
            elif chunk.status == "completed":
                chunk.status = "pending"
                changed = True
        return changed

    def _build_client(self, job: JobRecord) -> HyMT2Client:
        config = reload_config()
        job.config.model = config.model
        runtime_config = build_runtime_config(
            {
                "model": config.model,
                "temperature": job.config.temperature,
                "stream": job.config.stream,
                "chunk_size_chars": job.config.chunk_size_chars,
                "max_retries": job.config.max_retries,
                "request_timeout_seconds": job.config.request_timeout_seconds,
                "stream_timeout_seconds": job.config.stream_timeout_seconds,
                "stream_fallback": job.config.stream_fallback,
                "max_segments_per_chunk": job.config.max_segments_per_chunk,
                "default_target_language": job.target_language,
                "tokenhub_api_key": config.tokenhub_api_key,
                "tokenhub_base_url": config.tokenhub_base_url,
            }
        )
        return HyMT2Client(runtime_config)

    def _build_corpus_context(self, job: JobRecord, text: str) -> dict[str, Any]:
        if not job.config.use_corpus:
            return {"domain_prompt": "", "terms": [], "examples": []}
        try:
            corpus = self.corpus_manager.get_corpus(job.config.corpus_id)
        except FileNotFoundError:
            return {"domain_prompt": "", "terms": [], "examples": []}
        try:
            terms = (
                ensure_list(self.corpus_manager.select_relevant_terms(job.config.corpus_id, text, limit=30))
                if job.config.use_glossary
                else []
            )
            examples = (
                ensure_list(self.corpus_manager.select_relevant_examples(job.config.corpus_id, text, limit=3))
                if job.config.use_style_examples
                else []
            )
        except Exception as exc:
            print(f"[warn] corpus metadata ignored job_id={job.job_id} error={exc}")
            terms = []
            examples = []
        return {
            "domain_prompt": (corpus.get("domain_prompt", "")[:1500] if job.config.use_domain_prompt else ""),
            "terms": terms,
            "examples": examples,
        }

    def _parse_translation_response(
        self,
        response_text: str,
        segment_ids: list[str],
    ) -> TranslationParseResult:
        normalized_text = self._normalize_model_output(response_text)
        translation_match = re.search(
            r"<translation\b[^>]*>(.*?)</translation\s*>",
            normalized_text,
            re.DOTALL | re.IGNORECASE,
        )
        parse_scope = translation_match.group(1) if translation_match else normalized_text
        allowed_ids = set(segment_ids)
        translations: dict[str, str] = {}
        for _, segment_id, translated_text in SEGMENT_TRANSLATION_RE.findall(parse_scope):
            if segment_id in allowed_ids:
                translations[segment_id] = self._clean_translation_text(translated_text)

        if not translations and len(segment_ids) == 1:
            fallback_text = self._extract_loose_single_segment_translation(response_text)
            if fallback_text:
                translations[segment_ids[0]] = fallback_text

        missing_segment_ids = [segment_id for segment_id in segment_ids if not translations.get(segment_id)]
        return TranslationParseResult(
            translations=translations,
            summary="",
            terms={},
            missing_segment_ids=missing_segment_ids,
        )

    def _normalize_model_output(self, response_text: str) -> str:
        text = response_text.strip()
        fenced_match = FENCED_BLOCK_RE.fullmatch(text)
        if fenced_match:
            return fenced_match.group(1).strip()
        fenced_match = FENCED_BLOCK_RE.search(text)
        if fenced_match and "<segment" in fenced_match.group(1).lower():
            return fenced_match.group(1).strip()
        return text

    def _clean_translation_text(self, text: str) -> str:
        return text.strip().removeprefix("<![CDATA[").removesuffix("]]>").strip()

    def _find_first_retryable_segment(self, segments: list[SegmentRecord]) -> SegmentRecord | None:
        return next((segment for segment in segments if segment.status in {"pending", "running", "partial"}), None)

    def _reset_failed_segments_for_retry(self, segments: list[SegmentRecord], chunks: list[ChunkRecord]) -> bool:
        changed = False
        segment_map = {segment.segment_id: segment for segment in segments}
        for segment in segments:
            if segment.status == "failed":
                segment.status = "pending"
                segment.error = ""
                changed = True

        for chunk in chunks:
            if chunk.status == "failed":
                chunk.status = "pending"
                chunk.error = ""
                changed = True
            elif chunk.segment_ids:
                chunk_segments = [segment_map[segment_id] for segment_id in chunk.segment_ids if segment_id in segment_map]
                if chunk_segments and any(segment.status != "success" for segment in chunk_segments) and chunk.status == "completed":
                    chunk.status = "pending"
                    changed = True
        return changed

    def _model_output_preview(self, text: str, max_chars: int = 240) -> str:
        normalized = self._normalize_model_output(text or "")
        compact = re.sub(r"\s+", " ", normalized).strip()
        if not compact:
            return ""
        if len(compact) <= max_chars:
            return compact
        return f"{compact[:max_chars].rstrip()}..."

    def _is_model_refusal(self, text: str) -> bool:
        preview = self._model_output_preview(text, max_chars=320).lower()
        if not preview:
            return False
        return any(pattern in preview for pattern in REFUSAL_PATTERNS)

    def _extract_loose_single_segment_translation(self, response_text: str) -> str:
        normalized = self._normalize_model_output(response_text or "")
        if not normalized or self._is_model_refusal(normalized):
            return ""
        text = re.sub(r"</?translation\b[^>]*>", "", normalized, flags=re.IGNORECASE)
        text = re.sub(r"</?segment\b[^>]*>", "", text, flags=re.IGNORECASE)
        text = re.sub(r"<[^>]+>", "", text).strip()
        cleaned = self._clean_translation_text(text)
        if not cleaned or self._is_model_refusal(cleaned):
            return ""
        return cleaned

    def _build_segment_retry_messages(
        self,
        job: JobRecord,
        segment: SegmentRecord,
        corpus_context: dict[str, Any] | None = None,
    ) -> list[dict[str, str]]:
        corpus_context = corpus_context or {}
        terms = ensure_list(corpus_context.get("terms", []))
        term_lines = []
        for item in terms[:12]:
            source = str(item.get("source", "")).strip()
            target = str(item.get("target", "")).strip()
            if source and target:
                term_lines.append(f"- {source} => {target}")
        domain_prompt = str(corpus_context.get("domain_prompt", "")).strip()[:800]
        examples = ensure_list(corpus_context.get("examples", []))[:3]
        term_block = "\n".join(term_lines) if term_lines else "无"
        user_prompt = (
            f"请自动识别下面段落的原文语言，并翻译成{job.target_language}。\n\n"
            "要求：\n"
            "1. 只输出译文正文，不要 XML、标签、解释、前言、致歉或额外说明。\n"
            "2. 忠实翻译，不删减，不扩写，不总结。\n"
            "3. 无论原文主题是什么，都只做中性、客观、逐句的书面翻译。\n"
            "4. 如果原文中有专有名词、书名、DOI，请保留必要信息，不要改写成别的内容。\n\n"
            f"术语参考：\n{term_block}\n\n"
            f"领域提示：\n{domain_prompt or '无'}\n\n"
            f"风格参考译例（只模仿表达，不改变原意）：\n{format_examples(examples)}\n\n"
            f"原文：\n{segment.source_text}"
        )
        return [
            {
                "role": "system",
                "content": "你是专业图书翻译助手。你的任务只有翻译，不做审查解释，不输出标签，只返回译文正文。",
            },
            {"role": "user", "content": user_prompt},
        ]

    def _build_segment_failure_message(self, response_text: str, default_message: str = "无法解析模型输出") -> str:
        preview = self._model_output_preview(response_text)
        if self._is_model_refusal(response_text):
            return f"模型拒答或内容被拦截。模型返回预览：{preview}" if preview else "模型拒答或内容被拦截"
        if preview:
            return f"{default_message}。模型返回预览：{preview}"
        return default_message

    async def _translate_missing_segments(
        self,
        client: HyMT2Client,
        job: JobRecord,
        missing_segments: list[SegmentRecord],
        completed_chunks: list[ChunkRecord],
    ) -> tuple[dict[str, str], dict[str, str]]:
        translations: dict[str, str] = {}
        errors: dict[str, str] = {}
        for segment in missing_segments:
            corpus_context = self._build_corpus_context(job, segment.source_text)
            attempt_messages = [
                build_single_segment_messages(
                    job=job,
                    segment=segment,
                    completed_chunks=completed_chunks,
                    corpus_context=corpus_context,
                ),
                self._build_segment_retry_messages(job, segment, corpus_context),
            ]
            last_error = "无法解析模型输出"
            for messages in attempt_messages:
                response = await client.translate_single_segment(messages=messages)
                parse_result = self._parse_translation_response(response.text, [segment.segment_id])
                translated_text = parse_result.translations.get(segment.segment_id)
                if translated_text:
                    translations[segment.segment_id] = translated_text
                    last_error = ""
                    break

                fallback_text = self._extract_loose_single_segment_translation(response.text)
                if fallback_text:
                    translations[segment.segment_id] = fallback_text
                    last_error = ""
                    break

                last_error = self._build_segment_failure_message(response.text)

            if last_error:
                errors[segment.segment_id] = last_error
        return translations, errors

    def _extract_stream_translations(
        self,
        stream_buffer: str,
        segment_ids: list[str],
    ) -> dict[str, str]:
        content = self._normalize_model_output(stream_buffer)
        translation_match = re.search(r"<translation\b[^>]*>(.*)", content, re.DOTALL | re.IGNORECASE)
        if translation_match:
            content = translation_match.group(1)
        previews: dict[str, str] = {}
        for index, segment_id in enumerate(segment_ids):
            start_match = re.search(
                rf"<segment\b[^>]*\bid\s*=\s*(['\"]){re.escape(segment_id)}\1[^>]*>",
                content,
                re.IGNORECASE,
            )
            if not start_match:
                continue
            start_index = start_match.end()

            end_index = len(content)
            closing_match = re.search(r"</segment\s*>", content[start_index:], re.IGNORECASE)
            closing_index = start_index + closing_match.start() if closing_match else -1
            if closing_index != -1:
                end_index = min(end_index, closing_index)

            for next_segment_id in segment_ids[index + 1 :]:
                next_match = re.search(
                    rf"<segment\b[^>]*\bid\s*=\s*(['\"]){re.escape(next_segment_id)}\1[^>]*>",
                    content[start_index:],
                    re.IGNORECASE,
                )
                if next_match:
                    end_index = min(end_index, start_index + next_match.start())
                    break

            preview = self._clean_translation_text(content[start_index:end_index])
            if preview:
                previews[segment_id] = preview
        return previews

    async def _emit_job_snapshot(self, job: JobRecord) -> None:
        latest_job = await self.storage.load_job(job.job_id)
        await self.broker.publish(job.job_id, "job_updated", latest_job.model_dump())

    def _normalized_speed_mode(self, speed_mode: str | None) -> str:
        mode = (speed_mode or "stable").strip().lower()
        if mode not in {"stable", "balanced", "fast"}:
            return "stable"
        return mode

    async def _recompute_job_progress(self, job: JobRecord, segments: list[SegmentRecord]) -> JobRecord:
        job.completed_segments = sum(1 for segment in segments if segment.status == "success")
        job.failed_segments = sum(1 for segment in segments if segment.status == "failed")
        job.updated_at = utc_now_iso()
        return job

    def _find_first_unfinished_segment(self, segments: list[SegmentRecord]) -> SegmentRecord | None:
        return next((segment for segment in segments if segment.status != "success"), None)

    async def _save_progress_snapshot(
        self,
        job: JobRecord,
        segments: list[SegmentRecord],
        current_chapter: str = "",
    ) -> SegmentRecord | None:
        await self._recompute_job_progress(job, segments)
        next_segment = self._find_first_retryable_segment(segments)
        if next_segment:
            job.current_segment_id = next_segment.segment_id
            job.current_chapter = current_chapter or next_segment.chapter_title
        else:
            job.current_segment_id = ""
            job.current_chapter = ""
            job.status = "completed"
        job.last_run_heartbeat_at = utc_now_iso()
        await self.storage.save_job(job)
        print(f"[progress] completed={job.completed_segments} failed={job.failed_segments} next={job.current_segment_id or 'completed'}")
        return next_segment

    async def _fail_job_for_auth_error(
        self,
        job: JobRecord,
        chunk: ChunkRecord,
        source_segments: list[SegmentRecord],
        segments: list[SegmentRecord],
        chunks: list[ChunkRecord],
        raw_error: str,
    ) -> None:
        chunk.status = "failed"
        chunk.error = AUTH_FAILURE_MESSAGE
        chunk.completed_at = utc_now_iso()
        for segment in source_segments:
            if segment.status != "success":
                segment.status = "failed"
                segment.error = AUTH_FAILURE_MESSAGE

        job.status = "failed"
        job.last_error = AUTH_FAILURE_MESSAGE
        job.current_segment_id = source_segments[0].segment_id if source_segments else job.current_segment_id
        job.current_chapter = chunk.chapter_title
        await self._recompute_job_progress(job, segments)

        await self.storage.save_chunk(job.job_id, chunk)
        await self.storage.save_segments(job.job_id, segments)
        await self.storage.save_job(job)
        await self.storage.append_log(
            job.job_id,
            LogEntry(
                event="job_failed_auth",
                payload={
                    "chunk_id": chunk.chunk_id,
                    "segment_ids": chunk.segment_ids,
                    "error": raw_error,
                    "message": AUTH_FAILURE_MESSAGE,
                },
            ),
        )
        await export_outputs(self.storage, job, segments, chunks)
        await self.broker.publish(
            job.job_id,
            "chunk_failed",
            {
                "job_id": job.job_id,
                "chunk_id": chunk.chunk_id,
                "error": AUTH_FAILURE_MESSAGE,
                "segment_ids": chunk.segment_ids,
            },
        )
        await self.broker.publish(job.job_id, "job_failed", job.model_dump())

    async def _translate_chunk_with_api_logging(
        self,
        client: HyMT2Client,
        job: JobRecord,
        chunk: ChunkRecord,
        pending_segments: list[SegmentRecord],
        messages: list[dict[str, str]],
        on_delta,
    ):
        segment_ids = [segment.segment_id for segment in pending_segments]
        segment_range = f"{segment_ids[0]}..{segment_ids[-1]}" if segment_ids else ""
        stream_enabled = bool(job.config.stream)
        started_at = time.monotonic()
        first_response_seen = False

        def on_first_response() -> None:
            nonlocal first_response_seen
            if first_response_seen:
                return
            first_response_seen = True
            print(f"[api] first_response job_id={job.job_id} chunk_id={chunk.chunk_id}")

        def on_stream_fallback(exc: Exception) -> None:
            if isinstance(exc, TranslationTimeoutError):
                print(
                    f"[api] timeout job_id={job.job_id} chunk_id={chunk.chunk_id} "
                    f"timeout={exc.timeout_seconds}s"
                )
            else:
                print(f"[api] error job_id={job.job_id} chunk_id={chunk.chunk_id} error={exc}")
            print(f"[api] stream_fallback job_id={job.job_id} chunk_id={chunk.chunk_id} stream=false")

        print(
            f"[api] start job_id={job.job_id} chunk_id={chunk.chunk_id} "
            f"segments={segment_range} stream={str(stream_enabled).lower()} model={job.config.model}"
        )
        timeout_seconds = max(1, job.config.request_timeout_seconds)
        if stream_enabled and job.config.stream_fallback:
            timeout_seconds += max(1, job.config.stream_timeout_seconds) + 10
        try:
            response = await asyncio.wait_for(
                client.translate_chunk(
                    messages=messages,
                    stream=stream_enabled,
                    on_delta=on_delta,
                    on_first_response=on_first_response,
                    on_stream_fallback=on_stream_fallback,
                ),
                timeout=timeout_seconds,
            )
            elapsed = time.monotonic() - started_at
            print(
                f"[api] done job_id={job.job_id} chunk_id={chunk.chunk_id} "
                f"elapsed={elapsed:.1f}s chars={len(response.text)}"
            )
            return response
        except asyncio.TimeoutError as exc:
            print(f"[api] timeout job_id={job.job_id} chunk_id={chunk.chunk_id} timeout={timeout_seconds}s")
            raise TranslationTimeoutError("API request timed out", timeout_seconds=timeout_seconds) from exc
        except TranslationTimeoutError as exc:
            print(f"[api] timeout job_id={job.job_id} chunk_id={chunk.chunk_id} timeout={exc.timeout_seconds}s")
            raise
        except Exception as exc:
            print(f"[api] error job_id={job.job_id} chunk_id={chunk.chunk_id} error={exc}")
            raise

    async def _pause_job_for_timeout(
        self,
        job: JobRecord,
        chunk: ChunkRecord,
        source_segments: list[SegmentRecord],
        segments: list[SegmentRecord],
        chunks: list[ChunkRecord],
        error: str,
    ) -> None:
        for segment in source_segments:
            if segment.status == "running":
                segment.status = "pending"
            segment.error = ""
        chunk.status = "pending"
        chunk.error = error
        job.status = "paused"
        job.pause_requested = False
        job.last_error = error
        job.current_segment_id = source_segments[0].segment_id if source_segments else job.current_segment_id
        job.current_chapter = chunk.chapter_title
        await self._recompute_job_progress(job, segments)
        await self.storage.save_chunk(job.job_id, chunk)
        await self.storage.save_segments(job.job_id, segments)
        await self.storage.save_job(job)
        await self.storage.append_log(
            job.job_id,
            LogEntry(event="chunk_timeout", payload={"chunk_id": chunk.chunk_id, "error": error}),
        )
        await self.broker.publish(
            job.job_id,
            "chunk_failed",
            {"job_id": job.job_id, "chunk_id": chunk.chunk_id, "error": error, "segment_ids": chunk.segment_ids},
        )
        await self.broker.publish(job.job_id, "job_paused", job.model_dump())

    def _log_chunk_build(
        self,
        job: JobRecord,
        chunk: ChunkRecord,
        source_segments: list[SegmentRecord],
        pending_segments: list[SegmentRecord],
        reason_override: str = "",
    ) -> None:
        if not pending_segments:
            return
        max_segments = max(1, job.config.max_segments_per_chunk)
        source_chars = sum(len(segment.source_text) for segment in pending_segments)
        next_statuses = [
            f"{segment.segment_id}:{segment.status}"
            for segment in source_segments
            if segment.order > pending_segments[-1].order
        ][:5]
        known_reasons = {
            "reached_max_segments",
            "reached_char_limit",
            "stop_at_chapter_boundary_after_body",
            "merge_tiny_chapter_with_next",
            "single_large_segment",
            "non_pending_status",
            "no_more_segments",
        }
        if reason_override and reason_override in known_reasons:
            reason = reason_override
        elif reason_override:
            reason = reason_override
        elif len(pending_segments) >= max_segments:
            reason = "reached_max_segments"
        elif source_chars >= job.config.chunk_size_chars:
            reason = "reached_char_limit"
        elif not next_statuses:
            reason = "no_more_segments"
        elif any(segment.status == "success" for segment in source_segments if segment.order > pending_segments[-1].order):
            reason = "non_pending_status"
        else:
            reason = "unknown"
        print(
            f"[chunk-build] start={pending_segments[0].segment_id} count={len(pending_segments)} "
            f"reason={reason} source_chars={source_chars} max_chars={job.config.chunk_size_chars} "
            f"max_segments={max_segments} next_statuses={','.join(next_statuses) or 'none'}"
        )

    def _build_runtime_chunk_from_next(
        self,
        job: JobRecord,
        segments: list[SegmentRecord],
        next_segment: SegmentRecord,
    ) -> tuple[ChunkRecord, list[SegmentRecord], str]:
        start_index = next((index for index, segment in enumerate(segments) if segment.segment_id == next_segment.segment_id), -1)
        if start_index < 0:
            return (
                ChunkRecord(chunk_id=f"chunk_runtime_{next_segment.segment_id}", order=next_segment.order, segment_ids=[], source_text=""),
                [],
                "unknown",
            )

        selected: list[SegmentRecord] = []
        current_chars = 0
        reason = "no_more_segments"
        max_segments = max(1, job.config.max_segments_per_chunk)
        max_chars = max(1, job.config.chunk_size_chars)
        min_standalone = max(1, job.config.min_chars_for_standalone_chunk)
        merge_tiny = job.config.merge_tiny_chapter_segments
        chapter = next_segment.chapter_title
        crossed_chapter_for_tiny = False

        for segment in segments[start_index:]:
            if segment.status == "success":
                if selected:
                    reason = "non_pending_status"
                    break
                continue

            segment_chars = len(segment.source_text)

            if selected:
                if len(selected) >= max_segments:
                    reason = "reached_max_segments"
                    break
                if current_chars + segment_chars + 2 > max_chars:
                    reason = "reached_char_limit"
                    break
                if (segment.chapter_title or "") != (chapter or ""):
                    if merge_tiny and current_chars < min_standalone:
                        crossed_chapter_for_tiny = True
                    else:
                        reason = "stop_at_chapter_boundary_after_body"
                        break

            selected.append(segment)
            current_chars += segment_chars + 2
            chapter = segment.chapter_title or chapter

        if not selected:
            return (
                ChunkRecord(
                    chunk_id=f"chunk_runtime_{next_segment.segment_id}",
                    order=next_segment.order,
                    segment_ids=[],
                    source_text="",
                ),
                [],
                "unknown",
            )

        if len(selected) == 1 and len(selected[0].source_text) >= max_chars:
            reason = "single_large_segment"
        elif crossed_chapter_for_tiny and reason == "no_more_segments":
            reason = "merge_tiny_chapter_with_next"

        chunk_id = f"chunk_runtime_{selected[0].segment_id}"
        chunk = ChunkRecord(
            chunk_id=chunk_id,
            order=selected[0].order,
            chapter_title=chapter,
            segment_ids=[segment.segment_id for segment in selected],
            source_text="\n\n".join(segment.source_text for segment in selected),
        )
        for segment in selected:
            segment.chunk_id = chunk_id
        return chunk, selected, reason

    async def _run_job(self, job_id: str) -> None:
        print(f"[run] job_id={job_id} start")
        try:
            job = await self.storage.load_job(job_id)
            job.last_run_heartbeat_at = utc_now_iso()
            await self.storage.save_job(job)

            speed_mode = self._normalized_speed_mode(job.config.speed_mode)

            if speed_mode == "stable":
                print(f"[mode] speed_mode=stable using serial runner")
                await self._run_job_serial(job_id)
            else:
                print(f"[mode] speed_mode={speed_mode} using concurrent runner")
                await self._run_job_concurrent(job_id)
        except Exception as exc:
            try:
                job = await self.storage.load_job(job_id)
                job.status = "failed"
                job.last_error = str(exc)
                job.updated_at = utc_now_iso()
                await self.storage.save_job(job)
                await self.storage.append_log(
                    job_id,
                    LogEntry(event="job_failed", payload={"error": str(exc)}),
                )
                await self.broker.publish(job_id, "job_failed", job.model_dump())
                print(f"[run] failed error={exc}")
            except Exception:
                pass
        finally:
            self.tasks.pop(job_id, None)

    async def _run_job_serial(self, job_id: str) -> None:
        job = await self.storage.load_job(job_id)
        client = self._build_client(job)
        loop = asyncio.get_running_loop()

        await self.broker.publish(
            job_id,
            "job_started",
            {"job_id": job_id, "status": "running", "completed_segments": job.completed_segments},
        )

        try:
            while True:
                chunk = None
                chunk_id = None
                chunk_segments: list[SegmentRecord] = []
                next_segment = None
                latest_job = await self.storage.load_job(job_id)
                if self._normalized_speed_mode(latest_job.config.speed_mode) != "stable":
                    print(f"[mode-switch] stable -> {self._normalized_speed_mode(latest_job.config.speed_mode)}")
                    return await self._run_job(job_id)
                segments = await self.storage.load_segments(job_id)
                chunks = await self.storage.load_chunks(job_id)
                if latest_job.cancel_requested or latest_job.status == "cancelled":
                    latest_job.status = "cancelled"
                    await self.storage.save_job(latest_job)
                    await self.broker.publish(job_id, "job_cancelled", latest_job.model_dump())
                    print("[run-exit] reason=cancel_requested")
                    break
                if latest_job.pause_requested or latest_job.status == "pausing":
                    latest_job.status = "paused"
                    latest_job.pause_requested = False
                    latest_job.updated_at = utc_now_iso()
                    await self.storage.save_job(latest_job)
                    await export_outputs(self.storage, latest_job, segments, chunks)
                    await self.broker.publish(job_id, "job_paused", latest_job.model_dump())
                    print("[run-exit] reason=pause_requested")
                    return

                next_segment = self._find_first_retryable_segment(segments)
                if not next_segment:
                    latest_job.pause_requested = False
                    latest_job.cancel_requested = False
                    latest_job.current_segment_id = ""
                    latest_job.current_chapter = ""
                    await self._recompute_job_progress(latest_job, segments)
                    if any(segment.status == "failed" for segment in segments):
                        latest_job.status = "failed"
                        if not latest_job.last_error:
                            latest_job.last_error = "存在失败段落，可点击继续重试。"
                        await export_outputs(self.storage, latest_job, segments, chunks)
                        await self.storage.save_job(latest_job)
                        await self.broker.publish(job_id, "job_failed", latest_job.model_dump())
                        print(f"[progress] completed={latest_job.completed_segments} failed={latest_job.failed_segments} next=failed")
                        print("[run] failed")
                        print("[run-exit] reason=failed")
                    else:
                        latest_job.status = "completed"
                        latest_job.last_error = ""
                        await export_outputs(self.storage, latest_job, segments, chunks)
                        await self.storage.save_job(latest_job)
                        await self.broker.publish(job_id, "job_completed", latest_job.model_dump())
                        print(f"[progress] completed={latest_job.completed_segments} failed={latest_job.failed_segments} next=completed")
                        print("[run] completed")
                        print("[run-exit] reason=completed")
                    return

                job = latest_job
                job.status = "running"
                job.pause_requested = False
                job.current_segment_id = next_segment.segment_id
                job.current_chapter = next_segment.chapter_title
                job.last_run_heartbeat_at = utc_now_iso()
                await self._recompute_job_progress(job, segments)
                await self.storage.save_job(job)
                print(
                    f"[run-loop] status={job.status} pause_requested={job.pause_requested} "
                    f"cancel_requested={job.cancel_requested} current={next_segment.segment_id}"
                )

                chunk, pending_segments, chunk_reason = self._build_runtime_chunk_from_next(job, segments, next_segment)
                chunk_id = getattr(chunk, "chunk_id", None)
                chunk_segments = list(pending_segments)
                source_segments = pending_segments
                self._log_chunk_build(job, chunk, source_segments, pending_segments, chunk_reason)

                if not pending_segments:
                    pending_preview = [
                        f"{segment.segment_id}:{segment.status}"
                        for segment in segments
                        if segment.status != "success"
                    ][:10]
                    error = (
                        "chunk 构建异常：仍有 pending segment，但 build_chunk 返回 no_more_segments"
                        if pending_preview
                        else "chunk 构建异常：未找到可翻译 segment"
                    )
                    job.status = "failed"
                    job.last_error = error
                    await self.storage.save_job(job)
                    print(f"[run-exit] reason=chunk_build_bug statuses={','.join(pending_preview) or 'none'}")
                    await self.broker.publish(job_id, "job_failed", job.model_dump())
                    return

                if chunk_reason == "no_more_segments" and any(
                    segment.status != "success" and segment.order > pending_segments[-1].order for segment in segments
                ):
                    pending_preview = [
                        f"{segment.segment_id}:{segment.status}"
                        for segment in segments
                        if segment.order >= pending_segments[0].order
                    ][:10]
                    job.status = "failed"
                    job.last_error = "chunk 构建异常：仍有 pending segment，但 build_chunk 返回 no_more_segments"
                    await self.storage.save_job(job)
                    print(f"[run-exit] reason=chunk_build_bug statuses={','.join(pending_preview)}")
                    await self.broker.publish(job_id, "job_failed", job.model_dump())
                    return

                for segment in pending_segments:
                    if segment.status != "success":
                        segment.status = "running"
                        segment.error = ""

                chunk.status = "running"
                chunk.started_at = utc_now_iso()
                chunk.error = ""
                job.status = "running"
                job.current_segment_id = pending_segments[0].segment_id
                job.current_chapter = chunk.chapter_title
                job.last_run_heartbeat_at = utc_now_iso()
                await self.storage.save_segments(job_id, segments)
                await self.storage.save_chunk(job_id, chunk)
                await self._recompute_job_progress(job, segments)
                await self.storage.save_job(job)
                print(f"[run] chunk start segment={pending_segments[0].segment_id} count={len(pending_segments)}")

                await self.broker.publish(
                    job_id,
                    "chunk_started",
                    {
                        "job_id": job_id,
                        "chunk_id": chunk.chunk_id,
                        "segment_ids": chunk.segment_ids,
                        "current_segment_id": pending_segments[0].segment_id,
                        "chapter_title": chunk.chapter_title,
                    },
                )

                current_source_text = "\n\n".join(segment.source_text for segment in pending_segments)
                corpus_context = self._build_corpus_context(job, current_source_text)
                try:
                    chunk.used_terms = ensure_list(corpus_context.get("terms", []))
                    chunk.used_examples = ensure_list(corpus_context.get("examples", []))
                except Exception as exc:
                    print(f"[warn] chunk metadata ignored job_id={job_id} chunk_id={chunk.chunk_id} error={exc}")
                    chunk.used_terms = []
                    chunk.used_examples = []
                messages = build_messages(
                    job=job,
                    chunk=chunk,
                    source_segments=pending_segments,
                    completed_chunks=[item for item in chunks if item.status == "completed"],
                    corpus_context=corpus_context,
                )

                chunk_response = None
                parse_result = None
                last_error = ""
                for attempt in range(1, job.config.max_retries + 1):
                    chunk.attempt_count = attempt
                    stream_buffer = ""
                    emitted_previews: dict[str, str] = {}

                    def on_delta(delta: str) -> None:
                        nonlocal stream_buffer, emitted_previews
                        stream_buffer += delta
                        previews = self._extract_stream_translations(
                            stream_buffer,
                            [segment.segment_id for segment in pending_segments],
                        )
                        for segment_id, preview in previews.items():
                            previous_preview = emitted_previews.get(segment_id, "")
                            if preview == previous_preview:
                                continue
                            emitted_delta = preview[len(previous_preview) :]
                            emitted_previews[segment_id] = preview
                            asyncio.run_coroutine_threadsafe(
                                self.broker.publish(
                                    job_id,
                                    "segment_delta",
                                    {
                                        "job_id": job_id,
                                        "chunk_id": chunk.chunk_id,
                                        "segment_id": segment_id,
                                        "delta": emitted_delta,
                                    },
                                ),
                                loop,
                            )

                    try:
                        chunk_response = await self._translate_chunk_with_api_logging(
                            client=client,
                            job=job,
                            chunk=chunk,
                            pending_segments=pending_segments,
                            messages=messages,
                            on_delta=on_delta,
                        )
                        parse_result = self._parse_translation_response(
                            chunk_response.text,
                            [segment.segment_id for segment in pending_segments],
                        )
                        if not parse_result.missing_segment_ids:
                            break
                        if attempt == job.config.max_retries:
                            break
                        if parse_result.missing_segment_ids:
                            raise ValueError(f"Missing translations for: {', '.join(parse_result.missing_segment_ids)}")
                    except TranslationAuthError as exc:
                        await self._fail_job_for_auth_error(
                            job=job,
                            chunk=chunk,
                            source_segments=pending_segments,
                            segments=segments,
                            chunks=chunks,
                            raw_error=exc.raw_error,
                        )
                        return
                    except TranslationTimeoutError:
                        error = "当前 chunk 调用模型超时，已暂停，可点击继续重试。"
                        await self._pause_job_for_timeout(
                            job=job,
                            chunk=chunk,
                            source_segments=pending_segments,
                            segments=segments,
                            chunks=chunks,
                            error=error,
                        )
                        print(f"[run] timeout paused error={error}")
                        return
                    except Exception as exc:
                        last_error = str(exc)
                        chunk.error = last_error
                        await self.storage.append_log(
                            job_id,
                            LogEntry(
                                event="chunk_retry",
                                payload={
                                    "chunk_id": chunk.chunk_id,
                                    "attempt": attempt,
                                    "error": last_error,
                                },
                            ),
                        )

                if parse_result and parse_result.missing_segment_ids:
                    segment_by_id = {segment.segment_id: segment for segment in pending_segments}
                    missing_segments = [
                        segment_by_id[segment_id]
                        for segment_id in parse_result.missing_segment_ids
                        if segment_id in segment_by_id
                    ]
                    if missing_segments:
                        await self.storage.append_log(
                            job_id,
                            LogEntry(
                                event="segment_fallback",
                                payload={
                                    "chunk_id": chunk.chunk_id,
                                    "segment_ids": [segment.segment_id for segment in missing_segments],
                                },
                            ),
                        )
                        fallback_translations, fallback_errors = await self._translate_missing_segments(
                            client=client,
                            job=job,
                            missing_segments=missing_segments,
                            completed_chunks=[item for item in chunks if item.status == "completed"],
                        )
                        parse_result.translations.update(fallback_translations)
                        parse_result.missing_segment_ids = [
                            segment_id
                            for segment_id in parse_result.missing_segment_ids
                            if not parse_result.translations.get(segment_id)
                        ]

                if parse_result and not parse_result.missing_segment_ids and chunk_response:
                    chunk.status = "completed" if all(segment.status == "success" for segment in source_segments) else "pending"
                    chunk.summary = parse_result.summary
                    chunk.terms = parse_result.terms
                    chunk.response_text = chunk_response.text
                    chunk.stream_fallback_used = chunk_response.stream_fallback_used
                    chunk.completed_at = utc_now_iso()
                    chunk.error = chunk_response.raw_error if chunk_response.stream_fallback_used else ""

                    for segment in pending_segments:
                        segment.translated_text = parse_result.translations.get(segment.segment_id, "")
                        segment.summary = parse_result.summary
                        segment.terms = parse_result.terms
                        segment.status = "success"
                        segment.error = ""

                    chunk.status = "completed" if all(segment.status == "success" for segment in source_segments) else "pending"

                    await self.storage.save_chunk(job_id, chunk)
                    await self.storage.save_segments(job_id, segments)
                    await self.storage.append_log(
                        job_id,
                        LogEntry(
                            event="chunk_completed",
                            payload={
                                "chunk_id": chunk.chunk_id,
                                "segment_ids": chunk.segment_ids,
                                "stream_fallback_used": chunk.stream_fallback_used,
                            },
                        ),
                    )

                    for segment in pending_segments:
                        await self.broker.publish(job_id, "segment_completed", segment.model_dump())
                    await self.broker.publish(
                        job_id,
                        "chunk_completed",
                        {
                            "job_id": job_id,
                            "chunk_id": chunk.chunk_id,
                            "summary": chunk.summary,
                            "segment_ids": chunk.segment_ids,
                        },
                    )
                    print(f"[run] chunk success completed={sum(1 for segment in segments if segment.status == 'success')}")
                    print("[run] continue next")
                else:
                    chunk.status = "failed"
                    missing_ids = set(parse_result.missing_segment_ids if parse_result else [])
                    chunk.response_text = chunk_response.text if chunk_response else chunk.response_text
                    preview = self._model_output_preview(chunk_response.text if chunk_response else "")
                    chunk.error = (
                        "该段翻译解析失败，可点击继续重试。"
                        if parse_result and parse_result.missing_segment_ids
                        else last_error or "Failed to parse model output"
                    )
                    chunk.completed_at = utc_now_iso()
                    if preview:
                        if parse_result and parse_result.missing_segment_ids:
                            chunk.error = f"该段翻译解析失败，可点击继续重试。模型返回预览：{preview}"
                        elif not last_error or last_error == "Failed to parse model output":
                            chunk.error = f"无法解析模型输出。模型返回预览：{preview}"
                    for segment in pending_segments:
                        translated_text = parse_result.translations.get(segment.segment_id, "") if parse_result else ""
                        if translated_text:
                            segment.translated_text = translated_text
                            segment.status = "success"
                            segment.error = ""
                            continue
                        if segment.status != "success" and (not missing_ids or segment.segment_id in missing_ids):
                            segment.status = "failed"
                            segment.error = chunk.error
                    await self.storage.save_chunk(job_id, chunk)
                    await self.storage.save_segments(job_id, segments)
                    for segment in pending_segments:
                        if segment.status == "success":
                            await self.broker.publish(job_id, "segment_completed", segment.model_dump())
                    await self.broker.publish(
                        job_id,
                        "chunk_failed",
                        {
                            "job_id": job_id,
                            "chunk_id": chunk.chunk_id,
                            "error": chunk.error,
                            "segment_ids": chunk.segment_ids,
                        },
                    )
                    print(f"[run] chunk partial/failed error={chunk.error}")
                    print("[run] continue next")

                latest_job = await self.storage.load_job(job_id)
                job = latest_job
                await self._save_progress_snapshot(job, segments, chunk.chapter_title)

                if latest_job.cancel_requested or latest_job.status == "cancelled":
                    job.status = "cancelled"
                    await self.storage.save_job(job)
                    await self.broker.publish(job_id, "job_cancelled", job.model_dump())
                    print("[run-exit] reason=cancel_requested")
                    break

                if latest_job.pause_requested or latest_job.status == "pausing":
                    job.status = "paused"
                    job.pause_requested = False
                    job.updated_at = utc_now_iso()
                    await self.storage.save_job(job)
                    await export_outputs(self.storage, job, segments, chunks)
                    await self.broker.publish(job_id, "job_paused", job.model_dump())
                    print("[run-exit] reason=pause_requested")
                    return

                await self.storage.save_job(job)
                await self._emit_job_snapshot(job)

            job = await self.storage.load_job(job_id)
            await self._recompute_job_progress(job, segments)
            if job.status == "cancelled":
                await export_outputs(self.storage, job, segments, chunks)
                await self.storage.save_job(job)
                print("[run-exit] reason=cancelled")
                return

            if job.status == "paused":
                await export_outputs(self.storage, job, segments, chunks)
                await self.storage.save_job(job)
                print("[run-exit] reason=paused")
                return

            unfinished_segments = [segment for segment in segments if segment.status in {"pending", "running", "partial"}]
            if unfinished_segments:
                job.status = "running"
                job.current_segment_id = unfinished_segments[0].segment_id
                await self.storage.save_job(job)
                await self._emit_job_snapshot(job)
                print("[run] continue next")
                await asyncio.sleep(0)
                return await self._run_job_serial(job_id)

            if any(segment.status == "failed" for segment in segments) and any(
                segment.status in {"pending", "running", "partial"} for segment in segments
            ):
                job.status = "failed"
            elif any(segment.status == "failed" for segment in segments):
                job.status = "failed"
            else:
                job.status = "completed"
                job.pause_requested = False
                job.cancel_requested = False
                job.last_error = ""
                job.current_segment_id = ""
                job.current_chapter = ""

            await export_outputs(self.storage, job, segments, chunks)
            await self.storage.save_job(job)
            final_event = "job_completed" if job.status == "completed" else "job_failed"
            await self.broker.publish(job_id, final_event, job.model_dump())
            if job.status == "completed":
                print("[run] completed")
                print("[run-exit] reason=completed")
            else:
                print(f"[run] failed error={job.last_error}")
                print("[run-exit] reason=failed")
        except Exception as exc:
            job = await self.storage.load_job(job_id)
            job.status = "failed"
            job.last_error = str(exc) or AUTH_FAILURE_MESSAGE
            job.updated_at = utc_now_iso()
            await self.storage.save_job(job)
            await self.storage.append_log(
                job_id,
                LogEntry(event="job_failed", payload={"error": str(exc)}),
            )
            await self.broker.publish(job_id, "job_failed", job.model_dump())
        finally:
            self.tasks.pop(job_id, None)

    def _speed_config(self, job: JobRecord) -> dict[str, Any]:
        mode = self._normalized_speed_mode(job.config.speed_mode)
        if mode == "balanced":
            return {"concurrency": 2, "max_segments": max(1, job.config.max_segments_per_chunk)}
        if mode == "fast":
            return {"concurrency": 3, "max_segments": max(1, min(job.config.max_segments_per_chunk, 8))}
        return {"concurrency": 1, "max_segments": max(1, job.config.max_segments_per_chunk)}

    async def _run_job_concurrent(self, job_id: str) -> None:
        print(f"[run-concurrent] job_id={job_id} start")
        job = await self.storage.load_job(job_id)
        runner_mode = self._normalized_speed_mode(job.config.speed_mode)
        client = self._build_client(job)
        sc = self._speed_config(job)
        concurrency = sc["concurrency"]
        write_lock = asyncio.Lock()

        await self.broker.publish(
            job_id,
            "job_started",
            {"job_id": job_id, "status": "running", "completed_segments": job.completed_segments},
        )

        try:
            while True:
                latest_job = await self.storage.load_job(job_id)
                if self._normalized_speed_mode(latest_job.config.speed_mode) != runner_mode:
                    print(f"[mode-switch] {runner_mode} -> {self._normalized_speed_mode(latest_job.config.speed_mode)}")
                    return await self._run_job(job_id)
                segments = await self.storage.load_segments(job_id)
                chunks = await self.storage.load_chunks(job_id)

                if latest_job.cancel_requested or latest_job.status == "cancelled":
                    latest_job.status = "cancelled"
                    await self.storage.save_job(latest_job)
                    await self.broker.publish(job_id, "job_cancelled", latest_job.model_dump())
                    print("[run-concurrent-exit] reason=cancel_requested")
                    return

                if latest_job.pause_requested or latest_job.status == "pausing":
                    latest_job.status = "paused"
                    latest_job.pause_requested = False
                    latest_job.updated_at = utc_now_iso()
                    await self.storage.save_job(latest_job)
                    await export_outputs(self.storage, latest_job, segments, chunks)
                    await self.broker.publish(job_id, "job_paused", latest_job.model_dump())
                    print("[run-concurrent-exit] reason=pause_requested")
                    return

                unfinished = [s for s in segments if s.status not in ("success", "failed")]
                if not unfinished:
                    # No pending/running left; break to let final logic decide completed vs failed
                    break

                job = latest_job
                job.status = "running"
                job.pause_requested = False
                # Concurrent mode always non-stream
                job.config.stream = False

                # Build concurrent batch: multiple chunks
                batch_chunks: list[tuple[ChunkRecord, list[SegmentRecord], str]] = []
                assigned_ids: set[str] = set()
                # Exclude failed segments in the current run; resume will reset them to pending for retry
                active_segments = [s for s in segments if s.status != "failed"]
                for _ in range(concurrency):
                    next_seg = self._find_first_retryable_segment(
                        [s for s in active_segments if s.segment_id not in assigned_ids]
                    )
                    if not next_seg:
                        break
                    chunk, pending, reason = self._build_runtime_chunk_from_next(job, active_segments, next_seg)
                    if not pending:
                        break
                    self._log_chunk_build(job, chunk, pending, pending, reason)
                    for s in pending:
                        if s.status != "success":
                            s.status = "running"
                            s.error = ""
                        assigned_ids.add(s.segment_id)
                    batch_chunks.append((chunk, pending, reason))

                if not batch_chunks:
                    break

                # Save once: all batch segments are now running
                job.current_segment_id = batch_chunks[0][1][0].segment_id
                job.current_chapter = batch_chunks[0][0].chapter_title
                job.last_run_heartbeat_at = utc_now_iso()
                await self._recompute_job_progress(job, segments)
                await self.storage.save_segments(job_id, segments)
                for chunk, _, _ in batch_chunks:
                    chunk.status = "running"
                    chunk.started_at = utc_now_iso()
                    chunk.error = ""
                    await self.storage.save_chunk(job_id, chunk)
                await self.storage.save_job(job)

                print(f"[batch] start chunks={len(batch_chunks)} concurrency={concurrency}")

                for chunk, _, _ in batch_chunks:
                    await self.broker.publish(
                        job_id, "chunk_started",
                        {
                            "job_id": job_id,
                            "chunk_id": chunk.chunk_id,
                            "segment_ids": chunk.segment_ids,
                            "current_segment_id": chunk.segment_ids[0],
                            "chapter_title": chunk.chapter_title,
                        },
                    )

                # Concurrent API calls
                async def translate_one(chunk: ChunkRecord, pending: list[SegmentRecord]):
                    current_source = "\n\n".join(s.source_text for s in pending)
                    corpus_context = self._build_corpus_context(job, current_source)
                    try:
                        chunk.used_terms = ensure_list(corpus_context.get("terms", []))
                        chunk.used_examples = ensure_list(corpus_context.get("examples", []))
                    except Exception:
                        chunk.used_terms = []
                        chunk.used_examples = []
                    messages = build_messages(
                        job=job, chunk=chunk, source_segments=pending,
                        completed_chunks=[c for c in chunks if c.status == "completed"],
                        corpus_context=corpus_context,
                    )
                    last_error = ""
                    for attempt in range(1, job.config.max_retries + 1):
                        chunk.attempt_count = attempt
                        try:
                            response = await self._translate_chunk_with_api_logging(
                                client=client, job=job, chunk=chunk,
                                pending_segments=pending, messages=messages,
                                on_delta=lambda d: None,
                            )
                            parse_result = self._parse_translation_response(
                                response.text, [s.segment_id for s in pending],
                            )
                            if not parse_result.missing_segment_ids:
                                return chunk, pending, parse_result, response, None, {}
                            if attempt == job.config.max_retries:
                                missing_by_id = {s.segment_id: s for s in pending}
                                missing_segments = [
                                    missing_by_id[sid]
                                    for sid in parse_result.missing_segment_ids
                                    if sid in missing_by_id
                                ]
                                if missing_segments:
                                    fallback_translations, fallback_errors = await self._translate_missing_segments(
                                        client=client,
                                        job=job,
                                        missing_segments=missing_segments,
                                        completed_chunks=[c for c in chunks if c.status == "completed"],
                                    )
                                    parse_result.translations.update(fallback_translations)
                                    parse_result.missing_segment_ids = [
                                        sid for sid in parse_result.missing_segment_ids
                                        if not parse_result.translations.get(sid)
                                    ]
                                    return chunk, pending, parse_result, response, None, fallback_errors
                                return chunk, pending, parse_result, response, None, {}
                            if parse_result.missing_segment_ids:
                                raise ValueError(f"Missing: {', '.join(parse_result.missing_segment_ids)}")
                        except TranslationAuthError as exc:
                            return chunk, pending, None, None, ("auth", exc), {}
                        except TranslationTimeoutError:
                            return chunk, pending, None, None, ("timeout", None), {}
                        except Exception as exc:
                            last_error = str(exc)
                            chunk.error = last_error
                    return chunk, pending, None, None, ("error", last_error or "failed"), {}

                tasks = [
                    asyncio.create_task(translate_one(chunk, pending))
                    for chunk, pending, _ in batch_chunks
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # Serial write: lock + aggregate + save
                async with write_lock:
                    fresh_segments = await self.storage.load_segments(job_id)
                    fresh_chunks = await self.storage.load_chunks(job_id)
                    fresh_job = await self.storage.load_job(job_id)
                    seg_map = {s.segment_id: s for s in fresh_segments}
                    chunk_map = {c.chunk_id: c for c in fresh_chunks}

                    auth_failure = False
                    timeout_occurred = False
                    fatal_error = False

                    for idx, result in enumerate(results):
                        if isinstance(result, BaseException):
                            print(f"[batch] chunk exception idx={idx} error={result}")
                            fatal_error = True
                            continue
                        chunk, pending, parse_result, response, error_tuple, fallback_errors = result

                        if error_tuple:
                            err_type, err_val = error_tuple
                            if err_type == "auth":
                                chunk.status = "failed"
                                chunk.error = AUTH_FAILURE_MESSAGE
                                chunk.completed_at = utc_now_iso()
                                for s in pending:
                                    if s.segment_id in seg_map and seg_map[s.segment_id].status != "success":
                                        seg_map[s.segment_id].status = "failed"
                                        seg_map[s.segment_id].error = AUTH_FAILURE_MESSAGE
                                auth_failure = True
                            elif err_type == "timeout":
                                for s in pending:
                                    if s.segment_id in seg_map and seg_map[s.segment_id].status == "running":
                                        seg_map[s.segment_id].status = "pending"
                                        seg_map[s.segment_id].error = ""
                                chunk.status = "pending"
                                chunk.error = "超时，可重试"
                                timeout_occurred = True
                            else:
                                chunk.status = "failed"
                                chunk.error = str(err_val)[:200]
                                chunk.completed_at = utc_now_iso()
                                for s in pending:
                                    if s.segment_id in seg_map and seg_map[s.segment_id].status != "success":
                                        seg_map[s.segment_id].status = "failed"
                                        seg_map[s.segment_id].error = chunk.error
                            old = chunk_map.get(chunk.chunk_id)
                            if old:
                                old.status = chunk.status
                                old.error = chunk.error
                                old.completed_at = chunk.completed_at
                                old.attempt_count = chunk.attempt_count
                                old.response_text = chunk.response_text
                                old.stream_fallback_used = chunk.stream_fallback_used
                                old.used_terms = chunk.used_terms
                                old.used_examples = chunk.used_examples
                                await self.storage.save_chunk(job_id, old)
                            continue

                        # Success case
                        if parse_result and not parse_result.missing_segment_ids and response:
                            chunk.status = "completed"
                            chunk.summary = parse_result.summary
                            chunk.terms = parse_result.terms
                            chunk.response_text = response.text
                            chunk.stream_fallback_used = response.stream_fallback_used
                            chunk.completed_at = utc_now_iso()
                            chunk.error = response.raw_error if response.stream_fallback_used else ""

                            for s in pending:
                                sid = s.segment_id
                                if sid in seg_map:
                                    seg_map[sid].translated_text = parse_result.translations.get(sid, "")
                                    seg_map[sid].summary = parse_result.summary
                                    seg_map[sid].terms = parse_result.terms
                                    seg_map[sid].status = "success"
                                    seg_map[sid].error = ""

                            if parse_result.missing_segment_ids:
                                chunk.status = "failed"
                                chunk.error = "部分段落翻译缺失"
                                preview = self._model_output_preview(response.text if response else "")
                                if preview:
                                    chunk.error = f"部分段落翻译缺失。模型返回预览：{preview}"
                                for s in pending:
                                    if s.segment_id in seg_map and s.segment_id in parse_result.missing_segment_ids:
                                        seg_map[s.segment_id].status = "failed"
                                        seg_map[s.segment_id].error = chunk.error
                        else:
                            chunk.status = "failed"
                            chunk.error = "无法解析模型输出"
                            preview = self._model_output_preview(response.text if response else "")
                            if preview:
                                chunk.error = f"无法解析模型输出。模型返回预览：{preview}"
                            chunk.completed_at = utc_now_iso()
                            for s in pending:
                                if s.segment_id in seg_map and seg_map[s.segment_id].status != "success":
                                    seg_map[s.segment_id].status = "failed"
                                    seg_map[s.segment_id].error = chunk.error

                        if chunk.status == "failed":
                            if response and response.text:
                                chunk.response_text = response.text
                            if not chunk.completed_at:
                                chunk.completed_at = utc_now_iso()
                            default_message = (
                                "部分段落翻译缺失"
                                if parse_result and parse_result.missing_segment_ids
                                else "无法解析模型输出"
                            )
                            chunk.error = self._build_segment_failure_message(
                                response.text if response else "",
                                default_message=default_message,
                            )
                            for s in pending:
                                if s.segment_id in seg_map and seg_map[s.segment_id].status == "failed":
                                    seg_map[s.segment_id].error = fallback_errors.get(s.segment_id, chunk.error)

                        old = chunk_map.get(chunk.chunk_id)
                        if old:
                            old.status = chunk.status
                            old.summary = chunk.summary
                            old.terms = chunk.terms
                            old.response_text = chunk.response_text
                            old.stream_fallback_used = chunk.stream_fallback_used
                            old.completed_at = chunk.completed_at
                            old.error = chunk.error
                            old.attempt_count = chunk.attempt_count
                            old.used_terms = chunk.used_terms
                            old.used_examples = chunk.used_examples
                            await self.storage.save_chunk(job_id, old)

                    # Signal completed segments
                    new_segments = list(seg_map.values())
                    for s in new_segments:
                        if s.status == "success" and s.translated_text:
                            await self.broker.publish(job_id, "segment_completed", s.model_dump())
                    for chunk, _, _ in batch_chunks:
                        updated = chunk_map.get(chunk.chunk_id)
                        if updated and updated.status == "completed":
                            await self.broker.publish(job_id, "chunk_completed", {
                                "job_id": job_id, "chunk_id": chunk.chunk_id,
                                "summary": updated.summary, "segment_ids": chunk.segment_ids,
                            })
                        elif updated and updated.status in ("failed", "pending"):
                            await self.broker.publish(job_id, "chunk_failed", {
                                "job_id": job_id, "chunk_id": chunk.chunk_id,
                                "error": updated.error or "", "segment_ids": chunk.segment_ids,
                            })

                    # Save final state
                    await self.storage.save_segments(job_id, new_segments)
                    segments = new_segments

                    if auth_failure:
                        fresh_job.status = "failed"
                        fresh_job.last_error = AUTH_FAILURE_MESSAGE
                        await self._recompute_job_progress(fresh_job, new_segments)
                        await self.storage.save_job(fresh_job)
                        await export_outputs(self.storage, fresh_job, new_segments, fresh_chunks)
                        await self.broker.publish(job_id, "job_failed", fresh_job.model_dump())
                        print("[run-concurrent-exit] reason=auth_failure")
                        return

                    if timeout_occurred:
                        fresh_job.status = "paused"
                        fresh_job.pause_requested = False
                        fresh_job.last_error = "并发翻译超时，已暂停，可切换稳定模式继续"
                        await self._recompute_job_progress(fresh_job, new_segments)
                        await self.storage.save_job(fresh_job)
                        await export_outputs(self.storage, fresh_job, new_segments, fresh_chunks)
                        await self.broker.publish(job_id, "job_paused", fresh_job.model_dump())
                        print("[run-concurrent-exit] reason=timeout_paused")
                        return

                    if fatal_error:
                        fresh_job.status = "paused"
                        fresh_job.pause_requested = False
                        fresh_job.last_error = "并发翻译异常，已暂停，可切换稳定模式继续"
                        await self._recompute_job_progress(fresh_job, new_segments)
                        await self.storage.save_job(fresh_job)
                        await export_outputs(self.storage, fresh_job, new_segments, fresh_chunks)
                        await self.broker.publish(job_id, "job_paused", fresh_job.model_dump())
                        print("[run-concurrent-exit] reason=fatal_error_paused")
                        return

                    await self._recompute_job_progress(fresh_job, new_segments)
                    next_seg = self._find_first_retryable_segment(new_segments)
                    if next_seg:
                        fresh_job.current_segment_id = next_seg.segment_id
                        fresh_job.current_chapter = next_seg.chapter_title
                    fresh_job.last_run_heartbeat_at = utc_now_iso()
                    await self.storage.save_job(fresh_job)
                    print(f"[batch] save progress completed={fresh_job.completed_segments}")
                    await self._emit_job_snapshot(fresh_job)

                # Check pause/cancel after batch
                latest_job = await self.storage.load_job(job_id)
                if latest_job.cancel_requested or latest_job.status == "cancelled":
                    latest_job.status = "cancelled"
                    await self.storage.save_job(latest_job)
                    await self.broker.publish(job_id, "job_cancelled", latest_job.model_dump())
                    print("[run-concurrent-exit] reason=cancel_requested")
                    break
                if latest_job.pause_requested or latest_job.status == "pausing":
                    latest_job.status = "paused"
                    latest_job.pause_requested = False
                    latest_job.updated_at = utc_now_iso()
                    await self.storage.save_job(latest_job)
                    await export_outputs(self.storage, latest_job, segments, chunks)
                    await self.broker.publish(job_id, "job_paused", latest_job.model_dump())
                    print("[run-concurrent-exit] reason=pause_requested")
                    return

            # Final completion
            job = await self.storage.load_job(job_id)
            await self._recompute_job_progress(job, segments)
            if job.status in ("cancelled", "paused"):
                return
            if any(s.status == "failed" for s in segments):
                job.status = "failed"
            else:
                job.status = "completed"
                job.pause_requested = False
                job.cancel_requested = False
                job.last_error = ""
                job.current_segment_id = ""
                job.current_chapter = ""
            await export_outputs(self.storage, job, segments, chunks)
            await self.storage.save_job(job)
            final_event = "job_completed" if job.status == "completed" else "job_failed"
            await self.broker.publish(job_id, final_event, job.model_dump())
            print(f"[run-concurrent] {'completed' if job.status == 'completed' else 'failed'}")
        except Exception as exc:
            try:
                job = await self.storage.load_job(job_id)
                segments = await self.storage.load_segments(job_id)
                chunks = await self.storage.load_chunks(job_id)
                job.status = "paused"
                job.pause_requested = False
                job.last_error = "并发翻译异常，已暂停，可切换稳定模式继续"
                await self._recompute_job_progress(job, segments)
                await self.storage.save_segments(job_id, segments)
                for c in chunks:
                    await self.storage.save_chunk(job_id, c)
                await self.storage.save_job(job)
                await export_outputs(self.storage, job, segments, chunks)
                await self.broker.publish(job_id, "job_paused", job.model_dump())
                print(f"[batch] fallback_to_serial reason={exc}")
            except Exception:
                pass
            raise
