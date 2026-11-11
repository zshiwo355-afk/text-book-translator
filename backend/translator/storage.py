from __future__ import annotations

import asyncio
import json
import logging
import shutil
import os
import time
import uuid
from pathlib import Path
from typing import Any

import aiofiles
from pydantic import ValidationError

from backend.translator.validator import ChunkRecord, JobRecord, LogEntry, SegmentRecord, ensure_list


logger = logging.getLogger(__name__)


class StorageIntegrityError(ValueError):
    pass


class FileStorage:
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.uploads_dir = base_dir / "uploads"
        self.jobs_dir = base_dir / "jobs"
        self.outputs_dir = base_dir / "outputs"
        self.json_cache: dict[str, Any] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._export_locks: dict[str, asyncio.Lock] = {}

    async def ensure_base_dirs(self) -> None:
        for path in (self.uploads_dir, self.jobs_dir, self.outputs_dir):
            path.mkdir(parents=True, exist_ok=True)

    def job_dir(self, job_id: str) -> Path:
        return self.jobs_dir / job_id

    def chunks_dir(self, job_id: str) -> Path:
        return self.job_dir(job_id) / "chunks"

    def job_outputs_dir(self, job_id: str) -> Path:
        return self.job_dir(job_id) / "outputs"

    def job_json_path(self, job_id: str) -> Path:
        return self.job_dir(job_id) / "job.json"

    def segments_json_path(self, job_id: str) -> Path:
        return self.job_dir(job_id) / "segments.json"

    def chunk_json_path(self, job_id: str, chunk_id: str) -> Path:
        return self.chunks_dir(job_id) / f"{chunk_id}.json"

    def translation_log_path(self, job_id: str) -> Path:
        return self.job_outputs_dir(job_id) / "translation_log.json"

    def result_manifest_path(self, job_id: str) -> Path:
        return self.job_outputs_dir(job_id) / "result.json"

    def _get_lock(self, job_id: str) -> asyncio.Lock:
        if job_id not in self._locks:
            self._locks[job_id] = asyncio.Lock()
        return self._locks[job_id]

    def get_export_lock(self, job_id: str) -> asyncio.Lock:
        if job_id not in self._export_locks:
            self._export_locks[job_id] = asyncio.Lock()
        return self._export_locks[job_id]

    def _cache_key(self, path: Path) -> str:
        return str(path.resolve())

    def _replace_with_retry_sync(self, tmp_path: Path, target_path: Path, retries: int = 10) -> None:
        for i in range(retries):
            try:
                os.replace(tmp_path, target_path)
                return
            except PermissionError:
                time.sleep(0.1 * (i + 1))
        os.replace(tmp_path, target_path)

    async def _replace_with_retry(self, tmp_path: Path, target_path: Path, retries: int = 10) -> None:
        for i in range(retries):
            try:
                os.replace(tmp_path, target_path)
                return
            except PermissionError:
                await asyncio.sleep(0.1 * (i + 1))
        os.replace(tmp_path, target_path)

    def _atomic_write_json_sync(self, path: Path, payload: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
        backup_path = path.with_suffix(path.suffix + ".bak")
        data = json.dumps(payload, ensure_ascii=False, indent=2)
        try:
            with tmp_path.open("w", encoding="utf-8", newline="\n") as file:
                file.write(data)
                file.flush()
                os.fsync(file.fileno())
            if path.exists():
                try:
                    shutil.copy2(path, backup_path)
                except Exception as exc:
                    logger.warning("JSON backup write failed for %s: %s", path, exc)
            self._replace_with_retry_sync(tmp_path, path)
        except Exception:
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception as cleanup_exc:
                logger.warning("JSON tmp cleanup failed for %s: %s", tmp_path, cleanup_exc)
            raise

    async def _write_json(self, path: Path, payload: Any) -> None:
        await asyncio.to_thread(self._atomic_write_json_sync, path, payload)
        self.json_cache[self._cache_key(path)] = payload

    async def _read_json(self, path: Path, default: Any = None) -> Any:
        cache_key = self._cache_key(path)
        backup_path = path.with_suffix(path.suffix + ".bak")

        for target in (path, backup_path):
            try:
                async with aiofiles.open(target, "r", encoding="utf-8") as file:
                    payload = json.loads(await file.read())
                self.json_cache[cache_key] = payload
                return payload
            except Exception as exc:
                logger.warning("JSON read failed for %s: %s", target, exc)
        return default

    async def save_uploaded_file(self, file_name: str, content: bytes, job_id: str) -> Path:
        await self.ensure_base_dirs()
        suffix = Path(file_name).suffix
        safe_name = Path(file_name).name
        target_path = self.uploads_dir / f"{job_id}{suffix}"
        async with aiofiles.open(target_path, "wb") as file:
            await file.write(content)
        name_marker = target_path.with_suffix(f"{suffix}.name.txt")
        async with aiofiles.open(name_marker, "w", encoding="utf-8", newline="\n") as file:
            await file.write(safe_name)
        return target_path

    async def create_job(self, job: JobRecord, segments: list[SegmentRecord], chunks: list[ChunkRecord]) -> None:
        await self.ensure_base_dirs()
        self.job_dir(job.job_id).mkdir(parents=True, exist_ok=True)
        self.chunks_dir(job.job_id).mkdir(parents=True, exist_ok=True)
        self.job_outputs_dir(job.job_id).mkdir(parents=True, exist_ok=True)
        await self.save_segments(job.job_id, segments)
        for chunk in chunks:
            await self.save_chunk(job.job_id, chunk)
        await self._write_json(self.translation_log_path(job.job_id), [])
        # job.json is the ready marker: readers never see a half-created task.
        await self.save_job(job)

    async def find_job_by_client_request_id(self, client_request_id: str) -> JobRecord | None:
        if not client_request_id or not self.jobs_dir.exists():
            return None
        # ponytail: a directory scan is enough for the local single-user store;
        # replace it with an index only if task volume makes this measurable.
        for job_dir in self.jobs_dir.iterdir():
            if not job_dir.is_dir():
                continue
            try:
                job = await self.load_job(job_dir.name)
            except Exception:
                continue
            if job.client_request_id == client_request_id:
                return job
        return None

    async def save_result_manifest(self, job_id: str, payload: dict[str, Any]) -> None:
        await self._write_json(self.result_manifest_path(job_id), payload)

    async def load_result_manifest(self, job_id: str) -> dict[str, Any] | None:
        payload = await self._read_json(self.result_manifest_path(job_id))
        return payload if isinstance(payload, dict) else None

    async def clear_result_manifest(self, job_id: str) -> None:
        path = self.result_manifest_path(job_id)
        self.json_cache.pop(self._cache_key(path), None)
        for target in (path, path.with_suffix(path.suffix + ".bak")):
            target.unlink(missing_ok=True)

    async def save_job(self, job: JobRecord) -> None:
        async with self._get_lock(job.job_id):
            await self._write_json(self.job_json_path(job.job_id), job.model_dump())

    async def load_job(self, job_id: str) -> JobRecord:
        payload = await self._read_json(self.job_json_path(job_id))
        if payload is None:
            raise FileNotFoundError(f"job.json not available for job_id={job_id}")
        return JobRecord(**payload)

    async def save_segments(self, job_id: str, segments: list[SegmentRecord]) -> None:
        async with self._get_lock(job_id):
            await self._write_json(self.segments_json_path(job_id), [segment.model_dump() for segment in segments])

    async def load_segments(self, job_id: str) -> list[SegmentRecord]:
        data = await self._read_json(self.segments_json_path(job_id))
        error_message = f"任务 {job_id} 的段落进度文件缺失、损坏或不完整，无法安全继续；请恢复 segments.json 或其备份。"
        if not isinstance(data, list) or not data:
            raise StorageIntegrityError(error_message)
        try:
            segments = [SegmentRecord.model_validate(item) for item in data]
        except ValidationError as exc:
            raise StorageIntegrityError(error_message) from exc
        job = await self.load_job(job_id)
        if len(segments) != job.total_segments:
            raise StorageIntegrityError(error_message)
        return segments

    async def save_chunk(self, job_id: str, chunk: ChunkRecord) -> None:
        async with self._get_lock(job_id):
            await self._write_json(self.chunk_json_path(job_id, chunk.chunk_id), chunk.model_dump())

    async def load_chunks(self, job_id: str) -> list[ChunkRecord]:
        chunks: list[ChunkRecord] = []
        for path in sorted(self.chunks_dir(job_id).glob("chunk_*.json")):
            payload = await self._read_json(path)
            if payload is None:
                continue
            chunks.append(ChunkRecord(**payload))
        return chunks

    async def append_log(self, job_id: str, entry: LogEntry) -> None:
        async with self._get_lock(job_id):
            path = self.translation_log_path(job_id)
            entries = []
            if path.exists():
                entries = await self._read_json(path, default=[])
            if isinstance(entries, dict):
                entries = ensure_list(entries.get("events", []))
            else:
                entries = ensure_list(entries)
            entries.append(entry.model_dump())
            await self._write_json(path, entries)
