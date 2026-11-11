from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Annotated

from fastapi import FastAPI, File, Form, Header, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, StringConstraints
from sse_starlette.sse import EventSourceResponse

from backend.config import reload_config, save_api_settings
from backend.translator.corpus_manager import CorpusManager
from backend.translator.exporter import build_output_filenames, export_outputs
from backend.translator.pipeline import (
    EmptyDocumentError,
    EventBroker,
    IdempotencyConflictError,
    JobAlreadyRunningError,
    TranslationPipeline,
)
from backend.translator.storage import FileStorage, StorageIntegrityError


try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass


app = FastAPI(title="Text Book Translator")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://127.0.0.1:5173", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

storage = FileStorage(Path(__file__).resolve().parent / "data")
broker = EventBroker()
pipeline = TranslationPipeline(storage=storage, broker=broker)
corpus_manager = CorpusManager(storage.base_dir / "corpus")


class CorpusCreatePayload(BaseModel):
    name: str
    description: str = ""


class DomainPromptPayload(BaseModel):
    domain_prompt: str


RequiredCorpusText = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class GlossaryPayload(BaseModel):
    source: RequiredCorpusText
    target: RequiredCorpusText
    note: str = ""
    enabled: bool = True


class ExamplePayload(BaseModel):
    source: RequiredCorpusText
    target: RequiredCorpusText
    note: str = ""
    enabled: bool = True


class CorpusImportJsonPayload(BaseModel):
    mode: str
    target_corpus_id: str = "default"
    data: dict


class SpeedModePayload(BaseModel):
    speed_mode: str = "stable"


class PausePayload(BaseModel):
    reason: str = "unknown"


class ApiSettingsPayload(BaseModel):
    base_url: str
    api_key: str | None = None
    model: RequiredCorpusText


def _public_api_settings(config) -> dict[str, str | bool]:
    return {
        "base_url": config.tokenhub_base_url,
        "model": config.model,
        "has_api_key": bool(config.tokenhub_api_key.strip()),
    }


def _error_detail(code: str, message: str, retryable: bool = False) -> dict[str, str | bool]:
    return {"code": code, "message": message, "retryable": retryable}


@app.exception_handler(StorageIntegrityError)
async def handle_storage_integrity_error(_request: Request, exc: StorageIntegrityError):
    return JSONResponse(
        status_code=409,
        content={"detail": _error_detail("JOB_DATA_CORRUPT", str(exc), retryable=False)},
    )


def _job_not_found(exc: FileNotFoundError) -> HTTPException:
    return HTTPException(
        status_code=404,
        detail=_error_detail("JOB_NOT_FOUND", str(exc)),
    )


async def _result_payload(job) -> tuple[bool, list[dict]]:
    if job.status not in {"completed", "failed", "paused", "cancelled"}:
        return False, []
    manifest = await storage.load_result_manifest(job.job_id)
    if not manifest or manifest.get("job_id") != job.job_id or manifest.get("status") != job.status:
        return False, []
    if manifest.get("completed_segments") != job.completed_segments or manifest.get("total_segments") != job.total_segments:
        return False, []

    expected = build_output_filenames(job.file_name)
    outputs = []
    for item in manifest.get("outputs", []):
        output_type = item.get("type")
        if output_type not in expected or item.get("filename") != expected[output_type]:
            return False, []
        if not (storage.job_outputs_dir(job.job_id) / expected[output_type]).is_file():
            return False, []
        outputs.append(item)
    return bool(outputs), outputs


@app.on_event("startup")
async def on_startup() -> None:
    await storage.ensure_base_dirs()
    corpus_manager.ensure_default_corpus()
    config = reload_config()
    print(
        "Config loaded:\n"
        f"- has_api_key: {bool(config.tokenhub_api_key.strip())}\n"
        f"- base_url: {config.tokenhub_base_url}\n"
        f"- model: {config.model}"
    )


@app.get("/api/health")
async def health():
    config = reload_config()
    return {
        "ok": True,
        "service": "text-book-translator",
        "api_version": "1",
        "configured": bool(config.tokenhub_api_key.strip()),
        "capabilities": {
            "file_types": [".txt", ".md"],
            "output_types": ["translated.txt", "translated.md", "bilingual.txt", "bilingual.md"],
            "control_actions": ["start", "status", "pause", "resume", "cancel"],
            "idempotent_upload": True,
            "single_worker_required": True,
        },
    }


@app.get("/api/config/check")
async def check_config():
    return _public_api_settings(reload_config())


@app.get("/api/settings")
async def get_api_settings():
    return _public_api_settings(reload_config())


@app.put("/api/settings")
async def update_api_settings(payload: ApiSettingsPayload):
    try:
        config = save_api_settings(payload.base_url, payload.api_key, payload.model)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _public_api_settings(config)


@app.post("/api/jobs/upload")
async def upload_job(
    file: UploadFile = File(...),
    target_language: str = Form("简体中文"),
    translation_mode: str = Form("忠实翻译"),
    stream: bool = Form(True),
    chunk_size_chars: int = Form(3500),
    corpus_id: str = Form("default"),
    use_corpus: bool = Form(True),
    use_glossary: bool = Form(True),
    use_style_examples: bool = Form(True),
    use_domain_prompt: bool = Form(True),
    translate_mode: str = Form("faithful"),
    translation_level: int = Form(3),
    speed_mode: str = Form("stable"),
    client_request_id: str = Form(""),
):
    suffix = Path(file.filename or "").suffix.lower()
    if suffix not in {".txt", ".md"}:
        raise HTTPException(status_code=400, detail="Only .txt and .md files are supported.")

    client_request_id = client_request_id.strip()
    if client_request_id and (
        len(client_request_id) > 128
        or not client_request_id.isascii()
        or any(not (char.isalnum() or char in "-_.:") for char in client_request_id)
    ):
        raise HTTPException(
            status_code=400,
            detail=_error_detail("INVALID_CLIENT_REQUEST_ID", "client_request_id 只能包含 ASCII 字母、数字、-_.:，且不超过 128 个字符"),
        )

    content = await file.read()
    try:
        job, reused = await pipeline.create_job_from_upload(
            file_name=file.filename or "book.txt",
            content=content,
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
        )
    except IdempotencyConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail=_error_detail("IDEMPOTENCY_CONFLICT", str(exc)),
        ) from exc
    except EmptyDocumentError as exc:
        raise HTTPException(
            status_code=400,
            detail=_error_detail("EMPTY_DOCUMENT", str(exc)),
        ) from exc
    return {**job.model_dump(), "reused": reused}


@app.get("/api/corpus")
async def list_corpora():
    return corpus_manager.list_corpora()


@app.get("/api/corpus/{corpus_id}")
async def get_corpus(corpus_id: str):
    try:
        return corpus_manager.get_corpus(corpus_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Corpus not found") from exc


@app.post("/api/corpus")
async def create_corpus(payload: CorpusCreatePayload):
    return corpus_manager.create_corpus(payload.name, payload.description)


@app.put("/api/corpus/{corpus_id}")
async def save_corpus(corpus_id: str, payload: dict):
    try:
        return corpus_manager.save_corpus(corpus_id, payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.delete("/api/corpus/{corpus_id}")
async def delete_corpus(corpus_id: str):
    try:
        corpus_manager.delete_corpus(corpus_id)
        return {"ok": True}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.put("/api/corpus/{corpus_id}/domain-prompt")
async def update_domain_prompt(corpus_id: str, payload: DomainPromptPayload):
    return corpus_manager.update_domain_prompt(corpus_id, payload.domain_prompt)


@app.post("/api/corpus/{corpus_id}/glossary")
async def add_glossary(corpus_id: str, payload: GlossaryPayload):
    return corpus_manager.add_glossary_term(corpus_id, payload.source, payload.target, payload.note, payload.enabled)


@app.put("/api/corpus/{corpus_id}/glossary/{term_id}")
async def update_glossary(corpus_id: str, term_id: str, payload: GlossaryPayload):
    return corpus_manager.update_glossary_term(corpus_id, term_id, payload.source, payload.target, payload.note, payload.enabled)


@app.delete("/api/corpus/{corpus_id}/glossary/{term_id}")
async def delete_glossary(corpus_id: str, term_id: str):
    corpus_manager.delete_glossary_term(corpus_id, term_id)
    return {"ok": True}


@app.post("/api/corpus/{corpus_id}/examples")
async def add_example(corpus_id: str, payload: ExamplePayload):
    return corpus_manager.add_style_example(corpus_id, payload.source, payload.target, payload.note, payload.enabled)


@app.put("/api/corpus/{corpus_id}/examples/{example_id}")
async def update_example(corpus_id: str, example_id: str, payload: ExamplePayload):
    return corpus_manager.update_style_example(corpus_id, example_id, payload.source, payload.target, payload.note, payload.enabled)


@app.delete("/api/corpus/{corpus_id}/examples/{example_id}")
async def delete_example(corpus_id: str, example_id: str):
    corpus_manager.delete_style_example(corpus_id, example_id)
    return {"ok": True}


@app.post("/api/corpus/{corpus_id}/import")
async def import_corpus(corpus_id: str, file: UploadFile = File(...)):
    try:
        data = json.loads((await file.read()).decode("utf-8-sig"))
        return corpus_manager.save_corpus(corpus_id, data)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/corpus/import-json")
async def import_corpus_json(payload: CorpusImportJsonPayload):
    try:
        return corpus_manager.import_corpus_json(payload.mode, payload.data, payload.target_corpus_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/corpus/{corpus_id}/export")
async def export_corpus(corpus_id: str):
    path = corpus_manager.base_dir / corpus_id / "corpus.json"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Corpus not found")
    return FileResponse(path, filename="corpus.json", media_type="application/json")


@app.post("/api/jobs/{job_id}/start")
async def start_job(job_id: str):
    try:
        await pipeline.get_job(job_id)
    except FileNotFoundError as exc:
        raise _job_not_found(exc) from exc
    config = reload_config()
    if not config.tokenhub_api_key.strip():
        raise HTTPException(status_code=400, detail="API_NOT_CONFIGURED: 请先配置模型接口的 API Key")
    try:
        job = await pipeline.start_job(job_id)
    except FileNotFoundError as exc:
        raise _job_not_found(exc) from exc
    return {"ok": True, **job.model_dump()}


@app.post("/api/jobs/{job_id}/pause")
async def pause_job(
    job_id: str,
    payload: PausePayload | None = None,
    x_manual_user_action: str | None = Header(default=None),
):
    try:
        current_job = await pipeline.get_job(job_id)
    except FileNotFoundError as exc:
        raise _job_not_found(exc) from exc
    if current_job.status == "completed":
        raise HTTPException(status_code=400, detail="任务已完成，不能暂停")
    reason = payload.reason if payload else "unknown"
    print(f"[pause] job_id={job_id} reason={reason or 'unknown'}")
    is_manual_click = reason == "user_click_pause" and x_manual_user_action == "true"
    if reason != "agent_request" and not is_manual_click:
        raise HTTPException(status_code=400, detail="暂停请求来源无效")
    try:
        job = await pipeline.request_pause(job_id)
    except FileNotFoundError as exc:
        raise _job_not_found(exc) from exc
    return {"ok": True, **job.model_dump()}


@app.post("/api/jobs/{job_id}/resume")
async def resume_job(job_id: str):
    try:
        current_job = await pipeline.get_job(job_id)
    except FileNotFoundError as exc:
        raise _job_not_found(exc) from exc
    if current_job.status not in {"completed", "cancelled"} and not reload_config().tokenhub_api_key.strip():
        raise HTTPException(status_code=400, detail="API_NOT_CONFIGURED: 请先配置模型接口的 API Key")
    try:
        job = await pipeline.resume_job(job_id)
    except FileNotFoundError as exc:
        raise _job_not_found(exc) from exc
    if job.status == "completed":
        return {"ok": True, **job.model_dump(), "message": "任务已完成，无需继续"}
    if job.status == "cancelled":
        return {"ok": False, **job.model_dump(), "message": "任务已停止，不能继续。"}
    return {"ok": True, "job_id": job.job_id, "status": job.status, **job.model_dump()}


@app.post("/api/jobs/{job_id}/repair-stuck")
async def repair_stuck_job(job_id: str):
    try:
        await pipeline.get_job(job_id)
    except FileNotFoundError as exc:
        raise _job_not_found(exc) from exc
    if pipeline.has_active_task(job_id):
        raise HTTPException(
            status_code=409,
            detail=_error_detail("JOB_ALREADY_RUNNING", "任务仍在运行，不能执行卡住修复", retryable=True),
        )
    try:
        job = await pipeline.repair_stuck_job(job_id)
    except FileNotFoundError as exc:
        raise _job_not_found(exc) from exc
    except JobAlreadyRunningError as exc:
        raise HTTPException(
            status_code=409,
            detail=_error_detail("JOB_ALREADY_RUNNING", str(exc), retryable=True),
        ) from exc
    return {"ok": True, **job.model_dump()}


@app.post("/api/jobs/{job_id}/cancel")
async def cancel_job(job_id: str):
    try:
        job = await pipeline.request_cancel(job_id)
    except FileNotFoundError as exc:
        raise _job_not_found(exc) from exc
    return {"ok": True, **job.model_dump()}


@app.post("/api/jobs/{job_id}/speed-mode")
async def update_speed_mode(job_id: str, payload: SpeedModePayload):
    valid = {"stable", "balanced", "fast"}
    if payload.speed_mode not in valid:
        raise HTTPException(status_code=400, detail=f"speed_mode must be one of {valid}")
    job = await pipeline.set_speed_mode(job_id, payload.speed_mode)
    return {"ok": True, **job.model_dump()}


@app.get("/api/jobs/{job_id}")
async def get_job(job_id: str):
    try:
        job = await pipeline.get_job(job_id)
        payload = job.model_dump()
        payload.update(
            {
                "corpus_id": job.config.corpus_id,
                "translate_mode": job.config.translate_mode,
                "translation_level": job.config.translation_level,
                "use_corpus": job.config.use_corpus,
                "use_glossary": job.config.use_glossary,
                "use_style_examples": job.config.use_style_examples,
                "use_domain_prompt": job.config.use_domain_prompt,
                "speed_mode": job.config.speed_mode,
                "runner_active": pipeline.has_active_task(job_id),
            }
        )
        payload["result_ready"], payload["outputs"] = await _result_payload(job)
        return payload
    except FileNotFoundError as exc:
        raise _job_not_found(exc) from exc


@app.get("/api/jobs/{job_id}/segments")
async def get_segments(job_id: str):
    try:
        return [segment.model_dump() for segment in await pipeline.get_segments(job_id)]
    except Exception:
        return []


@app.get("/api/jobs/{job_id}/events")
async def get_events(job_id: str):
    queue = broker.subscribe(job_id)

    async def event_generator():
        yield {
            "event": "connected",
            "data": json.dumps({"job_id": job_id, "connected": True}, ensure_ascii=False),
        }
        try:
            while True:
                payload = await queue.get()
                yield {
                    "event": payload["event"],
                    "data": json.dumps(payload["data"], ensure_ascii=False),
                }
        finally:
            broker.unsubscribe(job_id, queue)

    return EventSourceResponse(event_generator())


@app.get("/api/jobs/{job_id}/download/{file_type}")
async def download_file(job_id: str, file_type: str):
    if file_type not in {"translated.txt", "translated.md", "bilingual.txt", "bilingual.md"}:
        raise HTTPException(status_code=404, detail="Unknown file type")

    try:
        job = await pipeline.get_job(job_id)
    except FileNotFoundError as exc:
        raise _job_not_found(exc) from exc
    result_ready, outputs = await _result_payload(job)
    if not result_ready and job.status in {"completed", "failed", "paused", "cancelled"}:
        segments = await pipeline.get_segments(job_id)
        chunks = await storage.load_chunks(job_id)
        await export_outputs(storage, job, segments, chunks)
        result_ready, outputs = await _result_payload(job)
    if not result_ready or file_type not in {item.get("type") for item in outputs}:
        raise HTTPException(status_code=404, detail="File not ready")
    output_filenames = build_output_filenames(job.file_name)
    path = storage.job_outputs_dir(job_id) / output_filenames[file_type]
    if not path.exists():
        raise HTTPException(status_code=404, detail="File not ready")
    return FileResponse(path, filename=output_filenames[file_type], media_type="application/octet-stream")
