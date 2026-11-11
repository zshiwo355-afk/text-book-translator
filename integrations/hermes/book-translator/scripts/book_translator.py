#!/usr/bin/env python3
"""Machine-readable client for the local Text Book Translator API."""

from __future__ import annotations

import argparse
import errno
import hashlib
import json
import os
import shutil
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Any


DEFAULT_API_URL = "http://127.0.0.1:8000"
DEFAULT_REQUEST_TIMEOUT = 30.0
TERMINAL_STATUSES = {"completed", "failed", "paused", "cancelled"}
OUTPUT_TYPES = (
    "translated.txt",
    "translated.md",
    "bilingual.txt",
    "bilingual.md",
)
USER_AGENT = "hermes-book-translator/1.0"


class ClientError(Exception):
    """A safe error that can be returned as machine-readable JSON."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        http_status: int | None = None,
        details: Any = None,
        exit_code: int = 2,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status
        self.details = details
        self.exit_code = exit_code

    def payload(self) -> dict[str, Any]:
        error: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.http_status is not None:
            error["http_status"] = self.http_status
        if self.details is not None:
            error["details"] = self.details
        return {"ok": False, "error": error}


class JsonArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise ClientError("INVALID_ARGUMENT", message)


def _emit(payload: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def _diagnose(message: str) -> None:
    sys.stderr.write(f"book-translator: {message}\n")
    sys.stderr.flush()


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than 0")
    return parsed


def _nonnegative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be 0 or greater")
    return parsed


def _state_path() -> Path:
    state_dir = os.environ.get("BOOK_TRANSLATOR_STATE_DIR")
    if state_dir:
        return Path(state_dir).expanduser() / "jobs.json"
    hermes_home = Path(os.environ.get("HERMES_HOME", str(Path.home() / ".hermes"))).expanduser()
    return hermes_home / "state" / "book-translator" / "jobs.json"


def _empty_state() -> dict[str, Any]:
    return {"version": 1, "last_job_id": None, "jobs": {}, "requests": {}}


def _load_state() -> dict[str, Any]:
    path = _state_path()
    if not path.exists():
        return _empty_state()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("state root is not an object")
        payload.setdefault("version", 1)
        payload.setdefault("last_job_id", None)
        payload.setdefault("jobs", {})
        payload.setdefault("requests", {})
        if not isinstance(payload["jobs"], dict) or not isinstance(payload["requests"], dict):
            raise ValueError("state mappings are invalid")
        return payload
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
        _diagnose(f"ignoring unreadable state file: {exc}")
        return _empty_state()


def _save_state(state: dict[str, Any]) -> None:
    path = _state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(state, ensure_ascii=False, indent=2).encode("utf-8")
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", prefix=f".{path.name}.", suffix=".tmp", dir=path.parent, delete=False
        ) as handle:
            tmp_path = Path(handle.name)
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    finally:
        if tmp_path is not None and tmp_path.exists():
            tmp_path.unlink(missing_ok=True)


def _remember_job(job: dict[str, Any], client_request_id: str | None = None) -> None:
    job_id = str(job.get("job_id") or "").strip()
    if not job_id:
        return
    try:
        state = _load_state()
        previous = state["jobs"].get(job_id, {})
        request_id = client_request_id or previous.get("client_request_id")
        entry = {
            "client_request_id": request_id,
            "file_name": str(job.get("file_name") or previous.get("file_name") or ""),
            "target_language": str(
                job.get("target_language") or previous.get("target_language") or ""
            ),
            "status": str(job.get("status") or previous.get("status") or "unknown"),
            "updated_at": str(job.get("updated_at") or ""),
            "recorded_at": int(time.time()),
        }
        state["jobs"][job_id] = entry
        state["last_job_id"] = job_id
        if request_id:
            state["requests"][request_id] = job_id

        if len(state["jobs"]) > 100:
            ordered = sorted(
                state["jobs"].items(),
                key=lambda item: int(item[1].get("recorded_at") or 0),
                reverse=True,
            )[:100]
            state["jobs"] = dict(ordered)
            kept_ids = set(state["jobs"])
            state["requests"] = {
                request_id: saved_job_id
                for request_id, saved_job_id in state["requests"].items()
                if saved_job_id in kept_ids
            }
        _save_state(state)
    except (OSError, ValueError, TypeError) as exc:
        _diagnose(f"could not persist job state: {exc}")


def _resolve_job_id(value: str | None) -> str:
    if value and value.strip():
        return value.strip()
    last_job_id = _load_state().get("last_job_id")
    if last_job_id:
        return str(last_job_id)
    raise ClientError(
        "JOB_ID_REQUIRED",
        "No job id was supplied and no previous Hermes translation job was found.",
    )


def _normalize_api_url(api_url: str) -> str:
    value = api_url.strip().rstrip("/")
    parsed = urllib.parse.urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ClientError("INVALID_API_URL", "API URL must be an http or https URL.")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ClientError(
            "INVALID_API_URL",
            "API URL must not contain credentials, a query, or a fragment.",
        )
    return value


def _json_from_bytes(data: bytes, *, context: str) -> dict[str, Any]:
    try:
        payload = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ClientError("INVALID_RESPONSE", f"{context} did not return valid JSON.") from exc
    if not isinstance(payload, dict):
        raise ClientError("INVALID_RESPONSE", f"{context} returned a non-object JSON value.")
    return payload


def _http_error(exc: urllib.error.HTTPError) -> ClientError:
    body = exc.read(65536)
    details: Any = None
    message = f"Translator API returned HTTP {exc.code}."
    code = "REQUEST_CONFLICT" if exc.code == 409 else "HTTP_ERROR"
    if body:
        try:
            parsed = json.loads(body.decode("utf-8", errors="replace"))
            details = parsed
            if isinstance(parsed, dict):
                detail = parsed.get("detail") or parsed.get("message")
                if isinstance(detail, dict):
                    structured_code = detail.get("code")
                    structured_message = detail.get("message")
                    if isinstance(structured_code, str) and structured_code.strip():
                        code = structured_code.strip()
                    if isinstance(structured_message, str) and structured_message.strip():
                        message = structured_message.strip()
                elif detail:
                    message = str(detail)
        except json.JSONDecodeError:
            details = body.decode("utf-8", errors="replace")[:1000]
    return ClientError(code, message, http_status=exc.code, details=details)


def _request_json(
    api_url: str,
    method: str,
    path: str,
    *,
    timeout: float,
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    url = f"{api_url}{path}"
    request_headers = {"Accept": "application/json", "User-Agent": USER_AGENT}
    if headers:
        request_headers.update(headers)
    request = urllib.request.Request(url, data=body, headers=request_headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return _json_from_bytes(response.read(), context=path)
    except urllib.error.HTTPError as exc:
        raise _http_error(exc) from exc
    except urllib.error.URLError as exc:
        reason = getattr(exc, "reason", exc)
        raise ClientError(
            "SERVICE_UNAVAILABLE",
            f"Could not reach the translator API: {reason}",
            exit_code=4,
        ) from exc
    except TimeoutError as exc:
        raise ClientError("REQUEST_TIMEOUT", "Translator API request timed out.", exit_code=4) from exc


def _post_json(
    api_url: str,
    path: str,
    payload: dict[str, Any],
    *,
    timeout: float,
) -> dict[str, Any]:
    return _request_json(
        api_url,
        "POST",
        path,
        timeout=timeout,
        body=json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
        headers={"Content-Type": "application/json; charset=utf-8"},
    )


def _quote_header_value(value: str) -> str:
    return value.replace("\\", "_").replace('"', "_").replace("\r", "_").replace("\n", "_")


def _encode_multipart(
    fields: dict[str, str], file_path: Path, file_bytes: bytes
) -> tuple[bytes, str]:
    boundary = f"----HermesBookTranslator{uuid.uuid4().hex}"
    chunks: list[bytes] = []
    for name, value in fields.items():
        safe_name = _quote_header_value(name)
        chunks.extend(
            [
                f"--{boundary}\r\n".encode("ascii"),
                f'Content-Disposition: form-data; name="{safe_name}"\r\n\r\n'.encode("ascii"),
                value.encode("utf-8"),
                b"\r\n",
            ]
        )

    fallback_name = f"book{file_path.suffix.lower()}"
    encoded_name = urllib.parse.quote(file_path.name, safe="")
    disposition = (
        f'Content-Disposition: form-data; name="file"; filename="{fallback_name}"; '
        f"filename*=UTF-8''{encoded_name}\r\n"
    )
    chunks.extend(
        [
            f"--{boundary}\r\n".encode("ascii"),
            disposition.encode("ascii"),
            b"Content-Type: text/plain; charset=utf-8\r\n\r\n",
            file_bytes,
            b"\r\n",
            f"--{boundary}--\r\n".encode("ascii"),
        ]
    )
    return b"".join(chunks), boundary


def _canonical_parameters(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "chunk_size_chars": args.chunk_size_chars,
        "corpus_id": args.corpus_id,
        "speed_mode": args.speed_mode,
        "stream": args.stream,
        "target_language": args.target_language,
        "translate_mode": args.translate_mode,
        "translation_level": args.translation_level,
        "translation_mode": args.translation_mode,
        "use_corpus": args.use_corpus,
        "use_domain_prompt": args.use_domain_prompt,
        "use_glossary": args.use_glossary,
        "use_style_examples": args.use_style_examples,
    }


def _client_request_id(file_bytes: bytes, parameters: dict[str, Any]) -> str:
    canonical = json.dumps(
        parameters, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    digest = hashlib.sha256()
    digest.update(file_bytes)
    digest.update(canonical)
    return digest.hexdigest()


def _status_summary(job: dict[str, Any]) -> dict[str, Any]:
    total = int(job.get("total_segments") or 0)
    completed = int(job.get("completed_segments") or 0)
    failed = int(job.get("failed_segments") or 0)
    return {
        "ok": True,
        "job_id": str(job.get("job_id") or ""),
        "status": str(job.get("status") or "unknown"),
        "progress": {
            "completed_segments": completed,
            "failed_segments": failed,
            "total_segments": total,
            "percent": round((completed / total) * 100, 2) if total else 0.0,
        },
        "current_chapter": str(job.get("current_chapter") or ""),
        "last_error": str(job.get("last_error") or ""),
        "runner_active": bool(job.get("runner_active", False)),
        "result_ready": bool(job.get("result_ready", False)),
        "outputs": job.get("outputs") if isinstance(job.get("outputs"), list) else [],
    }


def _get_job(args: argparse.Namespace, job_id: str) -> dict[str, Any]:
    encoded_job_id = urllib.parse.quote(job_id, safe="")
    return _request_json(
        args.api_url,
        "GET",
        f"/api/jobs/{encoded_job_id}",
        timeout=args.request_timeout,
    )


def command_preflight(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    payload = _request_json(
        args.api_url, "GET", "/api/health", timeout=args.request_timeout
    )
    service_ok = bool(payload.get("ok", False))
    configured = bool(payload.get("configured", False))
    result: dict[str, Any] = {
        "ok": service_ok and configured,
        "service": str(payload.get("service") or "text-book-translator"),
        "service_ok": service_ok,
        "configured": configured,
        "capabilities": payload.get("capabilities")
        if isinstance(payload.get("capabilities"), dict)
        else {},
    }
    if not service_ok:
        result["error"] = {
            "code": "SERVICE_UNHEALTHY",
            "message": "Translator service reported an unhealthy state.",
        }
        return result, 3
    if not configured:
        result["error"] = {
            "code": "API_NOT_CONFIGURED",
            "message": "Configure the model URL, API Key, and model in the translator UI first.",
        }
        return result, 3
    return result, 0


def command_submit(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    file_path = Path(args.file).expanduser()
    if not file_path.exists():
        raise ClientError("FILE_NOT_FOUND", f"Input file does not exist: {file_path}")
    if not file_path.is_file():
        raise ClientError("INVALID_FILE", f"Input path is not a regular file: {file_path}")
    if file_path.suffix.lower() not in {".txt", ".md"}:
        raise ClientError("UNSUPPORTED_FILE_TYPE", "Only .txt and .md files are supported.")
    try:
        file_bytes = file_path.read_bytes()
    except OSError as exc:
        raise ClientError("FILE_READ_FAILED", f"Could not read input file: {exc}") from exc
    if not file_bytes:
        raise ClientError("EMPTY_FILE", "Input file is empty.")

    parameters = _canonical_parameters(args)
    request_id = _client_request_id(file_bytes, parameters)
    form_fields = {
        "client_request_id": request_id,
        **{
            key: (str(value).lower() if isinstance(value, bool) else str(value))
            for key, value in parameters.items()
        },
    }
    body, boundary = _encode_multipart(form_fields, file_path, file_bytes)
    uploaded = _request_json(
        args.api_url,
        "POST",
        "/api/jobs/upload",
        timeout=args.request_timeout,
        body=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )
    job_id = str(uploaded.get("job_id") or "").strip()
    if not job_id:
        raise ClientError("INVALID_RESPONSE", "Upload response did not contain a job_id.")
    # Persist before start/resume so an interrupted follow-up request cannot
    # orphan the successfully created server job from this Hermes profile.
    _remember_job(uploaded, request_id)

    observed = _get_job(args, job_id)
    _remember_job(observed, request_id)
    status = str(observed.get("status") or uploaded.get("status") or "pending")
    runner_active = bool(observed.get("runner_active", False))
    encoded_job_id = urllib.parse.quote(job_id, safe="")
    action = "reuse"
    current = observed
    if status == "pending":
        _request_json(
            args.api_url,
            "POST",
            f"/api/jobs/{encoded_job_id}/start",
            timeout=args.request_timeout,
        )
        action = "start"
    elif status in {"paused", "failed"} or (
        status in {"running", "pausing"} and not runner_active
    ) or (
        status == "completed" and not bool(observed.get("result_ready", False))
    ):
        _request_json(
            args.api_url,
            "POST",
            f"/api/jobs/{encoded_job_id}/resume",
            timeout=args.request_timeout,
        )
        action = "resume"
    if action != "reuse":
        current = _get_job(args, job_id)

    merged = {**uploaded, **observed, **current, "job_id": job_id}
    _remember_job(merged, request_id)
    summary = _status_summary(merged)
    summary.update(
        {
            "client_request_id": request_id,
            "reused": bool(uploaded.get("reused", False)),
            "action": action,
        }
    )
    if summary["status"] == "cancelled":
        summary["ok"] = False
        summary["error"] = {
            "code": "JOB_CANCELLED",
            "message": "The matching translation job was already cancelled.",
        }
        return summary, 3
    return summary, 0


def command_status(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    job_id = _resolve_job_id(args.job_id)
    job = _get_job(args, job_id)
    _remember_job(job)
    return _status_summary(job), 0


def command_wait(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    job_id = _resolve_job_id(args.job_id)
    started = time.monotonic()
    previous_marker: tuple[Any, ...] | None = None
    while True:
        job = _get_job(args, job_id)
        marker = (
            job.get("status"),
            job.get("completed_segments"),
            job.get("failed_segments"),
            job.get("total_segments"),
        )
        if marker != previous_marker:
            _diagnose(
                "job={} status={} progress={}/{} failed={}".format(
                    job_id,
                    job.get("status", "unknown"),
                    job.get("completed_segments", 0),
                    job.get("total_segments", 0),
                    job.get("failed_segments", 0),
                )
            )
            previous_marker = marker
        status = str(job.get("status") or "unknown")
        if status in TERMINAL_STATUSES:
            _remember_job(job)
            result = _status_summary(job)
            result["ok"] = status == "completed" and bool(job.get("result_ready", False))
            if not result["ok"]:
                result["error"] = {
                    "code": "RESULT_NOT_READY" if status == "completed" else f"JOB_{status.upper()}",
                    "message": (
                        "Job completed but its output manifest is not ready."
                        if status == "completed"
                        else f"Translation job reached terminal status: {status}."
                    ),
                }
                return result, 3
            return result, 0
        if args.timeout_seconds and time.monotonic() - started >= args.timeout_seconds:
            _remember_job(job)
            result = _status_summary(job)
            result["ok"] = False
            result["error"] = {
                "code": "WAIT_TIMEOUT",
                "message": "Timed out while waiting; the translation job continues in the background.",
            }
            return result, 5
        time.sleep(args.poll_interval)


def _control_job(
    args: argparse.Namespace, action: str, payload: dict[str, Any] | None = None
) -> tuple[dict[str, Any], int]:
    job_id = _resolve_job_id(args.job_id)
    encoded_job_id = urllib.parse.quote(job_id, safe="")
    if payload is None:
        job = _request_json(
            args.api_url,
            "POST",
            f"/api/jobs/{encoded_job_id}/{action}",
            timeout=args.request_timeout,
        )
    else:
        job = _post_json(
            args.api_url,
            f"/api/jobs/{encoded_job_id}/{action}",
            payload,
            timeout=args.request_timeout,
        )
    _remember_job(job)
    result = _status_summary(job)
    api_ok = bool(job.get("ok", True))
    result["ok"] = api_ok
    result["action"] = action
    if not api_ok:
        result["error"] = {
            "code": "CONTROL_REJECTED",
            "message": str(job.get("message") or f"Job action was rejected: {action}."),
        }
    return result, 0 if api_ok else 3


def command_pause(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    return _control_job(args, "pause", {"reason": "agent_request"})


def command_resume(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    return _control_job(args, "resume")


def command_cancel(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    return _control_job(args, "cancel")


def _find_output(job: dict[str, Any], output_type: str) -> dict[str, Any]:
    if not bool(job.get("result_ready", False)):
        raise ClientError(
            "RESULT_NOT_READY",
            "Translation results are not ready. Check or wait for the job first.",
            exit_code=3,
        )
    outputs = job.get("outputs")
    if not isinstance(outputs, list):
        raise ClientError("INVALID_RESPONSE", "Job response did not contain an output manifest.")
    for item in outputs:
        if isinstance(item, dict) and item.get("type") == output_type:
            return item
    raise ClientError(
        "OUTPUT_NOT_AVAILABLE",
        f"Requested output is not available: {output_type}",
        exit_code=3,
    )


def _same_origin_url(api_url: str, download_url: str) -> str:
    resolved = urllib.parse.urljoin(f"{api_url}/", download_url)
    api_parts = urllib.parse.urlsplit(api_url)
    download_parts = urllib.parse.urlsplit(resolved)
    if (api_parts.scheme, api_parts.netloc) != (download_parts.scheme, download_parts.netloc):
        raise ClientError("INVALID_DOWNLOAD_URL", "Output URL points outside the translator API origin.")
    return resolved


def _safe_output_name(value: Any, output_type: str, job_id: str) -> str:
    candidate = Path(str(value or "")).name
    if not candidate or candidate in {".", ".."}:
        candidate = f"{job_id}-{output_type}"
    return candidate


def _unique_destination(output_dir: Path, file_name: str) -> Path:
    destination = output_dir / file_name
    if not destination.exists():
        return destination
    name_path = Path(file_name)
    for index in range(1, 10000):
        candidate = output_dir / f"{name_path.stem}-{index}{name_path.suffix}"
        if not candidate.exists():
            return candidate
    raise ClientError("OUTPUT_PATH_BUSY", "Could not choose a free output filename.")


def _stream_download(
    url: str,
    destination: Path,
    *,
    api_url: str,
    timeout: float,
    expected_size: int,
    expected_sha256: str,
) -> tuple[int, str]:
    request = urllib.request.Request(url, headers={"Accept": "*/*", "User-Agent": USER_AGENT})
    tmp_path: Path | None = None
    digest = hashlib.sha256()
    size = 0
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            _same_origin_url(api_url, response.geturl())
            with tempfile.NamedTemporaryFile(
                mode="wb",
                prefix=f".{destination.name}.",
                suffix=".tmp",
                dir=destination.parent,
                delete=False,
            ) as handle:
                tmp_path = Path(handle.name)
                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    handle.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)
                handle.flush()
                os.fsync(handle.fileno())
        actual_sha256 = digest.hexdigest()
        if size != expected_size:
            raise ClientError(
                "OUTPUT_SIZE_MISMATCH",
                f"Downloaded {size} bytes but the manifest expected {expected_size}.",
            )
        if actual_sha256.lower() != expected_sha256.lower():
            raise ClientError("OUTPUT_CHECKSUM_MISMATCH", "Downloaded output failed checksum verification.")
        # Publish exclusively: concurrent downloads must never overwrite a result.
        try:
            os.link(tmp_path, destination)
        except OSError as exc:
            if exc.errno not in {errno.ENOTSUP, errno.EOPNOTSUPP, errno.ENOSYS} and getattr(exc, "winerror", None) not in {1, 50}:
                raise
            # shortcut: filesystems without hard links require a copy; power loss
            # or process termination during this fallback can leave a partial file.
            created = False
            try:
                with destination.open("xb") as output:
                    created = True
                    with tmp_path.open("rb") as source:
                        shutil.copyfileobj(source, output)
                    output.flush()
                    os.fsync(output.fileno())
            except BaseException:
                if created:
                    destination.unlink(missing_ok=True)
                raise
        tmp_path.unlink()
        tmp_path = None
        return size, actual_sha256
    except urllib.error.HTTPError as exc:
        raise _http_error(exc) from exc
    except urllib.error.URLError as exc:
        reason = getattr(exc, "reason", exc)
        raise ClientError(
            "DOWNLOAD_FAILED", f"Could not download translation output: {reason}", exit_code=4
        ) from exc
    except TimeoutError as exc:
        raise ClientError("DOWNLOAD_TIMEOUT", "Translation output download timed out.", exit_code=4) from exc
    except FileExistsError as exc:
        raise ClientError("OUTPUT_PATH_BUSY", "Another download created this output; retry the download.") from exc
    finally:
        if tmp_path is not None and tmp_path.exists():
            tmp_path.unlink(missing_ok=True)


def command_download(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    job_id = _resolve_job_id(args.job_id)
    job = _get_job(args, job_id)
    output = _find_output(job, args.output_type)
    expected_size = output.get("size_bytes")
    expected_sha256 = output.get("sha256")
    if (
        type(expected_size) is not int or expected_size < 0
        or not isinstance(expected_sha256, str) or len(expected_sha256) != 64
        or any(char not in "0123456789abcdefABCDEF" for char in expected_sha256)
    ):
        raise ClientError(
            "INVALID_OUTPUT_MANIFEST",
            "Output manifest requires a nonnegative integer size_bytes and a 64-digit hexadecimal sha256.",
        )
    output_dir = Path(args.output_dir).expanduser()
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise ClientError("OUTPUT_DIR_FAILED", f"Could not create output directory: {exc}") from exc
    if not output_dir.is_dir():
        raise ClientError("INVALID_OUTPUT_DIR", f"Output path is not a directory: {output_dir}")

    file_name = _safe_output_name(output.get("filename"), args.output_type, job_id)
    destination = _unique_destination(output_dir, file_name)
    download_url = output.get("download_url")
    if not isinstance(download_url, str) or not download_url.strip():
        raise ClientError("INVALID_RESPONSE", "Output manifest is missing download_url.")
    resolved_url = _same_origin_url(args.api_url, download_url)
    size, actual_sha256 = _stream_download(
        resolved_url,
        destination,
        api_url=args.api_url,
        timeout=args.request_timeout,
        expected_size=expected_size,
        expected_sha256=expected_sha256,
    )
    _remember_job(job)
    return (
        {
            "ok": True,
            "job_id": job_id,
            "status": str(job.get("status") or "unknown"),
            "output_type": args.output_type,
            "output_path": str(destination.resolve()),
            "size_bytes": size,
            "sha256": actual_sha256,
        },
        0,
    )


def _add_common_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--api-url", default=DEFAULT_API_URL)
    parser.add_argument("--request-timeout", type=_positive_int, default=DEFAULT_REQUEST_TIMEOUT)


def _add_job_id(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--job-id",
        help="Translation job id; defaults to the latest job saved by this Hermes profile.",
    )


def build_parser() -> JsonArgumentParser:
    parser = JsonArgumentParser(
        prog="book_translator.py",
        description="Control local long-form translation jobs with JSON output.",
        add_help=False,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    preflight = subparsers.add_parser("preflight", add_help=False)
    _add_common_options(preflight)

    submit = subparsers.add_parser("submit", add_help=False)
    _add_common_options(submit)
    submit.add_argument("--file", required=True)
    submit.add_argument("--target-language", default="简体中文")
    submit.add_argument("--translation-mode", default="忠实翻译")
    submit.add_argument("--stream", action=argparse.BooleanOptionalAction, default=True)
    submit.add_argument("--chunk-size-chars", type=_positive_int, default=3500)
    submit.add_argument("--corpus-id", default="default")
    submit.add_argument("--use-corpus", action=argparse.BooleanOptionalAction, default=True)
    submit.add_argument("--use-glossary", action=argparse.BooleanOptionalAction, default=True)
    submit.add_argument("--use-style-examples", action=argparse.BooleanOptionalAction, default=True)
    submit.add_argument("--use-domain-prompt", action=argparse.BooleanOptionalAction, default=True)
    submit.add_argument(
        "--translate-mode",
        choices=("faithful", "natural", "psychology", "wiki", "bilingual_learning"),
        default="faithful",
    )
    submit.add_argument("--translation-level", type=int, choices=range(1, 6), default=3)
    submit.add_argument("--speed-mode", choices=("stable", "balanced", "fast"), default="stable")

    status = subparsers.add_parser("status", add_help=False)
    _add_common_options(status)
    _add_job_id(status)

    wait = subparsers.add_parser("wait", add_help=False)
    _add_common_options(wait)
    _add_job_id(wait)
    wait.add_argument("--poll-interval", type=_positive_int, default=5)
    wait.add_argument(
        "--timeout-seconds",
        type=_nonnegative_float,
        default=0,
        help="0 waits without a deadline; a timeout does not cancel the job.",
    )

    download = subparsers.add_parser("download", add_help=False)
    _add_common_options(download)
    _add_job_id(download)
    download.add_argument("--output-type", choices=OUTPUT_TYPES, default="bilingual.md")
    download.add_argument("--output-dir", required=True)

    for action in ("pause", "resume", "cancel"):
        control = subparsers.add_parser(action, add_help=False)
        _add_common_options(control)
        _add_job_id(control)

    return parser


def _help_payload(parser: JsonArgumentParser, argv: list[str]) -> dict[str, Any]:
    command = next((item for item in argv if item in parser._subparsers._group_actions[0].choices), None)
    selected = (
        parser._subparsers._group_actions[0].choices[command]
        if command
        else parser
    )
    return {
        "ok": True,
        "command": "help",
        "for": command or "book_translator.py",
        "help": selected.format_help(),
    }


def main(argv: list[str] | None = None) -> int:
    actual_argv = list(sys.argv[1:] if argv is None else argv)
    parser = build_parser()
    if any(item in {"-h", "--help"} for item in actual_argv):
        _emit(_help_payload(parser, actual_argv))
        return 0
    try:
        args = parser.parse_args(actual_argv)
        args.api_url = _normalize_api_url(args.api_url)
        handlers = {
            "preflight": command_preflight,
            "submit": command_submit,
            "status": command_status,
            "wait": command_wait,
            "download": command_download,
            "pause": command_pause,
            "resume": command_resume,
            "cancel": command_cancel,
        }
        payload, exit_code = handlers[args.command](args)
        _emit(payload)
        return exit_code
    except ClientError as exc:
        _diagnose(f"{exc.code}: {exc.message}")
        _emit(exc.payload())
        return exc.exit_code
    except KeyboardInterrupt:
        error = ClientError(
            "INTERRUPTED",
            "Command interrupted; an active translation job was not cancelled.",
            exit_code=130,
        )
        _diagnose(error.message)
        _emit(error.payload())
        return error.exit_code
    except Exception as exc:
        error = ClientError("INTERNAL_ERROR", f"Unexpected client error: {exc}")
        _diagnose(error.message)
        _emit(error.payload())
        return error.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
