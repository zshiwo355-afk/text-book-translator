from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
import threading
import unittest
from email.parser import BytesParser
from email.policy import default
from functools import wraps
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

try:
    from mcp import Client
    from mcp.client.stdio import StdioServerParameters
except ImportError:
    Client = None
    StdioServerParameters = None


SERVER_PATH = Path(__file__).resolve().parents[1] / "mcp_server.py"


def _with_client(test):
    @wraps(test)
    async def run(self):
        # AnyIO requires MCP client entry and exit to happen in the same task.
        async with self._new_client() as self.client:
            await test(self)
    return run


class _TranslatorHandler(BaseHTTPRequestHandler):
    def log_message(self, *_args) -> None:
        pass

    def _send(self, payload, status=200) -> None:
        body = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def _handle(self) -> None:
        fixture = self.server.fixture
        fixture.requests.append((self.command, self.path))
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        if self.path == "/api/health":
            self._send({"ok": True, "configured": True, "capabilities": {"file_types": [".txt", ".md"]}})
            return
        if self.path == "/api/jobs/upload" and self.command == "POST":
            message = BytesParser(policy=default).parsebytes(
                f"Content-Type: {self.headers['Content-Type']}\r\nMIME-Version: 1.0\r\n\r\n".encode() + body
            )
            fields = {
                part.get_param("name", header="content-disposition"): part.get_payload(decode=True)
                for part in message.iter_parts()
            }
            fixture.uploads.append(fields)
            request_id = fields["client_request_id"].decode()
            reused = request_id in fixture.identities
            if not reused:
                job_id = f"job_{len(fixture.jobs) + 1}"
                fixture.identities[request_id] = job_id
                fixture.jobs[job_id] = {
                    "job_id": job_id,
                    "file_name": "book.md",
                    "status": "pending",
                    "runner_active": False,
                    "total_segments": 1,
                    "completed_segments": 0,
                    "failed_segments": 0,
                    "result_ready": False,
                    "outputs": [],
                }
            job = fixture.jobs[fixture.identities[request_id]]
            self._send({**job, "reused": reused})
            return
        parts = self.path.strip("/").split("/")
        job = fixture.jobs.get(parts[2]) if len(parts) >= 3 else None
        if job is None:
            self._send({"detail": {"code": "JOB_NOT_FOUND", "message": "Unknown test job."}}, 404)
            return
        if self.command == "POST":
            action = parts[-1]
            status = {"start": "running", "resume": "running", "pause": "paused", "cancel": "cancelled"}.get(action)
            if status is None:
                self._send({"detail": "Unknown action"}, 404)
                return
            if action == "pause" and json.loads(body) != {"reason": "agent_request"}:
                self._send({"detail": "Missing agent pause reason"}, 400)
                return
            job.update(status=status, runner_active=status == "running")
            self._send({"ok": True, **job})
        elif len(parts) == 5 and parts[3] == "download":
            self._send(fixture.download_bytes)
        else:
            self._send(job)

    do_GET = _handle
    do_POST = _handle


@unittest.skipUnless(Client is not None, "Optional MCP v2 dependency is not installed")
class MCPServerTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.temp = self.enterContext(tempfile.TemporaryDirectory())
        self.workspace = Path(self.temp).resolve() / "workspace"
        self.workspace.mkdir()
        self.book = self.workspace / "一本书.md"
        self.book.write_text("# Chapter\n\nHello world.\n", encoding="utf-8")
        self.jobs = {}
        self.identities = {}
        self.requests = []
        self.uploads = []
        self.output_bytes = "# 译稿\n\n你好，世界。\n".encode()
        self.download_bytes = self.output_bytes
        self.http = ThreadingHTTPServer(("127.0.0.1", 0), _TranslatorHandler)
        self.http.fixture = self
        self.thread = threading.Thread(target=self.http.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self._close_http)

    def _close_http(self) -> None:
        self.http.shutdown()
        self.http.server_close()
        self.thread.join(timeout=2)

    def _new_client(self):
        self.hermes_home = Path(self.temp) / "unused-hermes-home"
        return Client(
            StdioServerParameters(
                command=sys.executable,
                args=[str(SERVER_PATH), "--workspace", str(self.workspace), "--api-url", f"http://127.0.0.1:{self.http.server_port}"],
                env={**os.environ, "HERMES_HOME": str(self.hermes_home)},
                cwd=self.temp,
            ),
            read_timeout_seconds=15,
        )

    async def _call(self, name, arguments=None, *, ok=True):
        result = await self.client.call_tool(name, arguments)
        self.assertEqual(bool(result.is_error), not ok, result)
        payload = result.structured_content
        self.assertIsInstance(payload, dict, result)
        self.assertEqual(payload["ok"], ok, payload)
        return payload

    def test_server_rejects_remote_or_ambiguous_backend_configuration(self) -> None:
        from mcp_server import create_server

        for url in (
            "https://example.com", "http://192.168.1.2:8000",
            "http://user:password@127.0.0.1:8000", "http://127.0.0.1:8000/other",
            "http://127.0.0.1:8000?redirect=remote", "http://127.0.0.1:invalid",
        ):
            with self.subTest(url=url), self.assertRaises(ValueError):
                create_server(self.workspace, url)

    def _complete(self, job_id) -> None:
        self.jobs[job_id].update(
            status="completed",
            runner_active=False,
            completed_segments=1,
            result_ready=True,
            outputs=[{
                "type": "bilingual.md",
                "filename": "book_bilingual.md",
                "download_url": f"/api/jobs/{job_id}/download/bilingual.md",
                "size_bytes": len(self.output_bytes),
                "sha256": hashlib.sha256(self.output_bytes).hexdigest(),
            }],
        )

    @_with_client
    async def test_stdio_workflow_reuses_jobs_controls_and_downloads_verified_output(self) -> None:
        tools = await self.client.list_tools()
        self.assertEqual(
            {tool.name for tool in tools.tools},
            {"translator_preflight", "submit_translation", "get_translation_status", "pause_translation", "resume_translation", "cancel_translation", "download_translation"},
        )
        preflight = await self._call("translator_preflight")
        self.assertTrue(preflight["configured"])
        first = await self._call("submit_translation", {"file_path": self.book.name})
        repeated = await self._call("submit_translation", {"file_path": str(self.book)})
        job_id = first["job_id"]
        self.assertEqual(repeated["job_id"], job_id)
        self.assertEqual(first["client_request_id"], repeated["client_request_id"])
        self.assertFalse(first["reused"])
        self.assertTrue(repeated["reused"])
        self.assertEqual(self.requests.count(("POST", f"/api/jobs/{job_id}/start")), 1)
        self.assertEqual(self.uploads[0]["file"], self.book.read_bytes())
        self.assertEqual(self.uploads[0]["target_language"].decode(), "简体中文")
        status = await self._call("get_translation_status", {"job_id": job_id})
        self.assertEqual(status["status"], "running")
        paused = await self._call("pause_translation", {"job_id": job_id})
        self.assertEqual(paused["status"], "paused")
        resumed = await self._call("resume_translation", {"job_id": job_id})
        self.assertEqual(resumed["status"], "running")

        self._complete(job_id)
        output_dir = self.workspace / "outputs"
        output_dir.mkdir()
        existing = output_dir / "book_bilingual.md"
        existing.write_text("Keep existing file", encoding="utf-8")
        downloaded = await self._call("download_translation", {"job_id": job_id})
        output = Path(downloaded["output_path"])
        self.assertEqual(output.parent, output_dir)
        self.assertNotEqual(output, existing)
        self.assertEqual(existing.read_text(encoding="utf-8"), "Keep existing file")
        self.assertEqual(output.read_bytes(), self.output_bytes)
        self.assertEqual(downloaded["sha256"], hashlib.sha256(self.output_bytes).hexdigest())
        downloaded_again = await self._call("download_translation", {"job_id": job_id})
        self.assertNotEqual(downloaded_again["output_path"], downloaded["output_path"])
        self.assertEqual(Path(downloaded_again["output_path"]).read_bytes(), self.output_bytes)
        self.assertEqual(output.read_bytes(), self.output_bytes)

        another = await self._call("submit_translation", {"file_path": self.book.name, "target_language": "日本語"})
        self.assertNotEqual(another["job_id"], job_id)
        cancelled = await self._call("cancel_translation", {"job_id": another["job_id"]})
        self.assertEqual(cancelled["status"], "cancelled")
        self.assertTrue((self.workspace / ".translator-state" / "jobs.json").is_file())
        self.assertFalse(self.hermes_home.exists(), "MCP must not write Hermes's default profile")

    @_with_client
    async def test_invalid_inputs_and_escaping_paths_never_reach_backend(self) -> None:
        outside = Path(self.temp) / "outside.md"
        outside.write_text("Outside workspace", encoding="utf-8")
        (self.workspace / "bad.txt").write_bytes(b"\xff\xfe")
        (self.workspace / "unsupported.epub").write_bytes(b"Not a supported format")
        with (self.workspace / "oversized.txt").open("wb") as handle:
            handle.seek(20 * 1024 * 1024)
            handle.write(b"x")
        for value in ("../outside.md", str(outside), "bad.txt", "unsupported.epub", "oversized.txt"):
            with self.subTest(file_path=value):
                payload = await self._call("submit_translation", {"file_path": value}, ok=False)
                self.assertTrue(payload["error"]["code"])
        for name, arguments in (
            ("get_translation_status", {"job_id": "../job_1"}),
            ("submit_translation", {"file_path": self.book.name, "corpus_id": "../default"}),
        ):
            rejected = await self.client.call_tool(name, arguments)
            self.assertTrue(rejected.is_error)
        await self._call("download_translation", {"job_id": "job_1", "output_dir": "../outside"}, ok=False)
        for name, target, directory, tool, arguments in (
            ("escape.md", outside, False, "submit_translation", {"file_path": "escape.md"}),
            ("escape-output", Path(self.temp), True, "download_translation", {"job_id": "job_1", "output_dir": "escape-output"}),
        ):
            with self.subTest(symlink=name):
                try:
                    (self.workspace / name).symlink_to(target, target_is_directory=directory)
                except (OSError, NotImplementedError) as exc:
                    self.skipTest(f"Symlinks are unavailable on this platform: {exc}")
                await self._call(tool, arguments, ok=False)
        self.assertEqual(self.requests, [])

    @_with_client
    async def test_backend_error_is_an_mcp_tool_error(self) -> None:
        payload = await self._call("get_translation_status", {"job_id": "missing_job"}, ok=False)
        self.assertEqual(payload["error"]["code"], "JOB_NOT_FOUND")
        self.assertEqual(payload["error"]["http_status"], 404)

    @_with_client
    async def test_corrupt_download_is_rejected_without_publishing_file(self) -> None:
        submitted = await self._call("submit_translation", {"file_path": self.book.name})
        job_id = submitted["job_id"]
        self._complete(job_id)
        self.download_bytes = b"x" * len(self.output_bytes)
        payload = await self._call("download_translation", {"job_id": job_id}, ok=False)
        self.assertEqual(payload["error"]["code"], "OUTPUT_CHECKSUM_MISMATCH")
        self.assertEqual(list((self.workspace / "outputs").iterdir()), [])


if __name__ == "__main__":
    unittest.main()
