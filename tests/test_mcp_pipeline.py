from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import socket
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import uvicorn

try:
    from mcp import Client
    from mcp.client.stdio import StdioServerParameters
except ImportError:
    Client = None
    StdioServerParameters = None

with patch("dotenv.load_dotenv"):
    from backend import app as app_module, config
from backend.translator.hy_mt2_client import HyMT2Client
from backend.translator.pipeline import EventBroker, TranslationPipeline
from backend.translator.storage import FileStorage
from backend.translator.validator import TranslationResponse


SERVER_PATH = Path(__file__).resolve().parents[1] / "mcp_server.py"


@unittest.skipUnless(Client is not None, "Optional MCP v2 dependency is not installed")
class MCPPipelineTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        self.workspace = self.temp / "workspace"
        self.workspace.mkdir()
        self.book = self.workspace / "测试书.md"
        self.book.write_text("# Chapter One\n\nHello world.\n\nA second paragraph.\n", encoding="utf-8")
        settings = config.AppConfig(
            tokenhub_api_key="test-only-key", tokenhub_base_url="http://127.0.0.1:9/v1",
            model="fake-translation-model", max_retries=1,
        )
        config_path = self.temp / "config.json"
        config_path.write_text(settings.model_dump_json(), encoding="utf-8")
        for name, value in (
            ("CONFIG_EXAMPLE_PATH", config_path),
            ("CONFIG_LOCAL_PATH", self.temp / "absent-local.json"),
            ("ENV_FILE_PATHS", ()), ("PROCESS_ENV", {}),
        ):
            self.enterContext(patch.object(config, name, value))
        config.load_config.cache_clear()
        self.addCleanup(config.load_config.cache_clear)
        self.storage = FileStorage(self.temp / "data")
        self.pipeline = TranslationPipeline(self.storage, EventBroker())
        for name, value in (
            ("storage", self.storage), ("pipeline", self.pipeline),
            ("broker", self.pipeline.broker), ("corpus_manager", self.pipeline.corpus_manager),
        ):
            self.enterContext(patch.object(app_module, name, value))
        for target in ("backend.app.reload_config", "backend.translator.pipeline.reload_config"):
            self.enterContext(patch(target, side_effect=lambda: settings.model_copy(deep=True)))

        self.model_calls = []
        self.allow_translation = threading.Event()

        async def translate_chunk(_client, messages, stream, **callbacks):
            # Match real segment IDs, excluding the seg_xxxxxx example in the prompt.
            ids = re.findall(r'<segment id="(seg_\d+)">', messages[-1]["content"])
            self.assertTrue(ids)
            self.model_calls.append(ids)
            while not self.allow_translation.is_set():
                await asyncio.sleep(0.01)
            if callbacks.get("on_first_response"):
                callbacks["on_first_response"]()
            text = "<translation>" + "".join(
                f'<segment id="{segment_id}">测试译文 {segment_id}。</segment>' for segment_id in ids
            ) + "</translation>"
            return TranslationResponse(text=text, used_stream=False)

        self.enterContext(patch.object(HyMT2Client, "translate_chunk", translate_chunk))
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.addCleanup(self.sock.close)
        self.api_url = f"http://127.0.0.1:{self.sock.getsockname()[1]}"
        self.server = uvicorn.Server(uvicorn.Config(
            app_module.app, host="127.0.0.1", port=0, log_level="error", access_log=False,
        ))
        self.thread = threading.Thread(target=self.server.run, kwargs={"sockets": [self.sock]}, daemon=True)
        self.thread.start()
        self.addCleanup(self._stop_server)
        deadline = time.monotonic() + 5
        while not self.server.started and self.thread.is_alive() and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertTrue(self.server.started, "Translator backend did not become ready")

    def _stop_server(self):
        self.allow_translation.set()
        self.server.should_exit = True
        self.thread.join(timeout=5)
        if self.thread.is_alive():
            self.server.force_exit = True
            self.thread.join(timeout=2)
        self.assertFalse(self.thread.is_alive(), "Translator backend failed to stop")

    def _new_client(self):
        return Client(
            StdioServerParameters(
                command=sys.executable,
                args=[str(SERVER_PATH), "--workspace", str(self.workspace), "--api-url", self.api_url],
                env={**os.environ, "HERMES_HOME": str(self.temp / "unused-hermes-home")},
                cwd=str(self.temp),
            ),
            read_timeout_seconds=15,
        )

    async def _call(self, client, tool, arguments=None):
        result = await client.call_tool(tool, arguments)
        self.assertFalse(result.is_error, result)
        payload = result.structured_content
        self.assertTrue(payload["ok"], payload)
        return payload

    async def test_stdio_real_pipeline_exports_reuses_and_survives_mcp_reconnect(self):
        async with self._new_client() as client:
            self.assertTrue((await self._call(client, "translator_preflight"))["configured"])
            submitted = await self._call(client, "submit_translation", {
                "file_path": self.book.name, "use_corpus": False,
            })
            job_id = submitted["job_id"]
            status = await self._call(client, "get_translation_status", {"job_id": job_id})
            self.assertEqual(status["status"], "running")
            self.assertTrue(status["runner_active"])
            self.assertTrue(self.model_calls)
            call_count = len(self.model_calls)
            repeated = await self._call(client, "submit_translation", {
                "file_path": str(self.book), "use_corpus": False,
            })
            self.assertEqual(repeated["job_id"], job_id)
            self.assertTrue(repeated["reused"])
            self.assertEqual(len(self.model_calls), call_count)

        # The model finishes after the MCP process has exited; the API owns the task.
        self.allow_translation.set()
        async with self._new_client() as client:
            deadline = time.monotonic() + 5
            while True:
                status = await self._call(client, "get_translation_status", {"job_id": job_id})
                if status["status"] == "completed" and status["result_ready"]:
                    break
                self.assertNotIn(status["status"], {"failed", "paused", "cancelled"}, status)
                self.assertLess(time.monotonic(), deadline, status)
                await asyncio.sleep(0.05)
            self.assertEqual(status["progress"]["completed_segments"], status["progress"]["total_segments"])
            self.assertGreater(status["progress"]["total_segments"], 1)
            self.assertEqual(len(self.model_calls), call_count)
            downloaded = await self._call(client, "download_translation", {"job_id": job_id})
            output = Path(downloaded["output_path"])
            self.assertTrue(output.is_relative_to(self.workspace))
            self.assertIn("Hello world.", output.read_text(encoding="utf-8"))
            self.assertIn("测试译文", output.read_text(encoding="utf-8"))
            self.assertEqual(hashlib.sha256(output.read_bytes()).hexdigest(), downloaded["sha256"])

        persisted = json.loads(self.storage.job_json_path(job_id).read_text(encoding="utf-8"))
        self.assertEqual(persisted["status"], "completed")
        segments = json.loads(self.storage.segments_json_path(job_id).read_text(encoding="utf-8"))
        self.assertEqual(status["progress"]["completed_segments"], len(segments))
        self.assertTrue(all(item["status"] == "success" and "测试译文" in item["translated_text"] for item in segments))


if __name__ == "__main__":
    unittest.main()
