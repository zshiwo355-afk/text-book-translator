import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fastapi.testclient import TestClient

with patch("dotenv.load_dotenv"):
    from backend.translator.pipeline import EventBroker, TranslationPipeline
from backend import app as app_module
from backend.translator.storage import FileStorage, StorageIntegrityError
from backend.translator.validator import JobConfigSnapshot, JobRecord, SegmentRecord


class StorageIntegrityTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.storage = FileStorage(Path(self.temp_dir.name))
        self.pipeline = TranslationPipeline(self.storage, EventBroker())
        self.config = JobConfigSnapshot(
            model="test-model", temperature=0.1, stream=False,
            chunk_size_chars=3500, max_retries=1, target_language="简体中文",
        )

    async def create_job(self, job_id):
        job = JobRecord(
            job_id=job_id, file_name="book.txt", source_path="unused.txt",
            target_language="简体中文", total_segments=1, status="paused",
            config=self.config,
        )
        segment = SegmentRecord(
            segment_id="seg_000001", order=1, source_text="Hello.",
            status="success", translated_text="你好。",
        )
        await self.storage.create_job(job, [segment], [])
        return job, segment

    async def test_start_and_resume_reject_unrecoverable_segments_without_changing_data(self):
        for action in ("start_job", "resume_job"):
            for index, content in enumerate((None, "{broken", "", "[]", "{}")):
                with self.subTest(action=action, content=content):
                    job, _ = await self.create_job(f"job_{action}_{index}")
                    path = self.storage.segments_json_path(job.job_id)
                    if content is None:
                        path.unlink()
                    else:
                        path.write_text(content, encoding="utf-8")
                    manifest = {"job_id": job.job_id, "outputs": []}
                    await self.storage.save_result_manifest(job.job_id, manifest)
                    with (
                        patch("backend.translator.pipeline.reload_config", return_value=SimpleNamespace(
                            model="test-model", tokenhub_api_key="test-only-key",
                        )),
                        patch.object(self.pipeline, "_create_background_task") as start,
                    ):
                        with self.assertRaisesRegex(StorageIntegrityError, "段落进度"):
                            await getattr(self.pipeline, action)(job.job_id)
                        start.assert_not_called()
                    self.assertEqual((await self.storage.load_job(job.job_id)).model_dump(), job.model_dump())
                    self.assertEqual(await self.storage.load_result_manifest(job.job_id), manifest)
                    self.assertEqual(path.read_text(encoding="utf-8") if path.exists() else None, content)

    async def test_start_and_resume_recover_valid_backup(self):
        for action in ("start_job", "resume_job"):
            for index, content in enumerate((None, "{broken", "")):
                with self.subTest(action=action, content=content):
                    job, segment = await self.create_job(f"job_backup_{action}_{index}")
                    await self.storage.save_segments(job.job_id, [segment])
                    path = self.storage.segments_json_path(job.job_id)
                    if content is None:
                        path.unlink()
                    else:
                        path.write_text(content, encoding="utf-8")
                    with (
                        patch("backend.translator.pipeline.reload_config", return_value=SimpleNamespace(
                            model="test-model", tokenhub_api_key="test-only-key",
                        )),
                        patch.object(self.pipeline, "_create_background_task"),
                    ):
                        result = await getattr(self.pipeline, action)(job.job_id)
                    self.assertEqual(result.completed_segments, 1)
                    self.assertEqual((await self.storage.load_segments(job.job_id))[0].translated_text, "你好。")

    async def test_api_reports_corrupt_progress_as_non_retryable_conflict(self):
        configured = SimpleNamespace(
            model="test-model", tokenhub_api_key="test-only-key",
            tokenhub_base_url="https://model.invalid/v1",
        )
        with (
            patch.object(app_module, "storage", self.storage),
            patch.object(app_module, "pipeline", self.pipeline),
            patch.object(app_module, "corpus_manager", self.pipeline.corpus_manager),
            patch.object(app_module, "reload_config", return_value=configured),
            patch("backend.translator.pipeline.reload_config", return_value=configured),
            patch.object(self.pipeline, "_create_background_task") as start,
            TestClient(app_module.app, raise_server_exceptions=False) as client,
        ):
            for action in ("start", "resume", "download/translated.md"):
                for index, content in enumerate((None, "{broken", "[]", "[{}]", "[42]", "count_mismatch")):
                    with self.subTest(action=action, content=content):
                        job, _ = await self.create_job(f"job_api_{action.split('/')[0]}_{index}")
                        path = self.storage.segments_json_path(job.job_id)
                        if content is None:
                            path.unlink()
                        elif content == "count_mismatch":
                            job.total_segments = 2
                            await self.storage.save_job(job)
                        else:
                            path.write_text(content, encoding="utf-8")
                        original = path.read_bytes() if path.exists() else None
                        response = client.request(
                            "GET" if action.startswith("download/") else "POST",
                            f"/api/jobs/{job.job_id}/{action}",
                        )
                        self.assertEqual(response.status_code, 409, response.text)
                        detail = response.json()["detail"]
                        self.assertEqual(detail["code"], "JOB_DATA_CORRUPT")
                        self.assertFalse(detail["retryable"])
                        self.assertIn("段落进度", detail["message"])
                        self.assertEqual((await self.storage.load_job(job.job_id)).model_dump(), job.model_dump())
                        self.assertEqual(path.read_bytes() if path.exists() else None, original)
            start.assert_not_called()


if __name__ == "__main__":
    unittest.main()
