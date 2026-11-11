from __future__ import annotations

import errno
import hashlib
import importlib.util
import io
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "integrations/hermes/book-translator/scripts/book_translator.py"
spec = importlib.util.spec_from_file_location("download_integrity_client", SCRIPT_PATH)
client = importlib.util.module_from_spec(spec)
spec.loader.exec_module(client)
API_URL = "http://127.0.0.1:8000"


class _Response(io.BytesIO):
    def geturl(self):
        return f"{API_URL}/api/jobs/job_1/download/bilingual.md"


class DownloadIntegrityTest(unittest.TestCase):
    def setUp(self) -> None:
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        self.directory = Path(temporary_directory.name)
        self.destination = self.directory / "book.md"
        self.content = "完整译稿。\n".encode()
        self.sha256 = hashlib.sha256(self.content).hexdigest()

    def _download(self):
        with patch.object(client.urllib.request, "urlopen", return_value=_Response(self.content)):
            return client._stream_download(
                f"{API_URL}/api/jobs/job_1/download/bilingual.md",
                self.destination,
                api_url=API_URL,
                timeout=1,
                expected_size=len(self.content),
                expected_sha256=self.sha256,
            )

    def test_destination_claimed_during_download_is_never_overwritten(self) -> None:
        original_link = client.os.link

        def claim_name(source, destination):
            destination.write_bytes(b"Another download won")
            original_link(source, destination)

        with patch.object(client.os, "link", side_effect=claim_name):
            with self.assertRaises(client.ClientError) as raised:
                self._download()
        self.assertEqual(raised.exception.code, "OUTPUT_PATH_BUSY")
        self.assertEqual(self.destination.read_bytes(), b"Another download won")
        self.assertEqual(list(self.directory.iterdir()), [self.destination])

    def test_unsupported_hardlinks_fall_back_to_a_verified_exclusive_copy(self) -> None:
        with patch.object(client.os, "link", side_effect=OSError(errno.ENOTSUP, "Unsupported")):
            size, sha256 = self._download()
        self.assertEqual((size, sha256), (len(self.content), self.sha256))
        self.assertEqual(self.destination.read_bytes(), self.content)
        self.assertEqual(list(self.directory.iterdir()), [self.destination])

    def test_copy_fallback_does_not_overwrite_an_existing_target(self) -> None:
        self.destination.write_bytes(b"Keep this file")
        with patch.object(client.os, "link", side_effect=OSError(errno.ENOTSUP, "Unsupported")):
            with self.assertRaises(client.ClientError) as raised:
                self._download()
        self.assertEqual(raised.exception.code, "OUTPUT_PATH_BUSY")
        self.assertEqual(self.destination.read_bytes(), b"Keep this file")
        self.assertEqual(list(self.directory.iterdir()), [self.destination])

    def test_failed_copy_removes_the_partial_output_and_temporary_file(self) -> None:
        def interrupted_copy(source, output):
            output.write(source.read(3))
            raise OSError(errno.ENOSPC, "Disk full")

        with (
            patch.object(client.os, "link", side_effect=OSError(errno.ENOTSUP, "Unsupported")),
            patch.object(client.shutil, "copyfileobj", side_effect=interrupted_copy),
        ):
            with self.assertRaises(OSError) as raised:
                self._download()
        self.assertEqual(raised.exception.errno, errno.ENOSPC)
        self.assertEqual(list(self.directory.iterdir()), [])

    def test_other_link_errors_are_not_silently_retried_as_copies(self) -> None:
        with (
            patch.object(client.os, "link", side_effect=PermissionError(errno.EACCES, "Denied")),
            patch.object(client.shutil, "copyfileobj") as copy,
        ):
            with self.assertRaises(PermissionError):
                self._download()
        copy.assert_not_called()
        self.assertEqual(list(self.directory.iterdir()), [])

    def test_invalid_manifest_never_starts_a_download(self) -> None:
        args = SimpleNamespace(
            job_id="job_1", output_type="bilingual.md", output_dir=str(self.directory / "outputs"),
            api_url=API_URL, request_timeout=1,
        )
        valid = {"size_bytes": len(self.content), "sha256": self.sha256}
        invalid_fields = [
            {}, {"sha256": self.sha256}, {"size_bytes": len(self.content)},
            {**valid, "size_bytes": True}, {**valid, "size_bytes": -1},
            {**valid, "size_bytes": "12"}, {**valid, "sha256": None},
            {**valid, "sha256": "a" * 63}, {**valid, "sha256": "z" * 64},
        ]
        for fields in invalid_fields:
            with self.subTest(fields=fields):
                output = {"type": "bilingual.md", "filename": "book.md", "download_url": "/download", **fields}
                job = {"job_id": "job_1", "result_ready": True, "outputs": [output]}
                with (
                    patch.object(client, "_get_job", return_value=job),
                    patch.object(client, "_stream_download") as download,
                    patch.object(client.urllib.request, "urlopen") as network,
                ):
                    with self.assertRaises(client.ClientError) as raised:
                        client.command_download(args)
                self.assertEqual(raised.exception.code, "INVALID_OUTPUT_MANIFEST")
                download.assert_not_called()
                network.assert_not_called()
        self.assertEqual(list(self.directory.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
