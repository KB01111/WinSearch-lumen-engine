import importlib.util
import hashlib
import io
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import zipfile

spec = importlib.util.spec_from_file_location('payload', Path(__file__).with_name('assemble-engine-payload.py'))
payload = importlib.util.module_from_spec(spec)
spec.loader.exec_module(payload)


class PayloadTests(unittest.TestCase):
    def test_publish_retries_transient_windows_lock_without_overwriting(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, target = root / "candidate", root / "output"
            source.mkdir()
            original = Path.rename
            locked = PermissionError("scanner holds directory")
            locked.winerror = 5
            attempts = []

            def rename(path, destination):
                attempts.append(path)
                if len(attempts) == 1:
                    raise locked
                return original(path, destination)

            with patch.object(Path, "rename", rename):
                payload.publish_directory(source, target)
            self.assertTrue(target.is_dir())
            self.assertEqual(len(attempts), 2)
            source.mkdir()
            with self.assertRaises(FileExistsError):
                payload.publish_directory(source, target)

    def test_checked_copy_hashes_exact_copied_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            content = b"verified bytes"
            (root / "source").write_bytes(content)
            expected = hashlib.sha256(content).hexdigest()
            record = payload.checked_copy(root / "source", root / "out", expected)
            self.assertEqual((root / "out").read_bytes(), content)
            self.assertEqual(record, {"sha256": expected, "sizeBytes": len(content)})

    def test_checked_copy_does_not_overwrite_retained_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "source").write_bytes(b"new")
            (root / "out").write_bytes(b"retained")
            with self.assertRaises(FileExistsError):
                payload.checked_copy(root / "source", root / "out", None)
            self.assertEqual((root / "out").read_bytes(), b"retained")

    def test_checked_copy_rejects_missing(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex((ValueError, FileNotFoundError), 'missing|No such'):
                payload.checked_copy(root / 'missing', root / 'out', '0' * 64)

    def test_checked_copy_rejects_corrupt(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'source').write_bytes(b'corrupt')
            with self.assertRaisesRegex(ValueError, 'hash'):
                payload.checked_copy(root / 'source', root / 'out', '0' * 64)
            self.assertFalse((root / 'out').exists())

    def test_checked_copy_rejects_links(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'source').write_bytes(b'bytes')
            try:
                (root / 'linked').symlink_to(root / 'source')
            except OSError:
                self.skipTest('host does not grant file symlink creation')
            with self.assertRaisesRegex(ValueError, 'reparse|link'):
                payload.checked_copy(root / 'linked', root / 'out', None)

    def test_rejects_stale_source_commit(self):
        with self.assertRaisesRegex(ValueError, 'commit'):
            payload.require_commit('0' * 40)

    def test_archive_copy_rejects_wrong_hash_before_writing(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = root / "test.zip"
            with zipfile.ZipFile(archive, "w") as package:
                package.writestr("runtime.dll", b"runtime")
            with self.assertRaisesRegex(ValueError, "hash"):
                payload.copy_zip(archive, "0" * 64, {"runtime.dll": "runtime.dll"}, root)
            self.assertFalse((root / "runtime.dll").exists())

    def test_archive_copy_checks_all_paths_before_writing_selected_file(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = root / "test.zip"
            with zipfile.ZipFile(archive, "w") as package:
                package.writestr("runtime.dll", b"runtime")
                package.writestr("../escape", b"escape")
            with self.assertRaisesRegex(ValueError, "path"):
                payload.copy_zip(archive, hashlib.sha256(archive.read_bytes()).hexdigest(),
                                 {"runtime.dll": "runtime.dll"}, root)
            self.assertFalse((root / "runtime.dll").exists())

    def test_archive_copy_selects_only_requested_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = root / "test.zip"
            with zipfile.ZipFile(archive, "w") as package:
                package.writestr("runtime.dll", b"runtime")
                package.writestr("weights.gguf", b"excluded")
            records = payload.copy_zip(archive, hashlib.sha256(archive.read_bytes()).hexdigest(),
                                       {"runtime.dll": "runtime.dll"}, root)
            self.assertEqual((root / "runtime.dll").read_bytes(), b"runtime")
            self.assertFalse((root / "weights.gguf").exists())
            self.assertEqual(records[0]["path"], "runtime.dll")

    def test_tar_copy_retains_dependency_licenses_and_rejects_links(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = root / "test.tgz"
            with tarfile.open(archive, "w:gz") as package:
                item = tarfile.TarInfo("licenses/dependency.txt")
                item.size = 7
                package.addfile(item, io.BytesIO(b"license"))
            payload.copy_tar(archive, hashlib.sha256(archive.read_bytes()).hexdigest(), root)
            self.assertEqual((root / "notices/pdfium/licenses/dependency.txt").read_bytes(), b"license")
            with tarfile.open(archive, "w:gz") as package:
                item = tarfile.TarInfo("licenses/linked")
                item.type = tarfile.SYMTYPE
                item.linkname = "../outside"
                package.addfile(item)
            with self.assertRaisesRegex(ValueError, "link|special"):
                payload.copy_tar(archive, hashlib.sha256(archive.read_bytes()).hexdigest(), root)


if __name__ == '__main__':
    unittest.main()
