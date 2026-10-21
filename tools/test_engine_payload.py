import importlib.util
import hashlib
import io
import json
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
    def assembly_fixture(self, root, *, mismatched_codec=False):
        pins = root / 'src/engine/third_party'
        binaries = root / 'binaries'
        pins.mkdir(parents=True)
        binaries.mkdir()
        (root / 'llama').mkdir()
        sha = lambda content: hashlib.sha256(content).hexdigest()
        native = {'iron_xrt_shim.dll': b'real shim', 'llama.dll': b'inference', 'pdfium.dll': b'pdf',
                  'codec.dll': b'wrong codec' if mismatched_codec else b'codec',
                  'vcruntime140.dll': b'crt'}
        executables = {name: name.encode() for name in
                      ('winsearchd.exe', 'winsearch-embed.exe', 'winsearch-extract.exe')}
        for name, content in {**native, **executables}.items():
            (binaries / name).write_bytes(content)
        for name in payload.REQUIRED_NPU_SOURCES:
            source = root / name
            source.parent.mkdir(parents=True, exist_ok=True)
            source.write_bytes(b'fixture source')
        inputs = root / payload.NATIVE_PROOF
        inputs.parent.mkdir(parents=True)
        inputs.write_text(json.dumps({
            'source_commit': payload.ENGINE_COMMIT, 'npu_runtime_compiled': True,
            'qualified_routes': {'text': True, 'imageTokenBudgets': [140, 280], 'cosineThreshold': .999, 'retrievalPassed': True},
            'native_source_sha256': {name: sha(b'fixture source') for name in payload.REQUIRED_NPU_SOURCES},
            'binary_sha256': {name: sha(content) for name, content in executables.items()},
            'adjacent_native_dll_sha256': {name: sha(content) for name, content in native.items()}}), encoding='utf-8')
        for file in ('THIRD-PARTY-NOTICES.txt', 'src/engine/third_party/licenses/FFmpeg-GPLv3.txt',
                     'src/engine/third_party/ffmpeg-source.md', 'src/engine/third_party/msvc-runtime.md',
                     'src/engine/third_party/code-grammars-LICENSES.txt',
                     'src/engine/third_party/taconite/LICENSE',
                     'src/engine/third_party/licenses/taconite-embeddinggemma2/LICENSE',
                     'src/engine/third_party/licenses/taconite-embeddinggemma2/LICENSE-PILLOW',
                     'src/engine/third_party/licenses/taconite-embeddinggemma2/LICENSE-PYTORCH',
                     'src/engine/third_party/licenses/taconite-bundle/LICENSE',
                     'src/engine/third_party/licenses/XRT-LICENSE',
                     'src/engine/crates/ws-embed/tests/fixtures/media/LICENSE-DigUp.txt'):
            path = root / file
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b'fixture notice')
        ffmpeg = root / 'ffmpeg.zip'
        decoder_files = {'ffmpeg.exe': b'decoder', 'ffprobe.exe': b'probe', 'codec.dll': b'codec'}
        with zipfile.ZipFile(ffmpeg, 'w') as archive:
            for name, content in decoder_files.items():
                archive.writestr('ffmpeg/bin/' + name, content)
            archive.writestr('ffmpeg/LICENSE.txt', b'decoder license')
        msvc = root / 'msvc.zip'
        with zipfile.ZipFile(msvc, 'w') as archive:
            archive.writestr('crt/vcruntime140.dll', b'crt')
        pdfium = root / 'pdfium.tgz'
        with tarfile.open(pdfium, 'w:gz') as archive:
            for name, content in {'bin/pdfium.dll': b'pdf', 'LICENSE': b'pdf license'}.items():
                item = tarfile.TarInfo(name)
                item.size = len(content)
                archive.addfile(item, io.BytesIO(content))
        for name, value in {
            'ffmpeg': {'directory': 'ffmpeg', 'required_binaries': list(decoder_files), 'sha256': sha(ffmpeg.read_bytes())},
            'pdfium': {'sha256': sha(pdfium.read_bytes())},
            'msvc-runtime': {'directory': 'crt', 'required_binaries': ['vcruntime140.dll'], 'sha256': sha(msvc.read_bytes())},
        }.items():
            (pins / (name + '.json')).write_text(json.dumps(value), encoding='utf-8')
        return binaries, ffmpeg, pdfium, msvc, root / 'llama', root / 'output'

    def test_assembly_accepts_archive_libraries_already_staged_beside_binaries(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            inputs = self.assembly_fixture(root)
            with patch.object(payload, 'ROOT', root), patch.object(payload, 'verify_real_npu_runtime'), patch.object(payload.subprocess, 'check_output', return_value=b'llama license'):
                result = payload.assemble(*inputs)
            manifest_bytes = (root / 'output/payload.json').read_bytes()
            files = json.loads(manifest_bytes)['files']
            self.assertEqual(result['manifestSha256'], hashlib.sha256(manifest_bytes).hexdigest())
            self.assertEqual(len({file['path'].casefold() for file in files}), len(files))
            for name, content in {'codec.dll': b'codec', 'vcruntime140.dll': b'crt', 'llama.dll': b'inference'}.items():
                self.assertEqual((root / 'output' / name).read_bytes(), content)

    def test_assembly_rejects_staged_archive_library_with_different_verified_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            inputs = self.assembly_fixture(root, mismatched_codec=True)
            with patch.object(payload, 'ROOT', root), patch.object(payload.subprocess, 'check_output', return_value=b'llama license'):
                with self.assertRaisesRegex(ValueError, 'native.*match'):
                    payload.assemble(*inputs)
            self.assertFalse((root / 'output').exists())

    def test_npu_proof_rejects_stub_missing_shim_stale_binary_and_changed_source(self):
        for mutation in ['stub', 'missing', 'binary', 'source', 'commit', 'parity']:
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                inputs = self.assembly_fixture(root)
                proof_path = root / payload.NATIVE_PROOF
                proof = json.loads(proof_path.read_text())
                if mutation == 'stub': proof['npu_runtime_compiled'] = False
                elif mutation == 'missing': del proof['adjacent_native_dll_sha256']['iron_xrt_shim.dll']
                elif mutation == 'commit': proof['source_commit'] = '0' * 40
                elif mutation == 'parity': proof['qualified_routes']['retrievalPassed'] = False
                elif mutation == 'binary': (inputs[0] / 'iron_xrt_shim.dll').write_bytes(b'stale')
                elif mutation == 'source': (root / next(iter(proof['native_source_sha256']))).write_bytes(b'changed')
                proof_path.write_text(json.dumps(proof))
                with patch.object(payload, 'ROOT', root), self.assertRaises(ValueError):
                    payload.assemble(*inputs)
                self.assertFalse((root / 'output').exists())

    def test_runtime_info_refuses_stub_even_with_pinned_dll(self):
        import subprocess
        with patch.object(payload.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0, b'{"npu_runtime_compiled":false}')):
            with self.assertRaisesRegex(ValueError, 'stub'):
                payload.verify_real_npu_runtime(Path('copied-package'))

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
