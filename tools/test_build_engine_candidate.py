"""Release provenance checks must also hold under Python optimization."""
import importlib.util
import io
import json
import os
from pathlib import Path
import struct
import tempfile
import tarfile
import unittest
from unittest.mock import patch
import zipfile

spec = importlib.util.spec_from_file_location("candidate", Path(__file__).with_name("build-engine-candidate.py"))
candidate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(candidate)


class CandidateTests(unittest.TestCase):
    def test_pinned_decoders_stage_without_a_cargo_target(self):
        spec = importlib.util.spec_from_file_location("payload", Path(__file__).with_name("assemble-engine-payload.py"))
        payload = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(payload)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            deployed = root / "binaries"
            deployed.mkdir()
            ffmpeg = root / "ffmpeg.zip"
            with zipfile.ZipFile(ffmpeg, "w") as archive:
                archive.writestr("pinned/bin/ffmpeg.exe", b"generated decoder")
            pdfium = root / "pdfium.tgz"
            with tarfile.open(pdfium, "w:gz") as archive:
                entry = tarfile.TarInfo("bin/pdfium.dll")
                entry.size = len(b"generated pdf")
                archive.addfile(entry, io.BytesIO(b"generated pdf"))
            (root / "ffmpeg.json").write_text(json.dumps(dict(directory="pinned",
                sha256=candidate.digest(ffmpeg), required_binaries=["ffmpeg.exe"])))
            (root / "pdfium.json").write_text(json.dumps(dict(sha256=candidate.digest(pdfium))))
            _, _, records = candidate.stage_decoders(payload, root, ffmpeg, pdfium, deployed)
            self.assertEqual({item["path"] for item in records}, {"ffmpeg.exe", "pdfium.dll"})
            self.assertEqual((deployed / "pdfium.dll").read_bytes(), b"generated pdf")
            ffmpeg.write_bytes(b"tampered")
            with self.assertRaisesRegex(ValueError, "hash"):
                candidate.stage_decoders(payload, root, ffmpeg, pdfium, deployed)

    def test_rejects_failed_validation_even_under_python_optimization(self):
        with self.assertRaisesRegex(ValueError, "failed"):
            candidate.require(False, "failed")

    def test_environment_cannot_override_native_sources_or_flags(self):
        with patch.dict(os.environ, {"LLAMA_CPP_DIR": "other", "WS_EMBED_SKIP_NATIVE": "1",
             "CARGO_BUILD_TARGET": "aarch64-pc-windows-msvc", "RUSTFLAGS": "--cfg other",
             "CMAKE_TOOLCHAIN_FILE": "other", "RUSTC_WRAPPER": "other"}, clear=True):
            env = candidate.clean_environment(Path("sdk"), Path("pinned-llama"))
        self.assertEqual(env["LLAMA_CPP_DIR"], "pinned-llama")
        self.assertEqual(env["XRT_ROOT"], "sdk")
        self.assertEqual(env["WS_EMBED_NO_CUDA"], "1")
        for name in ("WS_EMBED_SKIP_NATIVE", "CARGO_BUILD_TARGET", "RUSTFLAGS", "CMAKE_TOOLCHAIN_FILE", "RUSTC_WRAPPER"):
            self.assertNotIn(name, env)

    def test_rejects_wrong_native_revision_and_dirty_tree(self):
        def wrong(*args):
            return "wrong" if args[-1] == "HEAD" else ""
        with self.assertRaisesRegex(ValueError, "revision"):
            candidate.checked_tree(wrong, Path("source"), "pinned")
        def dirty(*args):
            return "pinned" if args[-1] == "HEAD" else " M source.cpp"
        with self.assertRaisesRegex(ValueError, "dirty"):
            candidate.checked_tree(dirty, Path("source"), "pinned")

    def test_rejects_mislabeled_architecture(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.exe"
            image = bytearray(128)
            image[:2] = b"MZ"
            struct.pack_into("<I", image, 0x3c, 80)
            image[80:86] = b"PE\0\0\x64\xaa"
            path.write_bytes(image)
            with self.assertRaisesRegex(ValueError, "x64"):
                candidate.checked_x64(path)
            image[84:86] = b"\x64\x86"
            path.write_bytes(image)
            candidate.checked_x64(path)


if __name__ == "__main__":
    unittest.main()
