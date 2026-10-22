"""Assemble a pinned, model-free Windows engine payload from explicit local inputs."""
import hashlib
import argparse
import io
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import tarfile
import tempfile
import time
import zipfile

ENGINE_COMMIT = "9b7b01d86dce6c13abdfedc2c842d3095bbd796d"
MAX_FILE_BYTES = 256 * 1024 * 1024
ROOT = Path(__file__).resolve().parents[1]
NATIVE_PROOF = Path('docs/validation/2026-10-22-production-baseline/release-native-inputs.json')
REQUIRED_NPU_SOURCES = {
    'src/engine/third_party/taconite/shim/iron_run_guard.h',
    'src/engine/third_party/taconite/shim/iron_xrt_shim.cpp',
    'src/engine/third_party/taconite/build.rs',
    'src/engine/crates/ws-embed/src/npu.rs',
    'src/engine/bins/winsearch-embed/src/server.rs',
    'src/engine/bins/winsearchd/src/embed_client.rs',
    'src/engine/Cargo.lock',
}


def verified_native_inputs():
    inputs = json.loads((ROOT / NATIVE_PROOF).read_text(encoding='utf-8'))
    if inputs.get('source_commit') != ENGINE_COMMIT or inputs.get('npu_runtime_compiled') is not True:
        raise ValueError('NPU source proof or real-shim build mismatch')
    if inputs.get('qualified_routes') != {'text': True, 'imageTokenBudgets': [140, 280],
                                        'cosineThreshold': .999, 'retrievalPassed': True}:
        raise ValueError('NPU reference/retrieval qualification is incomplete')
    sources = inputs.get('native_source_sha256', {})
    if not REQUIRED_NPU_SOURCES <= sources.keys():
        raise ValueError('missing native source proof')
    for name, expected in sources.items():
        safe_archive_path(name)
        with no_links(ROOT / name).open('rb') as stream:
            if hashlib.file_digest(stream, 'sha256').hexdigest() != expected:
                raise ValueError('native source does not match tested binary proof')
    if 'iron_xrt_shim.dll' not in inputs['adjacent_native_dll_sha256']:
        raise ValueError('real NPU shim is missing from binary proof')
    return inputs


def verify_real_npu_runtime(candidate):
    # Run only the hashed copied binary. No inference/model access is needed.
    system = Path(os.environ.get('SystemRoot', r'C:\Windows')) / 'System32'
    env = {key: value for key, value in os.environ.items() if not key.startswith(('WS_', 'TACONITE_', 'XRT_'))}
    env['PATH'] = str(system)
    result = subprocess.run([str(candidate / 'winsearch-embed.exe'), '--runtime-info'],
                            cwd=candidate, env=env, capture_output=True, check=True, timeout=10)
    if json.loads(result.stdout).get('npu_runtime_compiled') is not True:
        raise ValueError('NPU payload refuses a stub runtime build')


def no_links(path):
    path = Path(os.path.abspath(path))
    for current in [path, *path.parents]:
        info = current.lstat()
        if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
            raise ValueError("link/reparse path rejected")
    return path


def checked_copy(source, destination, expected):
    source, destination = no_links(source), Path(destination)
    no_links(destination.parent)
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(destination)
    if not source.is_file():
        raise ValueError("source must be a regular file")
    with source.open("rb") as incoming:
        return write_stream(incoming, destination, expected)


def write_stream(incoming, destination, expected=None):
    destination = Path(destination)
    no_links(destination.parent)
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(destination)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=destination.parent, prefix=".payload-", delete=False
        ) as outgoing:
            temporary = Path(outgoing.name)
            digest, size = hashlib.sha256(), 0
            while block := incoming.read(65536):
                size += len(block)
                if size > MAX_FILE_BYTES:
                    raise ValueError("payload file exceeds size bound")
                digest.update(block)
                outgoing.write(block)
        actual = digest.hexdigest()
        if size == 0 or (expected is not None and actual != expected):
            raise ValueError("payload file hash/size mismatch")
        # Windows rename fails if the destination appeared while copying.
        if destination.exists():
            raise FileExistsError(destination)
        temporary.rename(destination)
        return {"sha256": actual, "sizeBytes": size}
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()

def require_commit(commit):
    if commit != ENGINE_COMMIT:
        raise ValueError("engine commit does not match verified binary checkpoint")


def publish_directory(source, target):
    for attempt in range(6):
        if target.exists() or target.is_symlink():
            raise FileExistsError(target)
        try:
            source.rename(target)
            return
        except PermissionError as error:
            if getattr(error, "winerror", None) not in (5, 32) or attempt == 5:
                raise
            time.sleep(0.1 * 2**attempt)


def safe_archive_path(name):
    parts = name.rstrip("/").split("/")
    if not name or name.startswith("/") or any(
        part in ("", ".", "..") or part.endswith((".", " "))
        or re.search(r'[\\<>:"|?*\x00-\x1f]', part)
        or re.match(r"^(con|prn|aux|nul|com[0-9]|lpt[0-9])(?:\.|$)", part, re.I)
        for part in parts
    ):
        raise ValueError("unsafe archive path")
    return name.rstrip("/")


def verified_archive(path, expected):
    stream = no_links(path).open("rb")
    try:
        if hashlib.file_digest(stream, "sha256").hexdigest() != expected:
            raise ValueError("dependency archive hash mismatch")
        stream.seek(0)
        return stream
    except BaseException:
        stream.close()
        raise


def validate_entries(entries):
    seen, total = set(), 0
    if not entries or len(entries) > 20000:
        raise ValueError("archive entry count exceeds bound")
    for name, size, regular in entries:
        folded = safe_archive_path(name).casefold()
        if folded in seen:
            raise ValueError("duplicate/case alias archive path")
        seen.add(folded)
        total += size
        if not regular:
            raise ValueError("archive link/special file rejected")
        if size < 0 or size > MAX_FILE_BYTES or total > 2 * 1024**3:
            raise ValueError("archive expansion exceeds bound")


def archive_destination(root, relative):
    safe_archive_path(relative)
    target = root / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    no_links(target.parent)
    return target


def copy_zip(path, expected, selected, destination):
    with verified_archive(path, expected) as stream, zipfile.ZipFile(stream) as archive:
        infos = archive.infolist()
        validate_entries([(entry.filename, entry.file_size,
                           (entry.external_attr >> 16) & 0xF000 in (0, 0x4000, 0x8000)
                           and not entry.external_attr & 0x400) for entry in infos])
        records = []
        for source, target in selected.items():
            with archive.open(source) as incoming:
                record = write_stream(incoming, archive_destination(destination, target))
            records.append({"path": target, **record})
        return records


def copy_tar(path, expected, destination):
    with verified_archive(path, expected) as stream, tarfile.open(fileobj=stream, mode="r:gz") as archive:
        members = archive.getmembers()
        validate_entries([(item.name, item.size, item.isfile() or item.isdir()) for item in members])
        records = []
        for item in members:
            if item.isdir():
                continue
            target = "pdfium.dll" if item.name == "bin/pdfium.dll" else (
                "notices/pdfium/" + item.name if item.name == "LICENSE" or item.name.startswith("licenses/") else None
            )
            if target is not None:
                with archive.extractfile(item) as incoming:
                    record = write_stream(incoming, archive_destination(destination, target))
                records.append({"path": target, **record})
        return records


def assemble(binaries, ffmpeg_archive, pdfium_archive, msvc_archive, llama_source, output):
    """All expected hashes come from reviewed repository pins, never input metadata."""
    output = Path(os.path.abspath(output))
    no_links(output.parent)
    if output.exists() or output.is_symlink():
        raise FileExistsError(output)
    pins = ROOT / "src/engine/third_party"
    ffmpeg = json.loads((pins / "ffmpeg.json").read_text(encoding="utf-8"))
    pdfium = json.loads((pins / "pdfium.json").read_text(encoding="utf-8"))
    msvc = json.loads((pins / "msvc-runtime.json").read_text(encoding="utf-8"))
    inputs = verified_native_inputs()
    archive_libraries = {"pdfium.dll", *ffmpeg["required_binaries"], *msvc["required_binaries"]}
    candidate = Path(tempfile.mkdtemp(prefix=".engine-payload-", dir=output.parent))
    try:
        records = []
        binaries = no_links(binaries)
        for name in ("winsearchd.exe", "winsearch-embed.exe", "winsearch-extract.exe"):
            records.append({"path": name, **checked_copy(binaries / name, candidate / name, inputs["binary_sha256"][name])})
        for name, expected in inputs["adjacent_native_dll_sha256"].items():
            if name != "ggml-cuda.dll" and name not in archive_libraries:
                records.append({"path": name, **checked_copy(binaries / name, candidate / name, expected)})
        records.extend(copy_zip(ffmpeg_archive, ffmpeg["sha256"], {
            **{ffmpeg["directory"] + "/bin/" + name: name for name in ffmpeg["required_binaries"]},
            ffmpeg["directory"] + "/LICENSE.txt": "notices/ffmpeg/LICENSE.txt",
        }, candidate))
        pdfium_records = copy_tar(pdfium_archive, pdfium["sha256"], candidate)
        if not any(item["path"] == "pdfium.dll" and item["sha256"] == inputs["adjacent_native_dll_sha256"]["pdfium.dll"] for item in pdfium_records):
            raise ValueError("PDFium DLL does not match verified native inputs")
        records.extend(pdfium_records)
        records.extend(copy_zip(msvc_archive, msvc["sha256"], {
            msvc["directory"] + "/" + name: name for name in msvc["required_binaries"]
        }, candidate))
        for record in records:
            expected = inputs["adjacent_native_dll_sha256"].get(record["path"])
            if expected is not None and record["sha256"] != expected:
                raise ValueError("native library does not match verified inputs")
        llama_source = no_links(llama_source)
        license_bytes = subprocess.check_output(["git", "-C", str(llama_source), "show", "4fbc76dec51d0add466f0210855c0596589b60d4:LICENSE"])
        notices = {
            ROOT / "THIRD-PARTY-NOTICES.txt": "THIRD-PARTY-NOTICES.txt",
            pins / "licenses/FFmpeg-GPLv3.txt": "notices/ffmpeg/COPYING.GPLv3",
            pins / "ffmpeg-source.md": "notices/ffmpeg/SOURCE-INSTRUCTIONS.md",
            pins / "msvc-runtime.md": "notices/msvc/REDISTRIBUTION.md",
            pins / "code-grammars-LICENSES.txt": "notices/code-grammars-LICENSES.txt",
            ROOT / "src/engine/crates/ws-embed/tests/fixtures/media/LICENSE-DigUp.txt": "notices/DigUp-LICENSE.txt",
            pins / 'taconite/LICENSE': 'notices/taconite-LICENSE',
            **{pins / 'licenses/taconite-embeddinggemma2' / name: 'notices/embeddinggemma2-' + name
               for name in ['LICENSE', 'LICENSE-PILLOW', 'LICENSE-PYTORCH']},
            pins / 'licenses/taconite-bundle/LICENSE': 'notices/taconite-bundle-LICENSE',
            pins / 'licenses/XRT-LICENSE': 'notices/XRT-LICENSE',
        }
        for source, target in notices.items():
            records.append({"path": target, **checked_copy(source, archive_destination(candidate, target), None)})
        target = "notices/llama.cpp-LICENSE"
        records.append({"path": target, **write_stream(io.BytesIO(license_bytes), archive_destination(candidate, target))})
        verify_real_npu_runtime(candidate)
        manifest = {"schemaVersion": 1, "engineCommit": ENGINE_COMMIT,
                    "target": "x86_64-pc-windows-msvc", "files": sorted(records, key=lambda item: item["path"])}
        encoded = (json.dumps(manifest, indent=2, ensure_ascii=True) + "\n").encode("utf-8")
        (candidate / "payload.json").write_bytes(encoded)
        publish_directory(candidate, output)
        return {"manifestSha256": hashlib.sha256(encoded).hexdigest(), "files": len(records),
                "sizeBytes": sum(item["sizeBytes"] for item in records)}
    finally:
        if candidate.exists():
            no_links(candidate)
            if candidate.resolve().parent != output.parent.resolve():
                raise ValueError("unsafe assembly cleanup path")
            shutil.rmtree(candidate)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ("binaries", "ffmpeg-archive", "pdfium-archive", "msvc-archive", "llama-source", "output"):
        parser.add_argument("--" + flag, type=Path, required=True)
    parser.add_argument("--engine-commit", required=True)
    args = parser.parse_args()
    require_commit(args.engine_commit)
    print(json.dumps(assemble(args.binaries, args.ffmpeg_archive, args.pdfium_archive,
                              args.msvc_archive, args.llama_source, args.output)))


if __name__ == "__main__":
    main()
