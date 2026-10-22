"""Build and copy a source-bound CPU/NPU candidate; does not qualify or sign it."""
import argparse
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import os
import platform
from pathlib import Path
import shutil
import re
import struct
import subprocess  # nosec B404: local build tool intentionally invokes the installed toolchain.
import zipfile


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def checked_x64(path):
    with path.open("rb") as stream:
        require(stream.read(2) == b"MZ", "candidate is not a PE image")
        stream.seek(0x3c)
        stream.seek(struct.unpack("<I", stream.read(4))[0])
        require(stream.read(6) == b"PE\0\0\x64\x86", "candidate is not x64")


def checked_tree(git, source, revision):
    require(git("-C", str(source), "rev-parse", "HEAD") == revision, "native dependency revision mismatch")
    require(not git("-C", str(source), "status", "--porcelain", "--untracked-files=all"), "native dependency is dirty")
    return {name: digest(source / name) for name in git("-C", str(source), "ls-files").splitlines()}


def clean_environment(sdk, llama):
    prefixes = ("WS_", "TACONITE_", "XRT_", "LLAMA_", "CARGO_BUILD_", "CARGO_TARGET_", "CARGO_PROFILE_", "CMAKE_")
    excluded = {"RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER"}
    env = {key: value for key, value in os.environ.items()
           if not key.upper().startswith(prefixes) and key.upper() not in excluded}
    env.update(XRT_ROOT=str(sdk), LLAMA_CPP_DIR=str(llama), WS_EMBED_NO_CUDA="1",
               WS_EMBED_NO_VULKAN="1", CMAKE_BUILD_PARALLEL_LEVEL="4")
    return env


def stage_decoders(payload, pins, ffmpeg_archive, pdfium_archive, deployed):
    ffmpeg = json.loads((pins / "ffmpeg.json").read_text())
    pdfium = json.loads((pins / "pdfium.json").read_text())
    records = payload.copy_zip(ffmpeg_archive, ffmpeg["sha256"],
        {ffmpeg["directory"] + "/bin/" + name: name for name in ffmpeg["required_binaries"]}, deployed)
    records.extend(payload.copy_tar(pdfium_archive, pdfium["sha256"], deployed))
    require(any(item["path"] == "pdfium.dll" for item in records), "PDFium archive has no decoder")
    return ffmpeg, pdfium, records


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--target-directory", type=Path, required=True)
    parser.add_argument("--xrt-sdk", type=Path, required=True)
    parser.add_argument("--xrt-archive", type=Path, required=True)
    parser.add_argument("--llama-source", type=Path, required=True)
    parser.add_argument("--ffmpeg-archive", type=Path, required=True)
    parser.add_argument("--pdfium-archive", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = args.repository.resolve(strict=True)
    executables = {name: shutil.which(name) for name in ("git", "cargo", "rustc")}
    require(all(executables.values()), "git, cargo and rustc must be installed")
    def git(*arguments):
        return subprocess.check_output([executables["git"], *arguments], cwd=root, text=True).strip()  # nosec B603: resolved Git, argument array, no shell.
    require(os.name == "nt", "Windows native candidate only")
    require(not git("status", "--porcelain"), "candidate requires a clean source checkout")
    head = git("rev-parse", "HEAD")
    files = git("ls-files", "--", "src/engine").splitlines()
    sources = {name:digest(root / name) for name in files if name.endswith((".rs", ".cpp", ".h", ".json", ".toml", ".lock"))}
    pin = json.loads((root / "src/engine/third_party/xrt-sdk.json").read_text())
    require(args.xrt_archive.stat().st_size == pin["sizeBytes"] and digest(args.xrt_archive) == pin["sha256"], "SDK archive differs from the repository pin")
    sdk = args.xrt_sdk.resolve(strict=True)
    sdk_hashes = {}
    prefix = pin["root"] + "/"
    with zipfile.ZipFile(args.xrt_archive) as archive:
        for item in archive.infolist():
            if not item.filename.startswith(prefix) or item.is_dir():
                continue
            relative = item.filename[len(prefix):]
            require(relative and not Path(relative).is_absolute() and ".." not in Path(relative).parts, "candidate validation failed")
            path = sdk / relative
            require(path.is_file() and not path.is_symlink() and path.stat().st_size == item.file_size, "candidate validation failed")
            expected = hashlib.sha256(archive.read(item)).hexdigest()
            require(digest(path) == expected, "extracted SDK differs from its pinned archive")
            sdk_hashes[relative] = expected
    require(sdk_hashes and {p.relative_to(sdk).as_posix() for p in sdk.rglob("*") if p.is_file()} == set(sdk_hashes), "unexpected SDK files")
    llama = args.llama_source.resolve(strict=True)
    revision = re.search(r'const LLAMA_CPP_REV: &str = "([0-9a-f]{40})";',
                         (root / "src/engine/crates/ws-embed/build.rs").read_text()).group(1)
    llama_hashes = checked_tree(git, llama, revision)
    env = clean_environment(sdk, llama)
    rustc = subprocess.check_output([executables["rustc"], "--version", "--verbose"], cwd=root / "src/engine", env=env, text=True)  # nosec B603: resolved toolchain, fixed arguments.
    require("host: x86_64-pc-windows-msvc" in rustc.splitlines(), "candidate requires an x64 MSVC host toolchain")
    spec = importlib.util.spec_from_file_location("payload", root / "tools/assemble-engine-payload.py")
    payload = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(payload)
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    shutil.copyfile(__file__, output / "build-recipe.py")
    deployed = output / "binaries"
    deployed.mkdir()
    # Stage pinned decoders independently of any previous Cargo output.
    ffmpeg, pdfium, dependency_records = stage_decoders(payload, root / "src/engine/third_party",
        args.ffmpeg_archive, args.pdfium_archive, deployed)
    command = [executables["cargo"], "build", "--locked", "--release", "--workspace", "--all-targets", "--all-features", "-j", "4", "--target-dir", str(args.target_directory.resolve())]
    record = dict(scope=__doc__, source_commit=head, native_source_sha256=sources,
                  host=platform.platform(),
                  rustc=rustc,
                  cargo=subprocess.check_output([executables["cargo"], "--version"], cwd=root / "src/engine", env=env, text=True).strip(),  # nosec B603: resolved toolchain, fixed arguments.
                  target="x86_64-pc-windows-msvc", build_profile="release", command=command,
                  xrt_archive_sha256=pin["sha256"], xrt_sdk_sha256=sdk_hashes,
                  llama_commit=revision, llama_source_sha256=llama_hashes,
                  decoder_archive_sha256=dict(ffmpeg=ffmpeg["sha256"], pdfium=pdfium["sha256"]),
                  build_flags=dict(WS_EMBED_NO_CUDA="1", WS_EMBED_NO_VULKAN="1", jobs=4),
                  recipe_sha256=digest(Path(__file__)), started_utc=datetime.now(timezone.utc).isoformat(),
                  passed=False, failures=[])
    try:
        with (output / "build.log").open("wb") as log:
            result = subprocess.run(command, cwd=root / "src/engine", env=env, stdout=log, stderr=subprocess.STDOUT)  # nosec B603: resolved Cargo, fixed build arguments, no shell.
        record["exit_code"] = result.returncode
        require(result.returncode == 0, "native build failed; inspect build.log")
        require(git("rev-parse", "HEAD") == head and not git("status", "--porcelain"), "source changed during build")
        require(all(digest(root / name) == expected for name,expected in sources.items()), "runtime source changed during build")
        require(all(digest(sdk / name) == expected for name,expected in sdk_hashes.items()), "SDK changed during build")
        require(checked_tree(git, llama, revision) == llama_hashes, "llama source changed during build")
        binary = args.target_directory.resolve() / "release"
        record["test_executable_sha256"] = {p.name: digest(p) for p in sorted((binary / "deps").glob("*.exe"))}
        external = {*ffmpeg["required_binaries"], "pdfium.dll"}
        inputs = [binary / name for name in ("winsearchd.exe", "winsearch-cli.exe", "winsearch-embed.exe", "winsearch-extract.exe")]
        inputs.extend(sorted(p for p in binary.glob("*.dll") if p.name not in external))
        record["deployed_sha256"] = {item["path"]: item["sha256"] for item in dependency_records}
        for original in inputs:
            expected = digest(original)
            target = deployed / original.name
            shutil.copyfile(original, target)
            require(digest(original) == expected == digest(target), "binary changed during candidate copy")
            record["deployed_sha256"][original.name] = expected
        for path in deployed.iterdir():
            if path.suffix.lower() in (".exe", ".dll"):
                checked_x64(path)
        runtime = subprocess.run([str(deployed / "winsearch-embed.exe"), "--runtime-info"],
            cwd=deployed, env=env, capture_output=True, text=True, timeout=10, creationflags=subprocess.CREATE_NO_WINDOW)  # nosec B603: executable copied and hash-verified above.
        require(runtime.returncode == 0, "candidate validation failed")
        record["runtime_build"] = json.loads(runtime.stdout)
        require(record["runtime_build"].get("npu_runtime_compiled") is True, "a stub cannot satisfy this candidate profile")
        record["passed"] = True
    except Exception as error:
        record["failures"].append(f"{type(error).__name__}: {error}")
    finally:
        record["completed_utc"] = datetime.now(timezone.utc).isoformat()
        (output / "build-inputs.json").write_text(json.dumps(record, indent=2), encoding="utf-8")
    print(json.dumps(dict(source_commit=head, passed=record["passed"], scope=__doc__)))
    raise SystemExit(0 if record["passed"] else 1)


if __name__ == "__main__":
    main()
