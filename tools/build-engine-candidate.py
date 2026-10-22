"""Build and copy a source-bound CPU/NPU candidate; does not qualify or sign it."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
import platform
from pathlib import Path
import shutil
import subprocess
import zipfile


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--target-directory", type=Path, required=True)
    parser.add_argument("--xrt-sdk", type=Path, required=True)
    parser.add_argument("--xrt-archive", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = args.repository.resolve(strict=True)
    def git(*arguments):
        return subprocess.check_output(["git", *arguments], cwd=root, text=True).strip()
    assert os.name == "nt", "Windows native candidate only"
    assert not git("status", "--porcelain"), "candidate requires a clean source checkout"
    head = git("rev-parse", "HEAD")
    files = git("ls-files", "--", "src/engine").splitlines()
    sources = {name:digest(root / name) for name in files if name.endswith((".rs", ".cpp", ".h", ".json", ".toml", ".lock"))}
    pin = json.loads((root / "src/engine/third_party/xrt-sdk.json").read_text())
    assert args.xrt_archive.stat().st_size == pin["sizeBytes"] and digest(args.xrt_archive) == pin["sha256"], "SDK archive differs from the repository pin"
    sdk = args.xrt_sdk.resolve(strict=True)
    sdk_hashes = {}
    prefix = pin["root"] + "/"
    with zipfile.ZipFile(args.xrt_archive) as archive:
        for item in archive.infolist():
            if not item.filename.startswith(prefix) or item.is_dir():
                continue
            relative = item.filename[len(prefix):]
            assert relative and not Path(relative).is_absolute() and ".." not in Path(relative).parts
            path = sdk / relative
            assert path.is_file() and not path.is_symlink() and path.stat().st_size == item.file_size
            expected = hashlib.sha256(archive.read(item)).hexdigest()
            assert digest(path) == expected, "extracted SDK differs from its pinned archive"
            sdk_hashes[relative] = expected
    assert sdk_hashes and {p.relative_to(sdk).as_posix() for p in sdk.rglob("*") if p.is_file()} == set(sdk_hashes), "unexpected SDK files"
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    shutil.copyfile(__file__, output / "build-recipe.py")
    env = {key:value for key,value in os.environ.items() if not key.upper().startswith(("WS_", "TACONITE_", "XRT_"))}
    env.update(XRT_ROOT=str(sdk), WS_EMBED_NO_CUDA="1", WS_EMBED_NO_VULKAN="1", CMAKE_BUILD_PARALLEL_LEVEL="4")
    command = ["cargo", "build", "--locked", "--release", "--workspace", "--all-targets", "--all-features", "-j", "4", "--target-dir", str(args.target_directory.resolve())]
    record = dict(scope=__doc__, source_commit=head, native_source_sha256=sources,
                  host=platform.platform(),
                  rustc=subprocess.check_output(["rustc", "--version", "--verbose"], cwd=root / "src/engine", env=env, text=True),
                  cargo=subprocess.check_output(["cargo", "--version"], cwd=root / "src/engine", env=env, text=True).strip(),
                  target="x86_64-pc-windows-msvc", build_profile="release", command=command,
                  xrt_archive_sha256=pin["sha256"], xrt_sdk_sha256=sdk_hashes,
                  build_flags=dict(WS_EMBED_NO_CUDA="1", WS_EMBED_NO_VULKAN="1", jobs=4),
                  recipe_sha256=digest(Path(__file__)), started_utc=datetime.now(timezone.utc).isoformat(),
                  passed=False, failures=[])
    try:
        with (output / "build.log").open("wb") as log:
            result = subprocess.run(command, cwd=root / "src/engine", env=env, stdout=log, stderr=subprocess.STDOUT)
        record["exit_code"] = result.returncode
        assert result.returncode == 0, "native build failed; inspect build.log"
        assert git("rev-parse", "HEAD") == head and not git("status", "--porcelain"), "source changed during build"
        assert all(digest(root / name) == expected for name,expected in sources.items()), "runtime source changed during build"
        assert all(digest(sdk / name) == expected for name,expected in sdk_hashes.items()), "SDK changed during build"
        binary = args.target_directory.resolve() / "release"
        deployed = output / "binaries"
        deployed.mkdir()
        inputs = [binary / name for name in ("winsearchd.exe", "winsearch-cli.exe", "winsearch-embed.exe", "winsearch-extract.exe", "ffmpeg.exe", "ffprobe.exe")]
        inputs.extend(sorted(binary.glob("*.dll")))
        record["deployed_sha256"] = {}
        for original in inputs:
            expected = digest(original)
            target = deployed / original.name
            shutil.copyfile(original, target)
            assert digest(original) == expected == digest(target), "binary changed during candidate copy"
            record["deployed_sha256"][original.name] = expected
        runtime = subprocess.run([str(deployed / "winsearch-embed.exe"), "--runtime-info"],
            cwd=deployed, env=env, capture_output=True, text=True, timeout=10, creationflags=subprocess.CREATE_NO_WINDOW)
        assert runtime.returncode == 0
        record["runtime_build"] = json.loads(runtime.stdout)
        assert record["runtime_build"].get("npu_runtime_compiled") is True, "a stub cannot satisfy this candidate profile"
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
