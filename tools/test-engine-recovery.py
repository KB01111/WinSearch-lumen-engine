"""Owned native reset/restart/profile-isolation and unavailable-log-sink fixtures.

No model quality, installed migration, physical driver loss or low-disk acceptance.
"""
import argparse
from contextlib import closing, ExitStack
import csv
import hashlib
import importlib.util
import json
import os
import platform
from pathlib import Path
import shutil
import sqlite3
import subprocess
import tempfile
import threading
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("parity", ROOT / "tools/test-search-parity.py")
parity = importlib.util.module_from_spec(spec)
spec.loader.exec_module(parity)


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


class Profile:
    def __init__(self, name, output, deployed, sid):
        self.owner = parity.harness.OwnedJob()
        self.data = output / name
        self.data.mkdir()
        self.deployed, self.child, self.peer = deployed, None, None
        self.env = {k: v for k, v in os.environ.items() if not k.upper().startswith(("WS_", "WINSEARCH_"))}
        self.env.update(WINSEARCH_DATA_DIR=str(self.data), WINSEARCH_INSTANCE_ID="recovery-" + uuid.uuid4().hex,
                        RUST_LOG="winsearchd=info,winsearchd::indexer=debug,ws_index=warn,ws_platform=warn")
        self.pipe = r"\\.\pipe\winsearch-" + sid + "-" + self.env["WINSEARCH_INSTANCE_ID"]

    def start(self):
        with (self.data / "startup.log").open("ab") as log:
            self.child = subprocess.Popen([str(self.deployed / "winsearchd.exe"), "--defer-root-admission"],
                cwd=self.deployed, env=self.env, stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                creationflags=subprocess.CREATE_NO_WINDOW | 0x00000004)
        self.owner.assign(self.child)
        self.owner.resume(self.child)
        self.peer = parity.Frames(self.pipe, self.child)
        self.peer.request("set_config", config=dict(semantic_enabled=False, preferred_backend="cpu"))

    def configure(self, root):
        config = self.peer.request("get_indexing_config")["config"]
        config.update(roots=[str(root)], root_policies=[], code_roots=[], code_enabled=False,
                      paused=False, index_on_battery=True, respect_battery_saver=False)
        self.peer.request("set_indexing_config", config=config)
        return config

    def status(self):
        return self.peer.request("get_status")["status"]

    def raw(self, kind, **fields):
        self.peer.sequence += 1
        request = self.peer.sequence
        self.peer.send(dict(type=kind, id=request, **fields))
        while True:
            reply = self.peer.read()
            if reply.get("id") == request:
                return reply

    def query(self, text):
        frames = self.peer.search_frames(dict(text=text, mode="files", limit=20, include_semantic=False))
        if frames[-1]["outcome"]["disposition"] == "superseded":
            return []
        assert frames[-1]["outcome"]["disposition"] == "complete"
        return [hit for frame in frames for hit in frame["batch"]]

    def await_content(self, text):
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            hits = self.query(text)
            if len(hits) == 1 and hits[0].get("entry_reference"):
                return hits[0]["entry_reference"]
            time.sleep(.05)
        raise AssertionError("current generated content did not become searchable")

    def stop(self):
        if self.peer:
            self.peer.stream.close()
            self.peer = None
        started = time.perf_counter()
        self.owner.terminate()
        if self.child:
            self.child.wait(timeout=5)
        deadline = time.monotonic() + 5
        while self.owner.sample() and time.monotonic() < deadline:
            time.sleep(.01)
        elapsed = (time.perf_counter() - started) * 1000
        assert not self.owner.sample(), "owned worker survived Stop"
        return elapsed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary-directory", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--work-directory", type=Path, required=True)
    parser.add_argument("--build-profile", choices=("debug", "release"), required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    assert output.is_relative_to(ROOT / "artifacts") and not output.exists()
    output.mkdir(parents=True)
    source_files = subprocess.check_output(["git", "ls-files", "--cached", "--others", "--exclude-standard", "--", "src/engine"], cwd=ROOT, text=True).splitlines()
    report = dict(scope=__doc__, checks=[], failures=[], passed=False, harness_sha256=digest(Path(__file__)),
                  host=platform.platform(), build_profile=args.build_profile,
                  native_source_sha256={name:digest(ROOT / name) for name in source_files if name.endswith((".rs", ".toml", ".lock", ".cpp", ".h", ".json"))},
                  deployed_sha256={}, owned_jobs_empty=False, source_commit=subprocess.check_output(
                      ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                  source_diff_sha256=hashlib.sha256(subprocess.check_output(["git", "diff", "HEAD"], cwd=ROOT)).hexdigest())
    profiles = []
    watchdog = threading.Timer(120, lambda: [p.owner.terminate() for p in profiles])
    watchdog.daemon = True
    def stop_profiles():
        for profile in profiles:
            profile.stop()
    try:
        with ExitStack() as cleanup:
            directory = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="winsearch-recovery-", dir=args.work_directory.resolve(strict=True)))
            # Release executable/DLL mappings before temporary deployment cleanup,
            # including a failed assertion halfway through the native scenario.
            cleanup.callback(stop_profiles)
            work = Path(directory)
            deployed = work / "bin"
            deployed.mkdir()
            files = [args.binary_directory / name for name in ("winsearchd.exe", "winsearch-cli.exe", "winsearch-embed.exe", "winsearch-extract.exe")]
            files.extend(sorted(args.binary_directory.glob("*.dll")))
            for original in files:
                expected = digest(original)
                shutil.copyfile(original, deployed / original.name)
                assert digest(deployed / original.name) == expected
                report["deployed_sha256"][original.name] = expected
            runtime = subprocess.run([str(deployed / "winsearch-embed.exe"), "--runtime-info"],
                capture_output=True, text=True, timeout=10, creationflags=subprocess.CREATE_NO_WINDOW)
            assert runtime.returncode == 0
            report["runtime_build"] = json.loads(runtime.stdout)
            system = Path(os.environ["SystemRoot"]) / "System32"
            sid = next(csv.reader(subprocess.check_output([str(system / "whoami.exe"), "/user", "/fo", "csv", "/nh"], text=True).splitlines()))[1]
            profiles = [Profile(name, output, deployed, sid) for name in ("a", "b")]
            a, b = profiles
            (b.data / "logs").write_bytes(b"generated unavailable-log-directory fixture")
            roots = []
            for name in ("a", "b"):
                root = work / name
                root.mkdir()
                (root / "notes.txt").write_text("nativeresetfixture" + name, encoding="utf-8")
                roots.append(root)
            watchdog.start()
            for profile, root in zip(profiles, roots):
                profile.start()
                assert not profile.query("notes")
                profile.configure(root)
            a_ref, b_ref = a.await_content("nativeresetfixturea"), b.await_content("nativeresetfixtureb")
            report["checks"].append("blocked logging permits actual native search; two owned profiles admit independent roots")
            before_a, before_b = a.status(), b.status()
            assert before_a["index_id"] != before_b["index_id"]
            sentinel = a.data / "models/preserved-fixture.txt"
            sentinel.write_bytes(b"owned model-policy preservation fixture")
            policy = a.peer.request("get_indexing_config")["config"]
            request = dict(expected_index_id=before_a["index_id"], expected_code_index_id=before_a["code_index_id"])
            reply = a.peer.request("reset_index", request=request)["reset"]
            assert reply["index_id"] != before_a["index_id"] and reply["code_index_id"] != before_a["code_index_id"]
            assert a.raw("resolve_entry", reference=a_ref)["type"] == "error"
            assert a.raw("reset_index", request=request)["type"] == "error"
            assert a.status()["index_id"] == reply["index_id"]
            assert a.peer.request("get_indexing_config")["config"] == policy
            assert sentinel.read_bytes() == b"owned model-policy preservation fixture"
            assert b.status()["index_id"] == before_b["index_id"]
            assert b.status()["code_index_id"] == before_b["code_index_id"]
            assert b.raw("resolve_entry", reference=b_ref)["type"] == "resolved_entry"
            report["checks"].append("reset changes both namespaces, rejects stale references/replay and preserves policy/models/other profile")
            report["independent_stop_ms"] = a.stop()
            assert report["independent_stop_ms"] <= 1000
            assert b.await_content("nativeresetfixtureb")
            a.start()
            assert a.status()["index_id"] == reply["index_id"]
            assert not a.query("notes"), "restart must await current root grants"
            a.configure(roots[0])
            a.await_content("nativeresetfixturea")
            report["checks"].append("post-reset restart retains namespace and requires fresh root admission; independent Stop leaves profile B searchable")
            result = subprocess.run([str(deployed / "winsearch-cli.exe"), "diagnostics"],
                env={**a.env, "WS_PIPE": a.pipe}, capture_output=True, text=True, timeout=12)
            assert result.returncode == 0
            diagnostics = json.loads(result.stdout)
            assert diagnostics["available"] and diagnostics["files_indexed"] > 0
            assert "current_activity" not in diagnostics and "index_id" not in diagnostics
            assert str(work) not in result.stdout and str(output) not in result.stdout
            report["diagnostics"] = diagnostics
            report["checks"].append("actual CLI exports only sanitized status fields")
            for profile in profiles:
                profile.stop()
                for name in ("index.db", "code.db"):
                    with closing(sqlite3.connect(f"file:{(profile.data / 'db' / name).as_posix()}?mode=ro", uri=True)) as database:
                        assert database.execute("PRAGMA integrity_check").fetchone() == ("ok",)
            report["checks"].append("both ordinary/code stores pass integrity_check after Stop/restart")
            report["passed"] = True
    except Exception as error:
        report["failures"].append(f"{type(error).__name__}: {error}")
    finally:
        watchdog.cancel()
        if watchdog.ident:
            watchdog.join()
        empty = True
        for profile in profiles:
            try:
                profile.stop()
                empty &= not profile.owner.sample()
            except Exception as error:
                empty = False
                report["failures"].append(f"cleanup: {type(error).__name__}: {error}")
            finally:
                profile.owner.close()
        report["owned_jobs_empty"] = empty
        report["passed"] &= empty and not report["failures"]
        (output / "checks.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    raise SystemExit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
