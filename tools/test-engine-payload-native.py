"""Run the actual pinned payload outside the checkout, with only Windows on PATH.

This is no-model package acceptance, not installed Lumen or model-quality evidence.
"""
import argparse
import csv
from contextlib import closing
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
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
spec = importlib.util.spec_from_file_location("payload", ROOT / "tools/assemble-engine-payload.py")
payload = importlib.util.module_from_spec(spec)
spec.loader.exec_module(payload)


def sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--payload", type=Path, required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    source = payload.no_links(args.payload)
    assert sha(source / "payload.json") == args.manifest_sha256
    manifest = json.loads((source / "payload.json").read_text(encoding="utf-8"))
    payload.require_commit(manifest["engineCommit"])
    children, clients = [], []
    result = {"scope": __doc__, "manifestSha256": args.manifest_sha256,
              "engineCommit": manifest["engineCommit"], "checks": [], "ownedJobEmpty": False, "passed": False,
              "timedOut": False, "queryFrames": []}
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="winsearch-package-native-") as directory:
        working = Path(directory)
        assert not working.is_relative_to(ROOT)
        deployed = working / "engine"
        deployed.mkdir()
        for item in manifest["files"]:
            target = payload.archive_destination(deployed, item["path"])
            payload.checked_copy(source / item["path"], target, item["sha256"])
        shutil.copyfile(source / "payload.json", deployed / "payload.json")
        roots = working / "selected"
        roots.mkdir()
        (roots / "notes.txt").write_text("packagecontenttoken Generated local package fixture.\n", encoding="utf-8")
        parity.pdf(roots / "pages.pdf")
        for number in range(1100):
            (roots / f"budget-source-{number:04}.bin").write_bytes(b"x")
        (roots / "policy-visible.txt").write_text("visible fixture", encoding="utf-8")
        (roots / ".policy-hidden.txt").write_text("hidden fixture", encoding="utf-8")
        (roots / "policy-size-denied.txt").write_bytes(b"x" * 4097)
        (roots / "excluded").mkdir()
        (roots / "excluded/policy-excluded.txt").write_text("excluded fixture", encoding="utf-8")
        environment = {key: value for key, value in os.environ.items()
                       if key.upper() != "PATH" and not key.upper().startswith(("WS_", "WINSEARCH_"))}
        system32 = Path(os.environ["SystemRoot"]) / "System32"
        environment["PATH"] = str(system32)
        environment["WINSEARCH_DATA_DIR"] = str(working / "data")
        environment["WINSEARCH_INSTANCE_ID"] = "package-" + uuid.uuid4().hex
        sid = next(csv.reader(subprocess.check_output(
            [str(system32 / "whoami.exe"), "/user", "/fo", "csv", "/nh"], text=True).splitlines()))[1]
        pipe = r"\\.\pipe\winsearch-" + sid + "-" + environment["WINSEARCH_INSTANCE_ID"]
        owner = parity.harness.OwnedJob()
        def stop():
            result["timedOut"] = True
            owner.terminate()
        timer = threading.Timer(90, stop)
        timer.daemon = True
        timer.start()
        try:
            for executable in ("ffmpeg.exe", "ffprobe.exe", "winsearch-embed.exe"):
                version = "--version" if executable.startswith("winsearch") else "-version"
                child = subprocess.Popen([str(deployed / executable), version], cwd=working, env=environment,
                                         stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                         creationflags=subprocess.CREATE_NO_WINDOW)
                children.append(child)
                owner.assign(child)
                stdout, stderr = child.communicate(timeout=10)
                assert child.returncode == 0, (executable, stderr.decode(errors="replace"))
                assert stdout, executable
                result["checks"].append(executable + " starts outside checkout with System32-only PATH")
            with (working / "daemon.log").open("wb") as log:
                child = subprocess.Popen([str(deployed / "winsearchd.exe"), "--test-stdin-gate", "--defer-root-admission"],
                                         cwd=working, env=environment, stdin=subprocess.PIPE,
                                         stdout=log, stderr=log, creationflags=subprocess.CREATE_NO_WINDOW)
            children.append(child)
            owner.assign(child)
            child.stdin.write(b"\1")
            child.stdin.close()
            control = parity.harness.Pipe(pipe, child)
            search = parity.Frames(pipe, child)
            clients.extend([control, search])
            required = {"index.identity", "index.root_policies", "search.limits"}
            assert required.issubset(control.features), ("missing new engine contract", required - set(control.features))
            status = control.request("get_status")["status"]
            assert re.fullmatch("[0-9a-f]{32}", status.get("index_id", "")), "missing ordinary database identity"
            assert control.request("get_status")["status"]["index_id"] == status["index_id"]
            result["checks"].append("identity, per-folder policy and search-limit capabilities are active")
            config = control.request("get_indexing_config")["config"]
            assert not config["roots"] and not config.get("root_policies") and config["paused"]
            rule = {"root": str(roots), "exclusions": ["excluded"], "include_hidden": False, "max_file_size_bytes": 4096}
            config = {**config, "roots": [str(roots)], "paused": False, "root_policies": [rule]}
            control.request("set_indexing_config", config=config)
            deadline = time.monotonic() + 45
            result["supersededRetries"] = 0
            def complete(query):
                while True:
                    result["activeQuery"] = query["text"]
                    request = search.begin(query)
                    frames = []
                    while not frames or not frames[-1]["done"]:
                        frame = search.next_result(request)
                        frames.append(frame)
                        result["queryFrames"].append({"query": query["text"], "count": len(frame["batch"]),
                            "done": frame["done"], "elapsedSeconds": round(time.monotonic() - started, 3)})
                    disposition = frames[-1]["outcome"]["disposition"]
                    assert disposition in ("complete", "superseded"), frames[-1]
                    if disposition == "complete":
                        return frames
                    assert not frames[-1]["batch"], "superseded terminal must be empty"
                    result["supersededRetries"] += 1
                    assert child.poll() is None and time.monotonic() < deadline, "index never yielded a current snapshot"
                    time.sleep(.05)
            for token, expected in [("packagecontenttoken", "notes.txt"), ("pdfparitytoken", "pages.pdf")]:
                while True:
                    frames = complete({"text": token, "mode": "files", "limit": 5, "include_semantic": False})
                    hits = [hit for frame in frames for hit in frame["batch"]]
                    if any(Path(hit.get("path", "")).name == expected for hit in hits):
                        break
                    assert child.poll() is None and time.monotonic() < deadline, (token, hits)
                    time.sleep(.2)
                result["checks"].append("actual confined text/PDF extraction and public exact search: " + expected)
            frames = complete({"text": "policy", "mode": "files", "limit": 10000})
            assert [Path(hit["path"]).name for frame in frames for hit in frame["batch"]] == ["policy-visible.txt"]
            control.request("set_indexing_config", config={**config, "root_policies": [{**rule, "include_hidden": True}]})
            while True:
                frames = complete({"text": "policy-hidden", "mode": "files", "limit": 5})
                hits = [hit for frame in frames for hit in frame["batch"]]
                if frames[-1]["outcome"]["disposition"] == "complete" and hits:
                    assert Path(hits[0]["path"]).name == ".policy-hidden.txt"
                    break
                assert child.poll() is None and time.monotonic() < deadline, "hidden opt-in was not admitted"
                time.sleep(.2)
            result["checks"].append("per-folder hidden, size and exclusion restrictions apply; hidden opt-in reconciles")
            while True:
                frames = complete({"text": "budget-source", "mode": "files", "limit": 1000})
                hits = [hit for frame in frames for hit in frame["batch"]]
                if frames[-1]["outcome"]["disposition"] == "complete" and len(hits) == 1000:
                    break
                assert child.poll() is None and time.monotonic() < deadline, ("large exact search", len(hits))
                time.sleep(.2)
            assert 2 <= len(frames) <= 64 and all(len(frame["batch"]) <= 512 for frame in frames)
            while True:
                frames = complete({"text": "budget-source", "mode": "files", "limit": 10000})
                if sum(len(frame["batch"]) for frame in frames) == 1100:
                    break
                assert child.poll() is None and time.monotonic() < deadline, "10000-limit search lost eligible files"
                time.sleep(.2)
            result["checks"].append("1000 exact results stream in bounded frames; a 10000 limit preserves 1100 eligible files")
            result["checks"].append("deferred startup admits only explicit generated root")
            result["files"] = len(manifest["files"])
            result["passed"] = True
        except BaseException as error:
            result["failure"] = type(error).__name__ + ": " + str(error)
            result["childExitBeforeCleanup"] = children[-1].poll() if children else None
            raise
        finally:
            for client in clients:
                client.stream.close()
            owner.terminate()
            for child in children:
                child.wait(timeout=10)
            deadline = time.monotonic() + 5
            while owner.sample() and time.monotonic() < deadline:
                time.sleep(.05)
            result["ownedJobEmpty"] = not owner.sample()
            assert result["ownedJobEmpty"]
            timer.cancel()
            owner.close()
            result["elapsedSeconds"] = round(time.monotonic() - started, 3)
            result["databaseCounts"] = {}
            for database in (working / "data").rglob("*.db"):
                try:
                    with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True, timeout=1)) as db:
                        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_schema WHERE type='table'")}
                        result["databaseCounts"][database.name] = {name: db.execute("SELECT COUNT(*) FROM " + name).fetchone()[0]
                            for name in ("entries", "segments", "content_chunks", "code_files") if name in tables}
                except sqlite3.Error as error:
                    result["databaseCounts"][database.name] = {"error": type(error).__name__}
            args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
            print(json.dumps(result))


if __name__ == "__main__":
    main()
