"""Run the actual pinned payload outside the checkout, with only Windows on PATH.

This is no-model package acceptance, not installed Lumen or model-quality evidence.
"""
import argparse
import csv
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
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
              "engineCommit": manifest["engineCommit"], "checks": [], "ownedJobEmpty": False}
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
        timer = threading.Timer(90, owner.terminate)
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
            config = control.request("get_indexing_config")["config"]
            assert not config["roots"] and config["paused"]
            control.request("set_indexing_config", config={**config, "roots": [str(roots)], "paused": False})
            deadline = time.monotonic() + 45
            for token, expected in [("packagecontenttoken", "notes.txt"), ("pdfparitytoken", "pages.pdf")]:
                while True:
                    frames = search.search_frames({"text": token, "mode": "files", "limit": 5, "include_semantic": False})
                    assert frames[-1]["outcome"]["disposition"] == "complete", frames[-1]
                    hits = [hit for frame in frames for hit in frame["batch"]]
                    if any(Path(hit.get("path", "")).name == expected for hit in hits):
                        break
                    assert child.poll() is None and time.monotonic() < deadline, (token, hits)
                    time.sleep(.2)
                result["checks"].append("actual confined text/PDF extraction and public exact search: " + expected)
            result["checks"].append("deferred startup admits only explicit generated root")
            result["files"] = len(manifest["files"])
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
        args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(result))


if __name__ == "__main__":
    main()
