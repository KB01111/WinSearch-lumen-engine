"""Native exact-search scale gate on generated files; no semantic/backend qualification.

Uses isolated roots/data and a kill-on-close job. Retains all latency samples and
failed budgets. CPU/model/media/installed acceptance remain separate gates.
"""
import argparse
import csv
from contextlib import closing
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import platform
import sqlite3
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


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def percentiles(values):
    ordered = sorted(values)
    assert ordered
    return {f"p{p}": ordered[max(0, math.ceil(len(ordered) * p / 100) - 1)]
            for p in (50, 95, 99)}


def query(peer, text, limit):
    started = time.perf_counter()
    request = peer.begin(dict(text=text, mode="files", limit=limit,
                              include_semantic=False, categories=["file", "document"], extensions=[], file_kinds=[]))
    first_ms = None
    rows = frames = 0
    while True:
        frame = peer.next_result(request)
        elapsed = (time.perf_counter() - started) * 1000
        if first_ms is None:
            first_ms = elapsed
        frames += 1
        rows += len(frame["batch"])
        assert frames <= 64 and rows <= limit
        if frame["done"]:
            outcome = frame.get("outcome")
            assert outcome and outcome["disposition"] == "complete", frame
            assert outcome["semantic"]["phase"] == "disabled", frame
            return dict(first_batch_ms=first_ms, complete_ms=elapsed, results=rows, frames=frames)


def run(args, count):
    result = dict(files=count, segments=None, scope=__doc__, passed=False, samples=[], failures=[])
    with tempfile.TemporaryDirectory(prefix="winsearch-scale-", dir=args.work_directory) as directory:
        work = Path(directory)
        sources, data = work / "sources", work / "data"
        sources.mkdir()
        deployed = work / "bin"
        deployed.mkdir()
        inputs = [args.binary_directory / name for name in ("winsearchd.exe", "winsearch-cli.exe", "winsearch-embed.exe", "winsearch-extract.exe")]
        inputs.extend(sorted(args.binary_directory.glob("*.dll")))
        result["deployed_sha256"] = {}
        for original in inputs:
            expected = digest(original)
            target = deployed / original.name
            shutil.copyfile(original,target)
            assert digest(target) == expected, "benchmark payload changed while copying"
            result["deployed_sha256"][original.name] = expected
        corpus_hash = hashlib.sha256()
        total_bytes = 0
        for number in range(count):
            # Fixed 80% text / 20% metadata-only mix, in bounded directory fanout.
            folder = sources / f"group-{number // 200:04}"
            folder.mkdir(exist_ok=True)
            suffix = "bin" if number % 5 == 0 else "txt"
            name = f"scale-{number:06}.{suffix}"
            body = f"Generated scale document {number:06}. exactscalecorpus\n".encode()
            (folder / name).write_bytes(body)
            corpus_hash.update(f"{number // 200}/{name}\0".encode() + body)
            total_bytes += len(body)
        result.update(corpus_sha256=corpus_hash.hexdigest(), source_bytes=total_bytes,
                      modality_mix={"text": count - count // 5, "metadata_only": count // 5},
                      warm_definition="five unreported exact query warmups; populated OS/SQLite caches; no model")
        env = {key: value for key, value in os.environ.items()
               if not key.upper().startswith(("WS_", "WINSEARCH_"))}
        env.update(WINSEARCH_DATA_DIR=str(data), WINSEARCH_INSTANCE_ID="scale-" + uuid.uuid4().hex)
        system32 = Path(os.environ["SystemRoot"]) / "System32"
        sid = next(csv.reader(subprocess.check_output(
            [str(system32 / "whoami.exe"), "/user", "/fo", "csv", "/nh"], text=True).splitlines()))[1]
        pipe = r"\\.\pipe\winsearch-" + sid + "-" + env["WINSEARCH_INSTANCE_ID"]
        owner = parity.harness.OwnedJob()
        child = None
        peers = []
        stopped = threading.Event()
        monitor_errors = []
        peak_disk = [0]
        expired = threading.Event()
        def expire():
            expired.set()
            owner.terminate()
        watchdog = threading.Timer(args.run_timeout_seconds, expire)
        def monitor():
            while not stopped.wait(.1):
                try:
                    owner.sample()
                    peak_disk[0] = max(peak_disk[0], sum(p.stat().st_size for p in data.rglob("*") if p.is_file()))
                except Exception as error:
                    if len(monitor_errors) < 32:
                        monitor_errors.append(type(error).__name__)
        thread = threading.Thread(target=monitor)
        try:
            with (work / "daemon.log").open("wb") as log:
                child = subprocess.Popen([str(deployed / "winsearchd.exe"),
                    "--defer-root-admission"], env=env, cwd=work,
                    stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                    creationflags=subprocess.CREATE_NO_WINDOW | 0x00000004)
            owner.assign(child)
            watchdog.start()
            owner.resume(child)
            thread.start()
            control = parity.harness.Pipe(pipe, child)
            search = parity.Frames(pipe, child)
            peers.extend([control, search])
            assert "search.outcomes" in control.features and "search.limits" in control.features
            control.request("set_config", config=dict(semantic_enabled=False, preferred_backend="cpu"))
            config = control.request("get_indexing_config")["config"]
            config.update(roots=[str(sources)], root_policies=[], code_enabled=False,
                          code_roots=[], index_on_battery=True, respect_battery_saver=False, paused=False)
            indexing_started = time.perf_counter()
            control.request("set_indexing_config", config=config)
            database = data / "index" / "index.db"
            # Resolve the documented data layout instead of creating another store.
            candidates = list(data.rglob("index.db"))
            if candidates:
                database = candidates[0]
            deadline = time.monotonic() + args.index_timeout_seconds
            while True:
                assert child.poll() is None and time.monotonic() < deadline, "indexing did not settle"
                with closing(sqlite3.connect(f"file:{database.as_posix()}?mode=ro", uri=True)) as db:
                    files = db.execute("SELECT count(*) FROM entries WHERE is_dir=0 AND is_app=0").fetchone()[0]
                    pending = db.execute("SELECT count(*) FROM crawl_queue").fetchone()[0]
                    segments = db.execute("SELECT count(*) FROM segments").fetchone()[0]
                    content = db.execute("SELECT count(*) FROM content_chunks").fetchone()[0]
                    extraction = db.execute("SELECT count(*) FROM extract_jobs WHERE kind!='name'").fetchone()[0]
                    failed = db.execute("SELECT count(*) FROM extract_jobs WHERE kind!='name' AND state='failed'").fetchone()[0]
                    assert failed == 0, "generated text extraction failed"
                if files == count and pending == 0 and extraction == 0:
                    break
                time.sleep(.1)
            result.update(index_seconds=time.perf_counter() - indexing_started,
                          segments=segments, extracted_chunks=content)
            assert content >= count - count // 5, "generated text was not actually extracted"
            # Cold here means first query in this process, not cold OS disk caches.
            result["first_process_query"] = query(search, f"scale-{count - 1:06}", 20)
            for _ in range(5):
                query(search, "scale-", 20)
            for limit in args.limits:
                for iteration in range(args.iterations):
                    sample = query(search, "scale-", limit)
                    assert sample["results"] == min(limit, count)
                    sample.update(limit=limit, iteration=iteration)
                    result["samples"].append(sample)
                    if len(result["samples"]) % 10 == 0:
                        args.output.with_name(f"{args.output.stem}-{count}-raw.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
            result["latencies"] = {}
            for limit in args.limits:
                samples = [s for s in result["samples"] if s["limit"] == limit]
                result["latencies"][str(limit)] = {
                    field: percentiles([s[field] for s in samples])
                    for field in ("first_batch_ms", "complete_ms")}
            if "20" in result["latencies"]:
                first = result["latencies"]["20"]["first_batch_ms"]
                if first["p95"] > 100 or first["p99"] > 250:
                    result["failures"].append("warm exact first-batch target")
        except Exception as error:
            result["failures"].append(f"{type(error).__name__}: {error}")
        finally:
            for peer in peers:
                peer.stream.close()
            stopped.set()
            if thread.ident:
                thread.join()
            watchdog.cancel()
            if watchdog.ident:
                watchdog.join()
            stop_started = time.perf_counter()
            owner.terminate()
            if child:
                child.wait(timeout=5)
            result["independent_stop_ms"] = (time.perf_counter() - stop_started) * 1000
            remaining = owner.sample()
            result.update(owned_job_empty=not remaining,
                          peak_job_private_bytes=owner.peak_private_bytes,
                          peak_job_working_set_bytes=owner.peak_working_set,
                          peak_data_bytes=peak_disk[0], monitor_errors=monitor_errors)
            if expired.is_set():
                result["failures"].append("absolute corpus-run watchdog")
            owner.close()
            # Keep only aggregate profiling events; ordinary logs can contain
            # application inventory and generated source paths.
            profile_lines = []
            for log_path in sorted((data / "logs").glob("winsearchd.log.*")):
                profile_lines.extend(line for line in log_path.read_text(encoding="utf-8", errors="replace").splitlines()
                                     if "winsearchd::query_profile" in line)
            if profile_lines:
                args.output.with_name(f"{args.output.stem}-{count}-profile.log").write_text("\n".join(profile_lines) + "\n", encoding="utf-8")
        if owner.peak_private_bytes > args.max_private_mib * 1024 * 1024:
            result["failures"].append("declared private-memory ceiling")
        if peak_disk[0] > args.max_disk_mib * 1024 * 1024:
            result["failures"].append("declared data-disk ceiling")
        if monitor_errors or not result.get("owned_job_empty") or result["independent_stop_ms"] > 1000:
            result["failures"].append("resource monitor/Stop/owned-job gate")
        result["passed"] = not result["failures"]
        # Persist before TemporaryDirectory cleanup as well: a Windows sharing
        # violation must never erase the measurements that preceded it.
        args.output.with_name(f"{args.output.stem}-{count}-raw.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary-directory", type=Path, required=True)
    parser.add_argument("--build-profile", choices=("debug", "release"), required=True)
    parser.add_argument("--scenario", required=True, help="Record host conditions before collecting samples")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--work-directory", type=Path, required=True)
    parser.add_argument("--files", type=int, nargs="+", choices=(1000, 10000, 50000, 200000), default=[1000, 10000, 50000, 200000])
    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument("--limits", type=int, nargs="+", choices=(20, 1000, 10000), default=[20, 1000, 10000],
                        help="Select a diagnostic subset; omitted result limits remain unverified")
    parser.add_argument("--index-timeout-seconds", type=int, default=1800)
    parser.add_argument("--run-timeout-seconds", type=int, default=3600)
    parser.add_argument("--max-private-mib", type=int, required=True)
    parser.add_argument("--max-disk-mib", type=int, required=True)
    args = parser.parse_args()
    assert os.name == "nt" and args.iterations >= 100
    assert args.index_timeout_seconds > 0 and args.run_timeout_seconds > 0 and args.max_private_mib > 0 and args.max_disk_mib > 0
    args.binary_directory = args.binary_directory.resolve(strict=True)
    args.work_directory = args.work_directory.resolve(strict=True)
    assert not args.output.exists(), "retain previous reports"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    report = dict(schema_version=1, scope=__doc__, host=platform.platform(), processor=platform.processor(), cpus=os.cpu_count(), build_profile=args.build_profile, scenario=args.scenario,
        harness_sha256=digest(Path(__file__)),
        binaries={name: digest(args.binary_directory / name) for name in
                  ("winsearchd.exe", "winsearch-cli.exe", "winsearch-embed.exe", "winsearch-extract.exe")},
        declared_ceilings=dict(private_mib=args.max_private_mib, data_disk_mib=args.max_disk_mib), requested_limits=args.limits,
        unmeasured_result_limits=[limit for limit in (20, 1000, 10000) if limit not in args.limits],
        unverified=["semantic/hybrid/Related", "code", "idle CPU ten-minute gate", "power/sleep", "installed Lumen", "hardware backends"], corpora=[])
    try:
        for count in args.files:
            report["corpora"].append(run(args, count))
            args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    finally:
        args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    raise SystemExit(0 if len(report["corpora"]) == len(args.files) and all(r["passed"] for r in report["corpora"]) else 1)


if __name__ == "__main__":
    main()
