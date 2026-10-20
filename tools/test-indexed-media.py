"""Genuine offline media through the production daemon, worker, SQLite and public pipe.

Requires the pinned decoder/models and generated spoken/media fixtures. Outputs
all query results, including failures; never changes product admission or gates.
"""
import argparse
import csv
import ctypes as c
from ctypes import wintypes as w
import json
import os
from pathlib import Path
import shutil
import sqlite3
import struct
import subprocess
import tempfile
import threading
import time
import uuid


class OwnedJob:
    def __init__(self):
        assert c.sizeof(c.c_void_p) == 8, 'Windows x64 only'
        self.api = c.WinDLL('kernel32', use_last_error=True)
        signatures = {
            'CreateJobObjectW': ([c.c_void_p, w.LPCWSTR], w.HANDLE),
            'SetInformationJobObject': ([w.HANDLE, c.c_int, c.c_void_p, w.DWORD], w.BOOL),
            'AssignProcessToJobObject': ([w.HANDLE, w.HANDLE], w.BOOL),
            'TerminateJobObject': ([w.HANDLE, w.UINT], w.BOOL),
            'CloseHandle': ([w.HANDLE], w.BOOL),
            'QueryInformationJobObject': ([w.HANDLE, c.c_int, c.c_void_p, w.DWORD, c.c_void_p], w.BOOL),
            'OpenProcess': ([w.DWORD, w.BOOL, w.DWORD], w.HANDLE),
        }
        for name, (arguments, result) in signatures.items():
            function = getattr(self.api, name)
            function.argtypes, function.restype = arguments, result
        self.handle = self.api.CreateJobObjectW(None, None)
        assert self.handle, c.WinError(c.get_last_error())
        limits = c.create_string_buffer(144)
        struct.pack_into('<I', limits, 16, 0x2000)  # KILL_ON_JOB_CLOSE
        assert self.api.SetInformationJobObject(self.handle, 9, limits, len(limits)), c.WinError(c.get_last_error())
        self.memory = c.WinDLL('psapi').GetProcessMemoryInfo
        self.memory.argtypes, self.memory.restype = [w.HANDLE, c.c_void_p, w.DWORD], w.BOOL
        self.peak_working_set = 0
        self.peak_private_bytes = 0
        self.pids_seen = set()

    def assign(self, child):
        if not self.api.AssignProcessToJobObject(self.handle, int(child._handle)):
            error = c.WinError(c.get_last_error())
            child.kill()
            child.wait(timeout=5)
            raise error

    def sample(self):
        processes = c.create_string_buffer(8 + 8 * 256)
        assert self.api.QueryInformationJobObject(self.handle, 3, processes, len(processes), None), c.WinError(c.get_last_error())
        count = struct.unpack_from('<I', processes, 4)[0]
        pids = struct.unpack_from('<' + 'Q' * count, processes, 8)
        total_ws = total_private = 0
        for pid in pids:
            self.pids_seen.add(pid)
            process = self.api.OpenProcess(0x1000 | 0x10, False, pid)
            if not process:
                continue
            try:
                counters = c.create_string_buffer(80)
                struct.pack_into('<I', counters, 0, len(counters))
                if self.memory(process, counters, len(counters)):
                    total_ws += struct.unpack_from('<Q', counters, 16)[0]
                    total_private += struct.unpack_from('<Q', counters, 72)[0]
            finally:
                self.api.CloseHandle(process)
        self.peak_working_set = max(self.peak_working_set, total_ws)
        self.peak_private_bytes = max(self.peak_private_bytes, total_private)
        return pids

    def terminate(self):
        if self.handle:
            assert self.api.TerminateJobObject(self.handle, 1), c.WinError(c.get_last_error())

    def close(self):
        self.terminate()
        self.api.CloseHandle(self.handle)
        self.handle = None


class Pipe:
    def __init__(self, path, child):
        deadline = time.monotonic() + 10
        while True:
            try:
                self.stream = open(path, 'r+b', buffering=0)
                break
            except OSError:
                assert child.poll() is None and time.monotonic() < deadline, 'pipe not available'
                time.sleep(.05)
        self.sequence = 0
        reply = self.request('hello', protocol_version=1, client='indexed-media-acceptance', client_version='1.0')
        assert reply['type'] == 'hello_ack' and reply['protocol_version'] == 1, reply
        self.features = reply.get('features', [])

    def exact(self, count):
        result = bytearray()
        while len(result) < count:
            part = self.stream.read(count - len(result))
            assert part, 'daemon closed pipe'
            result.extend(part)
        return bytes(result)

    def request(self, kind, **fields):
        self.sequence += 1
        message = {'type': kind, **fields}
        if kind != 'hello':
            message['id'] = self.sequence
        raw = json.dumps(message).encode()
        frame = memoryview(struct.pack('<I', len(raw)) + raw)
        while frame:
            count = self.stream.write(frame)
            assert count, 'pipe write failed'
            frame = frame[count:]
        batches = []
        while True:
            length = struct.unpack('<I', self.exact(4))[0]
            assert 0 < length <= 16 * 1024 * 1024
            reply = json.loads(self.exact(length))
            if kind == 'hello' or reply.get('id') == self.sequence:
                assert reply['type'] != 'error', reply
                if kind != 'search':
                    return reply
                assert reply['type'] == 'results', reply
                batches.extend(reply['batch'])
                if reply['done']:
                    return batches


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    options = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    output = options.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    # Development-tree media is intentionally name-only under production policy.
    corpus = Path(tempfile.mkdtemp(prefix='winsearch-indexed-media-')).resolve()
    for original, target in [('spoken-fixtures/two-passages-65s.wav', 'item-a.wav'),
                             ('media-fixtures/時刻 clip.mp4', 'item-b.mp4'),
                             ('spoken-fixtures/railway.wav', 'item-c.wav')]:
        shutil.copyfile(root / 'artifacts' / original, corpus / target)
    sid = next(csv.reader(subprocess.check_output(['whoami', '/user', '/fo', 'csv', '/nh'], text=True).splitlines()))[1]
    instance = 'media-' + uuid.uuid4().hex
    decoder = root / 'artifacts/dependencies/ffmpeg-2026-09-30/ffmpeg-n9.0.2-17-g2a571b6068-win64-lgpl-shared-9.0/bin'
    data = output / 'data'
    summary = {'scope': __doc__, 'queries': [], 'failures': []}
    owner = OwnedJob()
    stopped = threading.Event()
    start = time.monotonic()
    child = None
    timer = None
    monitor = None
    try:
        with (output / 'daemon.log').open('wb') as log:
            child = subprocess.Popen([str(root / 'src/engine/target/debug/winsearchd.exe'), '--test-stdin-gate'],
                stdin=subprocess.PIPE, stdout=log, stderr=log, creationflags=subprocess.CREATE_NO_WINDOW,
                env={**os.environ, 'WINSEARCH_DATA_DIR': str(data), 'WINSEARCH_INSTANCE_ID': instance,
                     'WINSEARCH_TEST_DECODER_DIR': str(decoder), 'WS_EMBED_STUB': '0', 'WS_EMBED_BACKEND': 'cpu',
                     'WS_EMBED_MODEL': str(root / 'artifacts/test-models/embeddinggemma-2-Q8_0.gguf')})
            owner.assign(child)
            child.stdin.write(b'\1')
            child.stdin.close()
            timer = threading.Timer(180, owner.terminate)
            timer.daemon = True
            timer.start()

            def sample():
                while not stopped.wait(.1):
                    owner.sample()
            monitor = threading.Thread(target=sample, daemon=True)
            monitor.start()
            pipe = Pipe(r'\\.\pipe\winsearch-' + sid + '-' + instance, child)
            pipe.request('set_config', config={'semantic_enabled': True, 'preferred_backend': 'cpu'})
            config = pipe.request('get_indexing_config')['config']
            config.update(roots=[str(corpus)], respect_battery_saver=False, index_on_battery=True)
            pipe.request('set_indexing_config', config=config)
            db = data / 'db/index.db'
            deadline = time.monotonic() + 120
            while True:
                with sqlite3.connect(db.as_uri() + '?mode=ro', uri=True, timeout=2) as connection:
                    progress = connection.execute('SELECT kind,complete FROM content_progress').fetchall()
                    pending = connection.execute("SELECT count(*) FROM content_chunks WHERE embed=1 AND embed_state!='done'").fetchone()[0]
                    jobs = connection.execute('SELECT state,kind FROM extract_jobs').fetchall()
                if len(progress) == 3 and all(row[1] for row in progress) and pending == 0 and all(row[0] == 'done' for row in jobs):
                    break
                assert time.monotonic() < deadline, (progress, pending, jobs)
                time.sleep(.25)
            summary['index_seconds'] = round(time.monotonic() - start, 3)
            worker = Pipe(r'\\.\pipe\winsearch-embed-' + sid + '-' + instance, child)
            provenance = worker.request('status')
            assert provenance['model_loaded'] and provenance['stub'] is False and provenance['backend'] == 'cpu' and provenance['dim'] == 768, provenance
            summary['worker'] = provenance
            worker.stream.close()
            cases = [
                ('later_audio', 'baking a chocolate cake in the kitchen with flour butter eggs and sugar', 'audio', 'item-a.wav', 51),
                ('later_video_sound', 'baking a chocolate cake in the kitchen with flour butter eggs and sugar', 'video', 'item-b.mp4', 51),
                ('later_video_frame', 'a computer application screenshot with a red warning and a blue button', 'video', 'item-b.mp4', 33),
                ('negative_space', 'asteroid orbital mechanics and interstellar propulsion', None, None, None),
                ('negative_vet', 'veterinary vaccination records for a sick kitten', None, None, None),
                ('negative_finance', 'mortgage interest amortization and bank credit risk', None, None, None),
            ]
            for name, text, category, expected, timepoint in cases:
                results = pipe.request('search', query={'text': text, 'include_semantic': True, 'limit': 5,
                    'categories': [category] if category else []})
                summary['queries'].append({'name': name, 'query': text, 'results': results})
                passed = not results if expected is None else bool(results) and Path(results[0]['path']).name == expected
                if passed and expected:
                    location = results[0].get('location') or {}
                    if name == 'later_video_frame':
                        # The screenshot persists for the entire second shot.
                        passed = location.get('kind') == 'timestamp' and 32.75 <= location['seconds'] < 65
                        passed = passed and location['end_seconds'] == location['seconds'] and Path(results[0].get('thumbnail_path') or '').is_file()
                    else:
                        passed = location.get('kind') == 'timestamp' and location['seconds'] <= timepoint <= location['end_seconds']
                if not passed:
                    summary['failures'].append(name)
            pipe.stream.close()
            with sqlite3.connect(db.as_uri() + '?mode=ro', uri=True) as connection:
                summary['segments'] = connection.execute('SELECT kind,count(*) FROM segments GROUP BY kind').fetchall()
                audio = connection.execute("SELECT vector FROM segments WHERE kind='audio'").fetchall()
                assert len(audio) >= 7 and len({row[0] for row in audio}) >= 5, 'audio windows collapsed'
    finally:
        if timer:
            timer.cancel()
        stopped.set()
        if monitor:
            monitor.join(timeout=2)
        owner.terminate()
        if child:
            child.wait(timeout=5)
        deadline = time.monotonic() + 5
        while owner.sample() and time.monotonic() < deadline:
            time.sleep(.05)
        assert not owner.sample(), 'owned descendants survived termination'
        owner.close()
        summary.update(elapsed_seconds=round(time.monotonic()-start, 3), pids=sorted(owner.pids_seen),
            peak_job_working_set_bytes=owner.peak_working_set, peak_job_private_bytes=owner.peak_private_bytes)
        (output / 'results.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
        print(json.dumps({key: value for key, value in summary.items() if key != 'queries'}, indent=2))
        assert corpus.parent == Path(tempfile.gettempdir()).resolve() and corpus.name.startswith('winsearch-indexed-media-')
        shutil.rmtree(corpus)
    assert not summary['failures'], summary['failures']


if __name__ == '__main__':
    main()
