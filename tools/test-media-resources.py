"""Measure a generated two-hour sparse video and cancellation of its real decoder tree."""
import argparse
import ctypes as c
from ctypes import wintypes as w
import importlib.util
import json
from pathlib import Path
import struct
import subprocess
import threading
import time
import wave

root = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location('indexed_media_test', root / 'tools/test-indexed-media.py')
support = importlib.util.module_from_spec(spec)
spec.loader.exec_module(support)


def frames(data):
    result = []
    while data:
        assert len(data) >= 4
        length = struct.unpack_from('<I', data)[0]
        assert 0 < length <= 16 * 1024 * 1024 and len(data) >= length + 4
        result.append(json.loads(data[4:4+length]))
        data = data[4+length:]
    assert len(result) == 2 and 'video' in result[0]['supported_kinds'], result
    return result[1]


def run(source, output, decoder, cursor=0, plan=None, units=30, cancel=False):
    scratch = output / ('cancel' if cancel else f'batch-{cursor}')
    scratch.mkdir()
    owner = support.OwnedJob()
    done = threading.Event()
    peak_scratch = 0
    errors = []
    start = time.perf_counter()
    cancelled_at = None
    api = owner.api.QueryFullProcessImageNameW
    api.argtypes, api.restype = [w.HANDLE, w.DWORD, w.LPWSTR, c.POINTER(w.DWORD)], w.BOOL

    def sample():
        nonlocal peak_scratch
        try:
            while not done.wait(.02):
                owner.sample()
                peak_scratch = max(peak_scratch, sum(p.stat().st_size for p in scratch.iterdir() if p.is_file()))
        except Exception as error:
            errors.append(repr(error))

    child = subprocess.Popen([str(root / 'src/engine/target/debug/winsearch-extract.exe')],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, creationflags=subprocess.CREATE_NO_WINDOW,
        env={**support.os.environ, 'WINSEARCH_TEST_DECODER_DIR': str(decoder)})
    owner.assign(child)
    timer = threading.Timer(120, owner.terminate)
    timer.daemon = True
    timer.start()
    monitor = threading.Thread(target=sample, daemon=True)
    monitor.start()
    try:
        request = json.dumps({'path':str(source),'kind':'video','scratch_dir':str(scratch),
            'cursor':cursor,'max_units':units,'media_plan':plan}).encode()
        child.stdin.write(struct.pack('<I', len(request)) + request)
        child.stdin.close()
        child.stdin = None
        if cancel:
            deadline = time.perf_counter() + 15
            while cancelled_at is None:
                for pid in owner.sample():
                    process = owner.api.OpenProcess(0x1000, False, pid)
                    if not process:
                        continue
                    try:
                        buffer, size = c.create_unicode_buffer(32768), w.DWORD(32768)
                        if api(process, 0, buffer, c.byref(size)) and Path(buffer.value).name == 'ffmpeg.exe':
                            cancelled_at = time.perf_counter()
                            owner.terminate()
                            break
                    finally:
                        owner.api.CloseHandle(process)
                assert time.perf_counter() < deadline, 'real decoder was not observed before cancellation'
                time.sleep(.005)
        stdout, stderr = child.communicate(timeout=125)
        peak_scratch = max(peak_scratch, sum(p.stat().st_size for p in scratch.iterdir() if p.is_file()))
        assert cancelled_at is not None or child.returncode == 0, stderr[:8192]
        response = None if cancel else frames(stdout)
        if response is not None:
            assert response['output']['status'] == 'Extracted', response
    finally:
        timer.cancel()
        done.set()
        monitor.join(timeout=2)
        owner.terminate()
        child.wait(timeout=5)
        deadline = time.perf_counter() + 5
        while owner.sample() and time.perf_counter() < deadline:
            time.sleep(.01)
        assert not owner.sample(), 'owned descendants survived'
        owner.close()
    elapsed = time.perf_counter() - start
    metrics = {'cursor':cursor,'seconds':round(elapsed,3),'peak_job_working_set_bytes':owner.peak_working_set,
        'peak_job_private_bytes':owner.peak_private_bytes,'peak_scratch_bytes':peak_scratch,
        'pids_seen':sorted(owner.pids_seen),'monitor_errors':errors}
    assert not errors and 0 < owner.peak_working_set <= 1024**3, metrics
    assert peak_scratch < 2*1024**2, metrics  # small constant-color source, no giant decoded file
    if cancel:
        metrics['cancel_to_reap_seconds'] = round(time.perf_counter()-cancelled_at,6)
        assert metrics['cancel_to_reap_seconds'] < 2, metrics
    return response, metrics


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    options = parser.parse_args()
    output = options.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    decoder = root / 'artifacts/dependencies/ffmpeg-2026-09-30/ffmpeg-n9.0.2-17-g2a571b6068-win64-lgpl-shared-9.0/bin'
    source = output / 'two hour late soundtrack.mp4'
    generated = subprocess.run([str(decoder/'ffmpeg.exe'),'-hide_banner','-loglevel','error','-nostdin','-n',
        '-f','lavfi','-i','color=c=0x486878:s=160x100:r=1:d=7200','-itsoffset','7100',
        '-i',str(root/'artifacts/spoken-fixtures/railway.wav'),'-map','0:v:0','-map','1:a:0',
        '-c:v','mpeg4','-q:v','4','-threads','2','-c:a','aac','-t','7200',str(source)],
        capture_output=True,timeout=60,creationflags=subprocess.CREATE_NO_WINDOW)
    assert generated.returncode == 0, generated.stderr[:8192]
    summary = {'scope':__doc__,'source_bytes':source.stat().st_size,
        'source_video':{'duration_seconds':7200,'width':160,'height':100,'fps':1},
        'measurement':'job-tree process memory sampled every20ms; scratch sampled and final files counted',
        'timing_clock':vars(time.get_clock_info('perf_counter')), 'batches':[]}
    try:
        response, metrics = run(source,output,decoder)
        summary['batches'].append(metrics)
        plan = response['media']
        assert plan['duration_ms'] == 7_200_000 and plan['has_audio'], plan
        assert 1150 <= len(plan['frames_ms']) <= 1200
        assert plan['frames_ms'][0] == 0 and plan['frames_ms'][-1] == 7_199_000
        assert response['next_cursor'] == 30 << 16
        summary['selected_frames'] = len(plan['frames_ms'])
        summary['total_units'] = response['total_units']
        assert response['total_units'] == len(plan['frames_ms']) + 288
        for window in [0,284]:
            response, metrics = run(source,output,decoder,(1 << 62) | window,plan,1)
            summary['batches'].append(metrics)
            segments = response['output']['segments']
            if window == 0:
                assert segments == [] and response['next_cursor'] == (1 << 62) | 1
            else:
                assert len(segments) == 1 and segments[0]['text'] == ''
                assert (segments[0]['start_ms'],segments[0]['end_ms']) == (7_100_000,7_130_000)
                with wave.open(segments[0]['embedding_input']['Audio']['path'],'rb') as audio:
                    assert (audio.getnframes(),audio.getframerate()) == (480000,16000)
        _, metrics = run(source,output,decoder,cancel=True)
        summary['cancel'] = metrics
    finally:
        (output/'results.json').write_text(json.dumps(summary,indent=2),encoding='utf-8')
        print(json.dumps(summary,indent=2))


if __name__ == '__main__':
    main()
