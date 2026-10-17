"""Measure a generated two-hour sparse video and cancellation of its real decoder tree."""
import argparse
import ctypes as c
from ctypes import wintypes as w
import importlib.util
import hashlib
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


def digest(path):
    with path.open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()


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


def run(source, output, decoder, cursor=0, plan=None, selection=None, units=30, cancel=False):
    phase = 'plan' if plan else 'selection' if selection or cursor == 0 else 'phase'
    scratch = output / source.stem / ('cancel' if cancel else f'batch-{cursor}-{phase}')
    scratch.mkdir(parents=True)
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
            'cursor':cursor,'max_units':units,'media_plan':plan,'video_selection':selection}).encode()
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
    parser.add_argument('--reuse-sources',type=Path,
        help='reuse unchanged generated sources from an earlier artifact with post-run-input-hashes.json')
    options = parser.parse_args()
    output = options.output.resolve()
    assert output.is_relative_to(root/'artifacts')
    output.mkdir(parents=True, exist_ok=False)
    decoder = root / 'artifacts/dependencies/ffmpeg-2026-09-30/ffmpeg-n9.0.2-17-g2a571b6068-win64-lgpl-shared-9.0/bin'
    source_dir=options.reuse_sources.resolve() if options.reuse_sources else output
    source = source_dir / 'two hour late soundtrack.mp4'
    high = source_dir / 'high-resolution long-gop.mp4'
    if options.reuse_sources:
        assert source_dir.is_relative_to(root/'artifacts') and source_dir!=output
        prior=json.loads((source_dir/'post-run-input-hashes.json').read_text(encoding='utf-8'))['inputs']
        for key,path in [('sparse_source',source),('hd_source',high),
                         ('ffmpeg',decoder/'ffmpeg.exe'),('ffprobe',decoder/'ffprobe.exe')]:
            assert digest(path)==prior[key]['sha256'],('reused measurement input changed',key)
    else:
        generated = subprocess.run([str(decoder/'ffmpeg.exe'),'-hide_banner','-loglevel','error','-nostdin','-n',
            '-f','lavfi','-i','color=c=0x486878:s=160x100:r=1:d=7200','-itsoffset','7100',
            '-i',str(root/'artifacts/spoken-fixtures/railway.wav'),'-map','0:v:0','-map','1:a:0',
            '-c:v','mpeg4','-q:v','4','-threads','2','-c:a','aac','-t','7200',str(source)],
            capture_output=True,timeout=60,creationflags=subprocess.CREATE_NO_WINDOW)
        assert generated.returncode == 0, generated.stderr[:8192]
    summary = {'scope':__doc__,'source_bytes':source.stat().st_size,
        'source_video':{'duration_seconds':7200,'width':160,'height':100,'fps':1},
        'measurement':'job-tree process memory sampled every20ms; scratch sampled and final files counted',
        'timing_clock':vars(time.get_clock_info('perf_counter')), 'batches':[],
        'reused_source_dir':str(source_dir) if options.reuse_sources else None,
        'sha256':{name:digest(path) for name,path in [('harness',Path(__file__)),
            ('extractor',root/'src/engine/target/debug/winsearch-extract.exe'),
            ('ffmpeg',decoder/'ffmpeg.exe'),('ffprobe',decoder/'ffprobe.exe'),('sparse_source',source)]}}
    (output/'input-lock.json').write_text(json.dumps(summary,indent=2),encoding='utf-8')
    try:
        response, metrics = run(source,output,decoder)
        summary['batches'].append(metrics)
        while response['media'] is None:
            assert response['output']['segments'] == []
            state = response['video_selection']
            assert state and response['next_cursor'] == (1 << 61) | state['next_ms']
            assert len(summary['batches']) < 1000, 'selection failed to advance'
            response, metrics = run(source,output,decoder,response['next_cursor'],selection=state)
            summary['batches'].append(metrics)
        plan = response['media']
        assert plan['duration_ms'] == 7_200_000 and plan['has_audio'], plan
        assert 1150 <= len(plan['frames_ms']) <= 1200
        assert plan['frames_ms'][0] == 0 and plan['frames_ms'][-1] == 7_199_000
        assert response['next_cursor'] == 0
        summary['selected_frames'] = len(plan['frames_ms'])
        summary['total_units'] = response['total_units']
        assert response['total_units'] == len(plan['frames_ms']) + 288
        summary['selection_batches'] = len(summary['batches'])
        response, metrics = run(source,output,decoder,0,plan)
        summary['batches'].append(metrics)
        assert response['next_cursor'] == 30 << 16
        for window in [0,284]:
            response, metrics = run(source,output,decoder,(1 << 62) | window,plan=plan,units=1)
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
        if not options.reuse_sources:
            generated = subprocess.run([str(decoder/'ffmpeg.exe'),'-hide_banner','-loglevel','error','-nostdin','-n',
                '-f','lavfi','-i','color=c=0x486878:s=1920x1080:r=15:d=120',
                '-c:v','mpeg4','-q:v','8','-g','450','-threads','2','-an',str(high)],
                capture_output=True,timeout=120,creationflags=subprocess.CREATE_NO_WINDOW)
            assert generated.returncode == 0, generated.stderr[:8192]
        summary['sha256']['hd_source']=digest(high)
        summary['high_resolution'] = {'source_bytes':high.stat().st_size,
            'source_video':{'duration_seconds':120,'width':1920,'height':1080,'fps':15,'gop_frames':450},
            'scan_batches':[]}
        response, metrics = run(high,output,decoder)
        summary['high_resolution']['scan_batches'].append(metrics)
        while response['media'] is None:
            assert response['media'] is None and response['output']['segments'] == []
            state = response['video_selection']
            assert len(summary['high_resolution']['scan_batches'])<10
            response, metrics = run(high,output,decoder,response['next_cursor'],selection=state)
            summary['high_resolution']['scan_batches'].append(metrics)
        assert response['video_selection']['next_ms'] == 120_000
        assert response['media']['duration_ms']==120_000 and not response['media']['has_audio']
        assert response['media']['frames_ms'][0]==0 and response['media']['frames_ms'][-1]>=119_000
        assert response['next_cursor']==0
        summary['high_resolution']['plan']=response['media']
        _, metrics = run(high,output,decoder,cancel=True)
        summary['high_resolution']['cancel'] = metrics
    finally:
        (output/'results.json').write_text(json.dumps(summary,indent=2),encoding='utf-8')
        print(json.dumps(summary,indent=2))


if __name__ == '__main__':
    main()
