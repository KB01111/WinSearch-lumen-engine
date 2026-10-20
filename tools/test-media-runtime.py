"""Real CPU/NPU worker/reference/retrieval acceptance over generated public inputs."""
import argparse
import json
import math
import os
from pathlib import Path
import struct
import subprocess
import threading
import time
import uuid
import importlib.util
import hashlib


def validate_vectors(reply, expected_count, expected_backend='cpu'):
    assert reply['type'] == 'embeddings' and reply['stub'] is False and reply['backend'] == expected_backend, reply
    assert reply['dim'] == 768
    assert len(reply['vecs']) == expected_count, (expected_count, len(reply['vecs']))
    for vector in reply['vecs']:
        assert len(vector) == 768 and all(math.isfinite(v) for v in vector)
        assert abs(sum(v*v for v in vector) - 1) < 1e-3
    return reply['vecs']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ['worker', 'model', 'fixtures', 'spoken', 'output']:
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--backend', choices=['cpu', 'npu'], default='cpu')
    parser.add_argument('--npu-bundle', type=Path)
    parser.add_argument('--warm-repetitions', type=int, default=0)
    parser.add_argument('--clean-env', action='store_true')
    parser.add_argument('--working-directory', type=Path)
    args = parser.parse_args()
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    data = args.output / ('data-' + uuid.uuid4().hex)
    (data / 'models').mkdir(parents=True)
    # A different profile projector must never replace an explicit model's sibling.
    (data / 'models/mmproj-embeddinggemma-2-Q8_0.gguf').write_bytes(b'wrong profile projector')
    pipe = r'\\.\pipe\winsearch-media-' + uuid.uuid4().hex
    reference = json.loads((args.fixtures / 'reference-media-vectors.json').read_text())['vectors']
    assert 0 <= args.warm_repetitions <= 100
    assert args.backend != 'npu' or args.npu_bundle is not None
    def sha(path):
        with path.open('rb') as stream: return hashlib.file_digest(stream, 'sha256').hexdigest()
    summary = {'backend': args.backend, 'stub': False, 'references': {}, 'retrieval': [], 'jobs': [],
        'workerSha256': sha(args.worker), 'modelSha256': sha(args.model),
        'projectorSha256': sha(args.model.with_name('mmproj-embeddinggemma-2-Q8_0.gguf')),
        'bundleManifestSha256': sha(args.npu_bundle / 'manifest.txt') if args.npu_bundle else None}
    spec = importlib.util.spec_from_file_location('owned_media', Path(__file__).with_name('test-indexed-media.py'))
    owned = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(owned)
    owner = owned.OwnedJob()
    sample_stop = threading.Event()
    def sample_memory():
        while not sample_stop.wait(.05): owner.sample()
    sampler = threading.Thread(target=sample_memory, daemon=True)
    started = time.monotonic()
    with (args.output / 'worker.log').open('wb') as log:
        npu_args = ['--npu-bundle', str(args.npu_bundle.resolve())] if args.npu_bundle else []
        environment = {**os.environ, 'WS_EMBED_STUB': '0'}
        if args.clean_env:
            environment = {key: value for key, value in os.environ.items()
                if not key.startswith(('WS_', 'LLAMA_', 'XRT_', 'TACONITE_', 'GGML_', 'VK_', 'CUDA_'))}
            environment['PATH'] = str(Path(os.environ['SystemRoot']) / 'System32')
        environment.update(WINSEARCH_DATA_DIR=str(data), WINSEARCH_INSTANCE_ID='media-runtime-test')
        environment['RUST_LOG'] = 'ws_embed=info,winsearch_embed=info'
        child = subprocess.Popen([str(args.worker.resolve()), '--pipe', pipe, '--model', str(args.model.resolve()),
            '--backend', args.backend, '--idle-secs', '120', *npu_args], stdin=subprocess.DEVNULL, stdout=log, stderr=log,
            creationflags=subprocess.CREATE_NO_WINDOW, env=environment, cwd=args.working_directory)
        owner.assign(child)
        sampler.start()
        timer = threading.Timer(180, child.kill)
        timer.daemon = True
        timer.start()
        try:
            deadline = time.monotonic() + 10
            while True:
                try:
                    stream = open(pipe, 'r+b', buffering=0)
                    break
                except OSError:
                    if child.poll() is not None or time.monotonic() >= deadline:
                        raise RuntimeError('Worker pipe unavailable; inspect worker.log')
                    time.sleep(.05)
            with stream:
                sequence = 0

                def exact(n):
                    result = bytearray()
                    while len(result) < n:
                        part = stream.read(n - len(result))
                        if not part:
                            raise RuntimeError('Worker closed pipe')
                        result.extend(part)
                    return bytes(result)

                def request(kind, **fields):
                    nonlocal sequence
                    sequence += 1
                    payload = {'type': kind, **fields}
                    if kind != 'hello':
                        payload['id'] = sequence
                    raw = json.dumps(payload).encode()
                    frame = memoryview(struct.pack('<I', len(raw)) + raw)
                    while frame:
                        written = stream.write(frame)
                        if not written:
                            raise RuntimeError('Worker write failed')
                        frame = frame[written:]
                    length = struct.unpack('<I', exact(4))[0]
                    assert 0 < length <= 16 * 1024 * 1024
                    reply = json.loads(exact(length))
                    if kind != 'hello':
                        assert reply.get('id') == sequence, reply
                    return reply

                def embed(kind, **fields):
                    started = time.monotonic()
                    reply = request(kind, **fields)
                    expected = 'cpu' if kind == 'embed_audio' or fields.get('token_budget') == 70 else args.backend
                    count = len(fields['texts']) if kind == 'embed_texts' else 1
                    vectors = validate_vectors(reply, count, expected)
                    elapsed = time.monotonic() - started
                    summary['jobs'].append({'kind': kind, 'backend': reply['backend'], 'count': count,
                        'elapsedSeconds': elapsed, 'itemsPerSecond': count / elapsed})
                    return vectors

                def cosine(a, b):
                    return sum(x*y for x,y in zip(a,b)) / math.sqrt(sum(x*x for x in a)*sum(y*y for y in b))

                ack = request('hello', protocol_version=1, client='native-media-acceptance', client_version='1.0')
                assert ack['type'] == 'hello_ack' and ack['stub'] is False
                embed('embed_texts', texts=['task: search result | query: a train to Stockholm'])
                images = {}
                for name, file, budget in [('screen-280','screen.png',280), ('photo-140','photo.jpg',140), ('frame-70','photo.jpg',70)]:
                    vectors = embed('embed_image', path=str((args.fixtures / file).resolve()), token_budget=budget)
                    vector = vectors[0]
                    agreement = cosine(vector, reference[name])
                    summary['references'][name] = agreement
                    assert agreement >= .999, (name, agreement)
                    images[name] = vector
                audio = embed('embed_audio', path=str((args.fixtures / 'speech.wav').resolve()), window_ms=None)[0]
                summary['references']['speech'] = cosine(audio, reference['speech'])
                assert summary['references']['speech'] >= .999
                assert audio == embed('embed_audio', path=str((args.fixtures / 'speech.wav').resolve()), window_ms=None)[0]
                assert audio == embed('embed_audio', path=str((args.fixtures / 'speech.wav').resolve()), window_ms=[0,4000])[0]
                summary['image_pair_cosine'] = cosine(images['screen-280'], images['photo-140'])
                assert summary['image_pair_cosine'] < .95
                ack = request('hello', protocol_version=1, client='capability-after-load', client_version='1.0')
                assert {'embed_image','embed_audio','embed_texts'} <= set(ack['features'])
                invalid = request('embed_audio', path='missing.wav', window_ms=[0,30001])
                assert invalid['type'] == 'error' and invalid['code'] == 'invalid_request'
                spoken = [embed('embed_audio', path=str((args.spoken / (name + '.wav')).resolve()), window_ms=None)[0]
                          for name in ['railway','kitchen']]
                queries = [
                    'a computer application screenshot with a red warning and a blue button',
                    'a landscape with a tree and green hills under a blue sky',
                    'a train to Stockholm leaves the railway station from platform five',
                    'baking a chocolate cake in the kitchen with flour butter eggs and sugar',
                ]
                text_vectors = embed('embed_texts', texts=['task: search result | query: ' + q for q in queries])
                for index, (query, vector) in enumerate(zip(queries, text_vectors)):
                    candidates = [images['screen-280'],images['photo-140']] if index < 2 else spoken
                    scores = [cosine(vector, candidate) for candidate in candidates]
                    expected = index % 2
                    summary['retrieval'].append({'query': query, 'expected': expected, 'scores': scores})
                    assert scores[expected] > scores[1-expected], (query, scores)
                for _ in range(args.warm_repetitions):
                    embed('embed_texts', texts=['task: search result | query: a train to Stockholm'])
                status = request('status')
                assert status['type'] == 'worker_status' and status['stub'] is False
                if args.backend == 'npu':
                    assert status.get('npu') == 'ready', status
                summary['workerStatus'] = status
                end = request('shutdown')
                assert end['type'] == 'shutdown_ack'
            child.wait(timeout=5)
            assert child.returncode == 0
        finally:
            timer.cancel()
            if child.poll() is None:
                child.kill()
                child.wait(timeout=5)
            summary['worker_exit'] = child.returncode
            sample_stop.set()
            sampler.join(timeout=2)
            summary['peakWorkingSetBytes'] = owner.peak_working_set
            summary['peakPrivateBytes'] = owner.peak_private_bytes
            cleanup_deadline = time.monotonic() + 5
            while owner.sample() and time.monotonic() < cleanup_deadline: time.sleep(.025)
            summary['ownedJobEmpty'] = not owner.sample()
            owner.close()
            summary['elapsed_seconds'] = round(time.monotonic() - started, 3)
            (args.output / 'results.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
    assert summary['ownedJobEmpty'], 'Owned worker job did not become empty'
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
