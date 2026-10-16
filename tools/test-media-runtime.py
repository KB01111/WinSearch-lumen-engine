"""Real CPU worker/reference/retrieval acceptance over generated public inputs."""
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ['worker', 'model', 'fixtures', 'spoken', 'output']:
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args()
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    data = args.output / ('data-' + uuid.uuid4().hex)
    (data / 'models').mkdir(parents=True)
    # A different profile projector must never replace an explicit model's sibling.
    (data / 'models/mmproj-embeddinggemma-2-Q8_0.gguf').write_bytes(b'wrong profile projector')
    pipe = r'\\.\pipe\winsearch-media-' + uuid.uuid4().hex
    reference = json.loads((args.fixtures / 'reference-media-vectors.json').read_text())['vectors']
    summary = {'backend': 'cpu', 'stub': False, 'references': {}, 'retrieval': []}
    started = time.monotonic()
    with (args.output / 'worker.log').open('wb') as log:
        child = subprocess.Popen([str(args.worker.resolve()), '--pipe', pipe, '--model', str(args.model.resolve()),
            '--backend', 'cpu', '--idle-secs', '120'], stdin=subprocess.DEVNULL, stdout=log, stderr=log,
            creationflags=subprocess.CREATE_NO_WINDOW, env={**os.environ, 'WS_EMBED_STUB': '0',
            'WINSEARCH_DATA_DIR': str(data), 'WINSEARCH_INSTANCE_ID': 'media-runtime-test'})
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
                    reply = request(kind, **fields)
                    assert reply['type'] == 'embeddings' and reply['stub'] is False and reply['backend'] == 'cpu', reply
                    assert reply['dim'] == 768
                    for vector in reply['vecs']:
                        assert len(vector) == 768 and all(math.isfinite(v) for v in vector)
                        assert abs(sum(v*v for v in vector) - 1) < 1e-3
                    return reply['vecs']

                def cosine(a, b):
                    return sum(x*y for x,y in zip(a,b)) / math.sqrt(sum(x*x for x in a)*sum(y*y for y in b))

                ack = request('hello', protocol_version=1, client='native-media-acceptance', client_version='1.0')
                assert ack['type'] == 'hello_ack' and ack['stub'] is False
                images = {}
                for name, file, budget in [('screen-280','screen.png',280), ('photo-140','photo.jpg',140), ('frame-70','photo.jpg',70)]:
                    vectors = embed('embed_image', path=str((args.fixtures / file).resolve()), token_budget=budget)
                    assert len(vectors) == 1
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
                assert len(text_vectors) == 4
                for index, (query, vector) in enumerate(zip(queries, text_vectors)):
                    candidates = [images['screen-280'],images['photo-140']] if index < 2 else spoken
                    scores = [cosine(vector, candidate) for candidate in candidates]
                    expected = index % 2
                    summary['retrieval'].append({'query': query, 'expected': expected, 'scores': scores})
                    assert scores[expected] > scores[1-expected], (query, scores)
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
            summary['elapsed_seconds'] = round(time.monotonic() - started, 3)
            (args.output / 'results.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
