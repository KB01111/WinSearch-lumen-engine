import hashlib
import json
from pathlib import Path
import wave

root = Path(__file__).resolve().parent.parent / 'artifacts' / 'spoken-fixtures'
output = root / 'two-passages-65s.wav'
if output.exists():
    raise RuntimeError('Composite fixture exists; inspect before replacing')
rate = 16000
duration = 65
data = bytearray(rate * duration * 2)
passages = []
for filename, seconds in [('railway.wav', 4), ('kitchen.wav', 51)]:
    path = root / filename
    with wave.open(str(path), 'rb') as source:
        assert (source.getnchannels(), source.getsampwidth(), source.getframerate(), source.getcomptype()) == (1, 2, rate, 'NONE')
        frame_count = source.getnframes()
        samples = source.readframes(frame_count)
    start_byte = seconds * rate * 2
    assert start_byte + len(samples) <= len(data)
    data[start_byte:start_byte + len(samples)] = samples
    passages.append({'file': filename, 'start_ms': seconds * 1000,
                     'end_ms': seconds * 1000 + frame_count * 1000 / rate,
                     'frames': frame_count, 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
with wave.open(str(output), 'wb') as target:
    target.setnchannels(1)
    target.setsampwidth(2)
    target.setframerate(rate)
    target.writeframes(data)
manifest = {'file': output.name, 'duration_ms': duration * 1000,
            'passages': passages, 'sha256': hashlib.sha256(output.read_bytes()).hexdigest(),
            'expected_window_ms': [[0, 30000], [25000, 55000], [50000, 65000]],
            'method': 'PCM composition of local OS TTS at known offsets, silence elsewhere; no model inference'}
(root / 'composition.json').write_text(json.dumps(manifest, indent=2), encoding='utf-8')
print(json.dumps(manifest, indent=2))
