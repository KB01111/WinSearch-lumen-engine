"""Generate frozen synthetic retrieval inputs using local file-only Windows speech."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import wave
from PIL import Image, ImageDraw, ImageFont

root = Path(__file__).resolve().parent.parent
output = root/'artifacts/retrieval-fixtures'
assert not output.exists(), 'Inspect existing fixtures before regenerating'
output.mkdir(parents=True)
manifest = {'method': __doc__, 'voice': 'Microsoft Zira Desktop', 'files': []}
for split in ['development', 'held-out']:
    suite_path = root/'evals'/f'retrieval-{split}.json'
    suite = json.loads(suite_path.read_text(encoding='utf-8'))
    destination = output/split
    destination.mkdir()
    for name in ['screen', 'receipt', 'frame']:
        image = Image.new('RGB', (1000, 700), '#f3f4f6')
        draw = ImageDraw.Draw(image)
        draw.rectangle((30, 30, 970, 670), fill='white', outline='#a0a0a0', width=2)
        for ordinal, line in enumerate(suite['media_generation'][name]):
            font = ImageFont.truetype('C:/Windows/Fonts/segoeui.ttf', 34 if ordinal else 42)
            assert draw.textbbox((0, 0), line, font=font)[2] < 880, 'Panel text overflows'
            draw.text((65, 90 + ordinal*90), line, font=font, fill='#111111')
        image.save(destination/f'{name}.png')
    script = r'''
$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName System.Speech
$taskSuite = [IO.File]::ReadAllText($env:WS_FIXTURE_SUITE) | ConvertFrom-Json
$taskSpeech = New-Object System.Speech.Synthesis.SpeechSynthesizer
try {
    $taskSpeech.SetOutputToNull()
    $taskSpeech.SelectVoice('Microsoft Zira Desktop')
    $taskFormat = [System.Speech.AudioFormat.SpeechAudioFormatInfo]::new(16000, [System.Speech.AudioFormat.AudioBitsPerSample]::Sixteen, [System.Speech.AudioFormat.AudioChannel]::Mono)
    foreach ($taskKind in @('speech','soundtrack')) {
        $taskSpeech.SetOutputToWaveFile((Join-Path $env:WS_FIXTURE_OUTPUT ($taskKind+'.wav')), $taskFormat)
        $taskSpeech.Speak($taskSuite.media_generation.$taskKind)
        $taskSpeech.SetOutputToNull()
    }
} finally { $taskSpeech.Dispose() }
'''
    subprocess.run(['powershell', '-NoProfile', '-NonInteractive', '-Command', script],
                   env={**os.environ, 'WS_FIXTURE_SUITE': str(suite_path), 'WS_FIXTURE_OUTPUT': str(destination)},
                   check=True, timeout=60, capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
    for name, duration in [('speech', 30), ('soundtrack', 15)]:
        path = destination/f'{name}.wav'
        with wave.open(str(path), 'rb') as source:
            assert (source.getnchannels(), source.getsampwidth(), source.getframerate()) == (1, 2, 16000)
            pcm = source.readframes(source.getnframes())
        assert len(pcm) <= duration*32000, 'Speech exceeds the frozen window'
        with wave.open(str(path), 'wb') as target:
            target.setnchannels(1)
            target.setsampwidth(2)
            target.setframerate(16000)
            target.writeframes(pcm + bytes(duration*32000-len(pcm)))
    for path in sorted(destination.iterdir()):
        manifest['files'].append({'path': str(path.relative_to(root)), 'bytes': path.stat().st_size,
                                  'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
assert len(manifest['files']) == 10
(output/'generation.json').write_text(json.dumps(manifest, indent=2), encoding='utf-8')
print(json.dumps({'output': str(output), 'files': len(manifest['files']), 'method': __doc__}))
