"""Offline/public source fixtures; no product or model acceptance is claimed."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import wave
from PIL import Image, ImageDraw, ImageFont

root = Path(__file__).resolve().parent.parent
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--output', type=Path, default=root/'artifacts/media-fixtures')
parser.add_argument('--decoder-dir', type=Path, default=root/'artifacts/dependencies/ffmpeg-2026-09-30/ffmpeg-n9.0.2-17-g2a571b6068-win64-lgpl-shared-9.0/bin')
args = parser.parse_args()
output = args.output.resolve()
binary = args.decoder_dir.resolve()
inputs = root/'src/engine/crates/ws-embed/tests/fixtures/media'
audio = root/'artifacts/spoken-fixtures/two-passages-65s.wav'
assert audio.is_file(), 'Generate offline spoken fixtures first'
assert (binary/'ffmpeg.exe').is_file() and (binary/'ffprobe.exe').is_file()
output.mkdir(parents=True, exist_ok=False)


def run(arguments):
    result = subprocess.run([str(binary/'ffmpeg.exe'), '-hide_banner', '-loglevel', 'error', '-nostdin', '-n', *arguments],
                            capture_output=True, timeout=60, creationflags=subprocess.CREATE_NO_WINDOW)
    if result.returncode:
        raise RuntimeError(result.stderr.decode('utf-8', errors='replace')[:8192])


video = output/'時刻 clip.mp4'
scale = 'scale=480:300:force_original_aspect_ratio=decrease,pad=480:300:(ow-iw)/2:(oh-ih)/2,setsar=1,fps=4'
graph = f'[0:v]{scale},trim=duration=32.75,setpts=PTS-STARTPTS[a];[1:v]{scale},trim=duration=32.25,setpts=PTS-STARTPTS[b];[a][b]concat=n=2:v=1:a=0[v]'
run(['-loop','1','-framerate','4','-i',str(inputs/'photo.jpg'),'-loop','1','-framerate','4','-i',str(inputs/'screen.png'),
     '-i',str(audio),'-filter_complex',graph,'-map','[v]','-map','2:a:0','-c:v','mpeg4','-q:v','3','-pix_fmt','yuv420p',
     '-c:a','aac','-b:a','96k','-t','65',str(video)])
silent_video = output/'no audio clip.mp4'
run(['-i',str(video),'-map','0:v:0','-c:v','copy','-an','-sn','-dn',str(silent_video)])
audio_only = output/'audio only.m4a'
run(['-i',str(audio),'-map','0:a:0','-vn','-sn','-dn','-c:a','aac','-b:a','96k',str(audio_only)])
with wave.open(str(output/'-silence.wav'), 'wb') as target:
    target.setnchannels(1); target.setsampwidth(2); target.setframerate(16000)
    target.writeframes(bytes(16000*10*2))

for name, offset in [('short audio track.mp4', '0'), ('late audio track.mp4', '40')]:
    run(['-i',str(silent_video),'-itsoffset',offset,'-i',str(audio.with_name('railway.wav')),
         '-map','0:v:0','-map','1:a:0','-c:v','copy','-c:a','aac','-t','65',str(output/name)])

variable = output/'variable rate.mp4'
graph = ("[0:v]scale=480:300,setsar=1[p];[1:v]scale=480:300,setsar=1[s];"
         "[p][s]blend=all_expr='if(eq(mod(N,2),0),A,B)',"
         "select='eq(n,0)+eq(n,7)+eq(n,47)+eq(n,69)+eq(n,140)+eq(n,201)'[v]")
run(['-loop','1','-framerate','30','-i',str(inputs/'photo.jpg'),'-loop','1','-framerate','30','-i',str(inputs/'screen.png'),
     '-filter_complex',graph,'-map','[v]','-frames:v','6','-fps_mode','passthrough','-enc_time_base','1/30',
     '-c:v','mpeg4','-q:v','3','-pix_fmt','yuv420p',str(variable)])

screen = output/'screen-ocr.png'
image = Image.new('RGB', (1000,600), '#f4f5f7')
draw = ImageDraw.Draw(image)
font = ImageFont.truetype('C:/Windows/Fonts/segoeuib.ttf', 48)
small = ImageFont.truetype('C:/Windows/Fonts/segoeui.ttf', 36)
draw.rectangle((35,35,965,565), fill='white', outline='#bbbbbb', width=3)
draw.text((70,90),'DEVICE CONNECTION LOST',font=font,fill='#b00020')
draw.text((70,220),'Fault code E4821',font=small,fill='#101010')
draw.text((70,310),'Reconnect the device and retry.',font=small,fill='#101010')
image.save(screen)
run(['-loop','1','-framerate','4','-i',str(screen),'-t','2','-c:v','mpeg4','-q:v','2','-pix_fmt','yuv420p',str(output/'screen OCR.mp4')])

manifest = {'scope':__doc__, 'duration_ms':65000, 'shot_change_ms':32750,
            'speech_source':str(audio), 'late_speech_start_ms':40000, 'files':[]}
for path in sorted(output.glob('*')):
    if path.suffix not in ['.mp4','.m4a','.wav']:
        continue
    result = subprocess.run([str(binary/'ffprobe.exe'),'-v','error','-show_entries',
        'format=duration:stream=codec_type,codec_name,width,height,start_time,duration','-of','json','-i',str(path)],
        capture_output=True,timeout=10,check=True,creationflags=subprocess.CREATE_NO_WINDOW)
    metadata = json.loads(result.stdout)
    duration = float(metadata['format']['duration'])
    assert 0 < duration < 66, metadata
    if path.name in ['時刻 clip.mp4','no audio clip.mp4','short audio track.mp4','late audio track.mp4']:
        assert abs(duration-65) < .25
    manifest['files'].append({'name':path.name,'bytes':path.stat().st_size,
                             'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'probe':metadata})
frames = subprocess.run([str(binary/'ffprobe.exe'),'-v','error','-select_streams','v:0','-show_frames',
    '-show_entries','frame=best_effort_timestamp_time','-of','json','-i',str(variable)],
    capture_output=True,timeout=10,check=True,creationflags=subprocess.CREATE_NO_WINDOW)
timestamps = [float(f['best_effort_timestamp_time']) for f in json.loads(frames.stdout)['frames']]
assert len(timestamps) == 6 and all(abs(a-b/30) < .001 for a,b in zip(timestamps,[0,7,47,69,140,201])), timestamps
manifest['variable_frame_times_seconds'] = timestamps
(output/'generation.json').write_text(json.dumps(manifest,indent=2),encoding='utf-8')
print(json.dumps({'directory':str(output),'files':len(manifest['files']),'variable_frame_times':timestamps}))
