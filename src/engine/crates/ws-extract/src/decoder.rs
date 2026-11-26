//! Private, bounded decoder CLI. Only the extraction child calls these helpers.
use crate::{ExtractBudget, ExtractError};
use serde::Deserialize;
use std::{
    io::Read,
    path::{Component, Path, PathBuf, Prefix},
    process::{Command, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};
use ws_platform::child_job::ChildJob;

pub const MAX_STREAMS: usize = 32;
pub const MAX_DURATION_MS: u64 = ws_index::model::MAX_MEDIA_DURATION_MS;
pub const MAX_PCM_BYTES: usize = 30 * 16000 * 2;
pub const MAX_SHOT_SAMPLES: usize = ws_index::model::MAX_VIDEO_SHOT_SAMPLES;
pub const SHOT_PIXELS: usize = ws_index::model::VIDEO_GRAY_PIXELS;
const FORMATS: &str =
    "aac,ac3,aiff,ape,asf,au,avi,flac,flv,matroska,webm,mov,mp3,mpeg,mpegts,ogg,wav,wv";

#[derive(Debug, Clone)]
pub struct MediaInfo {
    pub duration_ms: u64,
    pub has_audio: bool,
    pub has_video: bool,
    pub video_pixels_per_second: Option<u64>,
}

#[derive(Deserialize)]
struct Probe {
    streams: Vec<Stream>,
    format: Format,
}
#[derive(Deserialize)]
struct Stream {
    codec_type: String,
    codec_name: Option<String>,
    width: Option<u32>,
    height: Option<u32>,
    avg_frame_rate: Option<String>,
    r_frame_rate: Option<String>,
    #[serde(default)]
    disposition: Disposition,
}
#[derive(Default, Deserialize)]
struct Disposition {
    #[serde(default)]
    attached_pic: u32,
}
#[derive(Deserialize)]
struct Format {
    duration: String,
}

pub fn parse_probe(bytes: &[u8]) -> Result<MediaInfo, ExtractError> {
    if bytes.len() > 65536 {
        return Err(decode("media probe exceeds 64 KiB"));
    }
    let probe: Probe =
        serde_json::from_slice(bytes).map_err(|e| decode(format!("invalid media probe: {e}")))?;
    let duration_ms = duration_ms(&probe.format.duration)?;
    if probe.streams.is_empty() || probe.streams.len() > MAX_STREAMS {
        return Err(decode("invalid media stream count"));
    }
    let mut info = MediaInfo {
        duration_ms,
        has_audio: false,
        has_video: false,
        video_pixels_per_second: None,
    };
    for stream in probe.streams {
        if matches!(stream.codec_type.as_str(), "audio" | "video")
            && stream
                .codec_name
                .as_deref()
                .is_none_or(|s| s.is_empty() || s == "unknown")
        {
            return Err(decode("unsupported media codec"));
        }
        match stream.codec_type.as_str() {
            "audio" => info.has_audio = true,
            "video" if stream.disposition.attached_pic == 0 => {
                let (width, height) = (stream.width.unwrap_or(0), stream.height.unwrap_or(0));
                if width == 0
                    || height == 0
                    || width > 16384
                    || height > 16384
                    || u64::from(width) * u64::from(height) > 67_108_864
                {
                    return Err(decode("media dimensions exceed bounded decoder input"));
                }
                if !info.has_video {
                    info.video_pixels_per_second = [
                        stream.avg_frame_rate.as_deref(),
                        stream.r_frame_rate.as_deref(),
                    ]
                    .into_iter()
                    .flatten()
                    .filter_map(|value| {
                        if value.len() > 32 {
                            return None;
                        }
                        let (numerator, denominator) = value.split_once('/')?;
                        let (numerator, denominator) = (
                            numerator.parse::<u64>().ok()?,
                            denominator.parse::<u64>().ok()?,
                        );
                        if numerator == 0 || denominator == 0 {
                            return None;
                        }
                        u64::from(width)
                            .checked_mul(u64::from(height))?
                            .checked_mul(numerator)
                            .map(|pixels| pixels.div_ceil(denominator))
                            .filter(|pixels| *pixels > 0)
                    })
                    .max();
                }
                info.has_video = true;
            }
            _ => (),
        }
    }
    if !info.has_audio && !info.has_video {
        return Err(decode("no supported media streams"));
    }
    Ok(info)
}

fn duration_ms(value: &str) -> Result<u64, ExtractError> {
    let invalid = || decode("media duration outside finite one-year bound");
    if value.is_empty() || value.len() > 32 {
        return Err(invalid());
    }
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    if whole.is_empty()
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || !fraction.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(invalid());
    }
    let seconds: u64 = whole.parse().map_err(|_| invalid())?;
    let mut millis = 0;
    for position in 0..3 {
        millis = millis * 10
            + u64::from(fraction.as_bytes().get(position).copied().unwrap_or(b'0') - b'0');
    }
    millis += u64::from(fraction.bytes().skip(3).any(|b| b != b'0'));
    let result = seconds
        .checked_mul(1000)
        .and_then(|s| s.checked_add(millis))
        .ok_or_else(invalid)?;
    if result == 0 || result > MAX_DURATION_MS {
        return Err(invalid());
    }
    Ok(result)
}

fn directory() -> Result<PathBuf, ExtractError> {
    #[cfg(debug_assertions)]
    if let Some(path) = std::env::var_os("WINSEARCH_TEST_DECODER_DIR") {
        return Ok(path.into());
    }
    Ok(std::env::current_exe()?
        .parent()
        .ok_or_else(|| decode("worker directory missing"))?
        .to_path_buf())
}

fn command(tool: &str) -> Result<Command, ExtractError> {
    let binary = directory()?.join(tool);
    if !binary.is_absolute()
        || !binary.is_file()
        || !ws_platform::content::local_content_allowed(&binary)
    {
        return Err(decode(format!(
            "pinned decoder dependency missing/unsafe: {}",
            binary.display()
        )));
    }
    let mut command = Command::new(binary);
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command.args([
        "-hide_banner",
        "-loglevel",
        "error",
        "-max_alloc",
        "67108864",
    ]);
    Ok(command)
}

fn input(command: &mut Command, path: &Path) -> Result<(), ExtractError> {
    let disk_path = matches!(path.components().next(), Some(Component::Prefix(p))
        if matches!(p.kind(), Prefix::Disk(_) | Prefix::VerbatimDisk(_)));
    if !path.is_absolute()
        || !disk_path
        || !path.is_file()
        || path
            .components()
            .any(|c| matches!(c, Component::Normal(s) if s.to_string_lossy().contains(':')))
        || !ws_platform::content::local_content_allowed(path)
    {
        return Err(decode(
            "media source must be an absolute resident local file",
        ));
    }
    // These demuxers do not include playlists/concat/image sequences. MOV external
    // tracks and absolute references stay disabled; file protocol alone is insufficient.
    command.args([
        "-protocol_whitelist",
        "file",
        "-format_whitelist",
        FORMATS,
        "-max_streams",
        "32",
        "-probesize",
        "5000000",
        "-analyzeduration",
        "10000000",
    ]);
    Ok(())
}

pub fn probe(path: &Path, budget: &ExtractBudget) -> Result<MediaInfo, ExtractError> {
    budget.check_deadline()?;
    budget.check_size(std::fs::metadata(path)?.len())?;
    let mut command = command("ffprobe.exe")?;
    input(&mut command, path)?;
    command.args(["-show_entries", "format=duration:stream=codec_type,codec_name,width,height,avg_frame_rate,r_frame_rate:stream_disposition=attached_pic",
        "-of", "json", "-i"]).arg(path);
    parse_probe(&capture(&mut command, 65536, budget)?)
}

pub fn audio_window(
    path: &Path,
    start_ms: u64,
    end_ms: u64,
    budget: &ExtractBudget,
) -> Result<Vec<u8>, ExtractError> {
    if end_ms <= start_ms || end_ms - start_ms > 30000 {
        return Err(decode("invalid audio window"));
    }
    let mut command = command("ffmpeg.exe")?;
    command.args(["-nostdin", "-threads", "2", "-xerror"]);
    input(&mut command, path)?;
    command
        .arg("-ss")
        .arg(seconds(start_ms))
        .arg("-i")
        .arg(path)
        .arg("-t")
        .arg(seconds(end_ms - start_ms))
        .arg("-af")
        .arg(format!(
            "aresample=16000:async=1:first_pts=0,apad=whole_len={}",
            (end_ms - start_ms) * 16
        ))
        .args([
            "-map",
            "0:a:0",
            "-vn",
            "-sn",
            "-dn",
            "-ac",
            "1",
            "-ar",
            "16000",
            "-c:a",
            "pcm_s16le",
            "-threads",
            "2",
            "-f",
            "s16le",
            "pipe:1",
        ]);
    let pcm = capture(&mut command, MAX_PCM_BYTES, budget)?;
    if pcm.len() != ((end_ms - start_ms) * 32) as usize {
        return Err(decode(
            "decoded audio does not cover the bounded source window",
        ));
    }
    Ok(pcm)
}

pub fn seconds(ms: u64) -> String {
    format!("{}.{:03}", ms / 1000, ms % 1000)
}

pub struct VideoSample {
    pub time_ms: u64,
    pub gray: Vec<u8>,
}

pub struct VideoFrame {
    pub time_ms: u64,
    pub png: Vec<u8>,
}

/// Select source frames rather than synthesizing an fps grid: timestamps stay
/// exact even for variable-rate or sparse videos.
pub fn sample_video(
    path: &Path,
    interval_ms: u64,
    budget: &ExtractBudget,
) -> Result<Vec<VideoSample>, ExtractError> {
    if interval_ms == 0 || interval_ms > MAX_DURATION_MS {
        return Err(decode("invalid shot sampling interval"));
    }
    let mut command = video_command(path)?;
    command.arg("-i").arg(path).args(["-map","0:V:0","-an","-sn","-dn","-vf"])
        .arg(format!("select='gte(t,0)*(isnan(prev_selected_t)+gte(t-prev_selected_t,{}))',trim=end_frame={},scale=32:18,format=gray,showinfo",
            seconds(interval_ms), MAX_SHOT_SAMPLES))
        .args(["-frames:v"]).arg(MAX_SHOT_SAMPLES.to_string())
        .args(["-fps_mode","passthrough","-c:v","rawvideo","-threads","2","-f","rawvideo","pipe:1"]);
    let (pixels, log) = capture_outputs(
        &mut command,
        MAX_SHOT_SAMPLES * SHOT_PIXELS,
        8 * 1024 * 1024,
        budget,
    )?;
    let times = frame_times(&log, MAX_SHOT_SAMPLES)?;
    if pixels.len() % SHOT_PIXELS != 0
        || times.len() != pixels.len() / SHOT_PIXELS
        || times.is_empty()
    {
        return Err(decode("shot sample pixels/timestamps do not agree"));
    }
    Ok(times
        .into_iter()
        .zip(pixels.chunks_exact(SHOT_PIXELS))
        .map(|(time_ms, p)| VideoSample {
            time_ms,
            gray: p.to_vec(),
        })
        .collect())
}

/// Decode only one source-time slice. Input seeking retains GOP preroll; the
/// filter's absolute PTS gate prevents an adjacent slice from owning the same frame.
pub fn sample_video_slice(
    path: &Path,
    start_ms: u64,
    end_ms: u64,
    interval_ms: u64,
    previous_ms: Option<u64>,
    budget: &ExtractBudget,
) -> Result<Vec<VideoSample>, ExtractError> {
    if start_ms >= end_ms
        || end_ms > MAX_DURATION_MS
        || end_ms - start_ms > ws_index::model::MAX_VIDEO_SELECTION_SPAN_MS
        || interval_ms == 0
        || interval_ms > MAX_DURATION_MS
        || previous_ms.is_some_and(|t| t >= start_ms)
    {
        return Err(decode("invalid bounded shot scan"));
    }
    let first_allowed =
        previous_ms.map_or(start_ms, |t| t.saturating_add(interval_ms).max(start_ms));
    let cap = ((end_ms - start_ms).div_ceil(interval_ms) + 1).min(512) as usize;
    let mut command = video_command(path)?;
    command.arg("-ss").arg(seconds(start_ms)).arg("-t").arg(seconds(end_ms-start_ms))
        .arg("-i").arg(path).args(["-map","0:V:0","-an","-sn","-dn","-vf"])
        .arg(format!("select='gte(t,{})*lt(t,{})*(isnan(prev_selected_t)+gte(t-prev_selected_t,{}))',scale=32:18,format=gray,showinfo",
            seconds(first_allowed), seconds(end_ms), seconds(interval_ms)))
        .args(["-frames:v"]).arg(cap.to_string())
        .args(["-fps_mode","passthrough","-c:v","rawvideo","-threads","2","-f","rawvideo","pipe:1"]);
    let (pixels, log) = capture_outputs(&mut command, cap * SHOT_PIXELS, 512 * 1024, budget)?;
    let times = frame_times(&log, cap)?;
    if pixels.len() % SHOT_PIXELS != 0
        || times.len() != pixels.len() / SHOT_PIXELS
        || times.iter().any(|t| *t < start_ms || *t >= end_ms)
        || times
            .windows(2)
            .any(|p| p[1] < p[0].saturating_add(interval_ms))
    {
        return Err(decode("bounded shot pixels/timestamps do not agree"));
    }
    Ok(times
        .into_iter()
        .zip(pixels.chunks_exact(SHOT_PIXELS))
        .map(|(time_ms, p)| VideoSample {
            time_ms,
            gray: p.to_vec(),
        })
        .collect())
}

fn video_command(path: &Path) -> Result<Command, ExtractError> {
    let mut command = command("ffmpeg.exe")?;
    command.args([
        "-nostdin",
        "-threads",
        "2",
        "-xerror",
        "-loglevel",
        "info",
        "-nostats",
        "-copyts",
        "-start_at_zero",
    ]);
    input(&mut command, path)?;
    Ok(command)
}

pub fn video_frame(
    path: &Path,
    time_ms: u64,
    budget: &ExtractBudget,
) -> Result<Option<VideoFrame>, ExtractError> {
    if time_ms >= MAX_DURATION_MS {
        return Err(decode("video frame time outside bound"));
    }
    let mut command = video_command(path)?;
    command.arg("-ss").arg(seconds(time_ms)).arg("-i").arg(path)
        .args(["-map","0:V:0","-an","-sn","-dn","-frames:v","1","-vf",
            "trim=end_frame=1,scale=w='min(1280,iw)':h='min(1280,ih)':force_original_aspect_ratio=decrease,showinfo",
            "-fps_mode","passthrough","-c:v","png","-threads","2","-f","image2pipe","pipe:1"]);
    let (png, log) = capture_outputs(&mut command, 16 * 1024 * 1024, 65536, budget)?;
    let times = frame_times(&log, 1)?;
    if png.is_empty() && times.is_empty() {
        return Ok(None);
    }
    if times.len() != 1 || !png.starts_with(b"\x89PNG\r\n\x1a\n") {
        return Err(decode("decoded frame pixels/timestamp missing"));
    }
    Ok(Some(VideoFrame {
        time_ms: times[0],
        png,
    }))
}

fn frame_times(log: &[u8], max_frames: usize) -> Result<Vec<u64>, ExtractError> {
    let mut base = None;
    let mut times = Vec::new();
    for line in String::from_utf8_lossy(log).lines() {
        if !line.starts_with("[Parsed_showinfo_") {
            continue;
        }
        if let Some((_, value)) = line.split_once("config in time_base: ") {
            let (a, b) = value
                .split(',')
                .next()
                .unwrap_or("")
                .trim()
                .split_once('/')
                .ok_or_else(|| decode("frame time base missing"))?;
            let numerator = a
                .parse::<u32>()
                .ok()
                .filter(|n| *n > 0)
                .ok_or_else(|| decode("invalid frame time base"))?;
            let denominator = b
                .parse::<u32>()
                .ok()
                .filter(|n| *n > 0)
                .ok_or_else(|| decode("invalid frame time base"))?;
            if base.is_some_and(|v| v != (numerator, denominator)) {
                return Err(decode("frame time base changed"));
            }
            base = Some((numerator, denominator));
        }
        let Some((_, body)) = line.split_once("] n:") else {
            continue;
        };
        let ordinal = body
            .split_whitespace()
            .next()
            .and_then(|v| v.parse::<usize>().ok())
            .ok_or_else(|| decode("invalid frame ordinal"))?;
        let pts = body
            .split_once(" pts:")
            .and_then(|(_, v)| v.split_whitespace().next())
            .and_then(|v| v.parse::<u64>().ok())
            .ok_or_else(|| decode("invalid source frame PTS"))?;
        let (n, d) = base.ok_or_else(|| decode("frame PTS before time base"))?;
        // Floor before an exact seek so rounding never skips the selected frame.
        let millis = u128::from(pts) * u128::from(n) * 1000 / u128::from(d);
        if ordinal != times.len()
            || times.len() >= max_frames
            || millis >= u128::from(MAX_DURATION_MS)
            || times.last().is_some_and(|last| u128::from(*last) > millis)
        {
            return Err(decode("frame timestamp/count exceeds source bounds"));
        }
        times.push(millis as u64);
    }
    Ok(times)
}
pub fn decode(message: impl Into<String>) -> ExtractError {
    ExtractError::Decode(message.into())
}

fn read_capped(
    mut pipe: impl Read,
    cap: usize,
    overflow: Arc<AtomicBool>,
) -> std::io::Result<Vec<u8>> {
    let mut result = Vec::new();
    let mut buffer = [0; 8192];
    loop {
        let n = pipe.read(&mut buffer)?;
        if n == 0 {
            return Ok(result);
        }
        if result.len() + n > cap {
            overflow.store(true, Ordering::Release);
            return Err(std::io::Error::other("decoder capture exceeds bound"));
        }
        result.extend_from_slice(&buffer[..n]);
    }
}

/// Nested jobs also work in tests; in production all decoders inherit the
/// worker's outer job, so assignment and timeout always retain tree ownership.
fn capture(
    command: &mut Command,
    cap: usize,
    budget: &ExtractBudget,
) -> Result<Vec<u8>, ExtractError> {
    capture_outputs(command, cap, 32768, budget).map(|(output, _)| output)
}

fn capture_outputs(
    command: &mut Command,
    cap: usize,
    error_cap: usize,
    budget: &ExtractBudget,
) -> Result<(Vec<u8>, Vec<u8>), ExtractError> {
    budget.check_deadline()?;
    let mut owned = ChildJob::spawn(command)?;
    let stdout = owned
        .child_mut()
        .stdout
        .take()
        .ok_or_else(|| decode("decoder stdout missing"))?;
    let stderr = owned
        .child_mut()
        .stderr
        .take()
        .ok_or_else(|| decode("decoder stderr missing"))?;
    let overflow = Arc::new(AtomicBool::new(false));
    let out_flag = Arc::clone(&overflow);
    let err_flag = Arc::clone(&overflow);
    let reader = thread::spawn(move || read_capped(stdout, cap, out_flag));
    let errors = thread::spawn(move || read_capped(stderr, error_cap, err_flag));
    let status = (|| loop {
        budget.check_deadline()?;
        if overflow.load(Ordering::Acquire) {
            return Err(decode("decoder output exceeds bounded capture"));
        }
        if let Some(status) = owned.child_mut().try_wait()? {
            return Ok(status);
        }
        thread::sleep(Duration::from_millis(5));
    })();
    // Do not join pipes until every owned writer has exited, including descendants.
    owned.terminate_and_reap()?;
    let output = reader
        .join()
        .map_err(|_| decode("decoder stdout reader failed"))?;
    let error = errors
        .join()
        .map_err(|_| decode("decoder stderr reader failed"))?;
    let status = status?;
    if overflow.load(Ordering::Acquire) {
        return Err(decode("decoder output exceeds bounded capture"));
    }
    let output = output?;
    let error = error?;
    if !status.success() {
        return Err(decode(format!(
            "decoder failed: {}",
            String::from_utf8_lossy(&error[..error.len().min(8192)])
        )));
    }
    budget.check_deadline()?;
    Ok((output, error))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_pts_are_exact_bounded_and_never_rounded_past_frame() {
        let log=b"[Parsed_showinfo_1 @ 0] config in time_base: 1/30000, frame_rate: 30/1\n[Parsed_showinfo_1 @ 0] n: 0 pts: 1001 pts_time:0.0333667\n[Parsed_showinfo_1 @ 0] n: 1 pts: 47000 pts_time:1.56667\n";
        assert_eq!(frame_times(log, 2).unwrap(), vec![33, 1566]);
        assert!(frame_times(log, 1).is_err());
        for bad in [
            "[Parsed_showinfo_1 @ 0] n: 0 pts: 1",
            "[Parsed_showinfo_1 @ 0] config in time_base: 0/0, frame_rate: 1/1",
            "[Parsed_showinfo_1 @ 0] config in time_base: 1/1, frame_rate: 1/1\n[Parsed_showinfo_1 @ 0] n: 0 pts: -1",
            "[Parsed_showinfo_1 @ 0] config in time_base: 1/1, frame_rate: 1/1\n[Parsed_showinfo_1 @ 0] n: 1 pts: 1",
        ] { assert!(frame_times(bad.as_bytes(),2).is_err(),"{bad}"); }
    }

    #[test]
    fn decoder_hang_crash_and_capture_bombs_are_bounded() {
        use std::time::Instant;
        for (script, cap, timeout, message) in [
            (
                "[Threading.Thread]::Sleep(60000)",
                64,
                Duration::from_millis(250),
                "deadline",
            ),
            ("exit 86", 64, Duration::from_secs(10), "decoder failed"),
            (
                "[Console]::Out.Write(('x' * 65536))",
                64,
                Duration::from_secs(10),
                "bound",
            ),
            (
                "[Console]::Error.Write(('x' * 65536))",
                64,
                Duration::from_secs(10),
                "bound",
            ),
        ] {
            let mut command = Command::new("powershell");
            command
                .args(["-NoProfile", "-NonInteractive", "-Command", script])
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            let start = Instant::now();
            let error =
                capture(&mut command, cap, &ExtractBudget::with_timeout(timeout)).unwrap_err();
            assert!(error.to_string().contains(message), "{script}: {error}");
            assert!(start.elapsed() < timeout + Duration::from_secs(2));
        }
    }

    #[test]
    fn probe_rejects_unbounded_or_unusable_metadata() {
        let audio = |duration: &str| {
            format!(
                r#"{{"streams":[{{"codec_type":"audio","codec_name":"aac"}}],"format":{{"duration":"{duration}"}}}}"#
            )
        };
        assert_eq!(
            parse_probe(audio("65.001").as_bytes()).unwrap().duration_ms,
            65001
        );
        assert_eq!(
            parse_probe(audio("65.000001").as_bytes())
                .unwrap()
                .duration_ms,
            65001
        );
        for duration in ["NaN", "inf", "-1", "0", "999999999999999"] {
            assert!(
                parse_probe(audio(duration).as_bytes()).is_err(),
                "{duration}"
            );
        }
        for value in [b"{}".as_slice(), b"not json", br#"{"streams":[],"format":{"duration":"1"}}"#,
            br#"{"streams":[{"codec_type":"video","codec_name":"h264","width":999999,"height":1}],"format":{"duration":"1"}}"#,
            br#"{"streams":[{"codec_type":"audio","codec_name":"unknown"}],"format":{"duration":"1"}}"#] {
            assert!(parse_probe(value).is_err());
        }
    }
}
