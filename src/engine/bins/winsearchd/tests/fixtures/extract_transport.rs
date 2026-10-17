//! Task-owned hostile transport fixture. Never installed with the application.
use std::io::{Read, Write};
use std::os::windows::process::CommandExt;
use std::process::{Command, Stdio};
use std::time::Duration;

fn frame(payload: &str) {
    let mut out = std::io::stdout().lock();
    out.write_all(&(payload.len() as u32).to_le_bytes())
        .unwrap();
    out.write_all(payload.as_bytes()).unwrap();
    out.flush().unwrap();
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode = args[1].as_str();
    if mode == "descendant" {
        loop {
            std::thread::sleep(Duration::from_secs(60));
        }
    }
    std::fs::write(&args[2], std::process::id().to_string()).unwrap();
    frame(r#"{"supported_kinds":["doc","image","audio"]}"#);
    if mode == "never-read" {
        loop {
            std::thread::sleep(Duration::from_secs(60));
        }
    }
    let mut length = [0; 4];
    std::io::stdin().read_exact(&mut length).unwrap();
    let mut input = vec![0; u32::from_le_bytes(length) as usize];
    std::io::stdin().read_exact(&mut input).unwrap();
    match mode {
        "audio-cache" | "audio-outside" => {
            let artifact = std::path::Path::new(&args[3]);
            let mut wav = Vec::new();
            wav.extend_from_slice(b"RIFF"); wav.extend_from_slice(&32036u32.to_le_bytes());
            wav.extend_from_slice(b"WAVEfmt "); wav.extend_from_slice(&16u32.to_le_bytes());
            wav.extend_from_slice(&1u16.to_le_bytes()); wav.extend_from_slice(&1u16.to_le_bytes());
            wav.extend_from_slice(&16000u32.to_le_bytes()); wav.extend_from_slice(&32000u32.to_le_bytes());
            wav.extend_from_slice(&2u16.to_le_bytes()); wav.extend_from_slice(&16u16.to_le_bytes());
            wav.extend_from_slice(b"data"); wav.extend_from_slice(&32000u32.to_le_bytes());
            wav.resize(32044, 12);
            std::fs::write(artifact, wav).unwrap();
            let path = args[3].replace('\\', "\\\\").replace('"', "\\\"");
            frame(&format!(r#"{{"output":{{"status":"Extracted","segments":[{{"kind":"audio","text":"","page":null,"line":null,"start_ms":0,"end_ms":1000,"thumbnail_hint":null,"embed":true,"embedding_input":{{"Audio":{{"path":"{path}"}}}}}}]}},"next_cursor":null,"total_units":1,"media":{{"duration_ms":1000,"visual_frames":0,"has_audio":true}}}}"#));
        }
        "oversized" => {
            std::io::stdout()
                .write_all(&u32::MAX.to_le_bytes())
                .unwrap();
        }
        "mid-frame" => {
            std::io::stdout().write_all(&40u32.to_le_bytes()).unwrap();
            std::io::stdout().write_all(b"{}").unwrap();
        }
        "outside" | "replace-root" => {
            if mode == "replace-root" {
                let scratch = std::path::Path::new(&args[4]);
                std::fs::remove_dir(scratch).unwrap();
                let external = std::path::Path::new(&args[3]).parent().unwrap();
                let result = Command::new("cmd")
                    .creation_flags(0x08000000)
                    .args(["/C", "mklink", "/J"])
                    .arg(scratch)
                    .arg(external)
                    .output()
                    .unwrap();
                assert!(
                    result.status.success(),
                    "{}",
                    String::from_utf8_lossy(&result.stderr)
                );
            }
            let path = args[3].replace('\\', "\\\\").replace('"', "\\\"");
            frame(&format!(
                r#"{{"output":{{"status":"Extracted","segments":[{{"kind":"doc","text":"x","page":null,"line":1,"start_ms":null,"end_ms":null,"thumbnail_hint":"{path}","embed":true}}]}},"next_cursor":null,"total_units":1}}"#
            ));
        }
        "tree" => {
            let mut child = Command::new(std::env::current_exe().unwrap())
                .arg("descendant")
                .stdin(Stdio::null())
                .spawn()
                .unwrap();
            std::fs::write(&args[3], child.id().to_string()).unwrap();
            let _ = child.wait();
        }
        _ => panic!("unknown fixture mode"),
    }
}
