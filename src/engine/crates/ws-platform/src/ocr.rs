//! WinRT `Windows.Media.Ocr` helper for extracting text and word bounding boxes
//! from BGRA8 bitmaps.
//!
//! Enumerates installed recognizer languages (`OcrEngine::AvailableRecognizerLanguages`)
//! and prefers user-profile languages (`OcrEngine::TryCreateFromUserProfileLanguages`),
//! falling back to the first installed recognizer language (capped by
//! `max_languages`). When no OCR language packs are installed on the machine,
//! returns an empty [`OcrExtraction`] without failing the caller.

use windows::Graphics::Imaging::{BitmapAlphaMode, BitmapPixelFormat, SoftwareBitmap};
use windows::Media::Ocr::OcrEngine;
use windows::Storage::Streams::DataWriter;
use windows::Win32::Foundation::{S_FALSE, S_OK};
use windows::Win32::System::Com::{CoInitializeEx, CoUninitialize, COINIT_MULTITHREADED};

#[link(name = "runtimeobject")]
extern "system" {
    fn RoInitialize(init_type: i32) -> windows::core::HRESULT;
    fn RoUninitialize();
}

const RO_INIT_MULTITHREADED: i32 = 1;

struct ComGuard {
    should_uninit_com: bool,
    should_uninit_ro: bool,
}

impl ComGuard {
    fn init() -> Self {
        let ro_hr = unsafe { RoInitialize(RO_INIT_MULTITHREADED) };
        let should_uninit_ro = ro_hr == S_OK || ro_hr == S_FALSE;
        let hr = unsafe { CoInitializeEx(None, COINIT_MULTITHREADED) };
        let should_uninit_com = hr == S_OK || hr == S_FALSE;
        Self {
            should_uninit_com,
            should_uninit_ro,
        }
    }
}

impl Drop for ComGuard {
    fn drop(&mut self) {
        if self.should_uninit_com {
            unsafe {
                CoUninitialize();
            }
        }
        if self.should_uninit_ro {
            unsafe {
                RoUninitialize();
            }
        }
    }
}

/// One recognized word with its pixel bounding box in the source bitmap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OcrWordBox {
    pub text: String,
    pub x: u32,
    pub y: u32,
    pub w: u32,
    pub h: u32,
}

/// Result of running OCR over an image frame.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OcrExtraction {
    pub text: String,
    pub words: Vec<OcrWordBox>,
    pub language_tag: Option<String>,
}

/// Lists BCP-47 language tags for all installed WinRT OCR recognizers on this system.
pub fn available_ocr_languages() -> Vec<String> {
    let _com = ComGuard::init();
    let Ok(list) = OcrEngine::AvailableRecognizerLanguages() else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let count = list.Size().unwrap_or(0);
    for i in 0..count {
        if let Ok(lang) = list.GetAt(i) {
            if let Ok(tag) = lang.LanguageTag() {
                out.push(tag.to_string_lossy());
            }
        }
    }
    out
}

/// Runs WinRT `Windows.Media.Ocr` on a 32-bpp BGRA8 buffer (`width * height * 4` bytes).
///
/// Tries `TryCreateFromUserProfileLanguages()` first; if that fails or yields no
/// text and multiple languages are installed, tries up to `max_languages`
/// installed languages and returns the result with the most recognized words.
pub fn recognize_bgra8_winrt(
    bgra8: &[u8],
    width: u32,
    height: u32,
    max_languages: usize,
) -> Result<OcrExtraction, String> {
    if width == 0 || height == 0 || bgra8.len() < (width as usize) * (height as usize) * 4 {
        return Ok(OcrExtraction::default());
    }

    let _com = ComGuard::init();
    let max_dim = OcrEngine::MaxImageDimension().unwrap_or(4096);
    if width > max_dim || height > max_dim {
        return Ok(OcrExtraction::default());
    }

    let writer = DataWriter::new().map_err(|e| format!("DataWriter::new: {e}"))?;
    writer
        .WriteBytes(bgra8)
        .map_err(|e| format!("DataWriter::WriteBytes: {e}"))?;
    let ibuf = writer
        .DetachBuffer()
        .map_err(|e| format!("DataWriter::DetachBuffer: {e}"))?;

    let software_bmp = SoftwareBitmap::CreateCopyWithAlphaFromBuffer(
        &ibuf,
        BitmapPixelFormat::Bgra8,
        width as i32,
        height as i32,
        BitmapAlphaMode::Premultiplied,
    )
    .or_else(|_| {
        SoftwareBitmap::CreateCopyFromBuffer(
            &ibuf,
            BitmapPixelFormat::Bgra8,
            width as i32,
            height as i32,
        )
    })
    .map_err(|e| format!("SoftwareBitmap::CreateCopyFromBuffer: {e}"))?;

    let mut best = OcrExtraction::default();

    // 1. Try user profile languages first.
    if let Ok(engine) = OcrEngine::TryCreateFromUserProfileLanguages() {
        if let Ok(ext) = run_engine(&engine, &software_bmp) {
            if !ext.words.is_empty() {
                return Ok(ext);
            }
            best = ext;
        }
    }

    // 2. Fallback: iterate up to `max_languages` installed recognizer languages
    // and pick the one that recognizes the most words.
    if let Ok(langs) = OcrEngine::AvailableRecognizerLanguages() {
        let count = (langs.Size().unwrap_or(0) as usize).min(max_languages.max(1));
        for i in 0..count {
            if let Ok(lang) = langs.GetAt(i as u32) {
                if let Ok(engine) = OcrEngine::TryCreateFromLanguage(&lang) {
                    if let Ok(ext) = run_engine(&engine, &software_bmp) {
                        if ext.words.len() > best.words.len() {
                            best = ext;
                        }
                    }
                }
            }
        }
    }

    Ok(best)
}

fn run_engine(engine: &OcrEngine, bitmap: &SoftwareBitmap) -> Result<OcrExtraction, String> {
    let op = engine
        .RecognizeAsync(bitmap)
        .map_err(|e| format!("RecognizeAsync: {e}"))?;
    let res = op.get().map_err(|e| format!("RecognizeAsync.get: {e}"))?;

    let language_tag = engine
        .RecognizerLanguage()
        .ok()
        .and_then(|l| l.LanguageTag().ok())
        .map(|s| s.to_string_lossy());

    let mut lines_text = Vec::new();
    let mut words = Vec::new();

    if let Ok(lines) = res.Lines() {
        let line_count = lines.Size().unwrap_or(0);
        for l_idx in 0..line_count {
            let Ok(line) = lines.GetAt(l_idx) else {
                continue;
            };
            if let Ok(t) = line.Text() {
                let s = t.to_string_lossy();
                if !s.trim().is_empty() {
                    lines_text.push(s.trim().to_string());
                }
            }
            if let Ok(line_words) = line.Words() {
                let w_count = line_words.Size().unwrap_or(0);
                for w_idx in 0..w_count {
                    if let Ok(word) = line_words.GetAt(w_idx) {
                        let w_text = word.Text().map(|h| h.to_string_lossy()).unwrap_or_default();
                        let rect = word.BoundingRect().unwrap_or_default();
                        if !w_text.trim().is_empty() {
                            words.push(OcrWordBox {
                                text: w_text.trim().to_string(),
                                x: rect.X.max(0.0).round() as u32,
                                y: rect.Y.max(0.0).round() as u32,
                                w: rect.Width.max(0.0).round() as u32,
                                h: rect.Height.max(0.0).round() as u32,
                            });
                        }
                    }
                }
            }
        }
    }

    let text = if !lines_text.is_empty() {
        lines_text.join("\n")
    } else {
        res.Text()
            .map(|h| h.to_string_lossy().trim().to_string())
            .unwrap_or_default()
    };

    Ok(OcrExtraction {
        text,
        words,
        language_tag,
    })
}
