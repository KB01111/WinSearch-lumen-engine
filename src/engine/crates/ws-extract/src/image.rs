//! Image extractor: WIC decoding, privacy-safe EXIF parsing, thumbnail cache
//! (`cache/thumbs` keyed by `(volume_serial, file_id, mtime)`), WinRT OCR,
//! and screenshot classification (`crates/ws-extract/src/image.rs`, Task 11 / P4.3).
//!
//! Privacy guarantee: EXIF GPS geotags (`GPSInfo` IFD pointer `0x8825`, GPS
//! latitude/longitude/altitude tags `0x0000..=0x001F`, XMP `exif:GPS*`, or any
//! WIC `/gps/` query path) are NEVER read or indexed. Only image dimensions,
//! date taken, and camera make/model are indexed.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use ws_index::model::{SEGMENT_KIND_IMAGE, SEGMENT_KIND_OCR};
use ws_platform::identity::file_identity;
use ws_platform::paths::thumbnail_cache_dir;
use ws_platform::wic::{decode_image_wic, write_bgra_thumbnail_png};

use crate::budget::ExtractBudget;
use crate::ocr::{build_ocr_segments, OcrProvider, WinRtOcrProvider};
use crate::{ExtractError, ExtractOutput, Extractor, SegmentDraft};

/// Maximum pixel edge when decoding an image for OCR / thumbnailing (downscales
/// huge dimensions inside WIC before copying pixels).
pub const DEFAULT_MAX_IMAGE_EDGE: u32 = 2048;

/// Maximum pixel edge for cached PNG thumbnails in `cache/thumbs`.
pub const DEFAULT_THUMBNAIL_MAX_EDGE: u32 = 320;

/// Extracted privacy-safe EXIF / image metadata.
///
/// Geotags (GPS latitude, longitude, altitude, track, destination) are
/// intentionally omitted and stripped so location data never enters the index.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExifMetadata {
    pub width: u32,
    pub height: u32,
    pub camera_make: Option<String>,
    pub camera_model: Option<String>,
    pub date_taken: Option<String>,
}

impl ExifMetadata {
    /// Formats the metadata into a concise searchable string (e.g.
    /// `"Image 1920x1080 Camera Canon EOS R5 Date 2026-10-15 14:30:00"`).
    pub fn to_searchable_text(&self, is_screenshot: bool) -> String {
        let mut parts = Vec::new();
        if is_screenshot {
            parts.push("Screenshot".to_string());
        } else {
            parts.push("Image".to_string());
        }
        if self.width > 0 && self.height > 0 {
            parts.push(format!("{}x{}", self.width, self.height));
        }
        if let Some(make) = &self.camera_make {
            let make = sanitize_exif_field(make);
            if !make.is_empty() {
                parts.push(format!("Camera {make}"));
            }
        }
        if let Some(model) = &self.camera_model {
            let model = sanitize_exif_field(model);
            if !model.is_empty() {
                let duplicate_prefix = self
                    .camera_make
                    .as_ref()
                    .is_some_and(|m| model.to_lowercase().starts_with(&m.to_lowercase()));
                if duplicate_prefix {
                    parts.push(model);
                } else {
                    parts.push(format!("Model {model}"));
                }
            }
        }
        if let Some(date) = &self.date_taken {
            let date = sanitize_exif_field(date);
            if !date.is_empty() {
                // Convert EXIF "YYYY:MM:DD HH:MM:SS" date colons to dashes for easier FTS queries.
                let normalized = normalize_exif_date(&date);
                parts.push(format!("Date {normalized}"));
            }
        }
        parts.join(" ")
    }
}

/// Returns `true` if `path` belongs to a known/configured screenshot folder
/// (`Screenshots`, `ScreenShots`, `Captures`) or has a screenshot filename
/// (`Screenshot*`, `Screen Shot*`, ` Bildschirmfoto*`, `Capture*`).
pub fn is_screenshot_path(path: &Path) -> bool {
    for ancestor in path.ancestors().skip(1) {
        if let Some(folder) = ancestor.file_name().and_then(|n| n.to_str()) {
            let lower = folder.to_ascii_lowercase();
            if matches!(
                lower.as_str(),
                "screenshots" | "screen shots" | "screencaptures" | "captures"
            ) {
                return true;
            }
        }
    }
    if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
        let lower = stem.to_ascii_lowercase();
        if lower.starts_with("screenshot")
            || lower.starts_with("screen shot")
            || lower.starts_with("screen_shot")
            || lower.starts_with("snip_")
        {
            return true;
        }
    }
    false
}

/// Computes the deterministic thumbnail cache file path under `cache_dir`
/// keyed by `(volume_serial, file_id, mtime)`.
pub fn thumbnail_cache_path(
    cache_dir: &Path,
    volume_serial: u32,
    file_id: u64,
    mtime_secs: i64,
) -> PathBuf {
    cache_dir.join(format!(
        "thumb_{volume_serial:08x}_{file_id:016x}_{mtime_secs}.png"
    ))
}

/// Resolves `(volume_serial, file_id, mtime_secs)` for `path` on disk.
pub fn stat_thumbnail_key(path: &Path) -> std::io::Result<(u32, u64, i64)> {
    let meta = std::fs::metadata(path)?;
    let mtime_secs = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let (vol, fid) = file_identity(path).unwrap_or((0, 0));
    Ok((vol, fid, mtime_secs))
}

/// Image extractor supporting WIC decode, privacy-filtered EXIF, thumbnail
/// caching, and WinRT (or injected) OCR.
pub struct ImageExtractor {
    thumb_dir: PathBuf,
    ocr: Arc<dyn OcrProvider>,
    max_decode_edge: u32,
    thumb_max_edge: u32,
}

impl Default for ImageExtractor {
    fn default() -> Self {
        Self {
            thumb_dir: thumbnail_cache_dir(),
            ocr: Arc::new(WinRtOcrProvider::default()),
            max_decode_edge: DEFAULT_MAX_IMAGE_EDGE,
            thumb_max_edge: DEFAULT_THUMBNAIL_MAX_EDGE,
        }
    }
}

impl ImageExtractor {
    pub fn new(thumb_dir: PathBuf, ocr: Arc<dyn OcrProvider>) -> Self {
        Self {
            thumb_dir,
            ocr,
            max_decode_edge: DEFAULT_MAX_IMAGE_EDGE,
            thumb_max_edge: DEFAULT_THUMBNAIL_MAX_EDGE,
        }
    }

    pub fn with_thumb_dir(mut self, thumb_dir: PathBuf) -> Self {
        self.thumb_dir = thumb_dir;
        self
    }

    pub fn with_ocr_provider(mut self, ocr: Arc<dyn OcrProvider>) -> Self {
        self.ocr = ocr;
        self
    }
}

impl Extractor for ImageExtractor {
    fn extract(&self, path: &Path, budget: &ExtractBudget) -> Result<ExtractOutput, ExtractError> {
        budget.check_deadline()?;
        let meta = std::fs::metadata(path)?;
        budget.check_size(meta.len())?;

        // 1. Decode image and query WIC metadata (degrades to Unsupported when
        // OS codec for HEIC/AVIF/RAW is absent or file is not a valid image).
        let wic = match decode_image_wic(path, self.max_decode_edge) {
            Ok(img) => img,
            Err(_) => return Ok(ExtractOutput::unsupported()),
        };
        budget.check_deadline()?;

        // 2. Also parse raw EXIF APP1 / TIFF IFD bytes as a fallback/supplement
        // (strictly skipping GPS IFD 0x8825 and any GPS tags).
        let raw_exif = budget
            .read_bounded(path)
            .ok()
            .and_then(|bytes| parse_exif_from_bytes(&bytes))
            .unwrap_or_default();

        let exif = ExifMetadata {
            width: wic.width,
            height: wic.height,
            camera_make: wic.camera_make.or(raw_exif.camera_make),
            camera_model: wic.camera_model.or(raw_exif.camera_model),
            date_taken: wic.date_taken.or(raw_exif.date_taken),
        };

        // 3. Render or reuse cached thumbnail keyed by (volume_serial, file_id, mtime).
        let thumb_path = match stat_thumbnail_key(path) {
            Ok((vol, fid, mtime)) => {
                let candidate = thumbnail_cache_path(&self.thumb_dir, vol, fid, mtime);
                if candidate.is_file() {
                    Some(candidate)
                } else if write_bgra_thumbnail_png(
                    &wic.bgra8,
                    wic.scaled_width,
                    wic.scaled_height,
                    self.thumb_max_edge,
                    &candidate,
                )
                .is_ok()
                {
                    Some(candidate)
                } else {
                    None
                }
            }
            Err(_) => None,
        };
        budget.check_deadline()?;

        // 4. Emit the image metadata segment (`kind = "image"`, carries `thumbnail_hint`).
        let is_screenshot = is_screenshot_path(path);
        let meta_text = exif.to_searchable_text(is_screenshot);
        let mut segments = Vec::new();
        if !meta_text.is_empty() && segments.len() < budget.max_segments {
            segments.push(SegmentDraft {
                embedding_input: crate::DraftEmbeddingInput::Text,
                kind: SEGMENT_KIND_IMAGE.to_string(),
                text: meta_text,
                page: None,
                start_ms: None,
                end_ms: None,
                line: None,
                thumbnail_hint: thumb_path.clone(),
                embed: true,
            });
        }

        // 5. Run OCR on the decoded BGRA8 bitmap and append `ocr` segments
        // (indexed in both FTS for exact error codes and vectors for semantics).
        if segments.len() < budget.max_segments {
            let ocr_res = self.ocr.recognize_bgra8(
                &wic.bgra8,
                wic.scaled_width,
                wic.scaled_height,
                budget,
            )?;
            let (ocr_segs, _bbox_json) =
                build_ocr_segments(&ocr_res, None, thumb_path.clone(), budget)?;
            for seg in ocr_segs {
                if segments.len() >= budget.max_segments {
                    break;
                }
                debug_assert_eq!(seg.kind, SEGMENT_KIND_OCR);
                segments.push(seg);
            }
        }

        Ok(ExtractOutput::extracted(segments))
    }
}

/// Pure-Rust TIFF/EXIF APP1 parser that extracts ONLY:
/// - `Make` (`0x010F` / 271)
/// - `Model` (`0x0110` / 272)
/// - `DateTime` (`0x0132` / 306)
/// - `DateTimeOriginal` (`0x9003` / 36867) in Exif SubIFD (`0x8769`)
/// - `PixelXDimension` (`0xA002`) / `PixelYDimension` (`0xA003`)
///
/// Explicitly ignores GPSInfo IFD pointer (`0x8825`) and never follows it.
pub fn parse_exif_from_bytes(bytes: &[u8]) -> Option<ExifMetadata> {
    let tiff_slice = find_tiff_header(bytes)?;
    parse_tiff_ifd(tiff_slice)
}

fn find_tiff_header(bytes: &[u8]) -> Option<&[u8]> {
    // 1. Raw TIFF header (`II\x2A\x00` or `MM\x00\x2A`)
    if bytes.len() >= 8 && (&bytes[0..4] == b"II*\0" || &bytes[0..4] == b"MM\0*") {
        return Some(bytes);
    }
    // 2. JPEG (`\xFF\xD8`) -> scan markers for APP1 (`\xFF\xE1`) with `"Exif\0\0"`
    if bytes.starts_with(&[0xFF, 0xD8]) {
        let mut i = 2usize;
        while i + 4 <= bytes.len() {
            if bytes[i] != 0xFF {
                break;
            }
            while i < bytes.len() && bytes[i] == 0xFF {
                i += 1;
            }
            if i >= bytes.len() {
                break;
            }
            let marker = bytes[i];
            i += 1;
            if marker == 0xD9 || marker == 0xDA {
                break; // EOI or SOS
            }
            if i + 2 > bytes.len() {
                break;
            }
            let seg_len = u16::from_be_bytes([bytes[i], bytes[i + 1]]) as usize;
            if seg_len < 2 || i + seg_len > bytes.len() {
                break;
            }
            let payload = &bytes[i + 2..i + seg_len];
            if marker == 0xE1 && payload.len() >= 14 && &payload[0..6] == b"Exif\0\0" {
                return Some(&payload[6..]);
            }
            i += seg_len;
        }
    }
    // 3. PNG `eXIf` chunk
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        let mut i = 8usize;
        while i + 8 <= bytes.len() {
            let len =
                u32::from_be_bytes([bytes[i], bytes[i + 1], bytes[i + 2], bytes[i + 3]]) as usize;
            let chunk_type = &bytes[i + 4..i + 8];
            let data_start = i + 8;
            let data_end = data_start.saturating_add(len);
            if data_end > bytes.len() {
                break;
            }
            if chunk_type == b"eXIf" && len >= 8 {
                return Some(&bytes[data_start..data_end]);
            }
            i = data_end.saturating_add(4); // skip CRC
        }
    }
    None
}

fn parse_tiff_ifd(tiff: &[u8]) -> Option<ExifMetadata> {
    if tiff.len() < 8 {
        return None;
    }
    let le = match &tiff[0..4] {
        b"II*\0" => true,
        b"MM\0*" => false,
        _ => return None,
    };
    let read_u16 = |off: usize| -> Option<u16> {
        let b = tiff.get(off..off + 2)?;
        Some(if le {
            u16::from_le_bytes([b[0], b[1]])
        } else {
            u16::from_be_bytes([b[0], b[1]])
        })
    };
    let read_u32 = |off: usize| -> Option<u32> {
        let b = tiff.get(off..off + 4)?;
        Some(if le {
            u32::from_le_bytes([b[0], b[1], b[2], b[3]])
        } else {
            u32::from_be_bytes([b[0], b[1], b[2], b[3]])
        })
    };

    let ifd0_offset = read_u32(4)? as usize;
    let mut meta = ExifMetadata::default();
    let mut exif_sub_ifd: Option<usize> = None;

    let read_ascii = |format: u16, count: usize, val_offset_pos: usize| -> Option<String> {
        if format != 2 || count == 0 || count > 256 {
            return None;
        }
        let slice = if count <= 4 {
            tiff.get(val_offset_pos..val_offset_pos + count)?
        } else {
            let off = read_u32(val_offset_pos)? as usize;
            tiff.get(off..off.checked_add(count)?)?
        };
        let s = String::from_utf8_lossy(slice);
        let cleaned = sanitize_exif_field(&s);
        (!cleaned.is_empty()).then_some(cleaned)
    };

    let read_int = |format: u16, val_offset_pos: usize| -> Option<u32> {
        match format {
            3 => read_u16(val_offset_pos).map(u32::from), // SHORT
            4 => read_u32(val_offset_pos),                // LONG
            _ => None,
        }
    };

    if let Some(entries) = read_u16(ifd0_offset) {
        let count = (entries as usize).min(256);
        for idx in 0..count {
            let entry_off = ifd0_offset + 2 + idx * 12;
            let Some(tag) = read_u16(entry_off) else {
                break;
            };
            let Some(format) = read_u16(entry_off + 2) else {
                break;
            };
            let Some(num_components) = read_u32(entry_off + 4) else {
                break;
            };
            let val_pos = entry_off + 8;

            match tag {
                0x0100 if meta.width == 0 => {
                    if let Some(w) = read_int(format, val_pos) {
                        meta.width = w;
                    }
                }
                0x0101 if meta.height == 0 => {
                    if let Some(h) = read_int(format, val_pos) {
                        meta.height = h;
                    }
                }
                0x010F => {
                    meta.camera_make = read_ascii(format, num_components as usize, val_pos);
                }
                0x0110 => {
                    meta.camera_model = read_ascii(format, num_components as usize, val_pos);
                }
                0x0132 if meta.date_taken.is_none() => {
                    meta.date_taken = read_ascii(format, num_components as usize, val_pos);
                }
                0x8769 => {
                    // Exif SubIFD offset
                    exif_sub_ifd = read_u32(val_pos).map(|v| v as usize);
                }
                0x8825 => {
                    // PRIVACY: GPSInfo IFD pointer — intentionally ignored!
                }
                _ => {}
            }
        }
    }

    if let Some(sub_off) = exif_sub_ifd {
        if let Some(entries) = read_u16(sub_off) {
            let count = (entries as usize).min(256);
            for idx in 0..count {
                let entry_off = sub_off + 2 + idx * 12;
                let Some(tag) = read_u16(entry_off) else {
                    break;
                };
                let Some(format) = read_u16(entry_off + 2) else {
                    break;
                };
                let Some(num_components) = read_u32(entry_off + 4) else {
                    break;
                };
                let val_pos = entry_off + 8;

                match tag {
                    0x9003 | 0x9004 => {
                        if let Some(dt) = read_ascii(format, num_components as usize, val_pos) {
                            meta.date_taken = Some(dt);
                        }
                    }
                    0xA002 if meta.width == 0 => {
                        if let Some(w) = read_int(format, val_pos) {
                            meta.width = w;
                        }
                    }
                    0xA003 if meta.height == 0 => {
                        if let Some(h) = read_int(format, val_pos) {
                            meta.height = h;
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    Some(meta)
}

fn sanitize_exif_field(raw: &str) -> String {
    let cleaned: String = raw
        .chars()
        .filter(|&c| c != '\0' && !c.is_control())
        .collect();
    let trimmed = cleaned.trim();
    // Privacy defense-in-depth: if any custom camera/date string somehow embeds
    // GPS coordinate keywords, drop it.
    let lower = trimmed.to_ascii_lowercase();
    if lower.contains("gps") || lower.contains("latitude") || lower.contains("longitude") {
        return String::new();
    }
    trimmed.to_string()
}

fn normalize_exif_date(date: &str) -> String {
    // Standard EXIF date is "YYYY:MM:DD HH:MM:SS" -> convert first two ':' to '-'
    let bytes = date.as_bytes();
    if bytes.len() >= 10 && bytes[4] == b':' && bytes[7] == b':' {
        let mut out = date.to_string();
        out.replace_range(4..5, "-");
        out.replace_range(7..8, "-");
        out
    } else {
        date.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ocr::{OcrResultData, OcrWordBox};
    use crate::ExtractionStatus;
    use std::time::Duration;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let dir = std::env::temp_dir().join(format!(
                "ws-extract-image-test-{}-{:?}-{}",
                std::process::id(),
                std::thread::current().id(),
                NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    struct StaticOcrProvider(OcrResultData);

    impl OcrProvider for StaticOcrProvider {
        fn recognize_bgra8(
            &self,
            _bgra8: &[u8],
            _width: u32,
            _height: u32,
            budget: &ExtractBudget,
        ) -> Result<OcrResultData, ExtractError> {
            budget.check_deadline()?;
            Ok(self.0.clone())
        }
    }

    /// Builds a valid PNG file at `path` via WIC (`write_bgra_thumbnail_png`)
    /// and optionally injects an `eXIf` chunk containing Camera Make, Camera Model,
    /// DateTimeOriginal, AND a GPS IFD with secret coordinates.
    fn write_fixture_png_with_exif(path: &Path, width: u32, height: u32, include_exif: bool) {
        let bgra = vec![200u8; (width as usize) * (height as usize) * 4];
        write_bgra_thumbnail_png(&bgra, width, height, width.max(height), path).unwrap();
        if !include_exif {
            return;
        }
        let png = std::fs::read(path).unwrap();
        let exif_tiff = build_tiff_with_camera_and_gps();
        let injected = inject_png_exif_chunk(&png, &exif_tiff);
        std::fs::write(path, injected).unwrap();
    }

    /// Constructs a little-endian TIFF payload with:
    /// - Tag 0x010F (Make) = "Canon"
    /// - Tag 0x0110 (Model) = "Canon EOS R5"
    /// - Tag 0x0132 (DateTime) = "2026:10:15 09:45:00"
    /// - Tag 0x8825 (GPSInfo IFD) pointing to GPS coordinates "37.7749N 122.4194W SECRET_GPS_LOC"
    fn build_tiff_with_camera_and_gps() -> Vec<u8> {
        let mut buf = Vec::new();
        // Little-endian TIFF header, IFD0 at offset 8
        buf.extend_from_slice(b"II*\0");
        buf.extend_from_slice(&8u32.to_le_bytes());

        // IFD0 has 4 entries: 2 + 4*12 + 4 = 54 bytes (ends at 8 + 54 = 62)
        buf.extend_from_slice(&4u16.to_le_bytes());

        let make_bytes = b"Canon\0";
        let model_bytes = b"Canon EOS R5\0";
        let date_bytes = b"2026:10:15 09:45:00\0";

        let make_off = 64u32;
        let model_off = make_off + make_bytes.len() as u32;
        let date_off = model_off + model_bytes.len() as u32;
        let gps_ifd_off = date_off + date_bytes.len() as u32;

        // Entry 1: Make (0x010F), ASCII (2)
        buf.extend_from_slice(&0x010Fu16.to_le_bytes());
        buf.extend_from_slice(&2u16.to_le_bytes());
        buf.extend_from_slice(&(make_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&make_off.to_le_bytes());

        // Entry 2: Model (0x0110), ASCII (2)
        buf.extend_from_slice(&0x0110u16.to_le_bytes());
        buf.extend_from_slice(&2u16.to_le_bytes());
        buf.extend_from_slice(&(model_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&model_off.to_le_bytes());

        // Entry 3: DateTime (0x0132), ASCII (2)
        buf.extend_from_slice(&0x0132u16.to_le_bytes());
        buf.extend_from_slice(&2u16.to_le_bytes());
        buf.extend_from_slice(&(date_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&date_off.to_le_bytes());

        // Entry 4: GPSInfo IFD pointer (0x8825), LONG (4), count 1
        buf.extend_from_slice(&0x8825u16.to_le_bytes());
        buf.extend_from_slice(&4u16.to_le_bytes());
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.extend_from_slice(&gps_ifd_off.to_le_bytes());

        // Next IFD = 0
        buf.extend_from_slice(&0u32.to_le_bytes());
        while buf.len() < make_off as usize {
            buf.push(0);
        }
        buf.extend_from_slice(make_bytes);
        buf.extend_from_slice(model_bytes);
        buf.extend_from_slice(date_bytes);

        // GPS IFD at `gps_ifd_off` with 1 entry containing secret location string
        let gps_str = b"37.7749N 122.4194W SECRET_GPS_LOC\0";
        let gps_str_off = gps_ifd_off + 2 + 12 + 4;
        buf.extend_from_slice(&1u16.to_le_bytes());
        buf.extend_from_slice(&0x0002u16.to_le_bytes()); // GPSLatitude tag
        buf.extend_from_slice(&2u16.to_le_bytes());
        buf.extend_from_slice(&(gps_str.len() as u32).to_le_bytes());
        buf.extend_from_slice(&gps_str_off.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.extend_from_slice(gps_str);
        buf
    }

    fn inject_png_exif_chunk(png: &[u8], exif_tiff: &[u8]) -> Vec<u8> {
        // Insert `eXIf` chunk right before `IEND` (last 12 bytes of a standard PNG).
        let iend_pos = png.len() - 12;
        let mut out = Vec::with_capacity(png.len() + exif_tiff.len() + 12);
        out.extend_from_slice(&png[..iend_pos]);
        out.extend_from_slice(&(exif_tiff.len() as u32).to_be_bytes());
        out.extend_from_slice(b"eXIf");
        out.extend_from_slice(exif_tiff);
        out.extend_from_slice(&0u32.to_be_bytes()); // dummy CRC (ignored by our eXIf scanner)
        out.extend_from_slice(&png[iend_pos..]);
        out
    }

    #[test]
    fn exif_extracts_camera_date_dimensions_and_never_indexes_gps_geotags() {
        let tmp = TempDir::new();
        let thumb_dir = tmp.0.join("thumbs");
        let img_path = tmp.0.join("vacation.png");
        write_fixture_png_with_exif(&img_path, 640, 480, true);

        let extractor = ImageExtractor::new(
            thumb_dir.clone(),
            Arc::new(StaticOcrProvider(OcrResultData::default())),
        );
        let out = extractor
            .extract(&img_path, &ExtractBudget::default())
            .unwrap();
        assert_eq!(out.status, ExtractionStatus::Extracted);
        assert_eq!(out.segments.len(), 1);
        let text = &out.segments[0].text;
        assert!(text.contains("640x480"), "expected dimensions in {text:?}");
        assert!(
            text.contains("Canon EOS R5"),
            "expected camera model in {text:?}"
        );
        assert!(text.contains("2026-10-15"), "expected date in {text:?}");
        // Privacy verification: GPS coordinates must never be indexed.
        assert!(
            !text.contains("37.7749")
                && !text.contains("122.4194")
                && !text.contains("SECRET_GPS_LOC"),
            "GPS geotag leaked into indexed text: {text:?}"
        );
    }

    #[test]
    fn screenshot_with_error_dialog_emits_ocr_segment_and_detects_screenshot_path() {
        let tmp = TempDir::new();
        let screenshots_dir = tmp.0.join("Screenshots");
        std::fs::create_dir_all(&screenshots_dir).unwrap();
        let shot_path = screenshots_dir.join("Screenshot 2026-10-15.png");
        write_fixture_png_with_exif(&shot_path, 800, 600, false);

        assert!(is_screenshot_path(&shot_path));

        let ocr_data = OcrResultData {
            text: "Fatal Error 0x80070005: CUDA out of memory in module renderer.dll".into(),
            words: vec![
                OcrWordBox {
                    text: "0x80070005:".into(),
                    x: 40,
                    y: 120,
                    w: 110,
                    h: 22,
                },
                OcrWordBox {
                    text: "CUDA".into(),
                    x: 160,
                    y: 120,
                    w: 60,
                    h: 22,
                },
            ],
            language_tag: Some("en-US".into()),
        };

        let extractor =
            ImageExtractor::new(tmp.0.join("thumbs"), Arc::new(StaticOcrProvider(ocr_data)));
        let out = extractor
            .extract(&shot_path, &ExtractBudget::default())
            .unwrap();
        assert_eq!(out.status, ExtractionStatus::Extracted);
        assert_eq!(out.segments.len(), 2);
        assert_eq!(out.segments[0].kind, SEGMENT_KIND_IMAGE);
        assert!(out.segments[0].text.contains("Screenshot 800x600"));
        assert_eq!(out.segments[1].kind, SEGMENT_KIND_OCR);
        assert!(out.segments[1].text.contains("0x80070005"));
        assert!(out.segments[1].text.contains("CUDA out of memory"));
    }

    #[test]
    fn thumbnail_cache_hits_on_unchanged_mtime_and_invalidates_on_change() {
        let tmp = TempDir::new();
        let thumb_dir = tmp.0.join("thumbs");
        let img_path = tmp.0.join("photo.png");
        write_fixture_png_with_exif(&img_path, 512, 384, false);

        let extractor = ImageExtractor::new(
            thumb_dir.clone(),
            Arc::new(StaticOcrProvider(OcrResultData::default())),
        );
        let budget = ExtractBudget::new(10 * 1024 * 1024, Duration::from_secs(5), 16);

        let out1 = extractor.extract(&img_path, &budget).unwrap();
        let thumb1 = out1.segments[0]
            .thumbnail_hint
            .clone()
            .expect("thumbnail should be rendered");
        assert!(thumb1.is_file());

        // Overwrite the cached thumbnail with a sentinel byte sequence; a second
        // extract with unchanged source mtime must hit the cache without rewriting it.
        std::fs::write(&thumb1, b"CACHED_THUMB_SENTINEL").unwrap();
        let out2 = extractor.extract(&img_path, &budget).unwrap();
        let thumb2 = out2.segments[0].thumbnail_hint.clone().unwrap();
        assert_eq!(thumb1, thumb2);
        assert_eq!(
            std::fs::read(&thumb2).unwrap(),
            b"CACHED_THUMB_SENTINEL",
            "unchanged mtime must reuse cached thumbnail"
        );
    }
}
