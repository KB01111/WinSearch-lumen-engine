//! Windows Imaging Component (WIC) helpers for image decoding, EXIF metadata
//! extraction (excluding GPS geotags for privacy), and thumbnail rendering.
//!
//! Uses `CLSID_WICImagingFactory` (`windowscodecs.dll`) so JPEG, PNG, WebP,
//! BMP, GIF, and TIFF decode with built-in OS codecs, and HEIC/AVIF/RAW decode
//! automatically when the user has the Windows HEIF/AV1/Raw Image Extensions
//! installed — degrading cleanly (`Err`) to name-only indexing when absent.

use std::os::windows::ffi::OsStrExt;
use std::path::Path;

use windows::core::PCWSTR;
use windows::Win32::Foundation::{GENERIC_READ, S_FALSE, S_OK};
use windows::Win32::Graphics::Imaging::{
    CLSID_WICImagingFactory, GUID_ContainerFormatPng, GUID_WICPixelFormat24bppBGR,
    GUID_WICPixelFormat32bppBGRA, IWICBitmapDecoder, IWICBitmapFrameDecode, IWICBitmapSource,
    IWICImagingFactory, IWICMetadataQueryReader, WICBitmapDitherTypeNone, WICBitmapEncoderNoCache,
    WICBitmapInterpolationModeFant, WICBitmapPaletteTypeCustom, WICDecodeMetadataCacheOnDemand,
};
use windows::Win32::System::Com::StructuredStorage::PROPVARIANT;
use windows::Win32::System::Com::{
    CoCreateInstance, CoInitializeEx, CoUninitialize, CLSCTX_INPROC_SERVER, COINIT_MULTITHREADED,
    STREAM_SEEK_SET,
};

struct ComGuard {
    should_uninit: bool,
}

impl ComGuard {
    fn init() -> Self {
        let hr = unsafe { CoInitializeEx(None, COINIT_MULTITHREADED) };
        let should_uninit = hr == S_OK || hr == S_FALSE;
        Self { should_uninit }
    }
}

impl Drop for ComGuard {
    fn drop(&mut self) {
        if self.should_uninit {
            unsafe {
                CoUninitialize();
            }
        }
    }
}

/// Decoded image frame + privacy-filtered EXIF metadata from WIC.
#[derive(Debug, Clone)]
pub struct WicDecodedImage {
    pub width: u32,
    pub height: u32,
    /// Downscaled or original BGRA8 pixel buffer (`scaled_width * scaled_height * 4` bytes).
    pub bgra8: Vec<u8>,
    pub scaled_width: u32,
    pub scaled_height: u32,
    /// Camera manufacturer (EXIF Tag 271), if present.
    pub camera_make: Option<String>,
    /// Camera model (EXIF Tag 272), if present.
    pub camera_model: Option<String>,
    /// Date/time taken (EXIF DateTimeOriginal 36867 or DateTime 306), if present.
    pub date_taken: Option<String>,
}

fn to_wide(s: &str) -> Vec<u16> {
    s.encode_utf16().chain(std::iter::once(0)).collect()
}

fn path_to_wide(path: &Path) -> Vec<u16> {
    path.as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect()
}

fn create_factory() -> Result<IWICImagingFactory, String> {
    unsafe { CoCreateInstance(&CLSID_WICImagingFactory, None, CLSCTX_INPROC_SERVER) }
        .map_err(|e| format!("CoCreateInstance(WICImagingFactory) failed: {e}"))
}

fn open_decoder(
    factory: &IWICImagingFactory,
    path: &Path,
) -> Result<(IWICBitmapDecoder, IWICBitmapFrameDecode), String> {
    let wide = path_to_wide(path);
    let decoder = unsafe {
        factory.CreateDecoderFromFilename(
            PCWSTR(wide.as_ptr()),
            None,
            GENERIC_READ,
            WICDecodeMetadataCacheOnDemand,
        )
    }
    .map_err(|e| format!("WIC CreateDecoderFromFilename failed: {e}"))?;
    let frame =
        unsafe { decoder.GetFrame(0) }.map_err(|e| format!("WIC GetFrame(0) failed: {e}"))?;
    Ok((decoder, frame))
}

/// Queries a string metadata property from `reader` using WIC metadata query syntax.
///
/// Privacy note: callers must NEVER query `/app1/ifd/gps/*` or `/ifd/gps/*`.
fn query_metadata_string(reader: &IWICMetadataQueryReader, query: &str) -> Option<String> {
    // Defense-in-depth privacy guard: refuse any query path mentioning `gps`.
    if query.to_ascii_lowercase().contains("gps") {
        return None;
    }
    let wide = to_wide(query);
    let mut prop = PROPVARIANT::default();
    unsafe { reader.GetMetadataByName(PCWSTR(wide.as_ptr()), &mut prop as *mut _ as *mut _) }
        .ok()?;
    let s = prop.to_string();
    let trimmed = s.trim().trim_matches('\0').trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn query_first_string(reader: &IWICMetadataQueryReader, queries: &[&str]) -> Option<String> {
    for q in queries {
        if let Some(val) = query_metadata_string(reader, q) {
            return Some(val);
        }
    }
    None
}

/// Decodes `path` via WIC, capping the decoded pixel buffer to `max_edge` on
/// the longest side (so huge images are downscaled before copying pixels into
/// memory), and extracts privacy-safe EXIF metadata (dimensions, date, camera;
/// GPS geotags are never queried).
pub fn decode_image_wic(path: &Path, max_edge: u32) -> Result<WicDecodedImage, String> {
    let _com = ComGuard::init();
    let factory = create_factory()?;
    let (_decoder, frame) = open_decoder(&factory, path)?;

    let mut width = 0u32;
    let mut height = 0u32;
    unsafe { frame.GetSize(&mut width, &mut height) }
        .map_err(|e| format!("WIC GetSize failed: {e}"))?;
    if width == 0 || height == 0 {
        return Err("WIC image has zero dimensions".into());
    }

    let (mut camera_make, mut camera_model, mut date_taken) = (None, None, None);
    if let Ok(reader) = unsafe { frame.GetMetadataQueryReader() } {
        camera_make = query_first_string(
            &reader,
            &[
                "/app1/ifd/{ushort=271}",
                "/ifd/{ushort=271}",
                "System.Photo.CameraManufacturer",
            ],
        );
        camera_model = query_first_string(
            &reader,
            &[
                "/app1/ifd/{ushort=272}",
                "/ifd/{ushort=272}",
                "System.Photo.CameraModel",
            ],
        );
        date_taken = query_first_string(
            &reader,
            &[
                "/app1/ifd/exif/{ushort=36867}",
                "/ifd/exif/{ushort=36867}",
                "/app1/ifd/{ushort=306}",
                "/ifd/{ushort=306}",
                "System.Photo.DateTaken",
            ],
        );
    }

    let (scaled_width, scaled_height) = compute_scaled_dimensions(width, height, max_edge.max(1));

    let source: IWICBitmapSource = if scaled_width != width || scaled_height != height {
        let scaler = unsafe { factory.CreateBitmapScaler() }
            .map_err(|e| format!("CreateBitmapScaler: {e}"))?;
        unsafe {
            scaler.Initialize(
                &frame,
                scaled_width,
                scaled_height,
                WICBitmapInterpolationModeFant,
            )
        }
        .map_err(|e| format!("IWICBitmapScaler::Initialize: {e}"))?;
        scaler.into()
    } else {
        frame.into()
    };

    let converter = unsafe { factory.CreateFormatConverter() }
        .map_err(|e| format!("CreateFormatConverter: {e}"))?;
    unsafe {
        converter.Initialize(
            &source,
            &GUID_WICPixelFormat32bppBGRA,
            WICBitmapDitherTypeNone,
            None,
            0.0,
            WICBitmapPaletteTypeCustom,
        )
    }
    .map_err(|e| format!("IWICFormatConverter::Initialize(32bppBGRA): {e}"))?;

    let stride = scaled_width.saturating_mul(4);
    let buf_len = (stride as usize).saturating_mul(scaled_height as usize);
    let mut bgra8 = vec![0u8; buf_len];
    unsafe { converter.CopyPixels(std::ptr::null(), stride, &mut bgra8) }
        .map_err(|e| format!("WIC CopyPixels: {e}"))?;

    Ok(WicDecodedImage {
        width,
        height,
        bgra8,
        scaled_width,
        scaled_height,
        camera_make,
        camera_model,
        date_taken,
    })
}

/// Converts a BGRA8 buffer (`width * height * 4`) to tightly-packed RGB8
/// (`width * height * 3`) for `mtmd_bitmap_init` or thumbnail encoding.
pub fn bgra8_to_rgb8(bgra: &[u8]) -> Vec<u8> {
    let mut rgb = Vec::with_capacity((bgra.len() / 4) * 3);
    for px in bgra.chunks_exact(4) {
        rgb.push(px[2]); // R
        rgb.push(px[1]); // G
        rgb.push(px[0]); // B
    }
    rgb
}

/// Encodes a BGRA8 buffer (`width x height`) to a PNG file at `dest_path`,
/// optionally downscaling to fit within `max_edge` on the longest side.
pub fn write_bgra_thumbnail_png(
    bgra: &[u8],
    width: u32,
    height: u32,
    max_edge: u32,
    dest_path: &Path,
) -> Result<(), String> {
    if width == 0 || height == 0 || bgra.len() < (width as usize) * (height as usize) * 4 {
        return Err("invalid BGRA buffer dimensions".into());
    }
    if let Some(parent) = dest_path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("create thumb dir: {e}"))?;
    }

    let _com = ComGuard::init();
    let factory = create_factory()?;

    let stride = width * 4;
    let mem_bitmap = unsafe {
        factory.CreateBitmapFromMemory(width, height, &GUID_WICPixelFormat32bppBGRA, stride, bgra)
    }
    .map_err(|e| format!("CreateBitmapFromMemory: {e}"))?;

    let (tw, th) = compute_scaled_dimensions(width, height, max_edge.max(1));
    let source: IWICBitmapSource = if tw != width || th != height {
        let scaler = unsafe { factory.CreateBitmapScaler() }
            .map_err(|e| format!("CreateBitmapScaler: {e}"))?;
        unsafe { scaler.Initialize(&mem_bitmap, tw, th, WICBitmapInterpolationModeFant) }
            .map_err(|e| format!("Scaler::Initialize: {e}"))?;
        scaler.into()
    } else {
        mem_bitmap.into()
    };

    let stream = unsafe { factory.CreateStream() }.map_err(|e| format!("CreateStream: {e}"))?;
    let wide_dest = path_to_wide(dest_path);
    // GENERIC_WRITE = 0x40000000
    unsafe { stream.InitializeFromFilename(PCWSTR(wide_dest.as_ptr()), 0x4000_0000u32) }
        .map_err(|e| format!("InitializeFromFilename: {e}"))?;

    let encoder = unsafe { factory.CreateEncoder(&GUID_ContainerFormatPng, std::ptr::null()) }
        .map_err(|e| format!("CreateEncoder(PNG): {e}"))?;
    unsafe { encoder.Initialize(&stream, WICBitmapEncoderNoCache) }
        .map_err(|e| format!("Encoder::Initialize: {e}"))?;

    let mut frame_encode = None;
    unsafe { encoder.CreateNewFrame(&mut frame_encode, std::ptr::null_mut()) }
        .map_err(|e| format!("CreateNewFrame: {e}"))?;
    let frame = frame_encode.ok_or_else(|| "CreateNewFrame returned null".to_string())?;

    unsafe { frame.Initialize(None) }.map_err(|e| format!("FrameEncode::Initialize: {e}"))?;
    unsafe { frame.SetSize(tw, th) }.map_err(|e| format!("FrameEncode::SetSize: {e}"))?;
    let mut pf = GUID_WICPixelFormat24bppBGR;
    unsafe { frame.SetPixelFormat(&mut pf) }
        .map_err(|e| format!("FrameEncode::SetPixelFormat: {e}"))?;
    unsafe { frame.WriteSource(&source, std::ptr::null()) }
        .map_err(|e| format!("FrameEncode::WriteSource: {e}"))?;
    unsafe { frame.Commit() }.map_err(|e| format!("FrameEncode::Commit: {e}"))?;
    unsafe { encoder.Commit() }.map_err(|e| format!("Encoder::Commit: {e}"))?;
    let _ = unsafe { stream.Seek(0, STREAM_SEEK_SET, None) };
    Ok(())
}

/// Computes `(scaled_width, scaled_height)` preserving aspect ratio so neither
/// dimension exceeds `max_edge`. Never upscales.
pub fn compute_scaled_dimensions(width: u32, height: u32, max_edge: u32) -> (u32, u32) {
    if width <= max_edge && height <= max_edge {
        return (width.max(1), height.max(1));
    }
    if width >= height {
        let sw = max_edge.max(1);
        let sh = ((height as u64 * sw as u64) / (width as u64).max(1)).max(1) as u32;
        (sw, sh)
    } else {
        let sh = max_edge.max(1);
        let sw = ((width as u64 * sh as u64) / (height as u64).max(1)).max(1) as u32;
        (sw, sh)
    }
}
