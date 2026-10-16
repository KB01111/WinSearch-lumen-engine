//! Pinned PDFium ABI. Called only by the bounded extraction child, serially.
use std::{
    ffi::{c_char, c_void},
    fs::File,
    io::{Read, Seek, SeekFrom},
    os::windows::ffi::OsStrExt,
    path::{Path, PathBuf},
    sync::{Mutex, MutexGuard},
};
type Handle = *mut c_void;
#[link(name = "kernel32")]
extern "system" {
    fn LoadLibraryExW(path: *const u16, file: Handle, flags: u32) -> Handle;
    fn GetProcAddress(module: Handle, name: *const c_char) -> *mut c_void;
    fn FreeLibrary(module: Handle) -> i32;
}
static SERIAL: Mutex<()> = Mutex::new(());
macro_rules! api {
    ($($name:ident($($arg:ty),*) -> $ret:ty;)+) => {
        #[allow(non_snake_case)]
        struct Api { $( $name: unsafe extern "system" fn($($arg),*) -> $ret, )+ }
        impl Api { unsafe fn load(module:Handle)->Result<Self,String> { Ok(Self { $( $name: {
            let p=GetProcAddress(module,concat!(stringify!($name),"\0").as_ptr().cast());
            if p.is_null() {return Err(concat!("PDFium dependency missing symbol ",stringify!($name)).into());}
            std::mem::transmute::<*mut c_void,unsafe extern "system" fn($($arg),*) -> $ret>(p)
        }, )+ }) } }
    }
}
api! {
    FPDF_InitLibrary() -> ();
    FPDF_DestroyLibrary() -> ();
    FPDF_LoadCustomDocument(*mut FileAccess,*const c_char) -> Handle;
    FPDF_GetLastError() -> u32;
    FPDF_CloseDocument(Handle) -> ();
    FPDF_GetPageCount(Handle) -> i32;
    FPDF_LoadPage(Handle,i32) -> Handle;
    FPDF_ClosePage(Handle) -> ();
    FPDF_GetPageWidthF(Handle) -> f32;
    FPDF_GetPageHeightF(Handle) -> f32;
    FPDFText_LoadPage(Handle) -> Handle;
    FPDFText_ClosePage(Handle) -> ();
    FPDFText_CountChars(Handle) -> i32;
    FPDFText_GetText(Handle,i32,i32,*mut u16) -> i32;
    FPDFBitmap_CreateEx(i32,i32,i32,Handle,i32) -> Handle;
    FPDFBitmap_Destroy(Handle) -> ();
    FPDF_RenderPageBitmap(Handle,Handle,i32,i32,i32,i32,i32,i32) -> ();
    FPDFPage_CountObjects(Handle) -> i32;
    FPDFPage_GetObject(Handle,i32) -> Handle;
    FPDFPageObj_GetType(Handle) -> i32;
}
// Windows unsigned long is 32 bits, including on x64. Callback is cdecl.
#[repr(C)]
struct FileAccess {
    len: u32,
    read: unsafe extern "C" fn(Handle, u32, *mut u8, u32) -> i32,
    context: Handle,
}
struct Source {
    file: File,
    len: u64,
    failed: bool,
}
unsafe extern "C" fn read_block(context: Handle, offset: u32, buffer: *mut u8, len: u32) -> i32 {
    let source = &mut *context.cast::<Source>();
    if buffer.is_null() || len == 0 || u64::from(offset) + u64::from(len) > source.len {
        source.failed = true;
        return 0;
    }
    let result = source
        .file
        .seek(SeekFrom::Start(u64::from(offset)))
        .and_then(|_| {
            source
                .file
                .read_exact(std::slice::from_raw_parts_mut(buffer, len as usize))
        });
    if result.is_err() {
        source.failed = true;
        return 0;
    }
    1
}
pub struct Pdfium {
    api: Api,
    module: Handle,
    _serial: MutexGuard<'static, ()>,
}
impl Pdfium {
    pub fn load() -> Result<Self, String> {
        let directory = dependency_directory()?;
        Self::load_from(&directory)
    }
    fn load_from(directory: &Path) -> Result<Self, String> {
        if !directory.is_absolute() {
            return Err("PDFium dependency directory must be absolute".into());
        }
        let dll = directory.join("pdfium.dll");
        if !crate::content::local_content_allowed(&dll) {
            return Err("PDFium dependency missing or unsafe".into());
        }
        let wide: Vec<u16> = dll.as_os_str().encode_wide().chain(Some(0)).collect();
        let serial = SERIAL.lock().map_err(|_| "PDFium serialization poisoned")?;
        // LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR | LOAD_LIBRARY_SEARCH_SYSTEM32.
        let module = unsafe { LoadLibraryExW(wide.as_ptr(), std::ptr::null_mut(), 0x1100) };
        if module.is_null() {
            return Err(format!(
                "PDFium dependency missing/unloadable: {}",
                dll.display()
            ));
        }
        let api = match unsafe { Api::load(module) } {
            Ok(api) => api,
            Err(e) => {
                unsafe { FreeLibrary(module) };
                return Err(e);
            }
        };
        unsafe { (api.FPDF_InitLibrary)() };
        Ok(Self {
            api,
            module,
            _serial: serial,
        })
    }
    pub fn open(&self, path: &Path, max_bytes: u64) -> Result<Document<'_>, String> {
        let file = File::open(path).map_err(|e| format!("PDF unreadable: {e}"))?;
        let len = file
            .metadata()
            .map_err(|e| format!("PDF unreadable metadata: {e}"))?
            .len();
        if len > max_bytes || len > u32::MAX as u64 {
            return Err("PDF source exceeds byte budget".into());
        }
        let mut source = Box::new(Source {
            file,
            len,
            failed: false,
        });
        let mut access = Box::new(FileAccess {
            len: len as u32,
            read: read_block,
            context: (&mut *source as *mut Source).cast(),
        });
        let raw = unsafe { (self.api.FPDF_LoadCustomDocument)(&mut *access, std::ptr::null()) };
        if raw.is_null() {
            return Err(if source.failed {
                "PDF unreadable callback failure".into()
            } else {
                match unsafe { (self.api.FPDF_GetLastError)() } {
                    4 => "PDF password protected".into(),
                    2 => "PDF unreadable".into(),
                    code => format!("PDF corrupt/invalid (PDFium {code})"),
                }
            });
        }
        let count = unsafe { (self.api.FPDF_GetPageCount)(raw) };
        if count < 0 || source.failed {
            unsafe { (self.api.FPDF_CloseDocument)(raw) };
            return Err("PDF unreadable page table".into());
        }
        Ok(Document {
            raw,
            api: &self.api,
            source,
            _access: access,
            page_count: count.max(0) as u32,
        })
    }
}
fn dependency_directory() -> Result<PathBuf, String> {
    // Explicit fixture override is absent from release builds.
    #[cfg(debug_assertions)]
    if let Some(path) = std::env::var_os("WINSEARCH_TEST_PDFIUM_DIR") {
        return Ok(PathBuf::from(path));
    }
    std::env::current_exe()
        .map(|p| p.parent().expect("executable parent").to_path_buf())
        .map_err(|e| e.to_string())
}
impl Drop for Pdfium {
    fn drop(&mut self) {
        unsafe {
            (self.api.FPDF_DestroyLibrary)();
            FreeLibrary(self.module);
        }
    }
}
pub struct Document<'a> {
    raw: Handle,
    api: &'a Api,
    source: Box<Source>,
    _access: Box<FileAccess>,
    pub page_count: u32,
}
impl Drop for Document<'_> {
    fn drop(&mut self) {
        unsafe { (self.api.FPDF_CloseDocument)(self.raw) }
    }
}
struct Owned {
    raw: Handle,
    close: unsafe extern "system" fn(Handle),
}
impl Drop for Owned {
    fn drop(&mut self) {
        unsafe { (self.close)(self.raw) }
    }
}
pub struct Page {
    pub text: String,
    pub bgra: Vec<u8>,
    pub width: u32,
    pub height: u32,
    pub visual: bool,
}
impl Document<'_> {
    pub fn page(&mut self, ordinal: u32) -> Result<Page, String> {
        let a = self.api;
        let raw = unsafe { (a.FPDF_LoadPage)(self.raw, ordinal as i32) };
        if raw.is_null() {
            return Err(format!("PDF corrupt page {}", ordinal + 1));
        }
        let page = Owned {
            raw,
            close: a.FPDF_ClosePage,
        };
        let (w, h) = unsafe { ((a.FPDF_GetPageWidthF)(raw), (a.FPDF_GetPageHeightF)(raw)) };
        if !w.is_finite()
            || !h.is_finite()
            || w <= 0.0
            || h <= 0.0
            || w / h > 10000.0
            || h / w > 10000.0
        {
            return Err(format!("PDF invalid page dimensions at {}", ordinal + 1));
        }
        let text_raw = unsafe { (a.FPDFText_LoadPage)(raw) };
        if text_raw.is_null() {
            return Err("PDF corrupt text page".into());
        }
        let text_page = Owned {
            raw: text_raw,
            close: a.FPDFText_ClosePage,
        };
        let count = unsafe { (a.FPDFText_CountChars)(text_raw) };
        if !(0..=500_000).contains(&count) {
            return Err("PDF page text exceeds bounded 500000 characters".into());
        }
        let mut buffer = vec![0u16; count as usize + 1];
        let written = unsafe { (a.FPDFText_GetText)(text_raw, 0, count, buffer.as_mut_ptr()) };
        if written < 0 || (count > 0 && written == 0) || written as usize > buffer.len() {
            return Err("PDF invalid text length".into());
        }
        let text = String::from_utf16_lossy(&buffer[..written.max(1) as usize - 1]);
        drop(text_page);
        let scale = (2048.0 / w.max(h)).min(2.0);
        let width = (w * scale).ceil().clamp(1.0, 2048.0) as u32;
        let height = (h * scale).ceil().clamp(1.0, 2048.0) as u32;
        let stride = width.checked_mul(4).ok_or("PDF stride overflow")?;
        let bytes = stride.checked_mul(height).ok_or("PDF area overflow")?;
        let mut bgra = vec![255u8; bytes as usize];
        let bitmap_raw = unsafe {
            (a.FPDFBitmap_CreateEx)(
                width as i32,
                height as i32,
                4,
                bgra.as_mut_ptr().cast(),
                stride as i32,
            )
        };
        if bitmap_raw.is_null() {
            return Err("PDF bitmap allocation failed".into());
        }
        let bitmap = Owned {
            raw: bitmap_raw,
            close: a.FPDFBitmap_Destroy,
        };
        unsafe {
            (a.FPDF_RenderPageBitmap)(bitmap_raw, raw, 0, 0, width as i32, height as i32, 0, 0)
        };
        let count = unsafe { (a.FPDFPage_CountObjects)(raw) };
        if !(0..=100000).contains(&count) {
            return Err("PDF page object budget exceeded".into());
        }
        let visual = (0..count)
            .any(|i| unsafe { (a.FPDFPageObj_GetType)((a.FPDFPage_GetObject)(raw, i)) != 1 });
        drop(bitmap);
        drop(page);
        if self.source.failed {
            return Err("PDF unreadable callback failure".into());
        }
        Ok(Page {
            text,
            bgra,
            width,
            height,
            visual,
        })
    }
}
