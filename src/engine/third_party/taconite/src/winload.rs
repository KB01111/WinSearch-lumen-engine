// SPDX-License-Identifier: Apache-2.0

//! Windows only (WinSearch port): resolves XRT's `xrt_coreutil.dll` and
//! `iron_xrt_shim.dll` before the first shim call.
//!
//! Final binaries delay-load the shim, and the shim imports XRT, so neither
//! DLL is needed to start the process. Loading them here, explicitly and in
//! dependency order, means the delay-load helper later finds both already
//! resident (the loader reuses a loaded module of the same name) and never
//! raises its unrecoverable "module not found" exception: a PC without the
//! AMD NPU driver gets an `Err` and the caller falls back.
//!
//! XRT search order: `%XILINX_XRT%` (and its `bin`), the driver's
//! `%SystemRoot%\System32\AMD`, then the standard DLL search (PATH). The shim
//! is looked up next to the executable, then through the standard search.

use std::ffi::{OsStr, OsString, c_void};
use std::os::windows::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

const XRT_DLL: &str = "xrt_coreutil.dll";
const SHIM_DLL: &str = "iron_xrt_shim.dll";

/// Search the loaded DLL's own directory for its dependencies, then the
/// default directories (application dir, System32, AddDllDirectory paths).
const LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR: u32 = 0x0000_0100;
const LOAD_LIBRARY_SEARCH_DEFAULT_DIRS: u32 = 0x0000_1000;

unsafe extern "system" {
    fn LoadLibraryExW(name: *const u16, file: *mut c_void, flags: u32) -> *mut c_void;
    fn GetModuleFileNameW(module: *mut c_void, buf: *mut u16, size: u32) -> u32;
    fn GetLastError() -> u32;
}

/// Loads XRT and the shim once per process; later calls return the cached result.
pub fn ensure_loaded() -> Result<(), String> {
    static LOADED: OnceLock<Result<(), String>> = OnceLock::new();
    LOADED.get_or_init(load).clone()
}

fn load() -> Result<(), String> {
    let mut tried = Vec::new();
    let xrt = xrt_candidates()
        .into_iter()
        .find_map(|p| load_path(&p).inspect_err(|e| tried.push(e.clone())).ok())
        .ok_or_else(|| {
            format!("XRT runtime ({XRT_DLL}) not found; install or update the AMD NPU driver ({})", tried.join("; "))
        })?;
    let _ = xrt; // stays loaded for the process lifetime

    tried.clear();
    shim_candidates()
        .into_iter()
        .find_map(|p| load_path(&p).inspect_err(|e| tried.push(e.clone())).ok())
        .map(|_| ())
        .ok_or_else(|| format!("{SHIM_DLL} not found next to the executable ({})", tried.join("; ")))
}

fn xrt_candidates() -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Some(root) = std::env::var_os("XILINX_XRT").filter(|v| !v.is_empty()) {
        let root = PathBuf::from(root);
        out.push(root.join(XRT_DLL));
        out.push(root.join("bin").join(XRT_DLL));
    }
    let system_root = std::env::var_os("SystemRoot").unwrap_or_else(|| OsString::from(r"C:\Windows"));
    out.push(PathBuf::from(system_root).join("System32").join("AMD").join(XRT_DLL));
    out.push(PathBuf::from(XRT_DLL));
    out
}

fn shim_candidates() -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Some(dir) = exe_dir() {
        out.push(dir.join(SHIM_DLL));
    }
    out.push(PathBuf::from(SHIM_DLL));
    out
}

fn exe_dir() -> Option<PathBuf> {
    let mut buf = vec![0u16; 32_768];
    // SAFETY: buf is valid for buf.len() u16s; a null module means this process's exe.
    let n = unsafe { GetModuleFileNameW(std::ptr::null_mut(), buf.as_mut_ptr(), buf.len() as u32) } as usize;
    if n == 0 || n >= buf.len() {
        return None;
    }
    PathBuf::from(OsString::from_wide(&buf[..n])).parent().map(Path::to_path_buf)
}

/// Absolute paths load with their own directory searched for dependencies;
/// a bare name uses the standard search order (which includes PATH).
fn load_path(path: &Path) -> Result<*mut c_void, String> {
    if path.is_absolute() && !path.is_file() {
        return Err(format!("{} missing", path.display()));
    }
    let wide: Vec<u16> = OsStr::new(path).encode_wide().chain(Some(0)).collect();
    let flags = if path.is_absolute() {
        LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR | LOAD_LIBRARY_SEARCH_DEFAULT_DIRS
    } else {
        0
    };
    // SAFETY: wide is NUL-terminated; no file handle is passed.
    let module = unsafe { LoadLibraryExW(wide.as_ptr(), std::ptr::null_mut(), flags) };
    if module.is_null() {
        // SAFETY: reads the calling thread's last-error value.
        let code = unsafe { GetLastError() };
        Err(format!("{}: LoadLibraryEx error {code}", path.display()))
    } else {
        Ok(module)
    }
}
