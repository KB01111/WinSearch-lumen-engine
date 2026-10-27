// SPDX-FileCopyrightText: Copyright (C) 2026 Brishen Hawkins
// SPDX-License-Identifier: Apache-2.0

//! Builds the C shim behind the `xrt` feature (WinSearch's port of upstream
//! taconite's build script, which drives `g++`/`ar` on Linux only).
//!
//! - **Windows (MSVC) with an XRT SDK** (`$XRT_ROOT`, default `C:\Xilinx\XRT`,
//!   holding `include\xrt\xrt_device.h` and `lib\xrt_coreutil.lib`; the SDK is
//!   `xrt_windows_sdk.zip` from XRT's releases): `iron_xrt_shim.cpp` becomes
//!   its own `iron_xrt_shim.dll`, linked against XRT, and the crate links its
//!   import library. Final binaries delay-load that DLL
//!   (`/DELAYLOAD:iron_xrt_shim.dll`, emitted by WinSearch's build scripts),
//!   so a PC without the AMD NPU driver still starts; `Session::open` loads
//!   XRT and the shim explicitly first and turns a missing runtime into
//!   `Error::Device`. The DLL is staged next to the profile's binaries.
//! - **Linux with XRT** (`$XRT_ROOT`, default `/opt/xilinx/xrt`): the shim is
//!   a static archive linked against `libxrt_coreutil.so` (upstream's dynamic
//!   mode; its static-XRT mode is not carried over).
//! - **No XRT SDK, or `TACONITE_XRT_STUB=1`**: `iron_xrt_stub.c` stands in.
//!   Every entry point fails with a clear message, the crate still links,
//!   and `XRT_LINKED` is false so callers fall back to another backend.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=shim");
    println!("cargo:rerun-if-env-changed=XRT_ROOT");
    println!("cargo:rerun-if-env-changed=TACONITE_XRT_STUB");
    println!("cargo::rustc-check-cfg=cfg(taconite_xrt_stub)");
    if env::var_os("CARGO_FEATURE_XRT").is_none() || env::var_os("DOCS_RS").is_some() {
        return;
    }

    let windows = env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows");
    let msvc = env::var("CARGO_CFG_TARGET_ENV").as_deref() == Ok("msvc");
    let forced_stub = env::var_os("TACONITE_XRT_STUB").is_some_and(|v| v == "1");
    match xrt_root(windows) {
        Some(root) if !forced_stub && windows && msvc => build_windows_dll(&root),
        Some(root) if !forced_stub && !windows => build_linux(&root),
        _ => build_stub(if forced_stub { "TACONITE_XRT_STUB=1" } else { "no XRT SDK found" }),
    }
}

/// `$XRT_ROOT` (or the platform default) when it holds XRT's headers and
/// core library; `None` means the stub.
fn xrt_root(windows: bool) -> Option<PathBuf> {
    let root = env::var_os("XRT_ROOT").map(PathBuf::from).unwrap_or_else(|| {
        PathBuf::from(if windows { r"C:\Xilinx\XRT" } else { "/opt/xilinx/xrt" })
    });
    let lib = root.join("lib");
    let has_lib = if windows {
        lib.join("xrt_coreutil.lib").is_file()
    } else {
        lib.join("libxrt_coreutil.so").exists() || lib.join("libxrt_coreutil.so.2").exists()
    };
    (root.join("include").join("xrt").join("xrt_device.h").is_file() && has_lib).then_some(root)
}

fn out_dir() -> PathBuf {
    PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR"))
}

fn build_stub(why: &str) {
    println!("cargo:warning=taconite: NPU runtime disabled ({why}); XRT calls fail cleanly");
    cc::Build::new()
        .file("shim/iron_xrt_stub.c")
        .include("shim")
        .warnings(false)
        .compile("taconite_xrt");
    println!("cargo:rustc-cfg=taconite_xrt_stub");
}

fn build_linux(root: &Path) {
    let lib = root.join("lib");
    cc::Build::new()
        .cpp(true)
        .std("c++17")
        .file("shim/iron_xrt_shim.cpp")
        .include("shim")
        .include(root.join("include"))
        .warnings(false)
        .compile("taconite_xrt");
    println!("cargo:rustc-link-search=native={}", lib.display());
    println!("cargo:rustc-link-lib=dylib=xrt_coreutil");
    println!("cargo:libdir={}", lib.display());
}

/// Compiles and links `iron_xrt_shim.dll` with the MSVC toolchain `cc`
/// resolves, then links its import library and stages the DLL.
fn build_windows_dll(root: &Path) {
    let out = out_dir();
    let dll = out.join("iron_xrt_shim.dll");
    let implib = out.join("iron_xrt_shim.lib");
    let compiler = cc::Build::new().cpp(true).get_compiler();
    let mut cmd = compiler.to_command();
    // XRT's public headers select std::any from __cplusplus, which MSVC
    // otherwise reports as C++98 even with /std:c++17.
    cmd.args(["/nologo", "/std:c++17",
        "/Zc:__cplusplus", "/EHsc", "/O2", "/MD", "/LD", "/D_CRT_SECURE_NO_WARNINGS"])
        .arg("/Ishim")
        .arg(format!("/I{}", root.join("include").display()))
        .arg("shim/iron_xrt_shim.cpp")
        .arg(format!("/Fo{}\\", out.display()))
        .arg(format!("/Fe{}", dll.display()))
        .arg("/link")
        .arg("/DEF:shim/iron_xrt_shim.def")
        .arg(format!("/LIBPATH:{}", root.join("lib").display()))
        .arg("xrt_coreutil.lib")
        .arg(format!("/IMPLIB:{}", implib.display()));
    let status = cmd.status().expect("run the MSVC compiler for iron_xrt_shim.dll");
    assert!(status.success(), "building iron_xrt_shim.dll against {} failed", root.display());

    println!("cargo:rustc-link-search=native={}", out.display());
    println!("cargo:rustc-link-lib=dylib=iron_xrt_shim");
    println!("cargo:shim_dll={}", dll.display());
    stage(&dll);
}

/// Copies the DLL next to the profile's binaries and test executables
/// (OUT_DIR is `target/<profile>/build/<crate>-<hash>/out`).
fn stage(dll: &Path) {
    let Some(profile) = out_dir().ancestors().nth(3).map(Path::to_path_buf) else {
        println!("cargo:warning=taconite: profile dir not found; iron_xrt_shim.dll not staged");
        return;
    };
    for dir in [profile.clone(), profile.join("deps")] {
        fs::create_dir_all(&dir).expect("create shim staging directory");
        let target = dir.join("iron_xrt_shim.dll");
        if let Err(error) = fs::copy(dll, &target) {
            // Windows may lock a loaded DLL. An identical copy is acceptable;
            // silently retaining a stale shim would invalidate native proof.
            assert!(
                fs::read(dll)
                    .ok()
                    .zip(fs::read(&target).ok())
                    .is_some_and(|(source, staged)| source == staged),
                "cannot stage current shim at {}: {error}",
                target.display()
            );
        }
    }
}
