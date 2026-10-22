//! Builds the pinned llama.cpp tree with cmake and links the flat C shim.
//!
//! - Source: `LLAMA_CPP_DIR` env var if set, otherwise a shallow clone of
//!   [`LLAMA_CPP_REV`] cached under `target/ws-embed-llama/`. The source tree is
//!   NOT committed to the repository.
//! - Backends are built as separately loaded DLLs (`GGML_BACKEND_DL=ON`). The CPU
//!   variant always builds. CUDA is enabled only when the CUDA toolkit is found,
//!   Vulkan only when `VULKAN_SDK` is set. Missing SDKs are skipped, not errors.
//! - Emits link flags for `llama`, `mtmd`, `ggml` and `ggml-base`.
//!
//! Set `WS_EMBED_SKIP_NATIVE=1` to skip the native build (pure-Rust tests only);
//! the crate then fails to link, so use it only with `cargo check`.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Pinned llama.cpp revision. Record changes in `third_party/llama.cpp.md`.
///
/// This is a commit, not a release tag: v0.6.0 has no `gemma-embedding2`
/// architecture (EmbeddingGemma 2), and the first commit that adds it is 4fbc76dec
/// (2026-10-06), 23 commits after v0.6.0. Pin by SHA so the build is reproducible.
const LLAMA_CPP_REV: &str = "4fbc76dec51d0add466f0210855c0596589b60d4";
const LLAMA_CPP_REPO: &str = "https://github.com/ggml-org/llama.cpp";
/// Short label used for cache directory names.
const LLAMA_CPP_LABEL: &str = "4fbc76de";

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=shim/ws_embed_shim.cpp");
    println!("cargo:rerun-if-env-changed=LLAMA_CPP_DIR");
    println!("cargo:rerun-if-env-changed=WS_EMBED_SKIP_NATIVE");
    println!("cargo:rerun-if-env-changed=VULKAN_SDK");
    println!("cargo:rerun-if-env-changed=CUDA_PATH");
    println!("cargo:rerun-if-env-changed=WS_EMBED_NO_CUDA");
    println!("cargo:rerun-if-env-changed=WS_EMBED_NO_VULKAN");

    delay_load_npu_shim();

    if env::var_os("WS_EMBED_SKIP_NATIVE").is_some() {
        println!("cargo:warning=ws-embed: WS_EMBED_SKIP_NATIVE set, native build skipped");
        return;
    }

    let src = llama_source_dir();
    let cuda = cuda_available();
    let vulkan = vulkan_available();
    println!("cargo:warning=ws-embed: CUDA backend {}", on_off(cuda));
    println!("cargo:warning=ws-embed: Vulkan backend {}", on_off(vulkan));

    let install = build_llama(&src, cuda, vulkan);
    build_shim(&src, &install);
    link(&install);
}

/// The NPU runtime's XRT shim (`iron_xrt_shim.dll`, see
/// `third_party/taconite/build.rs`) is delay-loaded so this crate's tests and
/// examples start on PCs without the AMD NPU driver; `delayimp` reaches every
/// dependent binary, which adds its own `/DELAYLOAD` (`winsearch-embed`).
/// A stub build has no such import; MSVC then only warns (LNK4199).
fn delay_load_npu_shim() {
    if env::var("CARGO_CFG_TARGET_ENV").as_deref() == Ok("msvc") {
        println!("cargo:rustc-link-lib=delayimp");
        println!("cargo:rustc-link-arg=/DELAYLOAD:iron_xrt_shim.dll");
    }
}

fn on_off(b: bool) -> &'static str {
    if b {
        "enabled"
    } else {
        "skipped (SDK not found)"
    }
}

fn manifest_dir() -> PathBuf {
    PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"))
}

fn out_dir() -> PathBuf {
    PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR"))
}

/// `target/ws-embed-llama/` lives under the workspace target dir so it is
/// gitignored and shared across builds.
fn cache_root() -> PathBuf {
    // OUT_DIR is target/<profile>/build/<crate>-<hash>/out. Walk up to target/.
    let mut p = out_dir();
    for _ in 0..5 {
        if p.file_name().map(|n| n == "target").unwrap_or(false) {
            return p.join("ws-embed-llama");
        }
        if !p.pop() {
            break;
        }
    }
    manifest_dir().join("target").join("ws-embed-llama")
}

fn llama_source_dir() -> PathBuf {
    if let Some(dir) = env::var_os("LLAMA_CPP_DIR") {
        let p = PathBuf::from(dir);
        assert!(
            p.join("CMakeLists.txt").is_file(),
            "LLAMA_CPP_DIR has no CMakeLists.txt: {}",
            p.display()
        );
        return p;
    }
    let dest = cache_root().join(format!("llama.cpp-{LLAMA_CPP_LABEL}"));
    if dest.join("CMakeLists.txt").is_file() {
        return dest;
    }
    let tmp = cache_root().join(format!(
        "llama.cpp-{LLAMA_CPP_LABEL}.tmp-{}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&tmp);
    fs::create_dir_all(&tmp).expect("create llama.cpp temp cache dir");
    // Fetch exactly the pinned commit (shallow, no full history) into an
    // isolated per-PID directory, then atomically rename into place.
    let git = |args: &[&str]| {
        let status = Command::new("git")
            .arg("-C")
            .arg(&tmp)
            .args(["-c", "core.longpaths=true"])
            .args(args)
            .status()
            .expect("run git (git must be on PATH)");
        assert!(
            status.success(),
            "git {args:?} failed for llama.cpp {LLAMA_CPP_REV}"
        );
    };
    git(&["init", "--quiet"]);
    git(&["remote", "add", "origin", LLAMA_CPP_REPO]);
    git(&["fetch", "--depth", "1", "--quiet", "origin", LLAMA_CPP_REV]);
    git(&["checkout", "--quiet", "FETCH_HEAD"]);
    if dest.join("CMakeLists.txt").is_file() {
        let _ = fs::remove_dir_all(&tmp);
        return dest;
    }
    let _ = fs::remove_dir_all(&dest);
    if fs::rename(&tmp, &dest).is_err() {
        let _ = fs::remove_dir_all(&tmp);
        assert!(
            dest.join("CMakeLists.txt").is_file(),
            "failed to move llama.cpp checkout into {}",
            dest.display()
        );
    }
    dest
}

/// `WS_EMBED_NO_CUDA=1` forces a CPU-only build even when the toolkit is present
/// (used to verify the no-GPU path, and for CPU-only CI).
fn cuda_available() -> bool {
    if env::var_os("WS_EMBED_NO_CUDA").is_some() {
        return false;
    }
    if env::var_os("CUDA_PATH").is_some() {
        return true;
    }
    Path::new(r"C:\Program Files\NVIDIA GPU Computing Toolkit\CUDA").is_dir()
}

/// Vulkan is built only when the SDK is present (`VULKAN_SDK`), and
/// `WS_EMBED_NO_VULKAN=1` forces it off.
fn vulkan_available() -> bool {
    if env::var_os("WS_EMBED_NO_VULKAN").is_some() {
        return false;
    }
    env::var_os("VULKAN_SDK").is_some()
}

fn cmake_exe() -> PathBuf {
    let fallback = PathBuf::from(r"C:\Program Files\CMake\bin\cmake.exe");
    if fallback.is_file() {
        return fallback;
    }
    PathBuf::from("cmake")
}

/// Configure and build llama.cpp, returning the install prefix.
///
/// The build dir is keyed by the backend set as well as tag and profile. cmake
/// caches GGML_CUDA/GGML_VULKAN, so a CPU-only build sharing a dir with a CUDA
/// build would silently keep CUDA on.
fn build_llama(src: &Path, cuda: bool, vulkan: bool) -> PathBuf {
    let variant = format!(
        "{}-{}-{}",
        LLAMA_CPP_LABEL,
        if cuda { "cuda" } else { "nocuda" },
        if vulkan { "vulkan" } else { "novulkan" },
    );
    let build = cache_root().join(format!("build-{variant}-{}", profile()));
    let install = cache_root().join(format!("install-{variant}-{}", profile()));

    let mut cfg = Command::new(cmake_exe());
    cfg.arg("-S").arg(src).arg("-B").arg(&build);
    cfg.arg("-G")
        .arg("Visual Studio 17 2022")
        .arg("-A")
        .arg("x64");
    cfg.arg(format!("-DCMAKE_INSTALL_PREFIX={}", install.display()));
    cfg.arg(format!("-DCMAKE_BUILD_TYPE={}", profile()));
    cfg.args([
        "-DBUILD_SHARED_LIBS=ON",
        "-DGGML_BACKEND_DL=ON",
        "-DGGML_CPU_ALL_VARIANTS=ON",
        "-DLLAMA_BUILD_COMMON=OFF",
        "-DLLAMA_BUILD_TOOLS=OFF",
        "-DLLAMA_BUILD_TESTS=OFF",
        "-DLLAMA_BUILD_EXAMPLES=OFF",
        "-DLLAMA_BUILD_SERVER=OFF",
        "-DLLAMA_BUILD_APP=OFF",
        "-DLLAMA_BUILD_UI=OFF",
        "-DLLAMA_OPENSSL=OFF",
        "-DLLAMA_BUILD_MTMD=ON",
        "-DLLAMA_TOOLS_INSTALL=OFF",
        "-DLLAMA_BUILD_IS_DEV=OFF",
        "-DGGML_OPENMP=OFF",
    ]);
    cfg.arg(format!("-DGGML_CUDA={}", on(cuda)));
    cfg.arg(format!("-DGGML_VULKAN={}", on(vulkan)));

    run(&mut cfg, "cmake configure");
    run(
        Command::new(cmake_exe())
            .arg("--build")
            .arg(&build)
            .arg("--config")
            .arg(profile())
            .arg("--parallel"),
        "cmake build",
    );
    run(
        Command::new(cmake_exe())
            .arg("--install")
            .arg(&build)
            .arg("--config")
            .arg(profile()),
        "cmake install",
    );
    install
}

fn profile() -> &'static str {
    // Release-quality inference even in debug test builds.
    "Release"
}

fn on(b: bool) -> &'static str {
    if b {
        "ON"
    } else {
        "OFF"
    }
}

fn run(cmd: &mut Command, what: &str) {
    let status = cmd
        .status()
        .unwrap_or_else(|e| panic!("{what}: failed to start: {e}"));
    assert!(status.success(), "{what} failed with {status}");
}

/// Compile the flat C shim against the installed headers and link it.
fn build_shim(src: &Path, install: &Path) {
    let shim = manifest_dir().join("shim").join("ws_embed_shim.cpp");
    let include = install.join("include");
    let ggml_include = src.join("ggml").join("include");
    let mtmd_include = src.join("tools").join("mtmd");

    cc::Build::new()
        .cpp(true)
        .std("c++17")
        .file(&shim)
        .include(&include)
        .include(&ggml_include)
        .include(&mtmd_include)
        .warnings(false)
        .compile("ws_embed_shim");
}

/// Link llama, mtmd and ggml import libraries and stage the runtime DLLs.
fn link(install: &Path) {
    let lib = install.join("lib");
    println!("cargo:rustc-link-search=native={}", lib.display());
    for name in ["llama", "mtmd", "ggml", "ggml-base"] {
        println!("cargo:rustc-link-lib=dylib={name}");
    }
    stage_runtime_dlls(install);
}

/// Copy llama/ggml DLLs (including the dynamically loaded backends) next to the
/// profile output so binaries and test executables find them at startup.
/// ggml loads backend DLLs from the executable directory (GGML_BACKEND_DL).
fn stage_runtime_dlls(install: &Path) {
    let bin = install.join("bin");
    // OUT_DIR is target/<profile>/build/<crate>-<hash>/out; the profile dir is
    // three levels up from the out dir's parent. Stage into profile and deps.
    let out = out_dir();
    let Some(profile_dir) = out.ancestors().nth(3).map(Path::to_path_buf) else {
        println!("cargo:warning=ws-embed: could not locate target profile dir; DLLs not staged");
        return;
    };
    let targets = [profile_dir.clone(), profile_dir.join("deps")];
    let Ok(entries) = fs::read_dir(&bin) else {
        println!(
            "cargo:warning=ws-embed: install bin dir missing: {}",
            bin.display()
        );
        return;
    };
    let current: Vec<std::ffi::OsString> = entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().map(|e| e == "dll").unwrap_or(false))
        .filter_map(|p| p.file_name().map(|n| n.to_os_string()))
        .collect();

    for dir in &targets {
        if fs::create_dir_all(dir).is_err() {
            continue;
        }
        // Remove backend DLLs left by a previous configuration (e.g. ggml-cuda.dll
        // after a CPU-only rebuild). A stale backend would be loaded at runtime.
        if let Ok(existing) = fs::read_dir(dir) {
            for e in existing.flatten() {
                let p = e.path();
                let is_backend = p
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with("ggml") && n.ends_with(".dll"))
                    .unwrap_or(false);
                if is_backend && !current.iter().any(|c| Some(c.as_os_str()) == p.file_name()) {
                    let _ = fs::remove_file(&p);
                }
            }
        }
        for name in &current {
            // Ignore copy failures for DLLs currently in use by another process.
            let _ = fs::copy(bin.join(name), dir.join(name));
        }
    }
}
