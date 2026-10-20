//! Delay-loads the NPU runtime's XRT shim so the worker starts on PCs without
//! the AMD NPU driver; `taconite`'s `Session::open` loads XRT and the shim
//! explicitly before the first call and reports a missing runtime as an
//! error, which the worker answers by falling back to GPU/CPU.

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    if std::env::var("CARGO_CFG_TARGET_ENV").as_deref() == Ok("msvc") {
        println!("cargo:rustc-link-arg=/DELAYLOAD:iron_xrt_shim.dll");
    }
}
