//! Application catalog: Start Menu and Desktop items (shortcuts and
//! executables) with shortcut target resolution via the shell's IShellLinkW.
//!
//! Scanning is metadata-only; shortcuts are resolved to their targets so
//! search can explain matches and the UI can show what would launch.

use std::path::{Path, PathBuf};

/// One catalogued application.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppEntry {
    /// Display name (shortcut/file stem, cleaned up).
    pub name: String,
    /// Launchable path (.lnk or .exe).
    pub path: String,
    /// Resolved target executable when known.
    pub target: String,
    /// Extra search keywords (target name, arguments).
    pub keywords: String,
}

/// Start Menu / Desktop locations, per-user and machine-wide.
pub fn app_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Some(appdata) = std::env::var_os("APPDATA") {
        roots.push(PathBuf::from(appdata).join("Microsoft\\Windows\\Start Menu"));
    }
    if let Some(programdata) = std::env::var_os("PROGRAMDATA") {
        roots.push(PathBuf::from(programdata).join("Microsoft\\Windows\\Start Menu"));
    }
    if let Some(profile) = std::env::var_os("USERPROFILE") {
        roots.push(PathBuf::from(profile).join("Desktop"));
    }
    if let Some(public) = std::env::var_os("PUBLIC") {
        roots.push(PathBuf::from(public).join("Desktop"));
    }
    roots
}

/// Outcome of scanning application catalog roots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogScan {
    pub apps: Vec<AppEntry>,
    /// True when every existing catalog root (and its subdirectories) was read
    /// without non-`NotFound` I/O errors.
    pub complete: bool,
}

/// Recursively scans catalog roots for application items, tracking whether all
/// existing catalog directories were read cleanly.
pub fn scan_catalog_full(roots: &[PathBuf]) -> CatalogScan {
    let mut apps = Vec::new();
    let mut complete = true;
    for root in roots {
        if !scan_dir(root, 0, &mut apps) {
            complete = false;
        }
    }
    apps.sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()));
    apps.dedup_by(|a, b| a.path.eq_ignore_ascii_case(&b.path));
    CatalogScan { apps, complete }
}

/// Recursively scans catalog roots for application items.
pub fn scan_catalog(roots: &[PathBuf]) -> Vec<AppEntry> {
    scan_catalog_full(roots).apps
}

fn scan_dir(dir: &Path, depth: usize, apps: &mut Vec<AppEntry>) -> bool {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return true,
        Err(_) => return false,
    };
    let mut complete = true;
    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                if e.kind() != std::io::ErrorKind::NotFound {
                    complete = false;
                }
                continue;
            }
        };
        let ft = match entry.file_type() {
            Ok(ft) => ft,
            Err(e) => {
                if e.kind() != std::io::ErrorKind::NotFound {
                    complete = false;
                }
                continue;
            }
        };
        let path = entry.path();
        let is_dir = if ft.is_symlink() {
            match std::fs::metadata(&path) {
                Ok(meta) => meta.is_dir(),
                Err(e) => {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        complete = false;
                    }
                    continue;
                }
            }
        } else {
            ft.is_dir()
        };
        if is_dir {
            if depth < 8 && !scan_dir(&path, depth + 1, apps) {
                complete = false;
            }
            continue;
        }
        let ext = path
            .extension()
            .map(|e| e.to_string_lossy().to_lowercase())
            .unwrap_or_default();
        match ext.as_str() {
            "lnk" => {
                if let Some(app) = shortcut_entry(&path) {
                    apps.push(app);
                }
            }
            "exe" => {
                let name = clean_name(
                    &path
                        .file_stem()
                        .map(|s| s.to_string_lossy().into_owned())
                        .unwrap_or_default(),
                );
                apps.push(AppEntry {
                    name,
                    path: path.to_string_lossy().into_owned(),
                    target: path.to_string_lossy().into_owned(),
                    keywords: String::new(),
                });
            }
            _ => {}
        }
    }
    complete
}

fn shortcut_entry(lnk: &Path) -> Option<AppEntry> {
    let name = clean_name(&lnk.file_stem()?.to_string_lossy());
    let (target, args) = resolve_shortcut(lnk).unwrap_or_default();
    let target_stem = Path::new(&target)
        .file_stem()
        .map(|s| s.to_string_lossy().to_lowercase())
        .unwrap_or_default();
    let keywords = format!("{target_stem} {}", args.to_lowercase())
        .trim()
        .to_string();
    Some(AppEntry {
        name,
        path: lnk.to_string_lossy().into_owned(),
        target,
        keywords,
    })
}

/// Strips common shortcut-name noise ("Acme App.lnk - Shortcut" → "Acme App").
fn clean_name(stem: &str) -> String {
    let mut name = stem.trim().to_string();
    for suffix in [" - Shortcut", " (2)", " (3)"] {
        if let Some(stripped) = name.strip_suffix(suffix) {
            name = stripped.trim().to_string();
        }
    }
    name
}

/// Resolves a `.lnk` shortcut to `(target path, arguments)` using the shell's
/// `IShellLinkW`. Returns `None` when the shortcut cannot be loaded.
pub fn resolve_shortcut(lnk: &Path) -> Option<(String, String)> {
    use windows::core::{Interface, PCWSTR};
    use windows::Win32::System::Com::{
        CoCreateInstance, CoInitializeEx, CoUninitialize, IPersistFile, CLSCTX_INPROC_SERVER,
        COINIT_MULTITHREADED, STGM,
    };
    use windows::Win32::UI::Shell::{IShellLinkW, ShellLink};

    unsafe {
        // The crawler threads are MTA; RPC_E_CHANGED_MODE is fine too.
        let _ = CoInitializeEx(None, COINIT_MULTITHREADED);

        let result = (|| {
            let link: IShellLinkW =
                CoCreateInstance(&ShellLink, None, CLSCTX_INPROC_SERVER).ok()?;
            let persist: IPersistFile = link.cast().ok()?;
            let wide: Vec<u16> = lnk
                .to_string_lossy()
                .encode_utf16()
                .chain(std::iter::once(0))
                .collect();
            persist.Load(PCWSTR(wide.as_ptr()), STGM(0)).ok()?;

            let mut target_buf = [0u16; 32_768];
            link.GetPath(&mut target_buf, std::ptr::null_mut(), 0)
                .ok()?;
            let end = target_buf.iter().position(|&u| u == 0).unwrap_or(0);
            let target = String::from_utf16_lossy(&target_buf[..end]);

            let mut args_buf = [0u16; 1024];
            link.GetArguments(&mut args_buf).ok()?;
            let end = args_buf.iter().position(|&u| u == 0).unwrap_or(0);
            let args = String::from_utf16_lossy(&args_buf[..end]);

            Some((target, args))
        })();

        CoUninitialize();
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    struct TempTree(PathBuf);

    impl TempTree {
        fn new() -> Self {
            let dir = std::env::temp_dir().join(format!(
                "winsearch-catalog-test-{}-{:?}",
                std::process::id(),
                std::thread::current().id()
            ));
            let _ = fs::remove_dir_all(&dir);
            fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }
    }

    impl Drop for TempTree {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn catalogs_executables_with_display_names() {
        let tree = TempTree::new();
        let menu = tree.0.join("Programs");
        fs::create_dir_all(&menu).unwrap();
        fs::write(menu.join("Acme Editor.exe"), b"MZ").unwrap();
        fs::write(menu.join("notes.txt"), b"not an app").unwrap();

        let apps = scan_catalog(&[tree.0.clone()]);
        assert_eq!(apps.len(), 1);
        assert_eq!(apps[0].name, "Acme Editor");
        assert!(apps[0].path.ends_with("Acme Editor.exe"));
        assert_eq!(apps[0].target, apps[0].path);
    }

    #[test]
    fn display_names_are_cleaned() {
        assert_eq!(clean_name("Acme - Shortcut"), "Acme");
        assert_eq!(clean_name("Acme (2)"), "Acme");
        assert_eq!(clean_name("Acme"), "Acme");
    }

    #[test]
    fn resolves_real_shortcuts_created_by_the_shell() {
        use windows::core::{w, Interface, PCWSTR};
        use windows::Win32::System::Com::{
            CoCreateInstance, CoInitializeEx, CoUninitialize, IPersistFile, CLSCTX_INPROC_SERVER,
            COINIT_MULTITHREADED,
        };
        use windows::Win32::UI::Shell::{IShellLinkW, ShellLink};

        let tree = TempTree::new();
        let target = tree.0.join("Acme.exe");
        fs::write(&target, b"MZ").unwrap();
        let lnk = tree.0.join("Acme App.lnk");

        unsafe {
            let _ = CoInitializeEx(None, COINIT_MULTITHREADED);
            let link: IShellLinkW =
                CoCreateInstance(&ShellLink, None, CLSCTX_INPROC_SERVER).unwrap();
            let wide_target: Vec<u16> = target
                .to_string_lossy()
                .encode_utf16()
                .chain(std::iter::once(0))
                .collect();
            link.SetPath(PCWSTR(wide_target.as_ptr())).unwrap();
            link.SetArguments(w!("--flag")).unwrap();
            let persist: IPersistFile = link.cast().unwrap();
            let wide_lnk: Vec<u16> = lnk
                .to_string_lossy()
                .encode_utf16()
                .chain(std::iter::once(0))
                .collect();
            persist.Save(PCWSTR(wide_lnk.as_ptr()), true).unwrap();
            CoUninitialize();
        }

        let (resolved, args) = resolve_shortcut(&lnk).expect("shortcut should resolve");
        assert_eq!(
            PathBuf::from(resolved),
            target,
            "resolved target should round-trip"
        );
        assert!(args.contains("--flag"));

        // Full catalog scan picks the shortcut up as an app entry.
        let apps = scan_catalog(&[tree.0.clone()]);
        let app = apps
            .iter()
            .find(|a| a.name == "Acme App")
            .expect("shortcut in catalog");
        assert_eq!(app.target, target.to_string_lossy());
    }

    #[test]
    fn app_roots_cover_user_and_machine_menus() {
        let roots = app_roots();
        assert!(roots.len() >= 2, "expected per-user and machine locations");
    }
}
