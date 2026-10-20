use std::path::Path;
use ws_index::StoreError;
use ws_protocol::{IndexingConfig, IndexingMode};

#[path = "root_rules.rs"]
mod root_rules;
pub use root_rules::{include_hidden, metadata_allowed};

/// Callback contexts are integer registry keys, never dereferenced pointers.
/// Removal before unregister makes late callbacks harmless even if native
/// unregistration fails; weak values do not extend the indexer's lifetime.
pub struct PowerNotifications { handles:Vec<usize>, key:usize, _parameters:Box<windows::Win32::System::Power::DEVICE_NOTIFY_SUBSCRIBE_PARAMETERS> }
unsafe impl Send for PowerNotifications {}
unsafe impl Sync for PowerNotifications {}
fn power_listeners()->&'static std::sync::Mutex<std::collections::HashMap<usize,std::sync::Weak<crate::indexer::Indexer>>> {
    static LISTENERS:std::sync::OnceLock<std::sync::Mutex<std::collections::HashMap<usize,std::sync::Weak<crate::indexer::Indexer>>>>=std::sync::OnceLock::new();
    LISTENERS.get_or_init(Default::default)
}
unsafe extern "system" fn power_changed(context:*const core::ffi::c_void,_kind:u32,_setting:*const core::ffi::c_void)->u32 {
    let indexer=power_listeners().lock().unwrap_or_else(|p|p.into_inner()).get(&(context as usize)).and_then(|w|w.upgrade());
    if let Some(indexer)=indexer {indexer.notify_work();}
    0
}
impl PowerNotifications {
    pub fn register(indexer:&std::sync::Arc<crate::indexer::Indexer>)->std::io::Result<Self> {
        use windows::{core::GUID,Win32::{Foundation::HANDLE,System::Power::{DEVICE_NOTIFY_SUBSCRIBE_PARAMETERS,PowerSettingRegisterNotification},UI::WindowsAndMessaging::DEVICE_NOTIFY_CALLBACK}};
        static NEXT:std::sync::atomic::AtomicUsize=std::sync::atomic::AtomicUsize::new(1);
        let key=NEXT.fetch_add(1,std::sync::atomic::Ordering::Relaxed);
        power_listeners().lock().unwrap_or_else(|p|p.into_inner()).insert(key,std::sync::Arc::downgrade(indexer));
        let parameters=Box::new(DEVICE_NOTIFY_SUBSCRIBE_PARAMETERS {Callback:Some(power_changed),Context:key as *mut _});
        let mut owned=Self {handles:Vec::new(),key,_parameters:parameters};
        for guid in [GUID::from_u128(0x5d3e9a59_e9d5_4b00_a6bd_ff34ff516548),GUID::from_u128(0xe00958c0_c213_4ace_ac77_fecced2eeea5)] {
            let mut registration=std::ptr::null_mut();
            let error=unsafe {PowerSettingRegisterNotification(&guid,DEVICE_NOTIFY_CALLBACK,HANDLE((&*owned._parameters as *const DEVICE_NOTIFY_SUBSCRIBE_PARAMETERS).cast_mut().cast()),&mut registration)};
            if error.0!=0 {return Err(std::io::Error::from_raw_os_error(error.0 as i32));}
            owned.handles.push(registration as usize);
        }
        Ok(owned)
    }
}
impl Drop for PowerNotifications {
    fn drop(&mut self) {
        power_listeners().lock().unwrap_or_else(|p|p.into_inner()).remove(&self.key);
        for handle in self.handles.drain(..) {let error=unsafe {windows::Win32::System::Power::PowerSettingUnregisterNotification(windows::Win32::System::Power::HPOWERNOTIFY(handle as isize))};if error.0!=0 {tracing::warn!(code=error.0,"power notification unregister failed");}}
    }
}

/// Deadline of the next daily scheduled opening, not a periodic poll.
pub fn schedule_wait(c:&IndexingConfig)->Option<std::time::Duration> {
    if c.paused || c.mode!=IndexingMode::Scheduled {return None;}
    let t=unsafe {windows::Win32::System::SystemInformation::GetLocalTime()};
    let minute=t.wHour*60+t.wMinute;
    if hours_contains(c.hours_start,c.hours_end,minute) {return None;}
    let delta=(i32::from(c.hours_start)-i32::from(minute)).rem_euclid(1440) as u64;
    Some(std::time::Duration::from_millis(delta*60_000-u64::from(t.wSecond)*1000-u64::from(t.wMilliseconds)))
}

pub fn hours_contains(start: u16, end: u16, minute: u16) -> bool {
    start == end
        || if start < end {
            minute >= start && minute < end
        } else {
            minute >= start || minute < end
        }
}
pub fn validate(config: &IndexingConfig) -> Result<IndexingConfig, StoreError> {
    let bad = |s: &str| StoreError::InvalidConfig(s.into());
    if config.roots.len() > 128
        || config.code_roots.len() > 128
        || config.excluded_paths.len() > 512
        || config.excluded_extensions.len() > 256
    {
        return Err(bad("indexing list limit exceeded"));
    }
    if config.hours_start > 1439 || config.hours_end > 1439 {
        return Err(bad("hours must be 0..1439"));
    }
    for p in config
        .roots
        .iter()
        .chain(&config.code_roots)
        .chain(&config.excluded_paths)
    {
        if p.contains('\0')
            || !Path::new(p).is_absolute()
            || Path::new(p)
                .components()
                .any(|c| matches!(c, std::path::Component::ParentDir))
        {
            return Err(bad(
                "indexing paths must be absolute without NUL or parent traversal",
            ));
        }
    }
    let mut c = config.clone();
    for ext in &mut c.excluded_extensions {
        *ext = ext.strip_prefix('.').unwrap_or(ext).to_ascii_lowercase();
        if ext.is_empty() || !ext.bytes().all(|b| b.is_ascii_alphanumeric()) {
            return Err(bad("invalid extension rule"));
        }
    }
    root_rules::validate(&mut c)?;
    Ok(c)
}
fn normalized(p: &str) -> String {
    p.replace('/', "\\").trim_end_matches('\\').to_lowercase()
}
pub fn contains(root: &str, path: &str) -> bool {
    let r = normalized(root);
    let p = normalized(path);
    p == r || p.strip_prefix(&r).is_some_and(|s| s.starts_with('\\'))
}
pub fn path_allowed(c: &IndexingConfig, path: &str) -> bool {
    path_allowed_with_visibility(c, path, true)
}
fn path_allowed_with_visibility(c: &IndexingConfig, path: &str, filter_hidden: bool) -> bool {
    let root = c
        .roots
        .iter()
        .filter(|r| contains(r, path))
        .max_by_key(|r| normalized(r).len());
    let Some(root) = root else {
        return false;
    };
    if !root_rules::relative_allowed(c, root, path, filter_hidden) { return false; }
    if c.excluded_paths
        .iter()
        .any(|e| contains(e, path) && normalized(e).len() >= normalized(root).len())
    {
        return false;
    }
    let ext = Path::new(path)
        .extension()
        .unwrap_or_default()
        .to_string_lossy()
        .to_ascii_lowercase();
    !c.excluded_extensions.contains(&ext)
}

pub fn code_path_allowed(c: &IndexingConfig, path: &str) -> bool {
    if !c.code_enabled {return false;}
    let mut code=c.clone();
    code.roots=c.code_roots.clone();
    path_allowed(&code,path)
}
pub fn code_control_path_allowed(c: &IndexingConfig, path: &str) -> bool {
    if !c.code_enabled {return false;}
    let mut code=c.clone();
    code.roots=c.code_roots.clone();
    path_allowed_with_visibility(&code,path,false)
}
pub fn pause_reason(
    c: &IndexingConfig,
    battery: Option<bool>,
    saver: Option<bool>,
    minute: u16,
) -> Option<&'static str> {
    if c.paused {
        Some("Paused by user")
    } else if c.respect_battery_saver && saver == Some(true) {
        Some("Battery saver")
    } else if !c.index_on_battery && battery == Some(true) {
        Some("On battery")
    } else if !c.index_on_battery && battery.is_none() {
        Some("Power state unavailable")
    } else if c.mode == IndexingMode::Scheduled
        && !hours_contains(c.hours_start, c.hours_end, minute)
    {
        Some("Outside scheduled hours")
    } else {
        None
    }
}
pub fn native_pause_reason(c: &IndexingConfig) -> Option<String> {
    use windows::Win32::System::{
        Power::{GetSystemPowerStatus, SYSTEM_POWER_STATUS},
        SystemInformation::GetLocalTime,
    };
    let mut p = SYSTEM_POWER_STATUS::default();
    let known = unsafe { GetSystemPowerStatus(&mut p) }.is_ok();
    let battery = if known {
        match p.ACLineStatus {
            0 => Some(true),
            1 => Some(false),
            _ => None,
        }
    } else {
        None
    };
    let saver = known.then_some(p.SystemStatusFlag == 1);
    let time = unsafe { GetLocalTime() };
    pause_reason(c, battery, saver, time.wHour * 60 + time.wMinute).map(str::to_owned)
}
pub use ws_platform::content::metadata_path_allowed as metadata_safe;

fn machine_name(name: &str) -> bool {
    let stem = Path::new(name)
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy()
        .to_ascii_lowercase();
    if [
        "img",
        "dsc",
        "pxl",
        "mvimg",
        "vid",
        "gopr",
        "dji",
        "photo",
        "whatsapp",
        "screenshot",
    ]
    .iter()
    .any(|p| stem.starts_with(p))
    {
        return false;
    }
    if stem.len() >= 16 && stem.bytes().all(|b| b.is_ascii_hexdigit()) {
        return true;
    }
    let prefix = stem.trim_end_matches(|c: char| c.is_ascii_digit());
    stem.len() - prefix.len() >= 3
        && !prefix.is_empty()
        && prefix.len() <= 12
        && !prefix.trim_end_matches(['_', '-']).is_empty()
        && prefix
            .trim_end_matches(['_', '-'])
            .bytes()
            .all(|b| b.is_ascii_alphabetic())
}
pub fn dataset_names(names: &[String]) -> bool {
    let mut counts = [(0usize, 0usize); 3];
    for name in names {
        let ext = Path::new(name)
            .extension()
            .unwrap_or_default()
            .to_string_lossy()
            .to_ascii_lowercase();
        let kind = match ext.as_str() {
            "jpg" | "jpeg" | "png" | "webp" | "bmp" | "gif" | "heic" => 0,
            "mp4" | "mov" | "mkv" | "avi" => 1,
            "wav" | "mp3" | "flac" | "m4a" => 2,
            _ => continue,
        };
        counts[kind].0 += 1;
        counts[kind].1 += usize::from(machine_name(name));
    }
    counts
        .iter()
        .any(|(all, machine)| *all >= 300 && machine * 10 >= all * 7)
}
pub fn ordinary_content_reason(c: &IndexingConfig, path: &Path) -> Option<&'static str> {
    let name = path.file_name().unwrap_or_default().to_string_lossy();
    if crate::crawl::is_excluded(&name, false)
        || ws_code::EXCLUDED_CODE_FILES.contains(&name.to_ascii_lowercase().as_str())
        || ws_code::EXCLUDED_CODE_SUFFIXES
            .iter()
            .any(|s| name.to_ascii_lowercase().ends_with(&s.to_ascii_lowercase()))
    {
        return Some("Sensitive or generated file");
    }
    for dir in path.parent().into_iter().flat_map(Path::ancestors) {
        if [
            ".git",
            "package.json",
            "Cargo.toml",
            "pyproject.toml",
            "go.mod",
        ]
        .iter()
        .any(|m| dir.join(m).exists())
            || std::fs::read_dir(dir).is_ok_and(|entries| {
                entries.take(4096).filter_map(Result::ok).any(|e| {
                    matches!(
                        e.path()
                            .extension()
                            .and_then(|e| e.to_str())
                            .map(str::to_ascii_lowercase)
                            .as_deref(),
                        Some("sln" | "slnx" | "csproj" | "fsproj" | "vbproj")
                    )
                })
            })
        {
            let ext = path
                .extension()
                .unwrap_or_default()
                .to_string_lossy()
                .to_ascii_lowercase();
            if !name.to_ascii_lowercase().starts_with("screenshot")
                || !matches!(
                    ext.as_str(),
                    "png" | "jpg" | "jpeg" | "webp" | "bmp" | "gif" | "heic"
                )
            {
                return Some("Project content is reserved for code search");
            }
        }
    }
    // Explicit selections bypass dataset classification, never project ancestry.
    for dir in path.parent().into_iter().flat_map(Path::ancestors) {
        if !c.roots.iter().any(|r| contains(r, &dir.to_string_lossy())) {
            break;
        }
        if !c
            .roots
            .iter()
            .any(|r| normalized(r) == normalized(&dir.to_string_lossy()))
        {
            if let Ok(entries) = std::fs::read_dir(dir) {
                let names: Vec<_> = entries
                    .take(4096)
                    .filter_map(Result::ok)
                    .filter(|e| e.file_type().is_ok_and(|t| t.is_file()))
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .collect();
                if dataset_names(&names) {
                    return Some("Machine-named media dataset");
                }
            }
        }
    }
    None
}
pub fn ordinary_content_allowed(c: &IndexingConfig, path: &Path) -> bool {
    ordinary_content_reason(c, path).is_none()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ws_protocol::{IndexingConfig, IndexingMode};
    #[test]
    fn schedule_wraps_and_equal_means_all_day() {
        assert!(hours_contains(1320, 360, 1380));
        assert!(hours_contains(1320, 360, 300));
        assert!(!hours_contains(1320, 360, 720));
        assert!(hours_contains(60, 60, 720));
    }
    #[test]
    fn consent_boundaries_and_reinclusion() {
        let mut c = IndexingConfig::default();
        c.roots = vec![r"C:\Data".into(), r"C:\Data\Skip\Chosen".into()];
        c.excluded_paths = vec![r"C:\Data\Skip".into()];
        assert!(path_allowed(&c, r"c:\DATA\notes.txt"));
        assert!(!path_allowed(&c, r"C:\Database\notes.txt"));
        assert!(!path_allowed(&c, r"C:\Data\Skip\notes.txt"));
        assert!(path_allowed(&c, r"C:\Data\Skip\Chosen\notes.txt"));
        c.excluded_extensions = vec!["txt".into()];
        assert!(!path_allowed(&c, r"C:\Data\notes.TXT"));
    }
    #[test]
    fn validates_and_normalizes_without_dropping_offline_roots() {
        let mut c = IndexingConfig::default();
        c.roots = vec![r"Z:\offline".into()];
        c.excluded_extensions = vec![".PDF".into()];
        assert_eq!(validate(&c).unwrap().excluded_extensions, vec!["pdf"]);
        c.roots = vec!["relative".into()];
        assert!(validate(&c).is_err());
        c.roots = vec!["C:\\bad\0path".into()];
        assert!(validate(&c).is_err());
        c.roots = vec![r"C:\data".into(); 129];
        assert!(validate(&c).is_err());
    }
    #[test]
    fn pause_precedence_and_unknown_power_is_conservative() {
        let mut c = IndexingConfig::default();
        assert_eq!(pause_reason(&c, None, None, 720), None);
        c.index_on_battery = false;
        assert_eq!(
            pause_reason(&c, None, None, 720),
            Some("Power state unavailable")
        );
        c.paused = true;
        assert_eq!(
            pause_reason(&c, Some(true), Some(true), 720),
            Some("Paused by user")
        );
        c.paused = false;
        assert_eq!(
            pause_reason(&c, Some(true), Some(true), 720),
            Some("Battery saver")
        );
        c.mode = IndexingMode::Scheduled;
        assert_eq!(
            pause_reason(&c, Some(false), Some(false), 720),
            Some("Outside scheduled hours")
        );
    }
    #[test]
    fn cloud_attributes_never_allow_content() {
        for flags in [0x1000, 0x40000, 0x400000, 0x400] {
            assert!(!ws_platform::content::content_attributes_allowed(flags));
        }
        assert!(ws_platform::content::content_attributes_allowed(0x20));
    }
    #[test]
    fn dataset_boundaries_and_camera_roll() {
        let names = |machine: usize, count: usize| {
            (0..count)
                .map(|i| {
                    if i < machine {
                        format!("clip_{i:06}.jpg")
                    } else {
                        format!("IMG_{i:06}.jpg")
                    }
                })
                .collect::<Vec<_>>()
        };
        assert!(!dataset_names(&names(299, 299)));
        assert!(!dataset_names(&names(209, 300)));
        assert!(dataset_names(&names(210, 300)));
        assert!(!dataset_names(&names(0, 500)));
        assert!(!dataset_names(
            &(0..500)
                .map(|i| format!("report_{i:06}.txt"))
                .collect::<Vec<_>>()
        ));
    }
    #[test]
    fn dataset_explicit_root_override_and_project_screenshots() {
        let base =
            std::env::temp_dir().join(format!("winsearch-policy-dataset-{}", std::process::id()));
        let media = base.join("media");
        std::fs::create_dir_all(&media).unwrap();
        for i in 0..300 {
            std::fs::write(media.join(format!("clip_{i:06}.jpg")), []).unwrap();
        }
        let file = media.join("clip_000000.jpg");
        let mut c = IndexingConfig::default();
        c.roots = vec![base.to_string_lossy().into_owned()];
        assert_eq!(
            ordinary_content_reason(&c, &file),
            Some("Machine-named media dataset")
        );
        c.roots.push(media.to_string_lossy().into_owned());
        assert_eq!(ordinary_content_reason(&c, &file), None);
        std::fs::create_dir_all(media.join(".git")).unwrap();
        assert_eq!(
            ordinary_content_reason(&c, &file),
            Some("Project content is reserved for code search")
        );
        assert_eq!(
            ordinary_content_reason(&c, &media.join("Screenshot 2026.png")),
            None
        );
        assert_eq!(
            ordinary_content_reason(&c, &media.join("Screenshot.txt")),
            Some("Project content is reserved for code search")
        );
        std::fs::remove_dir_all(&base).unwrap();
    }
    #[test]
    fn selected_project_subfolder_keeps_documents_name_only() {
        let base =
            std::env::temp_dir().join(format!("winsearch-project-subroot-{}", std::process::id()));
        let docs = base.join("repo").join("nested").join("docs");
        std::fs::create_dir_all(&docs).unwrap();
        let document = docs.join("design.txt");
        let screenshot = docs.join("Screenshot 2026.png");
        std::fs::write(&document, b"private repository design").unwrap();
        std::fs::write(&screenshot, []).unwrap();
        let mut config = IndexingConfig::default();
        config.roots = vec![docs.to_string_lossy().into_owned()];
        for marker in [".git", "package.json"] {
            let marker = base.join("repo").join(marker);
            std::fs::write(&marker, []).unwrap();
            assert_eq!(
                ordinary_content_reason(&config, &document),
                Some("Project content is reserved for code search")
            );
            assert_eq!(ordinary_content_reason(&config, &screenshot), None);
            std::fs::remove_file(marker).unwrap();
        }
        std::fs::remove_dir_all(base).unwrap();
    }
}
