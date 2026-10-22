//! Windows settings URI catalog: name/keyword matching over the documented
//! `ms-settings:` URI scheme. Pure data + matching, no OS calls.

/// One searchable Windows settings page.
#[derive(Debug, Clone, PartialEq)]
pub struct SettingsHit {
    pub name: String,
    pub uri: String,
    pub score: f64,
}

/// The catalog: (display name, ms-settings URI, extra keywords).
/// Entries follow the documented `ms-settings:` URI scheme.
const CATALOG: &[(&str, &str, &str)] = &[
    (
        "Windows Update",
        "ms-settings:windowsupdate",
        "update upgrade patch",
    ),
    (
        "Windows Update: Advanced options",
        "ms-settings:windowsupdate-options",
        "update",
    ),
    (
        "Windows Update: Update history",
        "ms-settings:windowsupdate-history",
        "update",
    ),
    (
        "Windows Security",
        "ms-settings:windowsdefender",
        "defender antivirus firewall security",
    ),
    (
        "System",
        "ms-settings:about",
        "system about pc info device name",
    ),
    (
        "Display",
        "ms-settings:display",
        "monitor resolution scale brightness hdr",
    ),
    (
        "Sound",
        "ms-settings:sound",
        "volume audio speaker microphone output input",
    ),
    ("Notifications", "ms-settings:notifications", "toast banner"),
    (
        "Focus assist",
        "ms-settings:focusassist",
        "do not disturb quiet hours",
    ),
    (
        "Battery saver",
        "ms-settings:batterysaver",
        "battery power energy",
    ),
    (
        "Power & sleep",
        "ms-settings:powersleep",
        "sleep turn off screen",
    ),
    ("Storage", "ms-settings:storage", "disk space usage sense"),
    ("Storage Sense", "ms-settings:storagesense", "disk cleanup"),
    (
        "Multitasking",
        "ms-settings:multitasking",
        "snap desktops timeline",
    ),
    (
        "Activation",
        "ms-settings:activation",
        "license product key",
    ),
    (
        "Troubleshoot",
        "ms-settings:troubleshoot",
        "fix problems diagnostics",
    ),
    ("Recovery", "ms-settings:recovery", "reset rollback"),
    ("Remote Desktop", "ms-settings:remotedesktop", "rdp"),
    (
        "Clipboard",
        "ms-settings:clipboard",
        "copy paste history sync",
    ),
    ("About / System info", "ms-settings:systeminfo", "system"),
    (
        "Projecting to this PC",
        "ms-settings:project",
        "cast miracast",
    ),
    (
        "Shared experiences",
        "ms-settings:crossdevice",
        "share across devices",
    ),
    (
        "Apps & features",
        "ms-settings:appsfeatures",
        "apps installed uninstall programs",
    ),
    (
        "Default apps",
        "ms-settings:defaultapps",
        "defaults browser handler",
    ),
    (
        "Optional features",
        "ms-settings:optionalfeatures",
        "windows features",
    ),
    ("Startup apps", "ms-settings:startupapps", "startup boot"),
    ("Offline maps", "ms-settings:maps", "map download"),
    (
        "Websites / AutoPlay",
        "ms-settings:autoplay",
        "autoplay media",
    ),
    ("USB", "ms-settings:usb", "usb devices"),
    (
        "Printers & scanners",
        "ms-settings:printers",
        "printer print scanner",
    ),
    (
        "Bluetooth & devices",
        "ms-settings:bluetooth",
        "bluetooth pair device",
    ),
    ("Devices", "ms-settings:devices", "devices"),
    ("Mouse", "ms-settings:mouse", "mouse pointer scroll"),
    (
        "Touchpad",
        "ms-settings:devices-touchpad",
        "touchpad gesture",
    ),
    (
        "Typing",
        "ms-settings:typing",
        "keyboard autocorrect suggestions",
    ),
    ("Pen & Windows Ink", "ms-settings:pen", "pen stylus ink"),
    (
        "Network & internet",
        "ms-settings:network-status",
        "network internet status",
    ),
    ("Wi-Fi", "ms-settings:network-wifi", "wifi wireless hotspot"),
    (
        "Airplane mode",
        "ms-settings:network-airplanemode",
        "airplane radio",
    ),
    ("VPN", "ms-settings:network-vpn", "vpn"),
    ("Proxy", "ms-settings:network-proxy", "proxy"),
    ("Ethernet", "ms-settings:network-ethernet", "ethernet lan"),
    ("Dial-up", "ms-settings:network-dialup", "dial up"),
    (
        "Personalization",
        "ms-settings:personalization",
        "personalize background wallpaper",
    ),
    (
        "Background",
        "ms-settings:personalization-background",
        "wallpaper desktop background",
    ),
    (
        "Colors",
        "ms-settings:personalization-colors",
        "color accent dark light mode",
    ),
    ("Lock screen", "ms-settings:lockscreen", "lock screen"),
    ("Themes", "ms-settings:themes", "theme"),
    ("Fonts", "ms-settings:fonts", "font"),
    (
        "Start",
        "ms-settings:personalization-start",
        "start menu tiles",
    ),
    ("Taskbar", "ms-settings:taskbar", "taskbar"),
    (
        "Accounts",
        "ms-settings:yourinfo",
        "account profile picture",
    ),
    (
        "Email & accounts",
        "ms-settings:emailandaccounts",
        "email accounts",
    ),
    (
        "Sign-in options",
        "ms-settings:signinoptions",
        "password pin hello face fingerprint login",
    ),
    (
        "Family & other users",
        "ms-settings:otherusers",
        "family child account",
    ),
    (
        "Work or school access",
        "ms-settings:workplace",
        "work school mdm",
    ),
    (
        "Date & time",
        "ms-settings:dateandtime",
        "date time clock timezone",
    ),
    (
        "Language",
        "ms-settings:regionlanguage",
        "language keyboard locale",
    ),
    ("Speech", "ms-settings:speech", "speech voice"),
    ("Gaming", "ms-settings:gaming", "game"),
    (
        "Xbox Game Bar",
        "ms-settings:gaming-gamebar",
        "game bar overlay",
    ),
    (
        "Captures / Game DVR",
        "ms-settings:gaming-gamedvr",
        "capture clip recording",
    ),
    (
        "Ease of Access",
        "ms-settings:easeofaccess",
        "accessibility",
    ),
    (
        "Magnifier",
        "ms-settings:easeofaccess-magnifier",
        "magnifier zoom",
    ),
    (
        "Narrator",
        "ms-settings:easeofaccess-narrator",
        "narrator screen reader",
    ),
    (
        "High contrast",
        "ms-settings:easeofaccess-highcontrast",
        "contrast theme",
    ),
    (
        "Keyboard accessibility",
        "ms-settings:easeofaccess-keyboard",
        "sticky keys onscreen",
    ),
    (
        "Mouse accessibility",
        "ms-settings:easeofaccess-mouse",
        "mouse keys",
    ),
    (
        "Color filters",
        "ms-settings:easeofaccess-colorfilter",
        "colorblind filter",
    ),
    (
        "Search permissions",
        "ms-settings:search-permissions",
        "search safe search",
    ),
    ("Privacy", "ms-settings:privacy", "privacy"),
    ("Location", "ms-settings:privacy-location", "location gps"),
    ("Camera", "ms-settings:privacy-camera", "camera webcam"),
    ("Microphone", "ms-settings:privacy-microphone", "microphone"),
    (
        "Voice activation",
        "ms-settings:privacy-voiceactivation",
        "voice activation",
    ),
    (
        "Notifications privacy",
        "ms-settings:privacy-notifications",
        "notifications",
    ),
    (
        "Account info",
        "ms-settings:privacy-accountinfo",
        "account info",
    ),
    ("Contacts", "ms-settings:privacy-contacts", "contacts"),
    ("Calendar", "ms-settings:privacy-calendar", "calendar"),
    ("Call history", "ms-settings:privacy-callhistory", "calls"),
    ("Email privacy", "ms-settings:privacy-email", "email"),
    ("Tasks", "ms-settings:privacy-tasks", "tasks"),
    ("Messaging", "ms-settings:privacy-messaging", "messages sms"),
    ("Radios", "ms-settings:privacy-radios", "radio bluetooth"),
    (
        "Activity history",
        "ms-settings:privacy-activityhistory",
        "timeline activity",
    ),
    (
        "Diagnostics",
        "ms-settings:privacy-feedback",
        "telemetry diagnostics feedback",
    ),
    ("Backup", "ms-settings:backup", "backup onedrive"),
    (
        "Find my device",
        "ms-settings:findmydevice",
        "find lost device",
    ),
    (
        "Windows Insider Program",
        "ms-settings:windowsinsider",
        "insider preview beta",
    ),
];

/// Matches `query` against the catalog. All query tokens must appear (prefix
/// or substring) in the name or keywords; results are ranked by match quality.
pub fn search_settings(query: &str, limit: usize) -> Vec<SettingsHit> {
    let q = query.trim().to_lowercase();
    if q.is_empty() {
        return Vec::new();
    }
    let tokens: Vec<&str> = q
        .split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .collect();
    if tokens.is_empty() {
        return Vec::new();
    }

    let mut hits = Vec::new();
    for (name, uri, keywords) in CATALOG {
        let name_l = name.to_lowercase();
        let mut score = 0.0;
        let mut matched_all = true;
        for token in &tokens {
            let name_hit = if name_l == q {
                1.0
            } else if name_l.starts_with(*token) {
                0.9
            } else if name_l.contains(token) {
                0.75
            } else if keywords.contains(token) {
                0.55
            } else {
                matched_all = false;
                break;
            };
            score += name_hit;
        }
        if !matched_all {
            continue;
        }
        // Whole-query match on the name beats token sums.
        if name_l.contains(&q) {
            score += 1.0;
        }
        let score = (score / tokens.len() as f64).min(1.0);
        hits.push(SettingsHit {
            name: (*name).to_string(),
            uri: (*uri).to_string(),
            score,
        });
    }
    hits.sort_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then_with(|| a.name.cmp(&b.name))
    });
    hits.truncate(limit);
    hits
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_name_wins() {
        let hits = search_settings("windows update", 5);
        assert_eq!(hits[0].name, "Windows Update");
        assert_eq!(hits[0].uri, "ms-settings:windowsupdate");
    }

    #[test]
    fn keyword_matching() {
        let hits = search_settings("bluetooth", 5);
        assert!(hits.iter().any(|h| h.name.starts_with("Bluetooth")));
        let hits = search_settings("wallpaper", 5);
        assert!(hits.iter().any(|h| h.name == "Background"));
    }

    #[test]
    fn all_tokens_must_match() {
        assert!(search_settings("update nonexistenttoken", 5).is_empty());
    }

    #[test]
    fn empty_query_returns_nothing() {
        assert!(search_settings("", 5).is_empty());
        assert!(search_settings("   ", 5).is_empty());
    }

    #[test]
    fn results_are_ranked_and_limited() {
        let hits = search_settings("privacy", 3);
        assert!(hits.len() <= 3);
        assert!(hits[0].score >= hits.last().unwrap().score);
    }
}
