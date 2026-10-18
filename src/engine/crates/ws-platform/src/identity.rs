//! Current-user identity: the user SID, formatted without allocating Win32
//! string APIs (the SID is parsed straight out of the token buffer we own).

use std::sync::OnceLock;

use windows::Win32::Foundation::{CloseHandle, HANDLE};
use windows::Win32::Security::{GetTokenInformation, TokenUser, PSID, TOKEN_QUERY, TOKEN_USER};
use windows::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

#[derive(Debug, thiserror::Error)]
pub enum IdentityError {
    #[error("windows api error: {0}")]
    Win32(#[from] windows::core::Error),
    #[error("token user buffer was malformed")]
    MalformedSid,
    #[error("WINSEARCH_INSTANCE_ID must be 1..48 ASCII letters, digits, underscore or hyphen")]
    InvalidInstanceId,
}

/// Returns the current user's SID as an SDDL string, e.g. `S-1-5-21-...`.
/// Cached: the token identity cannot change for a process lifetime.
pub fn current_user_sid() -> Result<String, IdentityError> {
    static SID: OnceLock<String> = OnceLock::new();
    if let Some(s) = SID.get() {
        return Ok(s.clone());
    }
    let sid = read_token_sid()?;
    let _ = SID.set(sid.clone());
    Ok(sid)
}

/// Unix seconds rounded toward the earlier instant, including pre-1970 metadata.
pub fn unix_seconds(time:std::time::SystemTime)->Option<i64> {
    match time.duration_since(std::time::UNIX_EPOCH) {
        Ok(duration)=>i64::try_from(duration.as_secs()).ok(),
        Err(error)=>{
            let duration=error.duration();
            duration.as_secs().checked_add(u64::from(duration.subsec_nanos()>0))
                .and_then(|value|i64::try_from(value).ok()).and_then(i64::checked_neg)
        }
    }
}

fn read_token_sid() -> Result<String, IdentityError> {
    unsafe {
        let mut token = HANDLE::default();
        OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token)?;

        let mut needed = 0u32;
        let probe = GetTokenInformation(token, TokenUser, None, 0, &mut needed);
        debug_assert!(probe.is_err());
        if needed == 0 {
            let _ = CloseHandle(token);
            return Err(IdentityError::MalformedSid);
        }

        let mut buf = vec![0u8; needed as usize];
        let result = GetTokenInformation(
            token,
            TokenUser,
            Some(buf.as_mut_ptr().cast()),
            needed,
            &mut needed,
        );
        let _ = CloseHandle(token);
        result?;

        let token_user = &*(buf.as_ptr() as *const TOKEN_USER);
        sid_to_string(token_user.User.Sid).ok_or(IdentityError::MalformedSid)
    }
}

/// Formats a SID (`revision`, identifier authority, subauthorities) as
/// `S-<rev>-<authority>-<sub0>-...`.
unsafe fn sid_to_string(sid: PSID) -> Option<String> {
    if sid.0.is_null() {
        return None;
    }
    let s = sid.0 as *const u8;
    let revision = *s;
    let sub_count = *s.add(1) as usize;
    let authority_bytes = std::slice::from_raw_parts(s.add(2), 6);
    let mut authority: u64 = 0;
    for b in authority_bytes {
        authority = (authority << 8) | *b as u64;
    }
    let sub_start = s.add(8) as *const u32;
    let mut out = format!("S-{revision}-{authority}");
    for i in 0..sub_count {
        out.push_str(&format!("-{}", *sub_start.add(i)));
    }
    Some(out)
}

/// The engine's named pipe is suffixed with the user SID so two accounts on one
/// machine never share a pipe.
pub fn pipe_name() -> Result<String, IdentityError> {
    Ok(format!(
        r"\\.\pipe\winsearch-{}{}",
        current_user_sid()?,
        instance_suffix()?
    ))
}

/// The embed worker's private pipe (winsearchd is its only client), also
/// per-user. The `winsearch-embed-` prefix keeps it distinct from the
/// frontend pipe above; both share the same DACL machinery.
pub fn embed_pipe_name() -> Result<String, IdentityError> {
    Ok(format!(
        r"\\.\pipe\winsearch-embed-{}{}",
        current_user_sid()?,
        instance_suffix()?
    ))
}

pub fn validate_instance_id(value: &str) -> Result<(), IdentityError> {
    if value.is_empty()
        || value.len() > 48
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(IdentityError::InvalidInstanceId);
    }
    Ok(())
}
pub fn instance_suffix() -> Result<String, IdentityError> {
    match std::env::var("WINSEARCH_INSTANCE_ID") {
        Ok(value) => {
            validate_instance_id(&value)?;
            Ok(format!("-{value}"))
        }
        Err(std::env::VarError::NotPresent) => Ok(String::new()),
        Err(_) => Err(IdentityError::InvalidInstanceId),
    }
}
pub fn isolated_object_name(base: &str) -> Result<String, IdentityError> {
    let suffix = instance_suffix()?;
    Ok(if suffix.is_empty() {
        base.to_owned()
    } else {
        format!("{base}-{}{suffix}", current_user_sid()?)
    })
}

/// Captures `(volume_serial, file_id)` for `path` via `GetFileInformationByHandleEx`
/// without reading file contents (`FILE_READ_ATTRIBUTES` only).
pub fn file_identity(path: &std::path::Path) -> Option<(u32, u64)> {
    use std::ffi::c_void;
    use std::mem::size_of;
    use windows::core::PCWSTR;
    use windows::Win32::Storage::FileSystem::{
        CreateFileW, FileIdInfo, GetFileInformationByHandleEx, FILE_FLAGS_AND_ATTRIBUTES,
        FILE_ID_INFO, FILE_READ_ATTRIBUTES, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
        OPEN_EXISTING,
    };

    let wide: Vec<u16> = path
        .to_string_lossy()
        .encode_utf16()
        .chain(std::iter::once(0))
        .collect();
    unsafe {
        let handle = CreateFileW(
            PCWSTR(wide.as_ptr()),
            FILE_READ_ATTRIBUTES.0,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            None,
            OPEN_EXISTING,
            FILE_FLAGS_AND_ATTRIBUTES(0),
            None,
        )
        .ok()?;
        let mut info: FILE_ID_INFO = std::mem::zeroed();
        let result = GetFileInformationByHandleEx(
            handle,
            FileIdInfo,
            &mut info as *mut FILE_ID_INFO as *mut c_void,
            size_of::<FILE_ID_INFO>() as u32,
        );
        let _ = CloseHandle(handle);
        result.ok()?;
        let file_id = u64::from_le_bytes(info.FileId.Identifier[..8].try_into().unwrap_or([0; 8]));
        Some((info.VolumeSerialNumber as u32, file_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn file_metadata_seconds_preserve_negative_fractional_instants() {
        use std::time::{Duration,UNIX_EPOCH};
        assert_eq!(unix_seconds(UNIX_EPOCH),Some(0));
        // Windows SystemTime represents 100ns ticks; 1ns arithmetic rounds away.
        let before=UNIX_EPOCH-Duration::new(0,100);
        assert_eq!(before.duration_since(UNIX_EPOCH).unwrap_err().duration(),Duration::from_nanos(100));
        assert_eq!(unix_seconds(UNIX_EPOCH+Duration::new(1,100)),Some(1));
        assert_eq!(unix_seconds(before),Some(-1));
        assert_eq!(unix_seconds(UNIX_EPOCH-Duration::new(1,100)),Some(-2));
    }
    #[test]
    fn instance_id_validation() {
        for value in ["", "a/b", "a b", "å", &"a".repeat(49)] {
            assert!(validate_instance_id(value).is_err());
        }
        for value in ["test-A_1", &"a".repeat(48)] {
            assert!(validate_instance_id(value).is_ok());
        }
    }

    #[test]
    fn reads_a_plausible_current_user_sid() {
        let sid = current_user_sid().unwrap();
        assert!(sid.starts_with("S-1-"), "got {sid}");
        assert!(sid.split('-').count() >= 4);
    }

    #[test]
    fn pipe_name_embeds_sid() {
        let name = pipe_name().unwrap();
        assert!(name.starts_with(r"\\.\pipe\winsearch-S-1-"));
    }

    #[test]
    fn embed_pipe_name_is_distinct_from_engine_pipe() {
        let embed = embed_pipe_name().unwrap();
        let engine = pipe_name().unwrap();
        assert!(embed.starts_with(r"\\.\pipe\winsearch-embed-S-1-"));
        assert_ne!(embed, engine);
    }
}
