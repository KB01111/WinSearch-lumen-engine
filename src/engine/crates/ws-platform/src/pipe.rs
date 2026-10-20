//! Named-pipe server/client with a per-user security descriptor.
//!
//! The pipe is created with an explicit DACL granting access to the current
//! user only and with `PIPE_REJECT_REMOTE_CLIENTS`, so nothing else on the
//! machine — and nothing over the network — can talk to the engine.

use std::io::{self, Read, Write};
use std::time::{Duration, Instant};

use tracing::{error, info, warn};
use windows::core::PCWSTR;
use windows::Win32::Foundation::{
    CloseHandle, ERROR_BROKEN_PIPE, ERROR_IO_PENDING, ERROR_PIPE_CONNECTED, GENERIC_ALL,
    GENERIC_READ, GENERIC_WRITE, HANDLE, WAIT_OBJECT_0, WAIT_TIMEOUT,
};
use windows::Win32::Security::{
    AddAccessAllowedAce, GetLengthSid, GetTokenInformation, InitializeAcl,
    InitializeSecurityDescriptor, SetSecurityDescriptorDacl, TokenUser as TokenUserClass,
    ACCESS_ALLOWED_ACE, ACL, ACL_REVISION, PSECURITY_DESCRIPTOR, PSID, SECURITY_ATTRIBUTES,
    SECURITY_DESCRIPTOR, TOKEN_QUERY, TOKEN_USER,
};
use windows::Win32::Storage::FileSystem::{
    CreateFileW, ReadFile, WriteFile, FILE_ATTRIBUTE_NORMAL, FILE_FLAG_OVERLAPPED, FILE_SHARE_NONE,
    OPEN_EXISTING, PIPE_ACCESS_DUPLEX,
};
use windows::Win32::System::Pipes::{
    ConnectNamedPipe, CreateNamedPipeW, PeekNamedPipe, PIPE_READMODE_BYTE,
    PIPE_REJECT_REMOTE_CLIENTS, PIPE_TYPE_BYTE, PIPE_UNLIMITED_INSTANCES, PIPE_WAIT,
};
use windows::Win32::System::Threading::{
    CreateEventW, GetCurrentProcess, OpenProcessToken, WaitForSingleObject, INFINITE,
};
use windows::Win32::System::IO::{CancelIoEx, GetOverlappedResult, OVERLAPPED};

/// `SECURITY_DESCRIPTOR_REVISION` from winnt.h.
const SECURITY_DESCRIPTOR_REVISION: u32 = 1;

const OUT_BUFFER_BYTES: u32 = 64 * 1024;
const IN_BUFFER_BYTES: u32 = 64 * 1024;
const FRAME_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// One connected pipe endpoint. Implements blocking `Read`/`Write`; the frame
/// codecs operate on top of it. Server endpoints bound a frame's writes to five
/// seconds, from its first byte through `flush`, and close on transport failure.
pub struct PipeStream {
    handle: Option<PipeHandle>,
    overlapped: bool,
    write_deadline: Option<Instant>,
}

/// HANDLE wrapper: created by us, moved across a thread boundary exactly once,
/// closed exactly once on drop (which also disconnects the pipe instance).
struct PipeHandle(HANDLE);
unsafe impl Send for PipeHandle {}

impl Drop for PipeHandle {
    fn drop(&mut self) {
        unsafe {
            let _ = CloseHandle(self.0);
        }
    }
}

impl PipeStream {
    fn new(handle: PipeHandle, overlapped: bool) -> Self {
        Self {
            handle: Some(handle),
            overlapped,
            write_deadline: None,
        }
    }

    /// Client side: connects to an existing engine pipe.
    pub fn connect(name: &str) -> io::Result<PipeStream> {
        let wide: Vec<u16> = name.encode_utf16().chain(std::iter::once(0)).collect();
        unsafe {
            let handle = CreateFileW(
                PCWSTR(wide.as_ptr()),
                (GENERIC_READ | GENERIC_WRITE).0,
                FILE_SHARE_NONE,
                None,
                OPEN_EXISTING,
                FILE_ATTRIBUTE_NORMAL,
                None,
            )
            .map_err(win32_io_error)?;
            Ok(Self::new(PipeHandle(handle), false))
        }
    }

    /// Non-blocking check for bytes waiting in the pipe's inbound buffer.
    /// Returns `Ok(Some(n))` when `n` bytes are queued (possibly 0), or
    /// `Ok(None)` when the peer has disconnected (`ERROR_BROKEN_PIPE`).
    pub fn peek_available(&self) -> io::Result<Option<usize>> {
        let Some(handle) = &self.handle else {
            return Ok(None);
        };
        let mut avail = 0u32;
        unsafe {
            match PeekNamedPipe(handle.0, None, 0, None, Some(&mut avail), None) {
                Ok(()) => Ok(Some(avail as usize)),
                Err(e) if is_disconnect(&e) => Ok(None),
                Err(e) => Err(win32_io_error(e)),
            }
        }
    }
}

impl Read for PipeStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let Some(handle) = &self.handle else {
            return Ok(0);
        };
        let result = if self.overlapped {
            let mut operation = PipeOperation::new()?;
            let started =
                unsafe { ReadFile(handle.0, Some(buf), None, Some(&mut operation.overlapped)) };
            operation.complete(handle.0, started, None)
        } else {
            let mut read = 0;
            unsafe { ReadFile(handle.0, Some(buf), Some(&mut read), None) }
                .map(|()| read)
                .map_err(win32_io_error)
        };
        match result {
            Err(error) if error.raw_os_error() == Some(ERROR_BROKEN_PIPE.to_hresult().0) => Ok(0),
            result => result.map(|read| read as usize),
        }
    }
}

/// Owns the event until completion, including cancellation. The OVERLAPPED and
/// the caller's buffer must stay alive until GetOverlappedResult finishes.
struct PipeOperation {
    _event: PipeHandle,
    overlapped: OVERLAPPED,
}

impl PipeOperation {
    fn new() -> io::Result<Self> {
        let event =
            PipeHandle(unsafe { CreateEventW(None, true, false, None) }.map_err(win32_io_error)?);
        Ok(Self {
            overlapped: OVERLAPPED {
                hEvent: event.0,
                ..Default::default()
            },
            _event: event,
        })
    }

    fn complete(
        &mut self,
        handle: HANDLE,
        started: windows::core::Result<()>,
        deadline: Option<Instant>,
    ) -> io::Result<u32> {
        if let Err(error) = started {
            if error.code() != ERROR_IO_PENDING.to_hresult() {
                return Err(win32_io_error(error));
            }
            let milliseconds = deadline.map_or(INFINITE, |deadline| {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    0
                } else {
                    remaining.as_millis().clamp(1, u128::from(INFINITE - 1)) as u32
                }
            });
            let wait = unsafe { WaitForSingleObject(self.overlapped.hEvent, milliseconds) };
            if wait != WAIT_OBJECT_0 {
                let error = if wait == WAIT_TIMEOUT {
                    io::Error::new(
                        io::ErrorKind::TimedOut,
                        "pipe frame write deadline exceeded",
                    )
                } else {
                    io::Error::last_os_error()
                };
                unsafe {
                    let _ = CancelIoEx(handle, Some(&self.overlapped));
                    let mut transferred = 0;
                    // Cancellation is a request, not completion; drain before releasing borrowed memory.
                    let _ = GetOverlappedResult(handle, &self.overlapped, &mut transferred, true);
                }
                return Err(error);
            }
        }
        let mut transferred = 0;
        unsafe { GetOverlappedResult(handle, &self.overlapped, &mut transferred, false) }
            .map_err(win32_io_error)?;
        Ok(transferred)
    }
}

impl Write for PipeStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let handle = self
            .handle
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "pipe closed"))?
            .0;
        let result = if self.overlapped {
            let deadline = *self
                .write_deadline
                .get_or_insert_with(|| Instant::now() + FRAME_WRITE_TIMEOUT);
            if Instant::now() >= deadline {
                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "pipe frame write deadline exceeded",
                ))
            } else {
                PipeOperation::new().and_then(|mut operation| {
                    let started = unsafe {
                        WriteFile(handle, Some(buf), None, Some(&mut operation.overlapped))
                    };
                    operation.complete(handle, started, Some(deadline))
                })
            }
        } else {
            let mut written = 0;
            unsafe { WriteFile(handle, Some(buf), Some(&mut written), None) }
                .map(|()| written)
                .map_err(win32_io_error)
        };
        if result.is_err() {
            self.handle.take();
        }
        result.map(|written| written as usize)
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.handle.is_none() {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "pipe closed"));
        }
        if self
            .write_deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            self.handle.take();
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "pipe frame write deadline exceeded",
            ));
        }
        self.write_deadline = None;
        Ok(())
    }
}

fn is_disconnect(e: &windows::core::Error) -> bool {
    e.code() == ERROR_BROKEN_PIPE.to_hresult()
}

fn win32_io_error(e: windows::core::Error) -> io::Error {
    io::Error::from_raw_os_error(e.code().0)
}

/// Runs the accept loop on the calling thread. For each accepted connection a
/// thread is spawned that runs `on_connection` and then disconnects. The loop
/// only ends if a pipe instance cannot be created.
pub fn serve<F>(name: &str, on_connection: F) -> io::Result<()>
where
    F: Fn(PipeStream) + Send + Sync + 'static,
{
    let security = UserOnlySecurity::new().map_err(|e| io::Error::other(e.to_string()))?;
    let wide_name: Vec<u16> = name.encode_utf16().chain(std::iter::once(0)).collect();
    let on_connection = std::sync::Arc::new(on_connection);

    info!(pipe = name, "pipe accept loop starting");
    loop {
        let handle = match unsafe { create_instance(&wide_name, &security) } {
            Ok(h) => h,
            Err(e) => {
                error!(error = %e, "failed to create named pipe instance");
                return Err(win32_io_error(e));
            }
        };

        let handle = PipeHandle(handle);
        if let Err(error) = accept_connection(&handle) {
            warn!(error = %error, "ConnectNamedPipe failed");
            continue;
        }

        let stream = PipeStream::new(handle, true);
        let handler = on_connection.clone();
        std::thread::spawn(move || handler(stream));
    }
}

fn accept_connection(handle: &PipeHandle) -> io::Result<()> {
    let mut operation = PipeOperation::new()?;
    let started = unsafe { ConnectNamedPipe(handle.0, Some(&mut operation.overlapped)) };
    // CreateFile can connect before the accept call; no operation remains pending in that case.
    if started
        .as_ref()
        .is_err_and(|error| error.code() == ERROR_PIPE_CONNECTED.to_hresult())
    {
        return Ok(());
    }
    operation.complete(handle.0, started, None).map(|_| ())
}

unsafe fn create_instance(
    wide_name: &[u16],
    security: &UserOnlySecurity,
) -> Result<HANDLE, windows::core::Error> {
    let attrs = security.attributes();
    let pipe_mode = PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT | PIPE_REJECT_REMOTE_CLIENTS;
    let handle = CreateNamedPipeW(
        PCWSTR(wide_name.as_ptr()),
        PIPE_ACCESS_DUPLEX | FILE_FLAG_OVERLAPPED,
        pipe_mode,
        PIPE_UNLIMITED_INSTANCES,
        OUT_BUFFER_BYTES,
        IN_BUFFER_BYTES,
        0,
        Some(&attrs as *const SECURITY_ATTRIBUTES),
    );
    if handle.is_invalid() {
        Err(windows::core::Error::from_win32())
    } else {
        Ok(handle)
    }
}

/// A DACL granting exactly one ACE: generic-all for the current user's SID.
struct UserOnlySecurity {
    /// Backing storage for TOKEN_USER (the SID points into it).
    _token_user_buf: Vec<u8>,
    /// Backing storage for the ACL (the SD's DACL points into it).
    _acl_buf: Vec<u8>,
    /// Backing storage for the SECURITY_DESCRIPTOR (attrs point into it).
    _sd: Box<SECURITY_DESCRIPTOR>,
}

impl UserOnlySecurity {
    fn new() -> Result<Self, windows::core::Error> {
        unsafe {
            let mut token = HANDLE::default();
            OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token)?;

            let mut needed = 0u32;
            let _ = GetTokenInformation(token, TokenUserClass, None, 0, &mut needed);
            let mut token_user_buf = vec![0u8; needed as usize];
            let filled = GetTokenInformation(
                token,
                TokenUserClass,
                Some(token_user_buf.as_mut_ptr().cast()),
                needed,
                &mut needed,
            );
            let _ = CloseHandle(token);
            filled?;

            let token_user = &*(token_user_buf.as_ptr() as *const TOKEN_USER);
            let sid: PSID = token_user.User.Sid;
            let sid_len = GetLengthSid(sid) as usize;

            // ACL header + one ACCESS_ALLOWED_ACE (ACE header + mask + SID,
            // minus the ACE's SidStart u32 that overlaps the SID bytes).
            let acl_len =
                std::mem::size_of::<ACL>() + std::mem::size_of::<ACCESS_ALLOWED_ACE>() + sid_len
                    - std::mem::size_of::<u32>();
            let mut acl_buf = vec![0u8; acl_len];
            let acl = acl_buf.as_mut_ptr() as *mut ACL;
            InitializeAcl(acl, acl_len as u32, ACL_REVISION)?;
            AddAccessAllowedAce(acl, ACL_REVISION, GENERIC_ALL.0, sid)?;

            let mut sd = Box::new(std::mem::zeroed::<SECURITY_DESCRIPTOR>());
            let sd_ptr = PSECURITY_DESCRIPTOR((&mut *sd as *mut SECURITY_DESCRIPTOR).cast());
            InitializeSecurityDescriptor(sd_ptr, SECURITY_DESCRIPTOR_REVISION)?;
            SetSecurityDescriptorDacl(sd_ptr, true, Some(acl as *const ACL), false)?;

            Ok(Self {
                _token_user_buf: token_user_buf,
                _acl_buf: acl_buf,
                _sd: sd,
            })
        }
    }

    fn attributes(&self) -> SECURITY_ATTRIBUTES {
        SECURITY_ATTRIBUTES {
            nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: &*self._sd as *const SECURITY_DESCRIPTOR as *mut _,
            bInheritHandle: false.into(),
        }
    }
}

#[cfg(test)]
#[path = "pipe_tests.rs"]
mod writer_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity;
    use ws_protocol::{read_message, write_message, ClientMessage, ServerMessage};

    #[test]
    fn client_server_round_trip_over_real_pipe() {
        let name = format!(r"\\.\pipe\winsearch-test-{}", std::process::id());
        let server_name = name.clone();

        std::thread::spawn(move || {
            serve(&server_name, |mut stream| {
                let msg: ClientMessage = read_message(&mut stream).unwrap().unwrap();
                if let ClientMessage::Ping { id } = msg {
                    write_message(&mut stream, &ServerMessage::Pong { id }).unwrap();
                }
            })
            .ok();
        });

        // Retry briefly while the server thread creates the pipe instance.
        let mut client = None;
        for _ in 0..100 {
            match PipeStream::connect(&name) {
                Ok(c) => {
                    client = Some(c);
                    break;
                }
                Err(_) => std::thread::sleep(std::time::Duration::from_millis(20)),
            }
        }
        let mut client = client.expect("client failed to connect");

        write_message(&mut client, &ClientMessage::Ping { id: 123 }).unwrap();
        let reply: ServerMessage = read_message(&mut client).unwrap().unwrap();
        assert_eq!(reply, ServerMessage::Pong { id: 123 });
    }

    #[test]
    fn sid_lookup_works() {
        let sid = identity::current_user_sid().unwrap();
        assert!(sid.starts_with("S-1-"));
    }
}
