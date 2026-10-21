//! Owns exactly one child and its descendants; closing the job kills the tree.
use std::io;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use std::os::windows::process::CommandExt;
use std::process::{Child, Command};
use std::time::{Duration, Instant};
use windows::core::PCWSTR;
use windows::Win32::Foundation::{HANDLE, WAIT_OBJECT_0};
use windows::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JobObjectBasicAccountingInformation,
    JobObjectExtendedLimitInformation, QueryInformationJobObject, SetInformationJobObject,
    TerminateJobObject, JOBOBJECT_BASIC_ACCOUNTING_INFORMATION,
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JOB_OBJECT_LIMIT_JOB_MEMORY,
    JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
};
use windows::Win32::System::Threading::{
    GetCurrentProcess, TerminateProcess, WaitForSingleObject, CREATE_NO_WINDOW,
};

/// Retires only this process, bypassing DLL detach/exit handlers which can
/// deadlock after a driver failure. Use only in disposable native workers.
pub fn terminate_current_process(code: u32) -> ! {
    // SAFETY: the pseudo-handle names only this process; the caller intends
    // unconditional termination, with no Rust or native cleanup callbacks.
    unsafe {
        let _ = TerminateProcess(GetCurrentProcess(), code);
    }
    // Successful self-termination never returns. Keep a failed API call from
    // resuming a process whose native state may already be compromised.
    std::process::abort()
}

pub struct ChildJob {
    child: Child,
    job: OwnedHandle,
    reaped: bool,
}

impl ChildJob {
    /// The caller must not send input until this returns: assignment precedes parsing.
    pub fn spawn(command: &mut Command) -> io::Result<Self> {
        unsafe {
            let raw = CreateJobObjectW(None, PCWSTR::null()).map_err(io::Error::other)?;
            let job = OwnedHandle::from_raw_handle(raw.0);
            let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
            limits.BasicLimitInformation.LimitFlags =
                JOB_OBJECT_LIMIT_JOB_MEMORY | JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            limits.JobMemoryLimit = 1024 * 1024 * 1024;
            SetInformationJobObject(
                raw,
                JobObjectExtendedLimitInformation,
                &limits as *const _ as _,
                std::mem::size_of_val(&limits) as u32,
            )
            .map_err(io::Error::other)?;
            let mut child = command.creation_flags(CREATE_NO_WINDOW.0).spawn()?;
            if let Err(error) = AssignProcessToJobObject(raw, HANDLE(child.as_raw_handle())) {
                let _ = child.kill();
                if WaitForSingleObject(HANDLE(child.as_raw_handle()), 1000) == WAIT_OBJECT_0 {
                    let _ = child.wait();
                }
                return Err(io::Error::other(error));
            }
            Ok(Self {
                child,
                job,
                reaped: false,
            })
        }
    }

    pub fn child_mut(&mut self) -> &mut Child {
        &mut self.child
    }

    fn job_handle(&self) -> HANDLE {
        HANDLE(self.job.as_raw_handle())
    }

    /// Kill only this owned tree, then reap its direct child. Pipe tasks unblock
    /// when the job's last inherited pipe handles close, including descendants.
    pub fn terminate_and_reap(&mut self) -> io::Result<()> {
        if self.reaped {
            return Ok(());
        }
        let deadline = Instant::now() + Duration::from_secs(1);
        unsafe {
            let _ = TerminateJobObject(self.job_handle(), 1);
        }
        let _ = self.child.kill();
        if unsafe { WaitForSingleObject(HANDLE(self.child.as_raw_handle()), 1000) } != WAIT_OBJECT_0
        {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "owned child termination did not complete",
            ));
        }
        self.child.wait()?;
        // The direct child may exit while a decoder still holds a pipe or
        // scratch file. Verify the whole job is empty before artifact promotion.
        loop {
            let mut accounting = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION::default();
            unsafe {
                QueryInformationJobObject(
                    Some(self.job_handle()),
                    JobObjectBasicAccountingInformation,
                    &mut accounting as *mut _ as _,
                    std::mem::size_of_val(&accounting) as u32,
                    None,
                )
                .map_err(io::Error::other)?;
            }
            if accounting.ActiveProcesses == 0 {
                break;
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "owned descendants did not terminate",
                ));
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        self.reaped = true;
        Ok(())
    }
}

impl Drop for ChildJob {
    fn drop(&mut self) {
        let _ = self.terminate_and_reap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use windows::Win32::Foundation::{CloseHandle, WAIT_OBJECT_0};
    use windows::Win32::System::JobObjects::QueryInformationJobObject;
    use windows::Win32::System::Threading::{
        OpenProcess, WaitForSingleObject, PROCESS_SYNCHRONIZE,
    };

    #[test]
    fn native_job_owns_memory_limit_and_kills_direct_child_on_drop() {
        let mut command = Command::new("powershell");
        command.args([
            "-NoProfile",
            "-Command",
            "$null=[Console]::ReadLine(); Start-Sleep -Seconds 60",
        ]);
        command
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null());
        let mut owned = ChildJob::spawn(&mut command).unwrap();
        unsafe {
            let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
            QueryInformationJobObject(
                Some(owned.job_handle()),
                JobObjectExtendedLimitInformation,
                &mut limits as *mut _ as _,
                std::mem::size_of_val(&limits) as u32,
                None,
            )
            .unwrap();
            assert_eq!(limits.JobMemoryLimit, 1024 * 1024 * 1024);
            assert!(limits
                .BasicLimitInformation
                .LimitFlags
                .contains(JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE));
            let process = OpenProcess(PROCESS_SYNCHRONIZE, false, owned.child_mut().id()).unwrap();
            owned
                .child_mut()
                .stdin
                .take()
                .unwrap()
                .write_all(b"go\n")
                .unwrap();
            drop(owned);
            assert_eq!(WaitForSingleObject(process, 1000), WAIT_OBJECT_0);
            CloseHandle(process).unwrap();
        }
    }
}
