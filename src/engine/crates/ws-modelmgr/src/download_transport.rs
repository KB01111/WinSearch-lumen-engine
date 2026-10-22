//! Keep TLS/proxy handling above a cancel-aware TCP transport with bounded waits.
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use ureq::unversioned::resolver::DefaultResolver;
use ureq::unversioned::transport::{
    Buffers, ConnectProxyConnector, ConnectionDetails, Connector, LazyBuffers, NextTimeout,
    RustlsConnector, Transport,
};

#[derive(Debug)]
struct DownloadConnector(Arc<AtomicBool>);

impl<In: Transport> Connector<In> for DownloadConnector {
    type Out = Box<dyn Transport>;

    fn connect(
        &self,
        details: &ConnectionDetails,
        chained: Option<In>,
    ) -> Result<Option<Self::Out>, ureq::Error> {
        if let Some(transport) = chained {
            return Ok(Some(transport.boxed()));
        }
        let deadline = Instant::now()
            + details
                .timeout
                .not_zero()
                .map(|v| *v)
                .unwrap_or(Duration::from_secs(1))
                .min(Duration::from_secs(1));
        let socket = connect_addresses(
            details.addrs.iter().copied(),
            deadline,
            details.timeout.reason,
            &self.0,
            |address, budget| TcpStream::connect_timeout(&address, budget),
        )?;
        socket.set_nodelay(details.config.no_delay())?;
        Ok(Some(Box::new(DownloadTransport {
            socket,
            canceled: self.0.clone(),
            buffers: LazyBuffers::new(
                details.config.input_buffer_size(),
                details.config.output_buffer_size(),
            ),
        })))
    }
}

fn connect_addresses<T>(
    mut addresses: impl ExactSizeIterator<Item = SocketAddr>,
    deadline: Instant,
    reason: ureq::Timeout,
    cancel: &AtomicBool,
    mut connect: impl FnMut(SocketAddr, Duration) -> io::Result<T>,
) -> Result<T, ureq::Error> {
    let mut error = io::Error::from(io::ErrorKind::ConnectionRefused);
    while let Some(address) = addresses.next() {
        if cancel.load(Ordering::Acquire) {
            return Err(io::Error::from(io::ErrorKind::Interrupted).into());
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(ureq::Error::Timeout(reason));
        }
        // Reserve a share for every remaining address (for example IPv4 after stalled IPv6).
        let share = remaining / u32::try_from(addresses.len() + 1).unwrap_or(u32::MAX);
        match connect(address, share.max(Duration::from_millis(1)).min(remaining)) {
            Ok(socket) => return Ok(socket),
            Err(next) => error = next,
        }
    }
    Err(error.into())
}

#[derive(Debug)]
struct DownloadTransport {
    socket: TcpStream,
    canceled: Arc<AtomicBool>,
    buffers: LazyBuffers,
}

impl Transport for DownloadTransport {
    fn buffers(&mut self) -> &mut dyn Buffers {
        &mut self.buffers
    }

    fn transmit_output(&mut self, amount: usize, timeout: NextTimeout) -> Result<(), ureq::Error> {
        let deadline = timeout.not_zero().map(|v| Instant::now() + *v);
        let mut sent = 0;
        while sent < amount {
            self.socket
                .set_write_timeout(Some(self.wait_budget(deadline, timeout)?))?;
            match self.socket.write(&self.buffers.output()[sent..amount]) {
                Ok(0) => return Err(io::Error::from(io::ErrorKind::WriteZero).into()),
                Ok(n) => sent += n,
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::TimedOut
                            | io::ErrorKind::WouldBlock
                            | io::ErrorKind::Interrupted
                    ) => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    fn await_input(&mut self, timeout: NextTimeout) -> Result<bool, ureq::Error> {
        let deadline = timeout.not_zero().map(|v| Instant::now() + *v);
        loop {
            self.socket
                .set_read_timeout(Some(self.wait_budget(deadline, timeout)?))?;
            match self.socket.read(self.buffers.input_append_buf()) {
                Ok(count) => {
                    self.buffers.input_appended(count);
                    return Ok(count > 0);
                }
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::TimedOut
                            | io::ErrorKind::WouldBlock
                            | io::ErrorKind::Interrupted
                    ) => {}
                Err(e) => return Err(e.into()),
            }
        }
    }

    // Each download owns its agent. Avoid probing/consuming bytes for connection reuse.
    fn is_open(&mut self) -> bool {
        false
    }
}

impl DownloadTransport {
    fn wait_budget(
        &self,
        deadline: Option<Instant>,
        timeout: NextTimeout,
    ) -> Result<Duration, ureq::Error> {
        if self.canceled.load(Ordering::Acquire) {
            return Err(io::Error::from(io::ErrorKind::ConnectionAborted).into());
        }
        let remaining = deadline
            .map(|d| d.saturating_duration_since(Instant::now()))
            .unwrap_or(Duration::MAX);
        if remaining.is_zero() {
            return Err(ureq::Error::Timeout(timeout.reason));
        }
        Ok(remaining.min(Duration::from_millis(250)))
    }
}

pub(super) fn with_agent<T>(cancel: &AtomicBool, transfer: impl FnOnce(ureq::Agent) -> T) -> T {
    let state = Arc::new(AtomicBool::new(cancel.load(Ordering::Acquire)));
    let (finished, done) = mpsc::channel::<()>();
    std::thread::scope(|scope| {
        let observing = state.clone();
        scope.spawn(move || loop {
            if cancel.load(Ordering::Acquire) {
                observing.store(true, Ordering::Release);
                break;
            }
            if !matches!(
                done.recv_timeout(Duration::from_millis(50)),
                Err(mpsc::RecvTimeoutError::Timeout)
            ) {
                break;
            }
        });
        let connector =
            ().chain(ConnectProxyConnector::default())
                .chain(DownloadConnector(state))
                .chain(RustlsConnector::default());
        let config = ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(30 * 60)))
            .timeout_resolve(Some(Duration::from_secs(1)))
            .timeout_connect(Some(Duration::from_secs(1)))
            .build();
        let result = transfer(ureq::Agent::with_parts(
            config,
            connector,
            DefaultResolver::default(),
        ));
        drop(finished);
        result
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stalled_first_address_preserves_time_for_a_reachable_second_address() {
        let addresses = [
            "[::1]:443".parse().unwrap(),
            "127.0.0.1:443".parse().unwrap(),
        ];
        let mut attempted = Vec::new();
        let result = connect_addresses(
            addresses.into_iter(),
            Instant::now() + Duration::from_millis(400),
            ureq::Timeout::Connect,
            &AtomicBool::new(false),
            |address, budget| {
                attempted.push(address);
                if address.is_ipv6() {
                    std::thread::sleep(budget);
                    Err(io::Error::from(io::ErrorKind::TimedOut))
                } else {
                    Ok("connected")
                }
            },
        );
        assert!(
            matches!(result, Ok("connected")),
            "reachable fallback was never attempted: {result:?}"
        );
        assert_eq!(attempted, addresses);
    }
}
