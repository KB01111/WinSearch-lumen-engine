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
    let config = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(30 * 60)))
        .timeout_resolve(Some(Duration::from_secs(1)))
        .timeout_connect(Some(Duration::from_secs(1)))
        .build();
    with_agent_config(cancel, config, transfer)
}

fn with_agent_config<T>(
    cancel: &AtomicBool,
    config: ureq::config::Config,
    transfer: impl FnOnce(ureq::Agent) -> T,
) -> T {
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
        // ureq 3.4.2 CONNECT recursively runs this same chain with a proxy-free
        // configuration to obtain DownloadTransport. Putting TCP first would
        // bypass CONNECT, whose Some(chained) branch passes through unchanged.
        let connector =
            ().chain(ConnectProxyConnector::default())
                .chain(DownloadConnector(state))
                .chain(RustlsConnector::default());
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
    fn connect_proxy_uses_the_bounded_transport_for_tunnel_tls_and_body_cancellation() {
        use std::net::TcpListener;
        fn header(socket: &mut TcpStream) -> Vec<u8> {
            let mut value = Vec::new();
            while !value.ends_with(b"\r\n\r\n") {
                let mut byte = [0];
                socket.read_exact(&mut byte).unwrap();
                value.push(byte[0]);
                assert!(value.len() < 16 * 1024);
            }
            value
        }
        for stage in ["complete", "connect_stall", "tls_stall", "body_stall"] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let address = listener.local_addr().unwrap();
            let (started, ready) = mpsc::channel();
            let (stop, stopped) = mpsc::channel();
            let server = std::thread::spawn(move || {
                let deadline = Instant::now() + Duration::from_secs(3);
                let mut socket = loop {
                    match listener.accept() {
                        Ok((socket, _)) => break socket,
                        Err(error)
                            if error.kind() == io::ErrorKind::WouldBlock
                                && Instant::now() < deadline =>
                        {
                            std::thread::sleep(Duration::from_millis(5))
                        }
                        Err(error) => panic!("proxy was bypassed: {error}"),
                    }
                };
                socket.set_nonblocking(false).unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let tunnel = header(&mut socket);
                let expected = if stage == "tls_stall" {
                    "CONNECT only-via-proxy.invalid:443 HTTP/1.1"
                } else {
                    "CONNECT only-via-proxy.invalid:80 HTTP/1.1"
                };
                assert!(tunnel.starts_with(expected.as_bytes()));
                if stage != "connect_stall" {
                    socket
                        .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
                        .unwrap();
                    if stage == "tls_stall" {
                        let mut record = [0; 5];
                        socket.read_exact(&mut record).unwrap();
                        assert_eq!(record[0], 22, "expected a real TLS handshake record");
                    } else {
                        assert!(header(&mut socket).starts_with(b"GET /model.bin HTTP/1.1"));
                        socket
                            .write_all(if stage == "complete" {
                                b"HTTP/1.1 200 OK\r\nContent-Length: 7\r\n\r\nfixture"
                            } else {
                                b"HTTP/1.1 200 OK\r\nContent-Length: 32\r\n\r\nprefix"
                            })
                            .unwrap();
                    }
                }
                started.send(()).unwrap();
                let _ = stopped.recv_timeout(Duration::from_secs(3));
            });
            let config = ureq::Agent::config_builder()
                .proxy(Some(
                    ureq::Proxy::new(&format!("http://{address}")).unwrap(),
                ))
                .timeout_global(Some(Duration::from_secs(10)))
                .timeout_connect(Some(Duration::from_secs(2)))
                .build();
            let cancel = Arc::new(AtomicBool::new(false));
            let observed = cancel.clone();
            let (finished, done) = mpsc::channel();
            let transfer = std::thread::spawn(move || {
                let result: Result<Vec<u8>, ureq::Error> =
                    with_agent_config(&observed, config, |agent| {
                        let scheme = if stage == "tls_stall" {
                            "https"
                        } else {
                            "http"
                        };
                        let mut response = agent
                            .get(format!("{scheme}://only-via-proxy.invalid/model.bin"))
                            .call()?;
                        let mut bytes = Vec::new();
                        response.body_mut().as_reader().read_to_end(&mut bytes)?;
                        Ok(bytes)
                    });
                let _ = finished.send(result);
            });
            ready
                .recv_timeout(Duration::from_secs(4))
                .expect("proxy tunnel was not established");
            if stage != "complete" {
                cancel.store(true, Ordering::Release);
            }
            let result = done.recv_timeout(Duration::from_millis(750));
            let _ = stop.send(());
            server.join().unwrap();
            transfer.join().unwrap();
            let result =
                result.expect("proxy/TLS/body cancellation exceeded its socket wait bound");
            if stage == "complete" {
                assert_eq!(result.unwrap(), b"fixture");
            } else {
                assert!(result.is_err(), "cancelled {stage} succeeded");
            }
        }
    }

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
