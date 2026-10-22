use super::*;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};
use ws_protocol::{read_message, write_message, ErrorCode, ServerMessage};

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[test]
fn connection_overload_is_rejected_and_released_slots_accept_new_peers() {
    let name = format!(
        r"\\.\pipe\winsearch-admission-{}-{}",
        std::process::id(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    );
    let serving = name.clone();
    let (entered, accepted) = mpsc::channel();
    let (left, retired) = mpsc::channel();
    std::thread::spawn(move || {
        serve(&serving, move |mut stream| {
            let _ = stream.write_all(b"a");
            let _ = entered.send(());
            let _ = stream.read(&mut [0]);
            let _ = left.send(());
        })
        .unwrap();
    });
    let connect = || {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match PipeStream::connect(&name) {
                Ok(stream) => break stream,
                Err(_) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(10))
                }
                Err(error) => panic!("admission server unavailable: {error}"),
            }
        }
    };
    let mut peers = Vec::new();
    for _ in 0..32 {
        let mut peer = connect();
        peer.read_exact(&mut [0]).unwrap();
        peers.push(peer);
        accepted.recv_timeout(Duration::from_secs(5)).unwrap();
    }
    let mut excess = connect();
    let (sent, received) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        let _ = sent.send(excess.read(&mut [0]));
    });
    let rejection = received.recv_timeout(Duration::from_secs(2));
    drop(peers.pop());
    retired.recv_timeout(Duration::from_secs(2)).unwrap();
    // Wait for handler scope/permit teardown, not only its final notification.
    let mut replacement = None;
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        let peer = connect();
        if accepted.recv_timeout(Duration::from_millis(50)).is_ok() {
            replacement = Some(peer);
            break;
        }
    }
    drop(replacement);
    drop(peers);
    reader.join().unwrap();
    assert!(
        matches!(rejection, Ok(Ok(0)) | Ok(Err(_))),
        "excess peer retained an unbounded handler: {rejection:?}"
    );
    assert!(
        Instant::now() < deadline,
        "released admission slot did not accept a new peer"
    );
}

fn connected_pair() -> (PipeStream, PipeStream) {
    let name = format!(
        r"\\.\pipe\winsearch-write-test-{}-{}",
        std::process::id(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    );
    let security = UserOnlySecurity::new().unwrap();
    let wide: Vec<u16> = name.encode_utf16().chain(Some(0)).collect();
    let handle = PipeHandle(unsafe { create_instance(&wide, &security) }.unwrap());
    let client = PipeStream::connect(&name).unwrap();
    accept_connection(&handle).unwrap();
    let server = PipeStream::new(handle, true);
    (server, client)
}

#[test]
fn inbound_header_and_body_trickles_share_one_absolute_frame_deadline() {
    for header_only in [true, false] {
        let (server, mut client) = connected_pair();
        let mut reader = server.framed_reader().unwrap();
        reader.finish_read_frame(); // An established peer may idle before starting a frame.
        let (sent, received) = mpsc::channel();
        let start = Instant::now();
        let reader_thread = std::thread::spawn(move || {
            let _ = sent.send(read_message::<_, ServerMessage>(&mut reader));
        });
        let writer = std::thread::spawn(move || {
            if header_only {
                // Start a header, then trickle its remaining bytes past five seconds.
                for byte in 100_u32.to_le_bytes() {
                    if client.write_all(&[byte]).is_err() {
                        break;
                    }
                    std::thread::sleep(Duration::from_secs(2));
                }
            } else {
                client.write_all(&100_u32.to_le_bytes()).unwrap();
                for _ in 0..20 {
                    if client.write_all(b" ").is_err() {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(500));
                }
            }
        });
        let result = received.recv_timeout(Duration::from_secs(7));
        drop(server); // Also release a regressed blocking reader before joining.
        reader_thread.join().unwrap();
        writer.join().unwrap();
        assert!(
            matches!(result, Ok(Err(ws_protocol::FrameError::Io(error))) if error.kind() == io::ErrorKind::TimedOut)
        );
        assert!(start.elapsed() < Duration::from_secs(9));
    }
}

#[test]
fn established_framed_reader_can_idle_then_receive_a_new_complete_frame() {
    let (server, mut client) = connected_pair();
    let mut reader = server.framed_reader().unwrap();
    reader.finish_read_frame();
    let writer = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(6));
        write_message(&mut client, &ServerMessage::Pong { id: 31 }).unwrap();
    });
    assert_eq!(
        read_message::<_, ServerMessage>(&mut reader).unwrap(),
        Some(ServerMessage::Pong { id: 31 })
    );
    reader.finish_read_frame();
    writer.join().unwrap();
}

fn large_message() -> ServerMessage {
    ServerMessage::Error {
        id: None,
        code: ErrorCode::Internal,
        message: "x".repeat(2 * 1024 * 1024),
    }
}

#[test]
fn stalled_frame_write_times_out_and_closes_the_endpoint() {
    let (mut server, client) = connected_pair();
    let (send, receive) = mpsc::channel();
    let writer = std::thread::spawn(move || {
        let result = write_message(&mut server, &large_message());
        let next = server.write_all(b"cannot append to a partial frame");
        let closed = server.peek_available();
        send.send((result, next, closed)).unwrap();
    });
    let result = receive.recv_timeout(Duration::from_secs(8));
    // Release the old blocking implementation too, so a failed regression leaves no writer behind.
    drop(client);
    writer.join().unwrap();
    let (result, next, closed) =
        result.expect("server write must expire while the peer stays open");
    assert!(matches!(result, Err(ws_protocol::FrameError::Io(error))
        if error.kind() == io::ErrorKind::TimedOut));
    assert!(
        next.is_err(),
        "a partially emitted frame must never be reused"
    );
    assert_eq!(
        closed.unwrap(),
        None,
        "the read loop must observe endpoint closure"
    );
}

#[test]
fn trickling_reader_cannot_extend_the_frame_deadline() {
    let (mut server, mut client) = connected_pair();
    let stop = Arc::new(AtomicBool::new(false));
    let reader_stop = Arc::clone(&stop);
    let reader = std::thread::spawn(move || {
        let mut bytes = [0; 4096];
        let mut received = 0;
        while !reader_stop.load(Ordering::Acquire) {
            match client.read(&mut bytes) {
                Ok(0) | Err(_) => break,
                Ok(count) => received += count,
            }
            std::thread::sleep(Duration::from_millis(250));
        }
        received
    });
    let (send, receive) = mpsc::channel();
    let writer = std::thread::spawn(move || {
        send.send(write_message(&mut server, &large_message()))
            .unwrap();
    });
    let result = receive.recv_timeout(Duration::from_secs(8));
    stop.store(true, Ordering::Release);
    let received = reader.join().unwrap();
    writer.join().unwrap();
    assert!(received > 0, "the peer must actually drain some bytes");
    assert!(
        matches!(result.expect("partial reads must not restart the write budget"),
        Err(ws_protocol::FrameError::Io(error)) if error.kind() == io::ErrorKind::TimedOut)
    );
}

#[test]
fn completed_frames_allow_idle_time_before_the_next_frame() {
    let (mut server, mut client) = connected_pair();
    let writer = std::thread::spawn(move || {
        write_message(&mut server, &ServerMessage::Pong { id: 17 }).unwrap();
        std::thread::sleep(Duration::from_secs(6));
        write_message(&mut server, &ServerMessage::Pong { id: 18 }).unwrap();
    });
    let start = Instant::now();
    assert_eq!(
        read_message::<_, ServerMessage>(&mut client).unwrap(),
        Some(ServerMessage::Pong { id: 17 })
    );
    assert_eq!(
        read_message::<_, ServerMessage>(&mut client).unwrap(),
        Some(ServerMessage::Pong { id: 18 })
    );
    assert!(start.elapsed() >= Duration::from_secs(6));
    writer.join().unwrap();
}

#[test]
fn frame_expiring_before_flush_closes_instead_of_reporting_success() {
    struct DelayedFlush(PipeStream);
    impl Write for DelayedFlush {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.write(bytes)
        }
        fn flush(&mut self) -> io::Result<()> {
            std::thread::sleep(Duration::from_secs(6));
            self.0.flush()
        }
    }
    let (server, mut client) = connected_pair();
    let writer = std::thread::spawn(move || {
        let mut delayed = DelayedFlush(server);
        let result = write_message(&mut delayed, &ServerMessage::Pong { id: 19 });
        (result, delayed.0.peek_available())
    });
    assert_eq!(
        read_message::<_, ServerMessage>(&mut client).unwrap(),
        Some(ServerMessage::Pong { id: 19 })
    );
    let (result, closed) = writer.join().unwrap();
    assert!(matches!(result, Err(ws_protocol::FrameError::Io(error))
        if error.kind() == io::ErrorKind::TimedOut));
    assert_eq!(closed.unwrap(), None);
}
