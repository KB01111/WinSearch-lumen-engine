use super::*;
use std::sync::mpsc;

#[test]
fn stalled_broadcast_does_not_lock_registration_and_removes_only_the_failed_client() {
    let indexer = Indexer::open(ws_index::IndexStore::open_in_memory().unwrap(), &[]).unwrap();
    let engine = EngineService::new_with_model_dir(
        indexer,
        std::env::temp_dir().join("winsearch-writer-unused-models"),
    );
    let name = format!(r"\\.\pipe\winsearch-broadcast-test-{}", std::process::id());
    let server_name = name.clone();
    let (accepted, connections) = mpsc::channel();
    std::thread::spawn(move || {
        ws_platform::pipe::serve(&server_name, move |stream| {
            let _ = accepted.send(Arc::new(Mutex::new(stream)));
        })
        .unwrap();
    });
    let connect = || {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            match PipeStream::connect(&name) {
                Ok(peer) => return peer,
                Err(_) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(10))
                }
                Err(error) => panic!("test pipe did not accept: {error}"),
            }
        }
    };
    let stalled_peer = connect();
    let stalled =
        engine.register_client(connections.recv_timeout(Duration::from_secs(10)).unwrap());
    let stalled_cancel = engine.begin_request_for_client(stalled.id(), 41);
    let healthy_peer = connect();
    let healthy_stream = connections.recv_timeout(Duration::from_secs(10)).unwrap();
    let pushing = engine.clone();
    let writer = std::thread::spawn(move || {
        pushing.push_to_all(&ServerMessage::Error {
            id: None,
            code: ErrorCode::Internal,
            message: "x".repeat(2 * 1024 * 1024),
        })
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while stalled.stream.try_lock().is_ok() {
        assert!(
            std::time::Instant::now() < deadline,
            "broadcast never began writing"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    let registering = engine.clone();
    let (send, receive) = mpsc::channel();
    let registrar = std::thread::spawn(move || {
        let healthy = registering.register_client(healthy_stream);
        send.send(healthy).unwrap();
    });
    let promptly_registered = receive.recv_timeout(Duration::from_millis(250));
    drop(stalled_peer);
    writer.join().unwrap();
    registrar.join().unwrap();
    let removed = !engine
        .clients
        .lock()
        .unwrap()
        .iter()
        .any(|client| client.id() == stalled.id());
    let cancelled = stalled_cancel.load(Ordering::Acquire);
    let healthy = promptly_registered
        .as_ref()
        .ok()
        .cloned()
        .unwrap_or_else(|| receive.recv().unwrap());
    let healthy_still_registered = engine
        .clients
        .lock()
        .unwrap()
        .iter()
        .any(|client| client.id() == healthy.id());
    engine.unregister_client(&healthy);
    engine.unregister_client(&stalled);
    drop(healthy_peer);
    assert!(
        promptly_registered.is_ok(),
        "a stalled writer held the global client registry"
    );
    assert!(
        removed,
        "a failed client must be removed from future broadcasts"
    );
    assert!(
        cancelled,
        "a failed transport must cancel that client's outstanding work"
    );
    assert!(
        healthy_still_registered,
        "failure cleanup must not unregister a concurrent healthy client"
    );
}
