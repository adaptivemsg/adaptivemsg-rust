use std::collections::HashSet;
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task::JoinHandle;

use crate::codec_msgpack::CodecMsgpackCompact;
use crate::codec_postcard::CodecPostcard;
use crate::connection::{ConnectionRef, STREAM_QUEUE_SIZE};
use crate::frame::FRAME_HEADER_LEN_V2;
use crate::message::{Message, MessageHandler, OkReply};
use crate::protocol::DEFAULT_MAX_FRAME;
use crate::recovery::{ClientRecoveryOptions, ServerRecoveryOptions};
use crate::{Client, Connection, Error, Server};

#[crate::message(register)]
struct RegressionEchoRequest {
    text: String,
}

#[crate::message(register)]
struct RegressionEchoReply {
    text: String,
}

#[crate::message_handler]
impl MessageHandler for RegressionEchoRequest {
    async fn handle(
        self: Box<Self>,
        _stream_ctx: crate::StreamContext,
    ) -> crate::Result<Option<Box<dyn Message>>> {
        Ok(Some(Box::new(RegressionEchoReply {
            text: self.text.clone(),
        })))
    }
}

static FLOODS_DONE: Mutex<Option<HashSet<u64>>> = Mutex::new(None);
static NEXT_FLOOD_ID: AtomicU64 = AtomicU64::new(1);

/// Makes the server send `count` replies on the stream it arrives on, or as
/// many as it can before the stream closes, then record `id` as done.
#[crate::message(register)]
struct RegressionFlood {
    id: u64,
    count: usize,
}

#[crate::message_handler]
impl MessageHandler for RegressionFlood {
    async fn handle(
        self: Box<Self>,
        stream_ctx: crate::StreamContext,
    ) -> crate::Result<Option<Box<dyn Message>>> {
        let mut result = Ok(None);
        for _ in 0..self.count {
            let sent = stream_ctx
                .stream
                .send(RegressionEchoReply {
                    text: "flood".to_string(),
                })
                .await;
            if let Err(err) = sent {
                result = Err(err.into());
                break;
            }
        }
        FLOODS_DONE
            .lock()
            .unwrap()
            .get_or_insert_with(HashSet::new)
            .insert(self.id);
        result
    }
}

/// Replies with the inbox capacity of the server stream it arrives on.
#[crate::message(register)]
struct RegressionQueueProbe {}

#[crate::message_handler]
impl MessageHandler for RegressionQueueProbe {
    async fn handle(
        self: Box<Self>,
        stream_ctx: crate::StreamContext,
    ) -> crate::Result<Option<Box<dyn Message>>> {
        Ok(Some(Box::new(RegressionEchoReply {
            text: stream_ctx.stream.inbox_tx.max_capacity().to_string(),
        })))
    }
}

fn flood(count: usize) -> RegressionFlood {
    RegressionFlood {
        id: NEXT_FLOOD_ID.fetch_add(1, Ordering::Relaxed),
        count,
    }
}

fn flood_done(id: u64) -> bool {
    FLOODS_DONE
        .lock()
        .unwrap()
        .as_ref()
        .is_some_and(|done| done.contains(&id))
}

fn ephemeral_tcp_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral tcp addr");
    let addr = listener.local_addr().expect("read local addr");
    drop(listener);
    format!("tcp://{addr}")
}

async fn start_server(server: Server, addr: &str) -> JoinHandle<()> {
    let serve_addr = addr.to_string();
    let handle = tokio::spawn(async move {
        let _ = server.serve(&serve_addr).await;
    });
    let dial_addr = addr.trim_start_matches("tcp://").to_string();
    for _ in 0..50u32 {
        if tokio::net::TcpStream::connect(&dial_addr).await.is_ok() {
            return handle;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("server did not become ready");
}

async fn wait_until(timeout: Duration, mut done: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if done() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    done()
}

fn server_recovery() -> ServerRecoveryOptions {
    ServerRecoveryOptions {
        enable: true,
        detached_ttl: Duration::from_secs(2),
        max_replay_bytes: 1 << 20,
        ack_every: 1,
        ack_delay: Duration::from_millis(1),
        heartbeat_interval: Duration::from_millis(50),
        heartbeat_timeout: Duration::from_millis(200),
    }
}

async fn recovery_client(addr: &str) -> Connection {
    let conn = Client::new()
        .with_connect_timeout(Duration::from_millis(500))
        .with_recv_timeout(Duration::from_secs(2))
        .with_recovery(ClientRecoveryOptions {
            enable: true,
            reconnect_min_backoff: Duration::from_millis(5),
            reconnect_max_backoff: Duration::from_millis(20),
            max_replay_bytes: 1 << 20,
        })
        .connect(addr)
        .await
        .expect("connect");
    conn
}

async fn echo(stream: &crate::Stream, text: &str) -> Result<RegressionEchoReply, Error> {
    stream
        .send_recv(RegressionEchoRequest {
            text: text.to_string(),
        })
        .await
}

/// `on_disconnect` runs once per connection. The task that accepted a resume
/// also waited for the connection to close and ran it again.
#[tokio::test]
async fn resume_does_not_repeat_on_disconnect() {
    let addr = ephemeral_tcp_addr();
    let disconnects = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&disconnects);
    let server = Server::new()
        .with_recovery(server_recovery())
        .on_disconnect(move |_| {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });
    let _server = start_server(server, &addr).await;
    let conn = recovery_client(&addr).await;
    let reply: RegressionEchoReply = conn
        .send_recv(RegressionEchoRequest {
            text: "first".into(),
        })
        .await
        .expect("first send_recv");
    assert_eq!(reply.text, "first");

    let before = conn.transport_generation_for_test();
    conn.close_transport_for_test();
    assert!(
        wait_until(Duration::from_secs(2), || conn
            .transport_generation_for_test()
            >= before + 2)
        .await,
        "no resume"
    );
    let reply: RegressionEchoReply = conn
        .send_recv(RegressionEchoRequest {
            text: "second".into(),
        })
        .await
        .expect("send_recv after resume");
    assert_eq!(reply.text, "second");

    conn.close().await;
    assert!(
        wait_until(Duration::from_secs(2), || disconnects
            .load(Ordering::SeqCst)
            >= 1)
        .await
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(disconnects.load(Ordering::SeqCst), 1);
}

/// A transport that fails while the reconnect loop is finishing must still be
/// replaced: its detach found the loop running and started none of its own.
#[tokio::test]
async fn reconnects_when_resumed_transport_fails_at_once() {
    let addr = ephemeral_tcp_addr();
    let _server = start_server(Server::new().with_recovery(server_recovery()), &addr).await;
    let conn = recovery_client(&addr).await;

    let fired = Arc::new(AtomicBool::new(false));
    let hook_fired = Arc::clone(&fired);
    *conn.reconnected_hook.lock().unwrap() = Some(Box::new(move |connection: &ConnectionRef| {
        hook_fired.store(true, Ordering::SeqCst);
        connection.close_transport_for_test();
    }));
    conn.close_transport_for_test();
    assert!(wait_until(Duration::from_secs(2), || fired.load(Ordering::SeqCst)).await);

    let reply: RegressionEchoReply = conn
        .send_recv(RegressionEchoRequest {
            text: "after".into(),
        })
        .await
        .expect("send_recv after the resumed transport failed");
    assert_eq!(reply.text, "after");
}

/// Closing a stream whose inbox is full must not stall the other streams.
/// The decode task waited for room in the inbox of the closed stream, and the
/// reader then waited for it.
#[tokio::test]
async fn closing_stream_with_full_inbox_does_not_stall_connection() {
    let addr = ephemeral_tcp_addr();
    let _server = start_server(Server::new(), &addr).await;
    let conn = Client::new()
        .with_recv_timeout(Duration::from_secs(2))
        .connect(&addr)
        .await
        .expect("connect");

    let flooded = conn.new_stream();
    let request = flood(3 * STREAM_QUEUE_SIZE);
    let id = request.id;
    flooded.send(request).await.expect("send flood request");
    assert!(
        wait_until(Duration::from_secs(2), || flooded.debug_state().inbox_depth
            == STREAM_QUEUE_SIZE)
        .await,
        "inbox did not fill"
    );
    flooded.close();
    // The reply below must queue behind the whole flood.
    assert!(
        wait_until(Duration::from_secs(2), || flood_done(id)).await,
        "the server flood never finished: the client stopped reading"
    );

    let stream = conn.new_stream();
    let reply = echo(&stream, "after")
        .await
        .expect("send_recv on another stream");
    assert_eq!(reply.text, "after");
    conn.close().await;
}

/// Frames that arrive on stream 0 after a protocol error closed it are
/// dropped without stalling the other streams.
#[tokio::test]
async fn default_stream_drops_frames_after_protocol_error() {
    let addr = ephemeral_tcp_addr();
    let _server = start_server(Server::new(), &addr).await;
    let conn = Client::new()
        .with_recv_timeout(Duration::from_secs(2))
        .connect(&addr)
        .await
        .expect("connect");

    let request = flood(5 * STREAM_QUEUE_SIZE);
    let id = request.id;
    conn.send(request).await.expect("send flood request");
    let result = conn.recv::<OkReply>().await;
    assert!(
        matches!(result, Err(Error::TypeMismatch { .. })),
        "recv got {:?}",
        result.err()
    );
    // The reply below must queue behind the whole flood.
    assert!(
        wait_until(Duration::from_secs(2), || flood_done(id)).await,
        "the server flood never finished: the client stopped reading"
    );

    let stream = conn.new_stream();
    let reply = echo(&stream, "after")
        .await
        .expect("send_recv on another stream");
    assert_eq!(reply.text, "after");
    conn.close().await;
}

/// The counters mean what they mean in Go: a message is counted when it is
/// delivered, sends count on the connection as well as the stream, and a
/// reply of the wrong type counts as a decode error and a protocol error.
#[tokio::test]
async fn debug_counters_match_go_runtime() {
    let addr = ephemeral_tcp_addr();
    let _server = start_server(Server::new(), &addr).await;
    let conn = Client::new()
        .with_recv_timeout(Duration::from_secs(2))
        .connect(&addr)
        .await
        .expect("connect");

    let stream = conn.new_stream();
    echo(&stream, "one").await.expect("echo");
    let counters = stream.debug_state().counters;
    assert_eq!(counters.data_messages_sent, 1);
    assert_eq!(counters.data_messages_received, 1);
    let counters = conn.debug_state().counters;
    assert_eq!(counters.data_messages_sent, 1);
    assert_eq!(counters.data_messages_received, 1);
    assert_eq!(counters.transport_attaches, 1);

    let idle = conn.new_stream();
    idle.set_recv_timeout(Duration::from_millis(10));
    assert!(matches!(
        idle.recv::<OkReply>().await,
        Err(Error::RecvTimeout)
    ));
    assert_eq!(
        idle.debug_state().last_failure_code,
        crate::debug::FAILURE_STREAM_RECV_TIMEOUT
    );
    let state = conn.debug_state();
    assert_eq!(
        state.last_failure_code,
        crate::debug::FAILURE_STREAM_RECV_TIMEOUT
    );
    assert_eq!(state.last_failure, "stream recv timeout");

    stream.send(flood(1)).await.expect("send flood request");
    let result = stream.recv::<OkReply>().await;
    assert!(
        matches!(result, Err(Error::TypeMismatch { .. })),
        "recv got {:?}",
        result.err()
    );
    let state = conn.debug_state();
    assert_eq!(state.counters.decode_errors, 1);
    assert_eq!(state.counters.protocol_errors, 1);
    assert_eq!(
        state.last_failure_code,
        crate::debug::FAILURE_STREAM_PROTOCOL
    );
    conn.close().await;
}

/// A peer that connects and sends nothing, or sends the hello but no attach,
/// is closed once the handshake timeout passes.
#[tokio::test]
async fn server_closes_peer_that_stalls_handshake() {
    let addr = ephemeral_tcp_addr();
    let server = Server::new()
        .with_recovery(server_recovery())
        .with_handshake_timeout(Duration::from_millis(100));
    let _server = start_server(server, &addr).await;

    for send_hello in [false, true] {
        let stream = tokio::net::TcpStream::connect(addr.trim_start_matches("tcp://"))
            .await
            .expect("connect");
        let (mut reader, mut writer) = stream.into_split();
        if send_hello {
            crate::protocol::handshake_client(
                &mut reader,
                &mut writer,
                &[CodecMsgpackCompact],
                DEFAULT_MAX_FRAME,
                crate::protocol::PROTOCOL_VERSION_V3,
            )
            .await
            .expect("handshake");
        }
        let mut byte = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(2), reader.read(&mut byte))
            .await
            .unwrap_or_else(|_| {
                panic!("hello sent {send_hello}: the server kept the connection open")
            });
        assert!(
            matches!(read, Ok(0) | Err(_)),
            "hello sent {send_hello}: read got {read:?}, want the server to close"
        );
    }
}

/// `with_handshake_timeout(Duration::ZERO)` removes the limit, so a silent
/// peer stays connected.
#[tokio::test]
async fn handshake_timeout_zero_disables_limit() {
    let addr = ephemeral_tcp_addr();
    let server = Server::new()
        .with_handshake_timeout(Duration::from_millis(50))
        .with_handshake_timeout(Duration::ZERO);
    let _server = start_server(server, &addr).await;

    let mut stream = tokio::net::TcpStream::connect(addr.trim_start_matches("tcp://"))
        .await
        .expect("connect");
    let mut byte = [0u8; 1];
    let read = tokio::time::timeout(Duration::from_millis(300), stream.read(&mut byte)).await;
    assert!(
        read.is_err(),
        "read got {read:?}, want the server to keep waiting"
    );
}

/// `Stream::close` and `new_stream` work from a thread outside the runtime;
/// both used to panic there.
#[tokio::test(flavor = "multi_thread")]
async fn stream_methods_work_outside_the_runtime() {
    let addr = ephemeral_tcp_addr();
    let _server = start_server(Server::new(), &addr).await;
    let conn = Client::new()
        .with_recv_timeout(Duration::from_secs(2))
        .connect(&addr)
        .await
        .expect("connect");
    let closing = conn.new_stream();
    echo(&closing, "before").await.expect("echo");

    let thread_conn = conn.clone();
    let thread_stream = closing.clone();
    let opened = std::thread::spawn(move || {
        thread_stream.close();
        thread_conn.new_stream()
    })
    .join()
    .expect("close and new_stream outside the runtime");

    let reply = echo(&opened, "after")
        .await
        .expect("echo on the new stream");
    assert_eq!(reply.text, "after");
    let result = closing
        .send(RegressionEchoRequest { text: "x".into() })
        .await;
    assert!(matches!(result, Err(Error::Closed)), "send got {result:?}");
    conn.close().await;
}

/// Each side's stream queue size applies to the streams it creates.
#[tokio::test]
async fn stream_queue_size_applies_to_new_streams() {
    let addr = ephemeral_tcp_addr();
    let _server = start_server(Server::new().with_stream_queue_size(4), &addr).await;
    let conn = Client::new()
        .with_stream_queue_size(8)
        .with_recv_timeout(Duration::from_secs(2))
        .connect(&addr)
        .await
        .expect("connect");
    let stream = conn.new_stream();
    assert_eq!(stream.inbox_tx.max_capacity(), 8);
    let reply: RegressionEchoReply = stream
        .send_recv(RegressionQueueProbe {})
        .await
        .expect("probe");
    assert_eq!(reply.text, "4");
    conn.close().await;

    let conn = Client::new().connect(&addr).await.expect("connect");
    assert_eq!(conn.new_stream().inbox_tx.max_capacity(), STREAM_QUEUE_SIZE);
    conn.close().await;
}

/// Shutdown closes established connections, v2 and v3, after running their
/// `on_disconnect`, and drops a connection still in the handshake.
#[tokio::test]
async fn serve_with_shutdown_closes_connections() {
    let addr = ephemeral_tcp_addr();
    let disconnects = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&disconnects);
    let server = Server::new()
        .with_recovery(server_recovery())
        .on_disconnect(move |_| {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let serve_addr = addr.clone();
    let served = tokio::spawn(async move {
        server
            .serve_with_shutdown(&serve_addr, async {
                let _ = stop_rx.await;
            })
            .await
    });

    let plain = Client::new()
        .with_connect_timeout(Duration::from_secs(2))
        .connect(&addr)
        .await
        .expect("connect");
    let recovered = recovery_client(&addr).await;
    let mut stalled = tokio::net::TcpStream::connect(addr.trim_start_matches("tcp://"))
        .await
        .expect("connect");

    stop_tx.send(()).expect("signal");
    let result = tokio::time::timeout(Duration::from_secs(10), served)
        .await
        .expect("serve_with_shutdown did not return")
        .expect("serve task");
    assert!(result.is_ok(), "serve_with_shutdown got {result:?}");
    assert_eq!(disconnects.load(Ordering::SeqCst), 2);
    assert!(
        wait_until(Duration::from_secs(2), || {
            plain.debug_state().closed && recovered.debug_state().closed
        })
        .await,
        "the clients did not see their connections close"
    );
    let mut byte = [0u8; 1];
    let read = tokio::time::timeout(Duration::from_secs(1), stalled.read(&mut byte))
        .await
        .expect("the server kept the stalled connection open");
    assert!(matches!(read, Ok(0) | Err(_)), "read got {read:?}");
}

/// A failed accept, here EMFILE, does not end `serve`; it accepts again.
#[tokio::test]
async fn server_keeps_accepting_after_accept_errors() {
    let listener = Arc::new(
        crate::transport::tcp::listen("127.0.0.1:0")
            .await
            .expect("listen"),
    );
    let addr = format!("tcp://{}", listener.local_addr().expect("local addr"));
    let failures = Arc::new(AtomicUsize::new(0));
    let accept = {
        let failures = Arc::clone(&failures);
        move || {
            let listener = Arc::clone(&listener);
            let failures = Arc::clone(&failures);
            async move {
                if failures.fetch_add(1, Ordering::SeqCst) < 3 {
                    return Err(Error::Io(std::io::Error::from_raw_os_error(24)));
                }
                let (stream, peer) = crate::transport::tcp::accept_stream(listener).await?;
                let (reader, writer) = stream.into_split();
                Ok((crate::transport::parts(reader, writer), peer))
            }
        }
    };
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let served = tokio::spawn(Server::new().serve_with_accept(accept, async {
        let _ = stop_rx.await;
    }));

    let conn = Client::new()
        .with_connect_timeout(Duration::from_secs(2))
        .with_recv_timeout(Duration::from_secs(2))
        .connect(&addr)
        .await
        .expect("connect");
    let reply = echo(&conn.new_stream(), "after").await.expect("echo");
    assert_eq!(reply.text, "after");
    assert!(failures.load(Ordering::SeqCst) > 3);
    conn.close().await;

    stop_tx.send(()).expect("signal");
    let result = tokio::time::timeout(Duration::from_secs(5), served)
        .await
        .expect("serve did not shut down")
        .expect("serve task");
    assert!(result.is_ok(), "serve got {result:?}");
}

/// Sends report `Error::Closed` once the connection has closed, including on
/// a stream opened afterwards.
#[tokio::test]
async fn send_after_connection_failure_reports_closed() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = format!("tcp://{}", listener.local_addr().expect("local addr"));
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let (mut reader, mut writer) = stream.into_split();
        crate::protocol::handshake_server(
            &mut reader,
            &mut writer,
            &[CodecPostcard, CodecMsgpackCompact],
            DEFAULT_MAX_FRAME,
            false,
        )
        .await
        .expect("handshake");
        // A frame with an unknown version fails the client's reader.
        writer
            .write_all(&[9u8; FRAME_HEADER_LEN_V2])
            .await
            .expect("write bad frame");
        let mut sink = Vec::new();
        let _ = reader.read_to_end(&mut sink).await;
    });
    let conn = Client::new().connect(&addr).await.expect("connect");
    assert!(wait_until(Duration::from_secs(2), || conn.debug_state().closed).await);

    let stream = conn.new_stream();
    for i in 0..100 {
        let result = stream
            .send(RegressionEchoRequest { text: "x".into() })
            .await;
        assert!(
            matches!(result, Err(Error::Closed)),
            "send {i} on a closed connection got {result:?}"
        );
    }
    peer.abort();
}
