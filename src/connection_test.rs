use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task::JoinHandle;

use crate::codec_msgpack::CodecMsgpackCompact;
use crate::message::{Message, MessageHandler, OkReply};
use crate::protocol::{handshake_client, handshake_server, DEFAULT_MAX_FRAME, PROTOCOL_VERSION_V2};
use crate::recovery::{ClientRecoveryOptions, ServerRecoveryOptions};
use crate::{Client, Error, Server};

#[crate::message(register)]
struct ConnEchoRequest {
    text: String,
}

#[crate::message(register)]
struct ConnEchoReply {
    text: String,
}

#[crate::message_handler]
impl MessageHandler for ConnEchoRequest {
    async fn handle(
        self: Box<Self>,
        _stream_ctx: crate::StreamContext,
    ) -> crate::Result<Option<Box<dyn Message>>> {
        Ok(Some(Box::new(ConnEchoReply {
            text: self.text.clone(),
        })))
    }
}

#[crate::message(register)]
struct ConnSlowRequest {}

#[crate::message_handler]
impl MessageHandler for ConnSlowRequest {
    async fn handle(
        self: Box<Self>,
        _stream_ctx: crate::StreamContext,
    ) -> crate::Result<Option<Box<dyn Message>>> {
        tokio::time::sleep(Duration::from_millis(300)).await;
        Ok(None)
    }
}

#[crate::message(register)]
struct ConnRecord {}

static RECORDED: AtomicUsize = AtomicUsize::new(0);

#[crate::message_handler]
impl MessageHandler for ConnRecord {
    async fn handle(
        self: Box<Self>,
        _stream_ctx: crate::StreamContext,
    ) -> crate::Result<Option<Box<dyn Message>>> {
        RECORDED.fetch_add(1, Ordering::SeqCst);
        Ok(None)
    }
}

/// Makes the server send `count` replies and then close the connection.
#[crate::message(register)]
struct ConnPushThenClose {
    count: usize,
}

#[crate::message_handler]
impl MessageHandler for ConnPushThenClose {
    async fn handle(
        self: Box<Self>,
        stream_ctx: crate::StreamContext,
    ) -> crate::Result<Option<Box<dyn Message>>> {
        for i in 0..self.count {
            stream_ctx
                .stream
                .send(ConnEchoReply {
                    text: i.to_string(),
                })
                .await?;
        }
        stream_ctx.stream.connection.close().await;
        Ok(None)
    }
}

/// Counted separately from `ConnRecord`, which other tests send concurrently.
#[crate::message(register)]
struct ConnReopenProbe {}

static REOPEN_PROBES: AtomicUsize = AtomicUsize::new(0);

#[crate::message_handler]
impl MessageHandler for ConnReopenProbe {
    async fn handle(
        self: Box<Self>,
        _stream_ctx: crate::StreamContext,
    ) -> crate::Result<Option<Box<dyn Message>>> {
        REOPEN_PROBES.fetch_add(1, Ordering::SeqCst);
        Ok(None)
    }
}

/// Makes the server close the stream it arrives on.
#[crate::message(register)]
struct ConnCloseStream {}

#[crate::message_handler]
impl MessageHandler for ConnCloseStream {
    async fn handle(
        self: Box<Self>,
        stream_ctx: crate::StreamContext,
    ) -> crate::Result<Option<Box<dyn Message>>> {
        stream_ctx.stream.close();
        Ok(None)
    }
}

fn ephemeral_tcp_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral tcp addr");
    let addr = listener.local_addr().expect("read local addr");
    drop(listener);
    addr.to_string()
}

async fn start_test_server(server: Server, addr: &str) -> JoinHandle<()> {
    let serve_addr = addr.to_string();
    let handle = tokio::spawn(async move {
        let _ = server.serve(&serve_addr).await;
    });
    for _ in 0..50u32 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
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
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    done()
}

fn disconnect_counter(server: Server) -> (Server, Arc<AtomicUsize>) {
    let disconnects = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&disconnects);
    let server = server.on_disconnect(move |_| {
        counter.fetch_add(1, Ordering::SeqCst);
        Ok(())
    });
    (server, disconnects)
}

#[tokio::test]
async fn send_recv_while_receiving_sends_nothing() {
    let addr = ephemeral_tcp_addr();
    let _server = start_test_server(Server::new(), &addr).await;
    let conn = Client::new()
        .connect(&format!("tcp://{addr}"))
        .await
        .expect("connect");
    let stream = conn.new_stream();
    stream.set_recv_timeout(Duration::from_millis(300));

    let receiver = {
        let stream = stream.clone();
        tokio::spawn(async move { stream.recv::<OkReply>().await })
    };
    assert!(wait_until(Duration::from_secs(1), || stream.recv_in_progress()).await);
    let result = stream.send_recv::<OkReply>(ConnRecord {}).await;
    assert!(
        matches!(result, Err(Error::ConcurrentRecv)),
        "got {:?}",
        result.err()
    );
    // Had the request been sent, its OkReply would have reached the recv.
    let received = receiver.await.expect("receiver task");
    assert!(
        matches!(received, Err(Error::RecvTimeout)),
        "got {:?}",
        received.err()
    );
    conn.close().await;
}

#[tokio::test]
async fn send_recv_discards_late_reply_after_timeout() {
    let addr = ephemeral_tcp_addr();
    let _server = start_test_server(Server::new(), &addr).await;
    let conn = Client::new()
        .connect(&format!("tcp://{addr}"))
        .await
        .expect("connect");
    let stream = conn.new_stream();

    stream.set_recv_timeout(Duration::from_millis(100));
    let slow = stream.send_recv::<OkReply>(ConnSlowRequest {}).await;
    assert!(
        matches!(slow, Err(Error::RecvTimeout)),
        "got {:?}",
        slow.err()
    );
    stream.set_recv_timeout(Duration::from_secs(2));
    let reply: ConnEchoReply = stream
        .send_recv(ConnEchoRequest {
            text: "second".into(),
        })
        .await
        .expect("second send_recv");
    assert_eq!(reply.text, "second");
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_send_recv_on_one_stream_never_mismatches() {
    let addr = ephemeral_tcp_addr();
    let _server = start_test_server(Server::new(), &addr).await;
    let conn = Client::new()
        .with_recv_timeout(Duration::from_secs(2))
        .connect(&format!("tcp://{addr}"))
        .await
        .expect("connect");

    let mut tasks = Vec::new();
    for g in 0..8 {
        let conn = conn.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..50 {
                let text = format!("{g}-{i}");
                let result: Result<ConnEchoReply, Error> =
                    conn.send_recv(ConnEchoRequest { text: text.clone() }).await;
                match result {
                    Ok(reply) => assert_eq!(reply.text, text, "request got another reply"),
                    Err(Error::ConcurrentRecv) => {}
                    Err(err) => panic!("send_recv: {err}"),
                }
            }
        }));
    }
    for task in tasks {
        task.await.expect("task");
    }
    conn.close().await;
}

#[tokio::test]
async fn server_with_max_frame_limits_connection() {
    let addr = ephemeral_tcp_addr();
    let _server = start_test_server(Server::new().with_max_frame(1024), &addr).await;
    let conn = Client::new()
        .with_recv_timeout(Duration::from_secs(2))
        .connect(&format!("tcp://{addr}"))
        .await
        .expect("connect");

    let big = conn
        .send(ConnEchoRequest {
            text: "x".repeat(2048),
        })
        .await;
    assert!(
        matches!(big, Err(Error::FrameTooLarge(_))),
        "got {:?}",
        big.err()
    );
    let reply: ConnEchoReply = conn
        .send_recv(ConnEchoRequest {
            text: "small".into(),
        })
        .await
        .expect("send_recv after the rejected send");
    assert_eq!(reply.text, "small");
    conn.close().await;
}

#[tokio::test]
async fn server_closes_connection_on_frame_above_max_frame() {
    let addr = ephemeral_tcp_addr();
    let _server = start_test_server(Server::new(), &addr).await;
    let mut socket = tokio::net::TcpStream::connect(&addr).await.expect("dial");
    let (mut reader, mut writer) = socket.split();
    let config = handshake_client(
        &mut reader,
        &mut writer,
        &[CodecMsgpackCompact],
        DEFAULT_MAX_FRAME,
        PROTOCOL_VERSION_V2,
    )
    .await
    .expect("handshake");
    assert_eq!(config.max_frame, DEFAULT_MAX_FRAME);

    // Send only the header: the server must reject it without waiting for, or
    // allocating, the payload.
    let mut header = [0u8; 10];
    header[0] = PROTOCOL_VERSION_V2;
    header[6..10].copy_from_slice(&(DEFAULT_MAX_FRAME + 1).to_be_bytes());
    writer.write_all(&header).await.expect("write header");
    let mut buf = [0u8; 1];
    let read = tokio::time::timeout(Duration::from_secs(2), reader.read(&mut buf))
        .await
        .expect("server did not close the connection");
    assert!(matches!(read, Ok(0)), "got {read:?}");
}

#[tokio::test]
async fn locally_closed_peer_stream_is_not_reopened() {
    let opened = Arc::new(AtomicUsize::new(0));
    let server = {
        let opened = Arc::clone(&opened);
        Server::new().on_new_stream(move |_| {
            opened.fetch_add(1, Ordering::SeqCst);
        })
    };
    let addr = ephemeral_tcp_addr();
    let _server = start_test_server(server, &addr).await;
    let conn = Client::new()
        .connect(&format!("tcp://{addr}"))
        .await
        .expect("connect");
    let stream = conn.new_stream();

    stream.send(ConnCloseStream {}).await.expect("send");
    assert!(
        wait_until(Duration::from_secs(2), || opened.load(Ordering::SeqCst)
            == 1)
        .await
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    stream
        .send(ConnReopenProbe {})
        .await
        .expect("send after the server closed");
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(
        opened.load(Ordering::SeqCst),
        1,
        "server opened the closed stream again"
    );
    assert_eq!(
        REOPEN_PROBES.load(Ordering::SeqCst),
        0,
        "server handled a message on a stream it had closed"
    );
    conn.close().await;
}

#[tokio::test]
async fn close_delivers_queued_messages() {
    for recovery in [false, true] {
        let mut server = Server::new();
        let mut client = Client::new();
        if recovery {
            server = server.with_recovery(ServerRecoveryOptions {
                enable: true,
                ..ServerRecoveryOptions::default()
            });
            client = client.with_recovery(ClientRecoveryOptions {
                enable: true,
                ..ClientRecoveryOptions::default()
            });
        }
        let addr = ephemeral_tcp_addr();
        let _server = start_test_server(server, &addr).await;

        const ROUNDS: usize = 20;
        const PER_ROUND: usize = 10;
        let before = RECORDED.load(Ordering::SeqCst);
        for _ in 0..ROUNDS {
            let conn = client
                .connect(&format!("tcp://{addr}"))
                .await
                .expect("connect");
            for _ in 0..PER_ROUND {
                conn.send(ConnRecord {}).await.expect("send");
            }
            conn.close().await;
        }
        let want = ROUNDS * PER_ROUND;
        wait_until(Duration::from_secs(5), || {
            RECORDED.load(Ordering::SeqCst) - before >= want
        })
        .await;
        assert_eq!(
            RECORDED.load(Ordering::SeqCst) - before,
            want,
            "recovery={recovery}: server lost messages sent before close"
        );
    }
}

#[tokio::test]
async fn message_larger_than_replay_buffer_fails_only_that_send() {
    let server = Server::new().with_recovery(ServerRecoveryOptions {
        enable: true,
        ..ServerRecoveryOptions::default()
    });
    let addr = ephemeral_tcp_addr();
    let _server = start_test_server(server, &addr).await;
    let conn = Client::new()
        .with_recv_timeout(Duration::from_secs(2))
        .with_recovery(ClientRecoveryOptions {
            enable: true,
            max_replay_bytes: 1 << 10,
            ..ClientRecoveryOptions::default()
        })
        .connect(&format!("tcp://{addr}"))
        .await
        .expect("connect");

    let big = conn
        .send(ConnEchoRequest {
            text: "x".repeat(2 << 10),
        })
        .await;
    assert!(
        matches!(big, Err(Error::FrameTooLarge(_))),
        "got {:?}",
        big.err()
    );
    let reply: ConnEchoReply = conn
        .send_recv(ConnEchoRequest {
            text: "small".into(),
        })
        .await
        .expect("send_recv after the rejected send");
    assert_eq!(reply.text, "small");
    conn.close().await;
}

#[tokio::test]
async fn send_waits_while_replay_buffer_is_full() {
    let server = Server::new().with_recovery(ServerRecoveryOptions {
        enable: true,
        ..ServerRecoveryOptions::default()
    });
    let addr = ephemeral_tcp_addr();
    let _server = start_test_server(server, &addr).await;
    let conn = Client::new()
        .with_recv_timeout(Duration::from_secs(5))
        .with_recovery(ClientRecoveryOptions {
            enable: true,
            max_replay_bytes: 4 << 10,
            ..ClientRecoveryOptions::default()
        })
        .connect(&format!("tcp://{addr}"))
        .await
        .expect("connect");

    // 50 KiB through a 4 KiB buffer: each send waits for room instead of failing.
    const COUNT: usize = 50;
    let text = "x".repeat(1 << 10);
    for i in 0..COUNT {
        conn.send(ConnEchoRequest {
            text: format!("{i}{text}"),
        })
        .await
        .unwrap_or_else(|err| panic!("send {i}: {err}"));
    }
    for i in 0..COUNT {
        let reply: ConnEchoReply = conn
            .recv()
            .await
            .unwrap_or_else(|err| panic!("recv {i}: {err}"));
        assert!(reply.text == format!("{i}{text}"), "reply {i} out of order");
    }
    conn.close().await;
}

#[tokio::test]
async fn send_after_close_fails() {
    let addr = ephemeral_tcp_addr();
    let _server = start_test_server(Server::new(), &addr).await;
    let conn = Client::new()
        .connect(&format!("tcp://{addr}"))
        .await
        .expect("connect");
    conn.close().await;
    let result = conn.send(ConnRecord {}).await;
    assert!(
        matches!(result, Err(Error::Closed)),
        "got {:?}",
        result.err()
    );
}

#[tokio::test]
async fn recovery_close_releases_server_connection() {
    let (server, disconnects) =
        disconnect_counter(Server::new().with_recovery(ServerRecoveryOptions {
            enable: true,
            detached_ttl: Duration::from_secs(10),
            ..ServerRecoveryOptions::default()
        }));
    let addr = ephemeral_tcp_addr();
    let _server = start_test_server(server, &addr).await;
    let conn = Client::new()
        .with_recovery(ClientRecoveryOptions {
            enable: true,
            ..ClientRecoveryOptions::default()
        })
        .connect(&format!("tcp://{addr}"))
        .await
        .expect("connect");
    assert_eq!(
        conn.debug_state().protocol,
        crate::protocol::PROTOCOL_VERSION_V3
    );

    conn.close().await;
    assert!(
        wait_until(Duration::from_secs(2), || disconnects
            .load(Ordering::SeqCst)
            == 1)
        .await,
        "server kept the recovery connection after close"
    );
}

#[tokio::test]
async fn recv_gets_messages_sent_before_peer_close() {
    for recovery in [false, true] {
        let mut server = Server::new();
        let mut client = Client::new().with_recv_timeout(Duration::from_secs(2));
        if recovery {
            server = server.with_recovery(ServerRecoveryOptions {
                enable: true,
                ..ServerRecoveryOptions::default()
            });
            client = client.with_recovery(ClientRecoveryOptions {
                enable: true,
                ..ClientRecoveryOptions::default()
            });
        }
        let addr = ephemeral_tcp_addr();
        let _server = start_test_server(server, &addr).await;
        let conn = client
            .connect(&format!("tcp://{addr}"))
            .await
            .expect("connect");

        const COUNT: usize = 100;
        conn.send(ConnPushThenClose { count: COUNT })
            .await
            .expect("send");
        for i in 0..COUNT {
            let reply: ConnEchoReply = conn
                .recv()
                .await
                .unwrap_or_else(|err| panic!("recovery={recovery}: recv {i}: {err}"));
            assert_eq!(reply.text, i.to_string());
        }
        let after = conn.recv::<ConnEchoReply>().await;
        assert!(
            matches!(after, Err(Error::Closed)),
            "recovery={recovery}: got {:?}",
            after.err()
        );
    }
}

#[tokio::test]
async fn close_returns_when_peer_stops_reading() {
    // The peer completes the handshake and then never reads.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind peer");
    let addr = listener.local_addr().expect("peer addr");
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept");
        let (mut reader, mut writer) = socket.split();
        let _ = handshake_server(
            &mut reader,
            &mut writer,
            &[CodecMsgpackCompact],
            DEFAULT_MAX_FRAME,
            false,
        )
        .await;
        tokio::time::sleep(Duration::from_secs(30)).await;
    });
    let conn = Client::new()
        .with_codecs(&[CodecMsgpackCompact])
        .connect(&format!("tcp://{addr}"))
        .await
        .expect("connect");
    let big = "x".repeat(1 << 20);
    for _ in 0..32 {
        conn.send(ConnEchoRequest { text: big.clone() })
            .await
            .expect("send");
    }

    let start = Instant::now();
    conn.close().await;
    assert!(
        start.elapsed() < Duration::from_secs(6),
        "close returned after {:?}",
        start.elapsed()
    );
}

#[tokio::test]
async fn dropping_the_last_handle_closes_the_connection() {
    let (server, disconnects) = disconnect_counter(Server::new());
    let addr = ephemeral_tcp_addr();
    let _server = start_test_server(server, &addr).await;
    let conn = Client::new()
        .connect(&format!("tcp://{addr}"))
        .await
        .expect("connect");
    let clone = conn.clone();

    drop(conn);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        disconnects.load(Ordering::SeqCst),
        0,
        "closed while a clone was alive"
    );
    drop(clone);
    assert!(
        wait_until(Duration::from_secs(2), || disconnects
            .load(Ordering::SeqCst)
            == 1)
        .await,
        "connection stayed open after its last handle was dropped"
    );
}

#[tokio::test]
async fn a_stream_handle_keeps_the_connection_open() {
    let (server, disconnects) = disconnect_counter(Server::new());
    let addr = ephemeral_tcp_addr();
    let _server = start_test_server(server, &addr).await;
    let stream = Client::new()
        .with_recv_timeout(Duration::from_secs(2))
        .connect(&format!("tcp://{addr}"))
        .await
        .expect("connect")
        .new_stream();

    let reply: ConnEchoReply = stream
        .send_recv(ConnEchoRequest {
            text: "still open".into(),
        })
        .await
        .expect("send_recv on a stream whose connection handle was dropped");
    assert_eq!(reply.text, "still open");
    assert_eq!(disconnects.load(Ordering::SeqCst), 0);

    drop(stream);
    assert!(
        wait_until(Duration::from_secs(2), || disconnects
            .load(Ordering::SeqCst)
            == 1)
        .await,
        "connection stayed open after its last stream handle was dropped"
    );
}
