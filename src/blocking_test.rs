use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::blocking::Client;
use crate::message::{Message, MessageHandler};
use crate::{Error, OkReply, Server};

#[crate::message(register)]
struct BlockingEchoRequest {
    text: String,
    delay_ms: u64,
}

#[crate::message(register)]
struct BlockingEchoReply {
    text: String,
}

#[crate::message(register)]
struct BlockingFail {}

/// Asks the server to push `count` events on the stream.
#[crate::message(register)]
struct BlockingSubscribe {
    count: u32,
}

#[crate::message(register)]
struct BlockingEvent {
    seq: u32,
}

#[crate::message(register)]
struct BlockingNote {}

static NOTES: AtomicUsize = AtomicUsize::new(0);

#[crate::message_handler]
impl MessageHandler for BlockingEchoRequest {
    async fn handle(
        self: Box<Self>,
        _stream_ctx: crate::StreamContext,
    ) -> crate::Result<Option<Box<dyn Message>>> {
        tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
        Ok(Some(Box::new(BlockingEchoReply {
            text: self.text.clone(),
        })))
    }
}

#[crate::message_handler]
impl MessageHandler for BlockingFail {
    async fn handle(
        self: Box<Self>,
        _stream_ctx: crate::StreamContext,
    ) -> crate::Result<Option<Box<dyn Message>>> {
        Err(anyhow::anyhow!("boom"))
    }
}

#[crate::message_handler]
impl MessageHandler for BlockingSubscribe {
    async fn handle(
        self: Box<Self>,
        stream_ctx: crate::StreamContext,
    ) -> crate::Result<Option<Box<dyn Message>>> {
        let count = self.count;
        stream_ctx.new_task(move |stream| async move {
            for seq in 0..count {
                if stream.send(BlockingEvent { seq }).await.is_err() {
                    return;
                }
            }
        })?;
        Ok(None)
    }
}

#[crate::message_handler]
impl MessageHandler for BlockingNote {
    async fn handle(
        self: Box<Self>,
        _stream_ctx: crate::StreamContext,
    ) -> crate::Result<Option<Box<dyn Message>>> {
        NOTES.fetch_add(1, Ordering::Relaxed);
        Ok(None)
    }
}

fn ephemeral_tcp_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral addr");
    let addr = listener.local_addr().expect("local addr").to_string();
    drop(listener);
    addr
}

/// Runs `serve` on a tokio runtime on a thread of its own; the test itself
/// runs no runtime.
fn spawn_runtime<F>(serve: impl FnOnce() -> F + Send + 'static)
where
    F: std::future::Future<Output = ()>,
{
    thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("server runtime");
        runtime.block_on(serve());
    });
}

fn spawn_server(server: Server, addr: String) {
    spawn_runtime(move || async move {
        let _ = server.serve(&addr).await;
    });
}

/// Serves `server` on a bound listener until `shutdown` completes. Binding
/// before the test connects keeps a parallel test from taking the port.
pub(crate) fn serve_listener(
    server: Server,
    listener: std::net::TcpListener,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) {
    listener
        .set_nonblocking(true)
        .expect("nonblocking listener");
    spawn_runtime(move || async move {
        let listener =
            Arc::new(tokio::net::TcpListener::from_std(listener).expect("tokio listener"));
        let accept = move || {
            let listener = Arc::clone(&listener);
            async move {
                let (stream, peer) = crate::transport::tcp::accept_stream(listener).await?;
                let (reader, writer) = stream.into_split();
                Ok((crate::transport::parts(reader, writer), peer))
            }
        };
        let _ = server.serve_with_accept(accept, shutdown).await;
    });
}

/// Starts `server` on a new port and returns its address.
pub(crate) fn start_server_until(
    server: Server,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind server");
    let addr = format!("tcp://{}", listener.local_addr().expect("server addr"));
    serve_listener(server, listener, shutdown);
    addr
}

pub(crate) fn start_server(server: Server) -> String {
    start_server_until(server, std::future::pending())
}

fn echo(text: &str) -> BlockingEchoRequest {
    BlockingEchoRequest {
        text: text.to_string(),
        delay_ms: 0,
    }
}

pub(crate) fn wait_for(what: &str, cond: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn blocking_send_recv_and_remote_error() {
    let addr = start_server(Server::new());
    let conn = Client::new().connect(&addr).expect("connect");

    let reply: BlockingEchoReply = conn.send_recv(echo("hi")).expect("send_recv");
    assert_eq!(reply.text, "hi");

    match conn.send_recv::<BlockingEchoReply>(BlockingFail {}) {
        Err(Error::Remote { code, message }) => {
            assert_eq!(code, "handler_error");
            assert!(message.contains("boom"), "{message}");
        }
        other => panic!("want a remote error, got {:?}", other.map(|r| r.text)),
    }
    // The stream is still usable after a remote error.
    let reply: BlockingEchoReply = conn.send_recv(echo("again")).expect("send_recv");
    assert_eq!(reply.text, "again");
    conn.close();
    assert!(conn.is_closed());
    assert!(matches!(conn.send(echo("late")), Err(Error::Closed)));
}

#[test]
fn blocking_concurrent_streams() {
    let addr = start_server(Server::new());
    let conn = Client::new().connect(&addr).expect("connect");
    thread::scope(|scope| {
        for worker in 0..8 {
            let conn = &conn;
            scope.spawn(move || {
                let stream = conn.new_stream();
                for i in 0..20 {
                    let text = format!("{worker}-{i}");
                    let reply: BlockingEchoReply =
                        stream.send_recv(echo(&text)).expect("send_recv");
                    assert_eq!(reply.text, text);
                }
                stream.close();
            });
        }
    });
    conn.close();
}

#[test]
fn blocking_recv_timeout_discards_late_reply() {
    let addr = start_server(Server::new());
    let conn = Client::new().connect(&addr).expect("connect");
    let stream = conn.new_stream();
    stream.set_recv_timeout(Duration::from_millis(50));
    let slow = BlockingEchoRequest {
        text: "slow".into(),
        delay_ms: 200,
    };
    assert!(matches!(
        stream.send_recv::<BlockingEchoReply>(slow),
        Err(Error::RecvTimeout)
    ));
    thread::sleep(Duration::from_millis(300));
    stream.set_recv_timeout(Duration::ZERO);
    let reply: BlockingEchoReply = stream.send_recv(echo("next")).expect("send_recv");
    assert_eq!(reply.text, "next");
    conn.close();
}

#[test]
fn blocking_server_push() {
    let addr = start_server(Server::new());
    let conn = Client::new()
        .with_recv_timeout(Duration::from_secs(10))
        .connect(&addr)
        .expect("connect");
    let events = conn.new_stream();
    events
        .send(BlockingSubscribe { count: 5 })
        .expect("subscribe");

    // The handler's OkReply and the pushed events may arrive in any order.
    let mut seqs = Vec::new();
    let mut acked = false;
    while seqs.len() < 5 || !acked {
        if events.peek_wire().expect("peek") == OkReply::wire_name_static() {
            let _: OkReply = events.recv().expect("ok reply");
            acked = true;
        } else {
            seqs.push(events.recv::<BlockingEvent>().expect("event").seq);
        }
    }
    assert_eq!(seqs, vec![0, 1, 2, 3, 4]);
    conn.close();
}

#[test]
fn blocking_close_delivers_queued_sends() {
    let addr = start_server(Server::new());
    let conn = Client::new().connect(&addr).expect("connect");
    let before = NOTES.load(Ordering::Relaxed);
    for _ in 0..100 {
        conn.send(BlockingNote {}).expect("send");
    }
    let started = Instant::now();
    conn.close();
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "close waited for the timeout"
    );
    wait_for("the server to handle every note", || {
        NOTES.load(Ordering::Relaxed) >= before + 100
    });
}

#[test]
fn blocking_drop_closes_connection() {
    let disconnects = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&disconnects);
    let server = Server::new().on_disconnect(move |_| {
        seen.fetch_add(1, Ordering::Relaxed);
        Ok(())
    });
    let addr = start_server(server);
    let conn = Client::new().connect(&addr).expect("connect");
    let stream = conn.new_stream();
    let reply: BlockingEchoReply = stream.send_recv(echo("x")).expect("send_recv");
    assert_eq!(reply.text, "x");
    drop(conn);
    // The stream still holds the connection open.
    let reply: BlockingEchoReply = stream.send_recv(echo("y")).expect("send_recv");
    assert_eq!(reply.text, "y");
    drop(stream);
    wait_for("on_disconnect", || disconnects.load(Ordering::Relaxed) == 1);
}

#[test]
fn blocking_connect_timeout_waits_for_server() {
    let addr = ephemeral_tcp_addr();
    let serve_addr = format!("tcp://{addr}");
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(300));
        spawn_server(Server::new(), serve_addr);
    });
    let started = Instant::now();
    let conn = Client::new()
        .with_connect_timeout(Duration::from_secs(5))
        .connect(&format!("tcp://{addr}"))
        .expect("connect once the server is up");
    assert!(started.elapsed() >= Duration::from_millis(250));
    let reply: BlockingEchoReply = conn.send_recv(echo("late start")).expect("send_recv");
    assert_eq!(reply.text, "late start");
    conn.close();
}

#[test]
fn blocking_connect_errors() {
    match Client::new().connect("tcp://127.0.0.1:1") {
        Err(Error::Io(err)) => assert_eq!(err.kind(), std::io::ErrorKind::ConnectionRefused),
        other => panic!("want connection refused, got {:?}", other.err()),
    }
    let started = Instant::now();
    match Client::new()
        .with_connect_timeout(Duration::from_millis(300))
        .connect("tcp://127.0.0.1:1")
    {
        Err(Error::ConnectTimeout(Some(_))) => {}
        other => panic!("want a connect timeout, got {:?}", other.err()),
    }
    assert!(started.elapsed() >= Duration::from_millis(250));
}

#[test]
fn blocking_type_mismatch_closes_stream() {
    let addr = start_server(Server::new());
    let conn = Client::new().connect(&addr).expect("connect");
    let stream = conn.new_stream();
    // The server answers a note with OkReply.
    match stream.send_recv::<BlockingEchoReply>(BlockingNote {}) {
        Err(Error::TypeMismatch { got, .. }) => assert_eq!(got, OkReply::wire_name_static()),
        other => panic!("want a type mismatch, got {:?}", other.map(|r| r.text)),
    }
    assert!(matches!(stream.send(echo("after")), Err(Error::Closed)));
    // Other streams are unaffected.
    let reply: BlockingEchoReply = conn.send_recv(echo("default")).expect("send_recv");
    assert_eq!(reply.text, "default");
    conn.close();
}

#[test]
fn blocking_debug_state() {
    let addr = start_server(Server::new());
    let conn = Client::new().connect(&addr).expect("connect");
    let stream = conn.new_stream();
    let _: BlockingEchoReply = stream.send_recv(echo("x")).expect("send_recv");
    stream.set_recv_timeout(Duration::from_millis(20));
    assert!(matches!(
        stream.recv::<BlockingEchoReply>(),
        Err(Error::RecvTimeout)
    ));

    let state = stream.debug_state();
    assert_eq!(state.id, stream.id());
    assert!(!state.closed);
    assert_eq!(state.recv_timeout, Duration::from_millis(20));
    assert_eq!(state.counters.data_messages_sent, 1);
    assert_eq!(state.counters.data_messages_received, 1);
    assert_eq!(
        state.last_failure_code,
        crate::debug::FAILURE_STREAM_RECV_TIMEOUT
    );

    let state = conn.debug_state();
    assert!(!state.closed);
    assert_eq!(state.protocol, crate::protocol::PROTOCOL_VERSION_V2);
    assert_eq!(state.stream_count, 2);
    assert!(state.recovery.is_none());
    assert_eq!(state.counters.streams_opened, 2);
    assert_eq!(state.counters.data_messages_sent, 1);
    assert_eq!(state.counters.frames_written, 1);
    assert_eq!(state.counters.frames_read, 1);
    assert_eq!(state.counters.transport_attaches, 1);

    stream.close();
    assert_eq!(conn.debug_state().counters.streams_closed, 1);
    conn.close();
    assert!(conn.debug_state().closed);
}

#[test]
fn blocking_connect_once() {
    let addr = start_server(Server::new());
    let reply: BlockingEchoReply = Client::new()
        .with_recv_timeout(Duration::from_secs(5))
        .connect_once(&addr)
        .send_recv(echo("once"))
        .expect("connect_once");
    assert_eq!(reply.text, "once");
}

#[cfg(target_os = "linux")]
#[test]
fn blocking_abstract_uds() {
    let addr = format!("uds://@am-blocking-test-{}", std::process::id());
    spawn_server(Server::new(), addr.clone());
    let conn = Client::new()
        .with_connect_timeout(Duration::from_secs(5))
        .connect(&addr)
        .expect("connect");
    let reply: BlockingEchoReply = conn.send_recv(echo("uds")).expect("send_recv");
    assert_eq!(reply.text, "uds");
    conn.close();
}
