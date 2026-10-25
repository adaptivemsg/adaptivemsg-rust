use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use tokio::task::JoinHandle;

use crate::message::{Message, MessageHandler};
use crate::stream::StreamInner;
use crate::{Client, Error, Server};

#[crate::message(register)]
struct CloseTestNote {}

#[crate::message(register)]
struct CloseTestSlowRequest {}

static SLOW_HANDLER_STREAM: Mutex<Option<Weak<StreamInner>>> = Mutex::new(None);
static RELEASE_SLOW_HANDLER: AtomicBool = AtomicBool::new(false);

#[crate::message_handler]
impl MessageHandler for CloseTestSlowRequest {
    async fn handle(
        self: Box<Self>,
        stream_ctx: crate::StreamContext,
    ) -> crate::Result<Option<Box<dyn Message>>> {
        *SLOW_HANDLER_STREAM.lock().unwrap() = Some(Arc::downgrade(&stream_ctx.stream));
        while !RELEASE_SLOW_HANDLER.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Ok(None)
    }
}

#[crate::message(register)]
struct CloseTestRecord {}

static RECORDED: AtomicUsize = AtomicUsize::new(0);

#[crate::message_handler]
impl MessageHandler for CloseTestRecord {
    async fn handle(
        self: Box<Self>,
        _stream_ctx: crate::StreamContext,
    ) -> crate::Result<Option<Box<dyn Message>>> {
        RECORDED.fetch_add(1, Ordering::SeqCst);
        Ok(None)
    }
}

fn ephemeral_tcp_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral tcp addr");
    let addr = listener.local_addr().expect("read local addr");
    drop(listener);
    addr.to_string()
}

async fn start_test_server(addr: &str) -> JoinHandle<()> {
    let serve_addr = addr.to_string();
    tokio::spawn(async move {
        let _ = Server::new().serve(&serve_addr).await;
    })
}

async fn wait_server_ready(addr: &str) {
    for _ in 0..50u32 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("server did not become ready");
}

async fn wait_until(timeout: Duration, mut done: impl FnMut() -> bool) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if done() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    done()
}

async fn connect_test_server() -> crate::Connection {
    let addr = ephemeral_tcp_addr();
    let _server = start_test_server(&addr).await;
    wait_server_ready(&addr).await;
    Client::new()
        .connect(&format!("tcp://{addr}"))
        .await
        .expect("connect")
}

#[tokio::test(flavor = "multi_thread")]
async fn handler_task_exits_when_connection_closes_during_handler() {
    let conn = connect_test_server().await;
    conn.new_stream()
        .send(CloseTestSlowRequest {})
        .await
        .expect("send");

    assert!(
        wait_until(Duration::from_secs(2), || SLOW_HANDLER_STREAM
            .lock()
            .unwrap()
            .is_some())
        .await,
        "handler did not start"
    );
    let server_stream = SLOW_HANDLER_STREAM.lock().unwrap().clone().unwrap();

    conn.close().await;
    assert!(
        wait_until(Duration::from_secs(2), || match server_stream.upgrade() {
            Some(stream) => stream.connection.is_closed(),
            None => true,
        })
        .await,
        "server did not see the close"
    );

    RELEASE_SLOW_HANDLER.store(true, Ordering::Release);
    assert!(
        wait_until(Duration::from_secs(2), || server_stream.upgrade().is_none()).await,
        "handler task still holds the stream after the connection closed"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn messages_sent_before_close_are_delivered() {
    let conn = connect_test_server().await;
    const STREAMS: usize = 200;
    RECORDED.store(0, Ordering::SeqCst);
    for _ in 0..STREAMS {
        let stream = conn.new_stream();
        stream.send(CloseTestRecord {}).await.expect("send");
        stream.close();
    }

    wait_until(Duration::from_secs(2), || {
        RECORDED.load(Ordering::SeqCst) == STREAMS
    })
    .await;
    assert_eq!(
        RECORDED.load(Ordering::SeqCst),
        STREAMS,
        "messages sent before close were not all handled"
    );
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn recv_returns_closed_when_stream_is_closed_while_waiting() {
    let conn = connect_test_server().await;
    let stream = conn.new_stream();
    let waiter = tokio::spawn({
        let stream = stream.clone();
        async move { stream.recv::<CloseTestNote>().await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    stream.close();
    let result = tokio::time::timeout(Duration::from_secs(2), waiter)
        .await
        .expect("recv still blocked after the stream closed")
        .expect("recv task panicked");
    assert!(
        matches!(result, Err(Error::Closed)),
        "got {:?}",
        result.err()
    );
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn connection_is_freed_after_close() {
    for use_default_stream in [false, true] {
        let conn = connect_test_server().await;
        if use_default_stream {
            conn.send(CloseTestNote {}).await.expect("send");
        } else {
            conn.new_stream()
                .send(CloseTestNote {})
                .await
                .expect("send");
        }

        conn.close().await;
        assert!(matches!(
            conn.send(CloseTestNote {}).await,
            Err(Error::Closed)
        ));
        assert!(matches!(
            conn.new_stream().send(CloseTestNote {}).await,
            Err(Error::Closed)
        ));

        let weak = Arc::downgrade(&conn);
        drop(conn);
        assert!(
            wait_until(Duration::from_secs(2), || weak.upgrade().is_none()).await,
            "connection not freed after close (use_default_stream={use_default_stream})"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn wait_closed_returns_when_connection_is_closed() {
    let conn = connect_test_server().await;
    let waiter = tokio::spawn({
        let conn = conn.clone();
        async move { conn.wait_closed().await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    conn.close().await;
    tokio::time::timeout(Duration::from_secs(2), waiter)
        .await
        .expect("wait_closed still blocked after close")
        .expect("wait_closed task panicked");
}
