use std::net::TcpListener;
use std::time::{Duration, Instant};

use tokio::io::AsyncWriteExt;
use tokio::task::JoinHandle;

use crate::message::{Message, MessageHandler};
use crate::recovery::ServerRecoveryOptions;
use crate::{Client, ClientRecoveryOptions, Error, Server};

#[crate::message(register)]
struct ClientEchoRequest {
    text: String,
}

#[crate::message(register)]
struct ClientEchoReply {
    text: String,
}

#[crate::message(register)]
struct ClientNote {}

/// Asks the server for the protocol version its connection negotiated.
#[crate::message(register)]
struct ClientProtocolRequest {}

#[crate::message(register)]
struct ClientProtocolReply {
    version: u8,
}

#[crate::message_handler]
impl MessageHandler for ClientProtocolRequest {
    async fn handle(
        self: Box<Self>,
        stream_ctx: crate::StreamContext,
    ) -> crate::Result<Option<Box<dyn Message>>> {
        Ok(Some(Box::new(ClientProtocolReply {
            version: stream_ctx.stream.connection.debug_state().protocol,
        })))
    }
}

#[crate::message_handler]
impl MessageHandler for ClientEchoRequest {
    async fn handle(
        self: Box<Self>,
        _stream_ctx: crate::StreamContext,
    ) -> crate::Result<Option<Box<dyn Message>>> {
        Ok(Some(Box::new(ClientEchoReply {
            text: self.text.clone(),
        })))
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

/// Accepts connections, writes `reply` to each instead of running the
/// adaptivemsg protocol, and keeps them open until the test ends.
async fn start_test_peer(reply: &'static [u8]) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test peer");
    let addr = listener.local_addr().expect("test peer addr").to_string();
    tokio::spawn(async move {
        let mut conns = Vec::new();
        while let Ok((mut conn, _)) = listener.accept().await {
            let _ = conn.write_all(reply).await;
            conns.push(conn);
        }
    });
    addr
}

#[tokio::test]
async fn test_client_connect_once() {
    let addr = ephemeral_tcp_addr();
    let _server = start_test_server(Server::new(), &addr).await;

    let reply: ClientEchoReply = Client::new()
        .with_recv_timeout(Duration::from_secs(5))
        .connect_once(&format!("tcp://{addr}"))
        .send_recv(ClientEchoRequest {
            text: "hello".into(),
        })
        .await
        .expect("connect_once send_recv failed");

    assert_eq!(reply.text, "hello");
}

#[tokio::test]
async fn test_client_connect_once_with_options() {
    let addr = ephemeral_tcp_addr();
    let _server = start_test_server(Server::new(), &addr).await;

    let reply: ClientEchoReply = Client::new()
        .with_connect_timeout(Duration::from_secs(5))
        .with_recv_timeout(Duration::from_secs(5))
        .with_codecs(&[crate::codec_msgpack::CodecMsgpackCompact])
        .connect_once(&format!("tcp://{addr}"))
        .send_recv(ClientEchoRequest {
            text: "options".into(),
        })
        .await
        .expect("connect_once send_recv with options failed");

    assert_eq!(reply.text, "options");
}

#[tokio::test]
async fn test_client_connect_once_connect_failure() {
    let result: Result<ClientEchoReply, Error> = Client::new()
        .with_recv_timeout(Duration::from_secs(1))
        .connect_once("tcp://127.0.0.1:1")
        .send_recv(ClientEchoRequest {
            text: "fail".into(),
        })
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_client_connect_once_recv_timeout() {
    let addr = ephemeral_tcp_addr();
    let _server = start_test_server(Server::new(), &addr).await;

    // The server has no handler for ClientNote, so the reply never comes.
    let start = Instant::now();
    let result = Client::new()
        .with_recv_timeout(Duration::from_millis(200))
        .connect_once(&format!("tcp://{addr}"))
        .send_recv::<ClientEchoReply>(ClientNote {})
        .await;

    assert!(
        matches!(result, Err(Error::RecvTimeout)),
        "got {:?}",
        result.err()
    );
    assert!(start.elapsed() < Duration::from_secs(2));
}

#[tokio::test]
async fn test_recv_timeout_applies_to_streams() {
    let addr = ephemeral_tcp_addr();
    let _server = start_test_server(Server::new(), &addr).await;
    let conn = Client::new()
        .with_recv_timeout(Duration::from_millis(200))
        .connect(&format!("tcp://{addr}"))
        .await
        .expect("connect");

    let result = conn.recv::<ClientEchoReply>().await;
    assert!(
        matches!(result, Err(Error::RecvTimeout)),
        "got {:?}",
        result.err()
    );
    let stream = conn.new_stream();
    assert_eq!(
        stream.debug_state().recv_timeout,
        Duration::from_millis(200)
    );
    stream.set_recv_timeout(Duration::from_secs(1));
    assert_eq!(stream.debug_state().recv_timeout, Duration::from_secs(1));
    stream.close();
    conn.close().await;
}

#[tokio::test]
async fn test_connect_timeout_waits_for_server() {
    let addr = ephemeral_tcp_addr();
    let endpoint = format!("tcp://{addr}");
    let connect = tokio::spawn(async move {
        Client::new()
            .with_connect_timeout(Duration::from_secs(5))
            .connect(&endpoint)
            .await
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    let _server = start_test_server(Server::new(), &addr).await;

    let conn = connect.await.expect("connect task").expect("connect");
    conn.close().await;
}

#[tokio::test]
async fn test_connect_retries_until_timeout() {
    let start = Instant::now();
    let result = Client::new()
        .with_connect_timeout(Duration::from_millis(500))
        .connect("tcp://127.0.0.1:1")
        .await;
    let elapsed = start.elapsed();

    // The error carries the last dial failure.
    assert!(
        matches!(&result, Err(Error::ConnectTimeout(Some(cause)))
            if matches!(cause.as_ref(), Error::Io(err) if err.kind() == std::io::ErrorKind::ConnectionRefused)),
        "got {:?}",
        result.err()
    );
    assert!(
        elapsed >= Duration::from_millis(400) && elapsed < Duration::from_secs(2),
        "connect returned after {elapsed:?}, want about 500ms"
    );
}

#[tokio::test]
async fn test_connect_without_timeout_fails_fast() {
    let start = Instant::now();
    let result = Client::new().connect("tcp://127.0.0.1:1").await;

    assert!(
        matches!(&result, Err(Error::Io(err)) if err.kind() == std::io::ErrorKind::ConnectionRefused),
        "got {:?}",
        result.err()
    );
    assert!(start.elapsed() < Duration::from_secs(1));
}

#[tokio::test]
async fn test_connect_does_not_retry_handshake_errors() {
    let addr = start_test_peer(b"HTTP/1.1 400").await;

    let start = Instant::now();
    let result = Client::new()
        .with_connect_timeout(Duration::from_secs(5))
        .connect(&format!("tcp://{addr}"))
        .await;

    assert!(
        matches!(result, Err(Error::BadHandshakeMagic)),
        "got {:?}",
        result.err()
    );
    assert!(start.elapsed() < Duration::from_secs(1));
}

#[tokio::test]
async fn test_connect_timeout_on_silent_peer() {
    let addr = start_test_peer(b"").await;

    let start = Instant::now();
    let result = Client::new()
        .with_connect_timeout(Duration::from_millis(300))
        .connect(&format!("tcp://{addr}"))
        .await;

    assert!(
        matches!(result, Err(Error::ConnectTimeout(None))),
        "got {:?}",
        result.err()
    );
    assert!(start.elapsed() < Duration::from_secs(1));
}

#[tokio::test]
async fn test_connect_once_does_not_use_recovery() {
    let addr = ephemeral_tcp_addr();
    let server = Server::new().with_recovery(ServerRecoveryOptions {
        enable: true,
        ..ServerRecoveryOptions::default()
    });
    let _server = start_test_server(server, &addr).await;
    let endpoint = format!("tcp://{addr}");
    let client = Client::new()
        .with_recv_timeout(Duration::from_secs(5))
        .with_recovery(ClientRecoveryOptions {
            enable: true,
            ..ClientRecoveryOptions::default()
        });

    let conn = client.connect(&endpoint).await.expect("connect");
    let reply: ClientProtocolReply = conn
        .send_recv(ClientProtocolRequest {})
        .await
        .expect("connect send_recv");
    conn.close().await;
    assert_eq!(reply.version, crate::protocol::PROTOCOL_VERSION_V3);

    let reply: ClientProtocolReply = client
        .connect_once(&endpoint)
        .send_recv(ClientProtocolRequest {})
        .await
        .expect("connect_once send_recv");
    assert_eq!(reply.version, crate::protocol::PROTOCOL_VERSION_V2);
}

#[tokio::test]
async fn test_reconnect_attempt_is_limited_by_connect_timeout() {
    let addr = ephemeral_tcp_addr();
    let server = Server::new().with_recovery(ServerRecoveryOptions {
        enable: true,
        detached_ttl: Duration::from_secs(10),
        ..ServerRecoveryOptions::default()
    });
    let server_task = start_test_server(server, &addr).await;
    let conn = Client::new()
        .with_connect_timeout(Duration::from_millis(200))
        .with_recovery(ClientRecoveryOptions {
            enable: true,
            reconnect_min_backoff: Duration::from_millis(5),
            reconnect_max_backoff: Duration::from_millis(20),
            ..ClientRecoveryOptions::default()
        })
        .connect(&format!("tcp://{addr}"))
        .await
        .expect("connect");

    // Replace the server with a peer that accepts but never answers, then
    // drop the transport so the client starts reconnecting.
    server_task.abort();
    let _ = server_task.await;
    let silent = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("rebind server address");
    tokio::spawn(async move {
        let mut conns = Vec::new();
        while let Ok((conn, _)) = silent.accept().await {
            conns.push(conn);
        }
    });
    conn.close_transport_for_test();

    tokio::time::sleep(Duration::from_secs(1)).await;
    let counters = conn.debug_state().counters;
    assert!(
        counters.reconnect_failures >= 2,
        "reconnect attempts did not time out: {} attempts, {} failures",
        counters.reconnect_attempts,
        counters.reconnect_failures
    );
    conn.close().await;
}
