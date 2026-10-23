use std::net::TcpListener;
use std::time::Duration;

use tokio::task::JoinHandle;

use crate::message::{Message, MessageHandler};
use crate::{Client, Error, Server};

#[crate::message(register)]
struct ClientEchoRequest {
    text: String,
}

#[crate::message(register)]
struct ClientEchoReply {
    text: String,
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

#[tokio::test]
async fn test_client_send_recv() {
    let addr = ephemeral_tcp_addr();
    let _server = start_test_server(&addr).await;
    wait_server_ready(&addr).await;
    let endpoint = format!("tcp://{addr}");

    let reply: ClientEchoReply = Client::new()
        .with_timeout(Duration::from_secs(5))
        .send_recv(
            &endpoint,
            ClientEchoRequest {
                text: "hello".into(),
            },
        )
        .await
        .expect("send_recv failed");

    assert_eq!(reply.text, "hello");
}

#[tokio::test]
async fn test_client_send_recv_with_options() {
    let addr = ephemeral_tcp_addr();
    let _server = start_test_server(&addr).await;
    wait_server_ready(&addr).await;
    let endpoint = format!("tcp://{addr}");

    let reply: ClientEchoReply = Client::new()
        .with_timeout(Duration::from_secs(10))
        .with_codecs(&[crate::codec_msgpack::CodecMsgpackCompact])
        .send_recv(
            &endpoint,
            ClientEchoRequest {
                text: "options".into(),
            },
        )
        .await
        .expect("send_recv with options failed");

    assert_eq!(reply.text, "options");
}

#[tokio::test]
async fn test_client_send_recv_connect_failure() {
    let result: Result<ClientEchoReply, Error> = Client::new()
        .with_timeout(Duration::from_secs(1))
        .send_recv(
            "tcp://127.0.0.1:1",
            ClientEchoRequest {
                text: "fail".into(),
            },
        )
        .await;

    assert!(result.is_err());
}
