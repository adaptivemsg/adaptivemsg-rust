use std::time::Duration;

use crate::client::Client;
use crate::codec::CodecID;
use crate::error::Error;
use crate::message::{Message, MessageDecode};

const DEFAULT_ONCE_TIMEOUT: Duration = Duration::from_secs(5);

/// Builder for a short-lived connection.
///
/// Created by [`once()`], configured with optional builder methods, and
/// executed by [`send_recv()`](OnceConn::send_recv). The connection is opened,
/// one request-reply exchange is performed, and the connection is closed
/// automatically.
///
/// This is a thin convenience layer over [`Client`] + [`Connection`](crate::Connection).
/// Each call pays the cost of TCP connect, handshake, and close. For repeated
/// calls to the same server, use a persistent connection instead.
///
/// # Examples
///
/// ```ignore
/// // Minimal — default timeout (5s), default codecs
/// let reply: EchoReply = am::once("tcp://127.0.0.1:8080")
///     .send_recv(EchoReq { text: "hello".into() })
///     .await?;
///
/// // With options
/// let reply: EchoReply = am::once("tcp://127.0.0.1:8080")
///     .with_timeout(Duration::from_secs(10))
///     .with_codecs(&[CodecMsgpackCompact])
///     .send_recv(EchoReq { text: "hello".into() })
///     .await?;
/// ```
pub struct OnceConn {
    addr: String,
    timeout: Duration,
    codecs: Option<Vec<CodecID>>,
}

/// Create a builder for a short-lived connection to `addr`.
///
/// The address format follows [`Client::connect`] — `tcp://host:port`,
/// `uds://path`, or `unix://path`.
///
/// # Examples
///
/// ```ignore
/// let reply: EchoReply = am::once("tcp://127.0.0.1:8080")
///     .send_recv(EchoReq { text: "hello".into() })
///     .await?;
/// ```
pub fn once(addr: &str) -> OnceConn {
    OnceConn {
        addr: addr.to_string(),
        timeout: DEFAULT_ONCE_TIMEOUT,
        codecs: None,
    }
}

impl OnceConn {
    /// Set the dial and receive timeout. Default is 5 seconds.
    ///
    /// The timeout applies to both the initial TCP connect / handshake and the
    /// receive wait for the reply.
    pub fn with_timeout(mut self, d: Duration) -> Self {
        self.timeout = d;
        self
    }

    /// Set the preferred codec list for the short-lived connection.
    ///
    /// When not set, the client default codec list is used
    /// (`CodecPostcard`, `CodecMsgpackCompact`, `CodecMsgpackMap`).
    pub fn with_codecs(mut self, codecs: &[CodecID]) -> Self {
        self.codecs = Some(codecs.to_vec());
        self
    }

    /// Dial, send one request, receive one reply, and close the connection.
    ///
    /// This method consumes `self` — the builder cannot be reused, enforcing
    /// one-shot semantics.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Client::connect`] and `Connection::send_recv`:
    ///
    /// - Connect failure — server unreachable or connection refused.
    /// - Handshake failure — no common codec or version mismatch.
    /// - Timeout — connect or recv exceeded the configured timeout.
    /// - Decode error — reply cannot be deserialized into `Rep`.
    /// - [`Error::Remote`] — server handler returned an error.
    pub async fn send_recv<Req: Message, Rep: MessageDecode + 'static>(
        self,
        req: Req,
    ) -> Result<Rep, Error> {
        let mut client = Client::new().with_timeout(self.timeout);
        if let Some(ref codecs) = self.codecs {
            client = client.with_codecs(codecs);
        }
        let conn = client.connect(&self.addr).await?;
        conn.set_recv_timeout(self.timeout);
        let reply: Rep = conn.send_recv(req).await?;
        conn.close();
        Ok(reply)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::MessageHandler;
    use crate::Server;
    use std::net::TcpListener;
    use tokio::task::JoinHandle;

    #[crate::message(register)]
    struct OnceEchoRequest {
        text: String,
    }

    #[crate::message(register)]
    struct OnceEchoReply {
        text: String,
    }

    #[crate::message_handler]
    impl MessageHandler for OnceEchoRequest {
        async fn handle(
            self: Box<Self>,
            _stream_ctx: crate::StreamContext,
        ) -> crate::Result<Option<Box<dyn Message>>> {
            Ok(Some(Box::new(OnceEchoReply {
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
    async fn test_once_send_recv() {
        let addr = ephemeral_tcp_addr();
        let _server = start_test_server(&addr).await;
        wait_server_ready(&addr).await;

        let reply: OnceEchoReply = once(&format!("tcp://{addr}"))
            .send_recv(OnceEchoRequest {
                text: "hello".into(),
            })
            .await
            .expect("send_recv failed");

        assert_eq!(reply.text, "hello");
    }

    #[tokio::test]
    async fn test_once_send_recv_with_options() {
        let addr = ephemeral_tcp_addr();
        let _server = start_test_server(&addr).await;
        wait_server_ready(&addr).await;

        let reply: OnceEchoReply = once(&format!("tcp://{addr}"))
            .with_timeout(Duration::from_secs(10))
            .with_codecs(&[crate::codec_msgpack::CodecMsgpackCompact])
            .send_recv(OnceEchoRequest {
                text: "options".into(),
            })
            .await
            .expect("send_recv with options failed");

        assert_eq!(reply.text, "options");
    }

    #[tokio::test]
    async fn test_once_send_recv_connect_failure() {
        let result: Result<OnceEchoReply, Error> = once("tcp://127.0.0.1:1")
            .with_timeout(Duration::from_secs(1))
            .send_recv(OnceEchoRequest {
                text: "fail".into(),
            })
            .await;

        assert!(result.is_err());
    }
}
