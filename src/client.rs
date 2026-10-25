use std::future::Future;
use std::time::Duration;

use tokio::time::{timeout_at, Instant};
use tracing::debug;

use crate::codec::CodecID;
use crate::codec_msgpack::{CodecMsgpackCompact, CodecMsgpackMap};
use crate::codec_postcard::CodecPostcard;
use crate::connection::{Connection, ConnectionRef};
use crate::error::Error;
use crate::message::{Message, MessageDecode};
use crate::protocol::{DEFAULT_MAX_FRAME, PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3};
use crate::recovery::{ClientRecoveryOptions, RecoveryState, ResumeConnector};
use crate::recovery_protocol::{
    read_attach_response, write_attach_request, AttachRequest, ATTACH_MODE_NEW, ATTACH_STATUS_OK,
};
use crate::registry::Registry;

#[derive(Clone)]
/// Client configuration for connecting to a server.
///
/// Uses a builder pattern to configure timeouts, codecs, and recovery before
/// connecting or performing a one-shot request-reply exchange.
///
/// # Example
///
/// ```rust,ignore
/// use adaptivemsg::Client;
/// use std::time::Duration;
///
/// let conn = Client::new()
///     .with_connect_timeout(Duration::from_secs(5))
///     .connect("tcp://127.0.0.1:9000")
///     .await?;
/// ```
pub struct Client {
    connect_timeout: Option<Duration>,
    recv_timeout: Option<Duration>,
    max_frame: u32,
    codecs: Vec<CodecID>,
    registry: Registry,
    recovery: ClientRecoveryOptions,
}

const CONNECT_RETRY_MIN_BACKOFF: Duration = Duration::from_millis(100);
const CONNECT_RETRY_MAX_BACKOFF: Duration = Duration::from_secs(2);

impl Default for Client {
    fn default() -> Self {
        Self {
            connect_timeout: None,
            recv_timeout: None,
            max_frame: DEFAULT_MAX_FRAME,
            codecs: vec![CodecPostcard, CodecMsgpackCompact, CodecMsgpackMap],
            registry: Registry::from_inventory(),
            recovery: ClientRecoveryOptions::default(),
        }
    }
}

impl Client {
    /// Create a client with default codecs and no timeouts.
    pub fn new() -> Self {
        Self::default()
    }

    /// Limit how long [`connect`](Self::connect) and [`OnceConn::send_recv`]
    /// may take to establish a connection.
    ///
    /// Until then, failed dials such as connection refused are retried,
    /// waiting 100ms between attempts and doubling up to 2s, so the client can
    /// start before its server. Handshake failures are returned at once, and a
    /// request is never sent twice. With recovery enabled the timeout also
    /// limits each reconnect attempt. When the timeout is reached the call
    /// returns [`Error::ConnectTimeout`]. `Duration::ZERO` means a single
    /// attempt with no time limit.
    pub fn with_connect_timeout(mut self, d: Duration) -> Self {
        self.connect_timeout = (!d.is_zero()).then_some(d);
        self
    }

    /// Set the initial receive timeout of every stream on connections made by
    /// this client, which includes waiting for the reply in
    /// [`OnceConn::send_recv`].
    ///
    /// `set_recv_timeout` on a connection or stream changes it for a single
    /// stream. `Duration::ZERO` means no timeout.
    pub fn with_recv_timeout(mut self, d: Duration) -> Self {
        self.recv_timeout = (!d.is_zero()).then_some(d);
        self
    }

    /// Override the codec preference list.
    pub fn with_codecs(mut self, codecs: &[CodecID]) -> Self {
        self.codecs = codecs.to_vec();
        self
    }

    /// Set the maximum payload size per frame that this client advertises.
    ///
    /// The effective limit for the connection is the smaller of both sides'
    /// values, so raising it also needs
    /// [`Server::with_max_frame`](crate::Server::with_max_frame). The default
    /// is 16 MiB.
    pub fn with_max_frame(mut self, max_frame: u32) -> Self {
        self.max_frame = max_frame;
        self
    }

    /// Configure client-side recovery behavior.
    pub fn with_recovery(mut self, recovery: ClientRecoveryOptions) -> Self {
        self.recovery = recovery.normalized();
        self
    }

    /// Connect to a server at `addr` using `tcp://`, `uds://`, or `unix://`.
    pub async fn connect(&self, addr: &str) -> Result<Connection, Error> {
        let connection = self.connect_with(addr, self.recovery.enable).await?;
        Ok(Connection::new_owned(connection))
    }

    /// Prepare a connection for exactly one request-reply exchange with `addr`.
    ///
    /// This does not dial: [`OnceConn::send_recv`] connects, exchanges the
    /// request and reply, and closes the connection. Use
    /// [`connect`](Self::connect) to send several requests to the same server.
    pub fn connect_once<'a>(&'a self, addr: &'a str) -> OnceConn<'a> {
        OnceConn { client: self, addr }
    }

    async fn connect_with(&self, addr: &str, use_recovery: bool) -> Result<ConnectionRef, Error> {
        debug!("client connect: {}", addr);
        let deadline = self.connect_timeout.map(|d| Instant::now() + d);
        let mut backoff = CONNECT_RETRY_MIN_BACKOFF;
        let mut last_dial_err = None;
        loop {
            let result = self.connect_attempt(addr, use_recovery, deadline).await;
            match (result, deadline) {
                (Ok(connection), _) => return Ok(connection),
                (Err(ConnectError::Dial(err)), Some(deadline)) => {
                    debug!("connect to {addr} failed, retrying: {err}");
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    tokio::time::sleep(backoff.min(remaining)).await;
                    backoff = (backoff * 2).min(CONNECT_RETRY_MAX_BACKOFF);
                    last_dial_err = Some(err);
                }
                (Err(ConnectError::Other(Error::ConnectTimeout(None))), _)
                    if last_dial_err.is_some() =>
                {
                    return Err(Error::ConnectTimeout(last_dial_err.map(Box::new)));
                }
                (Err(ConnectError::Dial(err) | ConnectError::Other(err)), _) => return Err(err),
            }
        }
    }

    async fn connect_attempt(
        &self,
        addr: &str,
        use_recovery: bool,
        deadline: Option<Instant>,
    ) -> Result<ConnectionRef, ConnectError> {
        let versions: &[u8] = if use_recovery {
            &[PROTOCOL_VERSION_V3, PROTOCOL_VERSION_V2]
        } else {
            &[PROTOCOL_VERSION_V2]
        };
        let mut last_err = None;
        for &version in versions {
            match self.connect_version(addr, version, deadline).await {
                Ok(connection) => return Ok(connection),
                Err(err) => {
                    let fallback = matches!(
                        err,
                        ConnectError::Other(Error::UnsupportedFrameVersion(PROTOCOL_VERSION_V2))
                    );
                    if version == PROTOCOL_VERSION_V3 && fallback {
                        last_err = Some(err);
                        continue;
                    }
                    return Err(err);
                }
            }
        }
        Err(last_err.unwrap_or(ConnectError::Other(Error::Closed)))
    }

    async fn connect_version(
        &self,
        addr: &str,
        version: u8,
        deadline: Option<Instant>,
    ) -> Result<ConnectionRef, ConnectError> {
        let mut pending = match before(deadline, dial_pending(addr, self.registry.clone())).await {
            Ok(pending) => pending,
            Err(err) if is_dial_error(&err) => return Err(ConnectError::Dial(err)),
            Err(err) => return Err(ConnectError::Other(err)),
        };
        if let Some(timeout) = self.recv_timeout {
            pending.connection().set_default_recv_timeout(timeout);
        }
        let (reader, writer) = pending.io_mut();
        let config = before(
            deadline,
            crate::protocol::handshake_client(
                reader,
                writer,
                &self.codecs,
                self.max_frame,
                version,
            ),
        )
        .await
        .map_err(ConnectError::Other)?;
        if config.version != PROTOCOL_VERSION_V3 {
            return pending
                .start_with_config(config, None, 0)
                .map_err(ConnectError::Other);
        }

        let request = AttachRequest {
            mode: ATTACH_MODE_NEW,
            ..AttachRequest::default()
        };
        let response = before(deadline, async move {
            write_attach_request(writer, &request).await?;
            read_attach_response(reader).await
        })
        .await
        .map_err(ConnectError::Other)?;
        if response.status != ATTACH_STATUS_OK {
            return Err(ConnectError::Other(Error::ResumeRejected(
                "initial attach rejected".to_string(),
            )));
        }

        let normalized = self.recovery.normalized();
        let addr_string = addr.to_string();
        let connector: ResumeConnector = std::sync::Arc::new(move || {
            let addr = addr_string.clone();
            Box::pin(async move { dial_transport(&addr).await })
        });
        let recovery = RecoveryState::new_client(
            normalized,
            response.negotiated,
            response.connection_id,
            response.resume_secret,
            connector,
            self.connect_timeout,
        );
        pending
            .start_with_config(config, Some(recovery), response.last_recv_seq)
            .map_err(ConnectError::Other)
    }
}

/// A connection for exactly one request-reply exchange, created by
/// [`Client::connect_once`].
///
/// It never uses recovery, even when [`Client::with_recovery`] is set:
/// recovery adds a round trip to every connection and does little for a
/// single exchange.
pub struct OnceConn<'a> {
    client: &'a Client,
    addr: &'a str,
}

impl OnceConn<'_> {
    /// Connect, send `msg` on the default stream, receive one reply, and close
    /// the connection.
    ///
    /// The connect timeout limits connecting and the recv timeout limits
    /// waiting for the reply. This consumes the `OnceConn`, so it can be used
    /// only once.
    pub async fn send_recv<Rep: MessageDecode + 'static>(
        self,
        msg: impl Message,
    ) -> Result<Rep, Error> {
        let conn = self.client.connect_with(self.addr, false).await?;
        let result = conn.send_recv(msg).await;
        // Nothing is left to deliver, so close at once instead of waiting, as
        // close does, for a peer that may not be responding.
        conn.close_now();
        result
    }
}

enum ConnectError {
    /// The server could not be reached; worth retrying.
    Dial(Error),
    Other(Error),
}

/// Whether a dial failure may succeed on a later attempt. Malformed or
/// unsupported addresses never will.
fn is_dial_error(err: &Error) -> bool {
    matches!(
        err,
        Error::Io(io) if !matches!(
            io.kind(),
            std::io::ErrorKind::InvalidInput | std::io::ErrorKind::Unsupported
        )
    )
}

/// Runs `fut` until `deadline`, reporting expiry as [`Error::ConnectTimeout`].
async fn before<T>(
    deadline: Option<Instant>,
    fut: impl Future<Output = Result<T, Error>>,
) -> Result<T, Error> {
    match deadline {
        Some(deadline) => timeout_at(deadline, fut)
            .await
            .map_err(|_| Error::ConnectTimeout(None))?,
        None => fut.await,
    }
}

async fn dial_pending(
    addr: &str,
    registry: Registry,
) -> Result<crate::connection::PendingConnection, Error> {
    if let Some(stripped) = addr.strip_prefix("uds://") {
        return Ok(crate::connection::ConnectionInner::new_pending(
            crate::transport::uds::dial(stripped).await?,
            registry,
            None,
            None,
        ));
    }
    if let Some(stripped) = addr.strip_prefix("unix://") {
        return Ok(crate::connection::ConnectionInner::new_pending(
            crate::transport::uds::dial(stripped).await?,
            registry,
            None,
            None,
        ));
    }
    if let Some(stripped) = addr.strip_prefix("tcp://") {
        return Ok(crate::connection::ConnectionInner::new_pending(
            crate::transport::tcp::dial(stripped).await?,
            registry,
            None,
            None,
        ));
    }
    Ok(crate::connection::ConnectionInner::new_pending(
        crate::transport::tcp::dial(addr).await?,
        registry,
        None,
        None,
    ))
}

async fn dial_transport(addr: &str) -> Result<crate::connection::TransportParts, Error> {
    if let Some(stripped) = addr.strip_prefix("uds://") {
        let stream = crate::transport::uds::dial(stripped).await?;
        return Ok(crate::connection::ConnectionInner::new_pending(
            stream,
            Registry::from_inventory(),
            None,
            None,
        )
        .into_transport_parts());
    }
    if let Some(stripped) = addr.strip_prefix("unix://") {
        let stream = crate::transport::uds::dial(stripped).await?;
        return Ok(crate::connection::ConnectionInner::new_pending(
            stream,
            Registry::from_inventory(),
            None,
            None,
        )
        .into_transport_parts());
    }
    if let Some(stripped) = addr.strip_prefix("tcp://") {
        let stream = crate::transport::tcp::dial(stripped).await?;
        return Ok(crate::connection::ConnectionInner::new_pending(
            stream,
            Registry::from_inventory(),
            None,
            None,
        )
        .into_transport_parts());
    }
    let stream = crate::transport::tcp::dial(addr).await?;
    Ok(crate::connection::ConnectionInner::new_pending(
        stream,
        Registry::from_inventory(),
        None,
        None,
    )
    .into_transport_parts())
}
