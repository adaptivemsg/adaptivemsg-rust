use std::time::Duration;

use tokio::time::timeout;
use tracing::debug;

use crate::codec::CodecID;
use crate::codec_msgpack::{CodecMsgpackCompact, CodecMsgpackMap};
use crate::codec_postcard::CodecPostcard;
use crate::connection::Connection;
use crate::error::Error;
use crate::protocol::{DEFAULT_MAX_FRAME, PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3};
use crate::recovery::{ClientRecoveryOptions, RecoveryState, ResumeConnector};
use crate::recovery_protocol::{
    read_attach_response, write_attach_request, AttachRequest, ATTACH_MODE_NEW, ATTACH_STATUS_OK,
};
use crate::registry::Registry;

#[derive(Clone)]
/// Client configuration for connecting to a server.
pub struct Client {
    timeout: Option<Duration>,
    max_frame: u32,
    codecs: Vec<CodecID>,
    registry: Registry,
    recovery: ClientRecoveryOptions,
}

impl Default for Client {
    fn default() -> Self {
        Self {
            timeout: None,
            max_frame: DEFAULT_MAX_FRAME,
            codecs: vec![CodecPostcard, CodecMsgpackCompact, CodecMsgpackMap],
            registry: Registry::from_inventory(),
            recovery: ClientRecoveryOptions::default(),
        }
    }
}

impl Client {
    /// Create a client with default codecs and no timeout.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set a connect timeout.
    pub fn with_timeout(mut self, d: Duration) -> Self {
        self.timeout = Some(d);
        self
    }

    /// Override the codec preference list.
    pub fn with_codecs(mut self, codecs: &[CodecID]) -> Self {
        self.codecs = codecs.to_vec();
        self
    }

    /// Set the maximum frame size to advertise.
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
        debug!("client connect: {}", addr);
        let versions = if self.recovery.enable {
            vec![PROTOCOL_VERSION_V3, PROTOCOL_VERSION_V2]
        } else {
            vec![PROTOCOL_VERSION_V2]
        };

        let mut last_err = None;
        for version in versions {
            let fut = self.connect_version(addr, version);
            let result = match self.timeout {
                Some(d) => timeout(d, fut).await.map_err(|_| Error::ConnectTimeout)?,
                None => fut.await,
            };
            match result {
                Ok(connection) => return Ok(connection),
                Err(err) => {
                    let fallback = matches!(err, Error::UnsupportedFrameVersion(PROTOCOL_VERSION_V2));
                    last_err = Some(err);
                    if version == PROTOCOL_VERSION_V3 && fallback {
                        continue;
                    }
                    return Err(last_err.expect("client error missing"));
                }
            }
        }
        Err(last_err.unwrap_or(Error::Closed))
    }

    async fn connect_version(&self, addr: &str, version: u8) -> Result<Connection, Error> {
        let mut pending = dial_pending(addr, self.registry.clone()).await?;
        let (reader, writer) = pending.io_mut();
        let config = crate::protocol::handshake_client(
            reader,
            writer,
            &self.codecs,
            self.max_frame,
            version,
        )
        .await?;
        if config.version != PROTOCOL_VERSION_V3 {
            return pending.start_with_config(config, None, 0);
        }

        let request = AttachRequest {
            mode: ATTACH_MODE_NEW,
            ..AttachRequest::default()
        };
        write_attach_request(writer, &request).await?;
        let response = read_attach_response(reader).await?;
        if response.status != ATTACH_STATUS_OK {
            return Err(Error::ResumeRejected("initial attach rejected".to_string()));
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
        );
        pending.start_with_config(config, Some(recovery), response.last_recv_seq)
    }
}

async fn dial_pending(addr: &str, registry: Registry) -> Result<crate::connection::PendingConnection, Error> {
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
        return Ok(crate::connection::ConnectionInner::new_pending(stream, Registry::from_inventory(), None, None).into_transport_parts());
    }
    if let Some(stripped) = addr.strip_prefix("unix://") {
        let stream = crate::transport::uds::dial(stripped).await?;
        return Ok(crate::connection::ConnectionInner::new_pending(stream, Registry::from_inventory(), None, None).into_transport_parts());
    }
    if let Some(stripped) = addr.strip_prefix("tcp://") {
        let stream = crate::transport::tcp::dial(stripped).await?;
        return Ok(crate::connection::ConnectionInner::new_pending(stream, Registry::from_inventory(), None, None).into_transport_parts());
    }
    let stream = crate::transport::tcp::dial(addr).await?;
    Ok(crate::connection::ConnectionInner::new_pending(stream, Registry::from_inventory(), None, None).into_transport_parts())
}
