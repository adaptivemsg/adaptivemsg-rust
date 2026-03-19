use std::time::Duration;

use tokio::time::timeout;
use tracing::debug;

use crate::codec::CodecID;
use crate::codec_msgpack::{CodecMsgpackCompact, CodecMsgpackMap};
use crate::codec_postcard::CodecPostcard;
use crate::connection::Connection;
use crate::error::Error;
use crate::protocol::DEFAULT_MAX_FRAME;
use crate::registry::Registry;

#[derive(Clone)]
/// Client configuration for connecting to a server.
pub struct Client {
    timeout: Option<Duration>,
    max_frame: u32,
    codecs: Vec<CodecID>,
    registry: Registry,
}

impl Default for Client {
    fn default() -> Self {
        Self {
            timeout: None,
            max_frame: DEFAULT_MAX_FRAME,
            codecs: vec![CodecPostcard, CodecMsgpackCompact, CodecMsgpackMap],
            registry: Registry::from_inventory(),
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

    /// Connect to a server at `addr` using `tcp://`, `uds://`, or `unix://`.
    pub async fn connect(&self, addr: &str) -> Result<Connection, Error> {
        debug!("client connect: {}", addr);
        let codecs = self.codecs.clone();
        let max_frame = self.max_frame;
        let registry = self.registry.clone();
        let fut = async {
            if let Some(stripped) = addr.strip_prefix("uds://") {
                return crate::transport::uds::connect(stripped, registry, &codecs, max_frame).await;
            }
            if let Some(stripped) = addr.strip_prefix("unix://") {
                return crate::transport::uds::connect(stripped, registry, &codecs, max_frame).await;
            }
            if let Some(stripped) = addr.strip_prefix("tcp://") {
                return crate::transport::tcp::connect(stripped, registry, &codecs, max_frame).await;
            }
            crate::transport::tcp::connect(addr, registry, &codecs, max_frame).await
        };

        match self.timeout {
            Some(d) => timeout(d, fut).await.map_err(|_| Error::ConnectTimeout)?,
            None => fut.await,
        }
    }
}
