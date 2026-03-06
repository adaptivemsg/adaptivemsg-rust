use std::time::Duration;

use tokio::time::timeout;
use tracing::debug;

use crate::error::Error;
use crate::stream::{Codec, Connection};

#[derive(Debug, Clone)]
pub struct Client {
    timeout: Option<Duration>,
    codec: Codec,
    max_frame: u32,
}

impl Default for Client {
    fn default() -> Self {
        Self {
            timeout: None,
            codec: Codec::Compact,
            max_frame: u32::MAX,
        }
    }
}

impl Client {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_timeout(mut self, d: Duration) -> Self {
        self.timeout = Some(d);
        self
    }

    pub fn with_codec(mut self, codec: Codec) -> Self {
        self.codec = codec;
        self
    }

    pub fn with_max_frame(mut self, max_frame: u32) -> Self {
        self.max_frame = max_frame;
        self
    }

    pub async fn connect(&self, addr: &str) -> Result<Connection, Error> {
        debug!("client connect: {}", addr);
        let codec = self.codec;
        let max_frame = self.max_frame;
        let fut = async {
            if let Some(stripped) = addr.strip_prefix("uds://") {
                return crate::transport::uds::connect(stripped, codec, max_frame).await;
            }
            if let Some(stripped) = addr.strip_prefix("unix://") {
                return crate::transport::uds::connect(stripped, codec, max_frame).await;
            }
            if let Some(stripped) = addr.strip_prefix("tcp://") {
                return crate::transport::tcp::connect(stripped, codec, max_frame).await;
            }
            crate::transport::tcp::connect(addr, codec, max_frame).await
        };

        match self.timeout {
            Some(d) => timeout(d, fut).await.map_err(|_| Error::ConnectTimeout)?,
            None => fut.await,
        }
    }
}
