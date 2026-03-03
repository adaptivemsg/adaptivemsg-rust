use std::time::Duration;

use tokio::time::timeout;
use tracing::debug;

use crate::error::Error;
use crate::stream::Connection;

#[derive(Debug, Clone)]
pub struct Client {
    timeout: Option<Duration>,
}

impl Default for Client {
    fn default() -> Self {
        Self { timeout: None }
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

    pub async fn connect(&self, addr: &str) -> Result<Connection, Error> {
        debug!("client connect: {}", addr);
        let fut = async {
            if let Some(stripped) = addr.strip_prefix("uds://") {
                return crate::transport::uds::connect(stripped).await;
            }
            if let Some(stripped) = addr.strip_prefix("unix://") {
                return crate::transport::uds::connect(stripped).await;
            }
            if let Some(stripped) = addr.strip_prefix("tcp://") {
                return crate::transport::tcp::connect(stripped).await;
            }
            crate::transport::tcp::connect(addr).await
        };

        match self.timeout {
            Some(d) => timeout(d, fut).await.map_err(|_| Error::ConnectTimeout)?,
            None => fut.await,
        }
    }
}
