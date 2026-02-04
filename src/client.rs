use std::time::Duration;

use tokio::time::timeout;
use tracing::debug;

use crate::error::Error;
use crate::stream::Connection;

#[derive(Debug, Clone, Copy)]
pub enum Transport {
    Tcp,
    Uds,
}

#[derive(Debug, Clone)]
pub struct Client {
    transport: Transport,
    timeout: Option<Duration>,
}

impl Default for Client {
    fn default() -> Self {
        Self {
            transport: Transport::Tcp,
            timeout: None,
        }
    }
}

impl Client {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_transport(mut self, transport: Transport) -> Self {
        self.transport = transport;
        self
    }

    pub fn with_timeout(mut self, d: Duration) -> Self {
        self.timeout = Some(d);
        self
    }

    pub async fn connect(&self, addr: &str) -> Result<Connection, Error> {
        debug!("client connect: {}", addr);
        let (transport, target) = detect_transport(self.transport, addr);
        let fut = async {
            match transport {
                Transport::Tcp => crate::transport::tcp::connect(target).await,
                Transport::Uds => crate::transport::uds::connect(target).await,
            }
        };

        match self.timeout {
            Some(d) => timeout(d, fut).await.map_err(|_| Error::ConnectTimeout)?,
            None => fut.await,
        }
    }
}

fn detect_transport(default_transport: Transport, addr: &str) -> (Transport, &str) {
    if let Some(stripped) = addr.strip_prefix("unix://") {
        return (Transport::Uds, stripped);
    }
    if let Some(stripped) = addr.strip_prefix("uds://") {
        return (Transport::Uds, stripped);
    }
    if let Some(stripped) = addr.strip_prefix("tcp://") {
        return (Transport::Tcp, stripped);
    }
    (default_transport, addr)
}
