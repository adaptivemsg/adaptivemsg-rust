use tokio::net::{UnixListener, UnixStream};

use crate::error::Error;
use crate::registry::Registry;
use crate::stream::Connection;
use crate::worker::WorkerConfig;

pub async fn connect(path: &str) -> Result<Connection, Error> {
    let stream = UnixStream::connect(path).await?;
    Ok(Connection::new(stream, None, None))
}

pub async fn listen(path: &str) -> Result<UnixListener, Error> {
    Ok(UnixListener::bind(path)?)
}

pub async fn accept(
    listener: &UnixListener,
    registry: Option<Registry>,
    worker_cfg: Option<WorkerConfig>,
) -> Result<Connection, Error> {
    let (stream, _) = listener.accept().await?;
    Ok(Connection::new(stream, registry, worker_cfg))
}
