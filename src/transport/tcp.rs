use tokio::net::{TcpListener, TcpStream};

use crate::error::Error;

pub(crate) async fn dial(addr: &str) -> Result<TcpStream, Error> {
    let stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;
    Ok(stream)
}

pub async fn listen(addr: &str) -> Result<TcpListener, Error> {
    Ok(TcpListener::bind(addr).await?)
}

pub(crate) async fn accept_stream<L>(listener: L) -> Result<(TcpStream, Option<String>), Error>
where
    L: std::ops::Deref<Target = TcpListener>,
{
    let (stream, peer_addr) = listener.accept().await?;
    stream.set_nodelay(true)?;
    Ok((stream, Some(peer_addr.to_string())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn listen_and_connect() {
        let listener = listen("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let connect_handle = tokio::spawn(async move {
            dial(&addr.to_string()).await
        });

        let (stream, peer_addr) = accept_stream(&listener).await.unwrap();
        assert!(peer_addr.is_some());
        assert!(stream.nodelay().unwrap());

        let client_stream = connect_handle.await.unwrap().unwrap();
        assert!(client_stream.nodelay().unwrap());
    }
}
