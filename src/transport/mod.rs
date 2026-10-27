use tokio::io::{AsyncRead, AsyncWrite, BufReader, BufWriter};

use crate::connection::TransportParts;
use crate::error::Error;

/// Lets one read from the transport take in several small frames.
const READ_BUFFER_SIZE: usize = 8 << 10;

#[cfg(feature = "tcp")]
pub mod tcp;

#[cfg(feature = "uds")]
pub mod uds;

#[cfg(feature = "quic")]
pub mod quic;

/// What an address names: `tcp://host:port`, `uds://path`, `unix://path`, or
/// a bare `host:port`, which is TCP.
pub(crate) enum Endpoint<'a> {
    Tcp(&'a str),
    Uds(&'a str),
}

impl<'a> Endpoint<'a> {
    pub(crate) fn parse(addr: &'a str) -> Self {
        if let Some(path) = addr
            .strip_prefix("uds://")
            .or_else(|| addr.strip_prefix("unix://"))
        {
            return Endpoint::Uds(path);
        }
        Endpoint::Tcp(addr.strip_prefix("tcp://").unwrap_or(addr))
    }

    pub(crate) fn not_enabled(&self) -> Error {
        let (name, addr) = match self {
            Endpoint::Tcp(addr) => ("tcp", addr),
            Endpoint::Uds(path) => ("uds", path),
        };
        Error::UnsupportedTransport(format!("{name} transport not enabled: {addr}"))
    }
}

/// Wraps the two halves of a transport for a connection.
pub(crate) fn parts<R, W>(reader: R, writer: W) -> TransportParts
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    TransportParts {
        reader: Box::new(BufReader::with_capacity(READ_BUFFER_SIZE, reader)),
        writer: Box::new(BufWriter::new(writer)),
    }
}

/// Dials `addr` and returns the transport halves.
pub(crate) async fn dial(addr: &str) -> Result<TransportParts, Error> {
    match Endpoint::parse(addr) {
        #[cfg(feature = "tcp")]
        Endpoint::Tcp(addr) => {
            let (reader, writer) = tcp::dial(addr).await?.into_split();
            Ok(parts(reader, writer))
        }
        #[cfg(feature = "uds")]
        Endpoint::Uds(path) => {
            let (reader, writer) = uds::dial(path).await?.into_split();
            Ok(parts(reader, writer))
        }
        #[allow(unreachable_patterns)]
        endpoint => Err(endpoint.not_enabled()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_endpoints() {
        for (addr, want) in [
            ("tcp://127.0.0.1:1", "tcp 127.0.0.1:1"),
            ("127.0.0.1:1", "tcp 127.0.0.1:1"),
            ("uds:///tmp/a.sock", "uds /tmp/a.sock"),
            ("unix:///tmp/a.sock", "uds /tmp/a.sock"),
            ("unix://@abstract", "uds @abstract"),
        ] {
            let got = match Endpoint::parse(addr) {
                Endpoint::Tcp(addr) => format!("tcp {addr}"),
                Endpoint::Uds(path) => format!("uds {path}"),
            };
            assert_eq!(got, want, "{addr}");
        }
    }
}
