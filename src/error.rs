use thiserror::Error;

/// Result type for handler code and application logic.
pub type Result<T> = std::result::Result<T, anyhow::Error>;

#[derive(Debug, Error)]
/// Protocol and transport errors from the runtime.
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("codec error: {0}")]
    Codec(String),
    #[error("frame too large: {0}")]
    FrameTooLarge(usize),
    #[error("unsupported frame version: {0}")]
    UnsupportedFrameVersion(u8),
    #[error("unsupported codec: {0}")]
    UnsupportedCodec(u8),
    #[error("no common codec")]
    NoCommonCodec,
    #[error("too many codecs: {0}")]
    TooManyCodecs(usize),
    #[error("handshake rejected")]
    HandshakeRejected,
    #[error("no common protocol version: client {client_min}-{client_max}, server {server_min}-{server_max}")]
    NoCommonVersion {
        client_min: u8,
        client_max: u8,
        server_min: u8,
        server_max: u8,
    },
    #[error("invalid handshake magic")]
    BadHandshakeMagic,
    #[error("unknown message type: {0}")]
    UnknownMessage(String),
    #[error("compact field count mismatch: expected {expected}, got {got}")]
    CompactFieldCount { expected: usize, got: usize },
    #[error("invalid message: {0}")]
    InvalidMessage(String),
    #[error("connect timeout")]
    ConnectTimeout,
    #[error("recv timeout")]
    RecvTimeout,
    #[error("message type mismatch: expected {expected}, got {got}")]
    TypeMismatch { expected: String, got: String },
    #[error("connection closed")]
    Closed,
    #[error("remote error: {code}: {message}")]
    Remote { code: String, message: String },
    #[error("only one handler task allowed per stream")]
    HandlerTaskBusy,
    #[error("concurrent recv on stream")]
    ConcurrentRecv,
    #[error("unsupported transport: {0}")]
    UnsupportedTransport(String),
    #[error("resume rejected: {0}")]
    ResumeRejected(String),
    #[error("replay buffer full: limit {limit}, size {size}")]
    ReplayBufferFull { limit: i64, size: i64 },
}

impl From<rmp_serde::encode::Error> for Error {
    fn from(err: rmp_serde::encode::Error) -> Self {
        Self::Codec(err.to_string())
    }
}

impl From<rmp_serde::decode::Error> for Error {
    fn from(err: rmp_serde::decode::Error) -> Self {
        Self::Codec(err.to_string())
    }
}

impl From<rmpv::encode::Error> for Error {
    fn from(err: rmpv::encode::Error) -> Self {
        Self::Codec(err.to_string())
    }
}

impl From<rmpv::decode::Error> for Error {
    fn from(err: rmpv::decode::Error) -> Self {
        Self::Codec(err.to_string())
    }
}

impl From<rmpv::ext::Error> for Error {
    fn from(err: rmpv::ext::Error) -> Self {
        Self::Codec(err.to_string())
    }
}

impl From<postcard::Error> for Error {
    fn from(err: postcard::Error) -> Self {
        Self::Codec(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_messages() {
        assert_eq!(Error::Closed.to_string(), "connection closed");
        assert_eq!(Error::RecvTimeout.to_string(), "recv timeout");
        assert_eq!(Error::ConnectTimeout.to_string(), "connect timeout");
        assert_eq!(Error::HandlerTaskBusy.to_string(), "only one handler task allowed per stream");
        assert_eq!(Error::ConcurrentRecv.to_string(), "concurrent recv on stream");
        assert_eq!(Error::NoCommonCodec.to_string(), "no common codec");
        assert_eq!(Error::BadHandshakeMagic.to_string(), "invalid handshake magic");
        assert_eq!(Error::HandshakeRejected.to_string(), "handshake rejected");
    }

    #[test]
    fn error_display_with_fields() {
        let e = Error::FrameTooLarge(9999);
        assert!(e.to_string().contains("9999"));

        let e = Error::TypeMismatch { expected: "Foo".into(), got: "Bar".into() };
        assert!(e.to_string().contains("Foo"));
        assert!(e.to_string().contains("Bar"));

        let e = Error::Remote { code: "handler.error".into(), message: "boom".into() };
        assert!(e.to_string().contains("handler.error"));
        assert!(e.to_string().contains("boom"));

        let e = Error::NoCommonVersion { client_min: 2, client_max: 3, server_min: 4, server_max: 5 };
        assert!(e.to_string().contains("2-3"));
        assert!(e.to_string().contains("4-5"));
    }

    #[test]
    fn from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "broken");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::Io(_)));
    }
}
