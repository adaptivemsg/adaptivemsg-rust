use thiserror::Error;

pub type Result<T> = std::result::Result<T, anyhow::Error>;

#[derive(Debug, Error)]
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
    #[error("connect timeout")]
    ConnectTimeout,
    #[error("recv timeout")]
    RecvTimeout,
    #[error("message type mismatch: expected {expected}, got {got}")]
    TypeMismatch { expected: &'static str, got: &'static str },
    #[error("connection closed")]
    Closed,
    #[error("remote error: {code}: {message}")]
    Remote { code: String, message: String },
    #[error("only one handler task allowed per stream")]
    HandlerTaskBusy,
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
