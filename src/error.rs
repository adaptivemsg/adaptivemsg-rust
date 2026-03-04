use thiserror::Error;

pub type Result<T> = std::result::Result<T, anyhow::Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("codec error: {0}")]
    Codec(#[from] postcard::Error),
    #[error("frame too large: {0}")]
    FrameTooLarge(usize),
    #[error("stream id mismatch: {a} != {b}")]
    StreamIdMismatch { a: u16, b: u16 },
    #[error("unsupported frame version: {major}.{minor}")]
    UnsupportedFrameVersion { major: u8, minor: u8 },
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
