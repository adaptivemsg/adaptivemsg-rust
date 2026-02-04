use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("codec error: {0}")]
    Codec(#[from] postcard::Error),
    #[error("frame too large: {0}")]
    FrameTooLarge(usize),
    #[error("message type mismatch: expected {expected}, got {got}")]
    TypeMismatch { expected: &'static str, got: &'static str },
    #[error("connection closed")]
    Closed,
}
