use thiserror::Error;

/// Result type for handler code and application logic.
pub type Result<T> = std::result::Result<T, anyhow::Error>;

#[derive(Debug, Error)]
/// Protocol and transport errors from the runtime.
pub enum Error {
    /// Transport I/O error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// Message encode/decode error.
    #[error("codec error: {0}")]
    Codec(String),
    /// The message is larger than the connection accepts: the negotiated
    /// `max_frame` or, with recovery, `max_replay_bytes`. It is not sent, and
    /// the connection stays usable. A received frame above `max_frame` closes
    /// the connection.
    #[error("frame too large: {0}")]
    FrameTooLarge(usize),
    /// Peer sent a frame with an unknown protocol version.
    #[error("unsupported frame version: {0}")]
    UnsupportedFrameVersion(u8),
    /// Peer selected an unregistered codec.
    #[error("unsupported codec: {0}")]
    UnsupportedCodec(u8),
    /// Handshake failed: no codec supported by both sides.
    #[error("no common codec")]
    NoCommonCodec,
    /// Client offered more codecs than the protocol allows.
    #[error("too many codecs: {0}")]
    TooManyCodecs(usize),
    /// Connection does not speak the adaptivemsg protocol.
    #[error("invalid handshake magic")]
    BadHandshakeMagic,
    /// Received a message type not in the registry.
    #[error("unknown message type: {0}")]
    UnknownMessage(String),
    /// Compact-mode message has wrong number of fields.
    #[error("compact field count mismatch: expected {expected}, got {got}")]
    CompactFieldCount { expected: usize, got: usize },
    /// Generic message validation error.
    #[error("invalid message: {0}")]
    InvalidMessage(String),
    /// Connecting exceeded the connect timeout. Holds the last failed dial when
    /// the timeout ended a retry.
    #[error("connect timeout{}", cause_suffix(.0))]
    ConnectTimeout(Option<Box<Error>>),
    /// No message received within the stream's recv timeout.
    #[error("recv timeout")]
    RecvTimeout,
    /// Decoded message type does not match the expected type parameter.
    #[error("message type mismatch: expected {expected}, got {got}")]
    TypeMismatch { expected: String, got: String },
    /// Operation attempted on a closed connection or stream.
    #[error("connection closed")]
    Closed,
    /// Remote handler returned an error via `ErrorReply`.
    #[error("remote error: {code}: {message}")]
    Remote { code: String, message: String },
    /// A handler task is already running on this stream.
    #[error("only one handler task allowed per stream")]
    HandlerTaskBusy,
    /// Multiple concurrent `recv` calls on the same stream.
    #[error("concurrent recv on stream")]
    ConcurrentRecv,
    /// Address scheme is not recognized (`tcp://`, `uds://`, `unix://`).
    #[error("unsupported transport: {0}")]
    UnsupportedTransport(String),
    /// Server rejected a v3 resume/attach request.
    #[error("resume rejected: {0}")]
    ResumeRejected(String),
}

fn cause_suffix(cause: &Option<Box<Error>>) -> String {
    cause
        .as_ref()
        .map(|cause| format!(": {cause}"))
        .unwrap_or_default()
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
        assert_eq!(Error::ConnectTimeout(None).to_string(), "connect timeout");
        let refused = std::io::Error::from(std::io::ErrorKind::ConnectionRefused);
        assert_eq!(
            Error::ConnectTimeout(Some(Box::new(Error::Io(refused)))).to_string(),
            "connect timeout: io error: connection refused"
        );
        assert_eq!(
            Error::HandlerTaskBusy.to_string(),
            "only one handler task allowed per stream"
        );
        assert_eq!(
            Error::ConcurrentRecv.to_string(),
            "concurrent recv on stream"
        );
        assert_eq!(Error::NoCommonCodec.to_string(), "no common codec");
        assert_eq!(
            Error::BadHandshakeMagic.to_string(),
            "invalid handshake magic"
        );
    }

    #[test]
    fn error_display_with_fields() {
        let e = Error::FrameTooLarge(9999);
        assert!(e.to_string().contains("9999"));

        let e = Error::TypeMismatch {
            expected: "Foo".into(),
            got: "Bar".into(),
        };
        assert!(e.to_string().contains("Foo"));
        assert!(e.to_string().contains("Bar"));

        let e = Error::Remote {
            code: "handler.error".into(),
            message: "boom".into(),
        };
        assert!(e.to_string().contains("handler.error"));
        assert!(e.to_string().contains("boom"));
    }

    #[test]
    fn from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "broken");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::Io(_)));
    }
}
