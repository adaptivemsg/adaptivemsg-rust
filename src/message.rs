use std::any::Any;

#[cfg(feature = "tokio")]
use async_trait::async_trait;
use rmpv::Value;

#[cfg(feature = "tokio")]
use crate::context::StreamContext;
use crate::error::Error;
#[cfg(feature = "tokio")]
use crate::error::Result;

/// Application message that can be encoded and decoded by codecs.
///
/// Typically derived via the [`#[message]`](macro@crate::message) attribute
/// macro. Manual implementation is rarely needed.
pub trait Message: Any + Send + Sync + 'static {
    /// Wire name used for routing and decoding.
    fn wire_name(&self) -> &'static str;
    /// Wire name for this type without a value instance.
    fn wire_name_static() -> &'static str
    where
        Self: Sized;
    /// Encode this message in MessagePack map form.
    fn encode_map(&self) -> std::result::Result<Vec<u8>, Error>;
    /// Encode this message in MessagePack compact array form.
    fn encode_compact(&self) -> std::result::Result<Vec<u8>, Error>;
    /// Encode this message in postcard form.
    fn encode_postcard(&self) -> std::result::Result<Vec<u8>, Error>;
    /// Return a type-erased reference for downcasting.
    fn as_any(&self) -> &dyn Any;
}

#[doc(hidden)]
pub trait MessageDecode: Message {
    fn decode_map(value: Value) -> std::result::Result<Self, Error>
    where
        Self: Sized;
    fn decode_compact(values: Vec<Value>) -> std::result::Result<Self, Error>
    where
        Self: Sized;
    fn decode_postcard(payload: &[u8]) -> std::result::Result<Self, Error>
    where
        Self: Sized;
}

/// Encodes a message serialized with `rmpv::ext::to_value`, which gives a
/// struct as an array of its fields, as `[wire, field1, field2, ...]`.
/// Serializing the whole struct, rather than each field, applies the serde
/// attributes of its fields.
#[doc(hidden)]
pub fn encode_compact_value(wire: &str, value: Value) -> std::result::Result<Vec<u8>, Error> {
    let Value::Array(fields) = value else {
        return Err(Error::Codec(
            "compact message must serialize as an array".to_string(),
        ));
    };
    let mut buf = Vec::with_capacity(wire.len() + 32);
    rmp::encode::write_array_len(&mut buf, (fields.len() + 1) as u32)
        .map_err(|err| Error::Codec(err.to_string()))?;
    rmp::encode::write_str(&mut buf, wire).map_err(|err| Error::Codec(err.to_string()))?;
    for field in &fields {
        rmpv::encode::write_value(&mut buf, field)?;
    }
    Ok(buf)
}

/// Decodes the fields of a compact message, the elements after the wire name.
#[doc(hidden)]
pub fn decode_compact_values<T>(
    values: Vec<Value>,
    field_count: usize,
) -> std::result::Result<T, Error>
where
    T: serde::de::DeserializeOwned,
{
    if values.len() != field_count {
        return Err(Error::CompactFieldCount {
            expected: field_count,
            got: values.len(),
        });
    }
    Ok(rmpv::ext::from_value(Value::Array(values))?)
}

impl dyn Message {
    pub(crate) fn downcast<T: Message>(
        self: Box<Self>,
    ) -> std::result::Result<Box<T>, Box<dyn Message>> {
        if self.as_any().is::<T>() {
            let raw = Box::into_raw(self);
            return Ok(unsafe { Box::from_raw(raw as *mut T) });
        }
        Err(self)
    }
}

/// Empty success reply sent when a handler returns `Ok(None)`.
#[crate::message(register)]
pub struct OkReply {}

/// Error reply sent when a handler returns an error.
#[crate::message(register)]
pub struct ErrorReply {
    code: String,
    message: String,
}

impl ErrorReply {
    /// Create an error reply from a code and message.
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }

    /// Machine-readable error code.
    pub fn code(&self) -> &str {
        &self.code
    }

    /// Human-readable error message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Split into `(code, message)` parts.
    pub fn into_parts(self) -> (String, String) {
        (self.code, self.message)
    }
}

#[cfg(feature = "tokio")]
#[async_trait]
/// Server-side handler for a message type.
///
/// Implement via the [`#[message_handler]`](macro@crate::message_handler)
/// attribute macro:
///
/// ```rust,ignore
/// #[message_handler]
/// impl MessageHandler for MyRequest {
///     async fn handle(self: Box<Self>, ctx: StreamContext)
///         -> Result<Option<Box<dyn Message>>>
///     {
///         Ok(Some(Box::new(MyResponse { /* ... */ })))
///     }
/// }
/// ```
pub trait MessageHandler: Message {
    /// Handle a request and optionally return a reply.
    ///
    /// Handled messages must be sent using `send_recv()`. `Ok(Some(msg))` sends
    /// `msg`, `Ok(None)` sends `OkReply`, and `Err(e)` sends `ErrorReply`.
    /// Use `adaptivemsg::Result` (anyhow) for application errors.
    async fn handle(self: Box<Self>, stream_ctx: StreamContext)
        -> Result<Option<Box<dyn Message>>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ok_reply_wire_name() {
        let msg = OkReply {};
        assert_eq!(msg.wire_name(), OkReply::wire_name_static());
        assert!(!msg.wire_name().is_empty());
    }

    #[test]
    fn error_reply_wire_name() {
        let msg = ErrorReply::new("test", "fail");
        assert_eq!(msg.wire_name(), ErrorReply::wire_name_static());
        assert!(!msg.wire_name().is_empty());
    }

    #[test]
    fn error_reply_fields() {
        let msg = ErrorReply::new("codec_error", "bad data");
        assert_eq!(msg.code(), "codec_error");
        assert_eq!(msg.message(), "bad data");
    }
}
