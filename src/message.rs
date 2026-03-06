use std::any::Any;

use async_trait::async_trait;
use rmpv::Value;

use crate::error::{Error, Result};
use crate::stream::StreamContext;

pub trait Message: Any + Send + Sync + 'static {
    fn wire_name(&self) -> &'static str;
    fn wire_name_static() -> &'static str
    where
        Self: Sized;
    fn encode_map(&self) -> std::result::Result<Vec<u8>, Error>;
    fn encode_compact(&self) -> std::result::Result<Vec<u8>, Error>;
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
}

impl dyn Message {
    pub(crate) fn downcast<T: Message>(
        self: Box<Self>,
    ) -> std::result::Result<Box<T>, Box<dyn Message>> {
        if self.as_any().is::<T>() {
            let raw = Box::into_raw(self);
            // Safety: the `Any` check ensures the cast target matches the concrete type.
            return Ok(unsafe { Box::from_raw(raw as *mut T) });
        }
        Err(self)
    }
}

#[crate::message]
pub struct OkReply {}

#[crate::message]
pub struct ErrorReply {
    code: String,
    message: String,
}

impl ErrorReply {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }

    pub fn code(&self) -> &str {
        &self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn into_parts(self) -> (String, String) {
        (self.code, self.message)
    }
}

#[async_trait]
pub trait MessageHandler: Message {
    /// Handled messages MUST be sent by clients using `send_recv()`.
    /// `Ok(Some(msg))` sends `msg`, `Ok(None)` sends `OkReply`, and `Err(e)` sends an error.
    /// The error type is `anyhow::Error` via `adaptivemsg::Result`.
    async fn handle(
        self: Box<Self>,
        stream_ctx: StreamContext,
    ) -> Result<Option<Box<dyn Message>>>;
}
