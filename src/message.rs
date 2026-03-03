use std::any::Any;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::stream::HandlerStream;

#[typetag::serde(tag = "type")]
pub trait Message: Any + Send + Sync + 'static {
    fn type_name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}

impl dyn Message {
    pub(crate) fn downcast<T: Message>(
        self: Box<Self>,
    ) -> std::result::Result<Box<T>, Box<dyn Message>> {
        if self.type_name() == std::any::type_name::<T>() {
            let raw = Box::into_raw(self);
            // Safety: the type_name check ensures the cast target matches the concrete type.
            return Ok(unsafe { Box::from_raw(raw as *mut T) });
        }
        Err(self)
    }
}

#[derive(Serialize, Deserialize)]
pub struct OkReply;

#[typetag::serde]
impl Message for OkReply {}

#[derive(Serialize, Deserialize)]
pub struct ErrorReply {
    code: String,
    message: String,
}

#[typetag::serde]
impl Message for ErrorReply {}

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
    async fn handle(self: Box<Self>, stream: HandlerStream) -> Result<Option<Box<dyn Message>>>;
}

// Helper to force serde to see trait object implementations.
#[allow(dead_code)]
fn _serde_compile_guard<T: Serialize + for<'de> Deserialize<'de>>() {}
