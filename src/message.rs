use std::any::Any;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::registry::ContextStream;

#[typetag::serde(tag = "type")]
pub trait Message: Any + Send + Sync + 'static {
    fn type_name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}

#[derive(Serialize, Deserialize)]
pub struct OkReply;

#[typetag::serde]
impl Message for OkReply {}

#[derive(Serialize, Deserialize)]
pub struct ErrorReply {
    pub code: String,
    pub message: String,
}

#[typetag::serde]
impl Message for ErrorReply {}

#[async_trait]
pub trait MessageHandler: Message {
    /// Handled messages MUST be sent by clients using `send_recv()`.
    /// `Ok(Some(msg))` sends `msg`, `Ok(None)` sends `OkReply`, and `Err(e)` sends an error.
    /// The error type is `anyhow::Error` via `adaptivemsg::Result`.
    async fn handle(
        self: Box<Self>,
        ctxstream: ContextStream,
    ) -> Result<Option<Box<dyn Message>>>;
}

// Helper to force serde to see trait object implementations.
#[allow(dead_code)]
fn _serde_compile_guard<T: Serialize + for<'de> Deserialize<'de>>() {}
