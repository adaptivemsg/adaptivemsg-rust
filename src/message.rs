use std::any::Any;

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};

use crate::error::Error;
use crate::registry::StreamContext;

#[typetag::serde(tag = "type")]
pub trait Message: Any + Send + Sync + 'static {
    fn type_name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}

pub trait MessageHandler: Message {
    fn handle(self: Box<Self>, ctx: StreamContext) -> BoxFuture<'static, Result<Option<Box<dyn Message>>, Error>>;
}

// Helper to force serde to see trait object implementations.
#[allow(dead_code)]
fn _serde_compile_guard<T: Serialize + for<'de> Deserialize<'de>>() {}
