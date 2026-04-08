use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use rmpv::Value;

use crate::context::StreamContext;
use crate::error::{Error, Result as HandlerResult};
use crate::message::{Message, MessageDecode, MessageHandler};

#[async_trait]
pub(crate) trait Handler: Send + Sync + 'static {
    async fn handle(
        &self,
        msg: Box<dyn Message>,
        stream_ctx: StreamContext,
    ) -> HandlerResult<Option<Box<dyn Message>>>;
}

pub(crate) trait MessageFactory: Send + Sync + 'static {
    fn decode_map(&self, value: Value) -> std::result::Result<Box<dyn Message>, Error>;
    fn decode_compact(&self, values: Vec<Value>) -> std::result::Result<Box<dyn Message>, Error>;
    fn decode_postcard(&self, payload: &[u8]) -> std::result::Result<Box<dyn Message>, Error>;
}

/// Registry of message handlers and message decoders.
///
/// Built automatically from `inventory` submissions when using
/// [`#[message]`](macro@crate::message) and
/// [`#[message_handler]`](macro@crate::message_handler) macros. Use
/// [`Registry::from_inventory`] to create one with all registered types.
#[derive(Default, Clone)]
pub struct Registry {
    handlers: Arc<HashMap<&'static str, Arc<dyn Handler>>>,
    messages: Arc<HashMap<&'static str, Arc<dyn MessageFactory>>>,
}

#[doc(hidden)]
pub struct KnownEntry {
    register: fn(&mut Registry),
}

#[doc(hidden)]
pub struct KnownMessageEntry {
    register: fn(&mut Registry),
}

inventory::collect!(KnownEntry);
inventory::collect!(KnownMessageEntry);

impl KnownEntry {
    pub const fn new(register: fn(&mut Registry)) -> Self {
        Self { register }
    }

    pub(crate) fn register(&self, reg: &mut Registry) {
        (self.register)(reg);
    }
}

impl KnownMessageEntry {
    pub const fn new(register: fn(&mut Registry)) -> Self {
        Self { register }
    }

    pub(crate) fn register(&self, reg: &mut Registry) {
        (self.register)(reg);
    }
}

impl Registry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a registry populated from inventory submissions.
    pub fn from_inventory() -> Self {
        let mut reg = Registry::new();
        for entry in inventory::iter::<KnownEntry> {
            entry.register(&mut reg);
        }
        for entry in inventory::iter::<KnownMessageEntry> {
            entry.register(&mut reg);
        }
        reg
    }

    /// Register a message handler type.
    pub fn register<T>(&mut self)
    where
        T: MessageHandler + 'static,
    {
        let wire_name = T::wire_name_static();
        let handler: Arc<dyn Handler> = Arc::new(KnownHandler::<T>(std::marker::PhantomData));
        Arc::make_mut(&mut self.handlers).insert(wire_name, handler);
    }

    pub(crate) fn handler(&self, wire_name: &str) -> Option<Arc<dyn Handler>> {
        self.handlers.get(wire_name).cloned()
    }

    pub(crate) fn has_handlers(&self) -> bool {
        !self.handlers.is_empty()
    }

    /// Register a message type for decoding without a handler.
    pub fn register_message<T>(&mut self)
    where
        T: MessageDecode + 'static,
    {
        let wire_name = T::wire_name_static();
        let factory: Arc<dyn MessageFactory> =
            Arc::new(KnownMessage::<T>(std::marker::PhantomData));
        Arc::make_mut(&mut self.messages).insert(wire_name, factory);
    }

    pub(crate) fn message(&self, wire_name: &str) -> Option<Arc<dyn MessageFactory>> {
        self.messages.get(wire_name).cloned()
    }
}

struct KnownHandler<T>(std::marker::PhantomData<T>);
struct KnownMessage<T>(std::marker::PhantomData<T>);

#[async_trait]
impl<T> Handler for KnownHandler<T>
where
    T: MessageHandler + 'static,
{
    async fn handle(
        &self,
        msg: Box<dyn Message>,
        stream_ctx: StreamContext,
    ) -> HandlerResult<Option<Box<dyn Message>>> {
        let expected = T::wire_name_static().to_string();
        let got = msg.wire_name().to_string();
        match msg.downcast::<T>() {
            Ok(val) => val.handle(stream_ctx).await,
            Err(_) => Err(Error::TypeMismatch { expected, got }.into()),
        }
    }
}

impl<T> MessageFactory for KnownMessage<T>
where
    T: MessageDecode + 'static,
{
    fn decode_map(&self, value: Value) -> std::result::Result<Box<dyn Message>, Error> {
        let msg = T::decode_map(value)?;
        Ok(Box::new(msg))
    }

    fn decode_compact(&self, values: Vec<Value>) -> std::result::Result<Box<dyn Message>, Error> {
        let msg = T::decode_compact(values)?;
        Ok(Box::new(msg))
    }

    fn decode_postcard(&self, payload: &[u8]) -> std::result::Result<Box<dyn Message>, Error> {
        let msg = T::decode_postcard(payload)?;
        Ok(Box::new(msg))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_inventory_includes_builtins() {
        let reg = Registry::from_inventory();
        assert!(reg.message("am.message.OkReply").is_some());
        assert!(reg.message("am.message.ErrorReply").is_some());
    }

    #[test]
    fn has_handlers_false_for_empty() {
        let reg = Registry::new();
        assert!(!reg.has_handlers());
    }

    #[test]
    fn unknown_wire_returns_none() {
        let reg = Registry::from_inventory();
        assert!(reg.message("nonexistent.Type").is_none());
        assert!(reg.handler("nonexistent.Type").is_none());
    }
}
