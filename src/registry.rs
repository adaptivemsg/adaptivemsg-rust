use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use anyhow::anyhow;

use crate::error::Result;
use crate::message::{Message, MessageHandler};
use crate::stream::Stream;
use crate::wire::Meta;

#[derive(Clone)]
pub struct ContextStream {
    pub stream: Stream,
    pub meta: Meta,
}

#[async_trait]
pub trait Handler: Send + Sync + 'static {
    async fn handle(
        &self,
        msg: Box<dyn Message>,
        ctxstream: ContextStream,
    ) -> Result<Option<Box<dyn Message>>>;
}

#[derive(Default, Clone)]
pub struct Registry {
    handlers: Arc<HashMap<&'static str, Arc<dyn Handler>>>,
}

pub struct KnownEntry {
    pub register: fn(&mut Registry),
}

inventory::collect!(KnownEntry);

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_inventory() -> Self {
        let mut reg = Registry::new();
        for entry in inventory::iter::<KnownEntry> {
            (entry.register)(&mut reg);
        }
        reg
    }

    pub fn register(&mut self, type_name: &'static str, handler: Arc<dyn Handler>) {
        let mut map = (*self.handlers).clone();
        map.insert(type_name, handler);
        self.handlers = Arc::new(map);
    }

    pub fn register_known<T>(&mut self)
    where
        T: MessageHandler + Message + 'static,
    {
        let type_name = std::any::type_name::<T>();
        let handler: Arc<dyn Handler> = Arc::new(KnownHandler::<T>::default());
        self.register(type_name, handler);
    }

    pub fn handler(&self, type_name: &str) -> Option<Arc<dyn Handler>> {
        self.handlers.get(type_name).cloned()
    }
}

struct KnownHandler<T>(std::marker::PhantomData<T>);

impl<T> Default for KnownHandler<T> {
    fn default() -> Self {
        Self(std::marker::PhantomData)
    }
}

#[async_trait]
impl<T> Handler for KnownHandler<T>
where
    T: MessageHandler + Message + 'static,
{
    async fn handle(
        &self,
        msg: Box<dyn Message>,
        ctxstream: ContextStream,
    ) -> Result<Option<Box<dyn Message>>> {
        let expected = std::any::type_name::<T>();
        let got = msg.type_name();
        let boxed_any: Box<dyn std::any::Any> = msg;
        match boxed_any.downcast::<T>() {
            Ok(val) => val.handle(ctxstream).await,
            Err(_) => Err(anyhow!("message type mismatch: expected {expected}, got {got}")),
        }
    }
}
