use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use crate::error::{Error, Result};
use crate::message::{Message, MessageHandler};
use crate::stream::HandlerStream;

#[async_trait]
pub trait Handler: Send + Sync + 'static {
    async fn handle(
        &self,
        msg: Box<dyn Message>,
        stream: HandlerStream,
    ) -> Result<Option<Box<dyn Message>>>;
}

#[derive(Default, Clone)]
pub struct Registry {
    handlers: Arc<HashMap<&'static str, Arc<dyn Handler>>>,
}

pub struct KnownEntry {
    register: fn(&mut Registry),
}

inventory::collect!(KnownEntry);

impl KnownEntry {
    pub fn new(register: fn(&mut Registry)) -> Self {
        Self { register }
    }

    pub fn register(&self, reg: &mut Registry) {
        (self.register)(reg);
    }
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_inventory() -> Self {
        let mut reg = Registry::new();
        for entry in inventory::iter::<KnownEntry> {
            entry.register(&mut reg);
        }
        reg
    }

    pub fn register<T>(&mut self)
    where
        T: MessageHandler + 'static,
    {
        let type_name = std::any::type_name::<T>();
        let handler: Arc<dyn Handler> = Arc::new(KnownHandler::<T>(std::marker::PhantomData));
        Arc::make_mut(&mut self.handlers).insert(type_name, handler);
    }

    pub fn handler(&self, type_name: &str) -> Option<Arc<dyn Handler>> {
        self.handlers.get(type_name).cloned()
    }
}

struct KnownHandler<T>(std::marker::PhantomData<T>);

#[async_trait]
impl<T> Handler for KnownHandler<T>
where
    T: MessageHandler + 'static,
{
    async fn handle(
        &self,
        msg: Box<dyn Message>,
        stream: HandlerStream,
    ) -> Result<Option<Box<dyn Message>>> {
        let expected = std::any::type_name::<T>();
        let got = msg.type_name();
        match msg.downcast::<T>() {
            Ok(val) => val.handle(stream).await,
            Err(_) => Err(Error::TypeMismatch { expected, got }.into()),
        }
    }
}
