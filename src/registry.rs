use std::collections::HashMap;
use std::sync::Arc;

use futures::future::BoxFuture;

use crate::error::Error;
use crate::message::{KnownMessage, Message};
use crate::stream::Stream;
use crate::wire::Meta;

#[derive(Clone)]
pub struct StreamContext {
    pub stream: Stream,
    pub meta: Meta,
}

pub trait Handler: Send + Sync + 'static {
    fn handle(
        &self,
        msg: Box<dyn Message>,
        ctx: StreamContext,
    ) -> BoxFuture<'static, Result<Option<Box<dyn Message>>, Error>>;
}

#[derive(Default, Clone)]
pub struct Registry {
    handlers: Arc<HashMap<&'static str, Arc<dyn Handler>>>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, type_name: &'static str, handler: Arc<dyn Handler>) {
        let mut map = (*self.handlers).clone();
        map.insert(type_name, handler);
        self.handlers = Arc::new(map);
    }

    pub fn register_known<T>(&mut self)
    where
        T: KnownMessage + Message + 'static,
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

impl<T> Handler for KnownHandler<T>
where
    T: KnownMessage + Message + 'static,
{
    fn handle(
        &self,
        msg: Box<dyn Message>,
        ctx: StreamContext,
    ) -> BoxFuture<'static, Result<Option<Box<dyn Message>>, Error>> {
        Box::pin(async move {
            let expected = std::any::type_name::<T>();
            let got = msg.type_name();
            let boxed_any: Box<dyn std::any::Any> = msg;
            match boxed_any.downcast::<T>() {
                Ok(val) => val.handle(ctx).await,
                Err(_) => Err(Error::TypeMismatch { expected, got }),
            }
        })
    }
}
