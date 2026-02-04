use std::collections::HashMap;
use std::sync::Arc;

use futures::future::BoxFuture;

use crate::error::Error;
use crate::message::Message;
use crate::stream::Stream;
use crate::wire::Meta;

#[derive(Clone)]
pub struct RequestCtx {
    pub stream: Stream,
    pub meta: Meta,
}

pub trait Handler: Send + Sync + 'static {
    fn handle(
        &self,
        msg: Box<dyn Message>,
        ctx: RequestCtx,
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

    pub fn handler(&self, type_name: &str) -> Option<Arc<dyn Handler>> {
        self.handlers.get(type_name).cloned()
    }
}
