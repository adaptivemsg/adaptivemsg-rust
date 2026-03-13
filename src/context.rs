use std::any::Any;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::task::JoinHandle;

use crate::error::Error;
use crate::stream::Stream;

pub type StreamContext = Arc<StreamContextInner>;
pub type Context = Arc<ContextInner>;

pub struct ContextInner {
    data: Mutex<Option<Arc<dyn Any + Send + Sync>>>,
}

impl ContextInner {
    pub(crate) fn new() -> Self {
        Self {
            data: Mutex::new(None),
        }
    }

    pub fn set_context<T>(&self, ctx: Arc<T>)
    where
        T: Any + Send + Sync + 'static,
    {
        *self.data.lock().unwrap() = Some(ctx);
    }

    pub fn get_context<T>(&self) -> Option<Arc<T>>
    where
        T: Any + Send + Sync + 'static,
    {
        self.data
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|ctx| Arc::clone(ctx).downcast::<T>().ok())
    }
}

pub struct StreamContextInner {
    pub(crate) stream: Stream,
    pub(crate) context: Context,
    handler_task_active: AtomicBool,
}

impl StreamContextInner {
    pub(crate) fn new(stream: Stream, context: Context) -> Self {
        Self {
            stream,
            context,
            handler_task_active: AtomicBool::new(false),
        }
    }

    pub fn set_context<T>(&self, ctx: Arc<T>)
    where
        T: Any + Send + Sync + 'static,
    {
        self.context.set_context(ctx);
    }

    pub fn get_context<T>(&self) -> Option<Arc<T>>
    where
        T: Any + Send + Sync + 'static,
    {
        self.context.get_context::<T>()
    }

    /// Spawn a background task tied to this stream; only one active task is allowed.
    pub fn new_task<F, Fut>(self: &Arc<Self>, f: F) -> Result<JoinHandle<()>, Error>
    where
        F: FnOnce(Stream) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let stream = Arc::clone(&self.stream);
        if self
            .handler_task_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(Error::HandlerTaskBusy);
        }
        let guard = HandlerTaskGuard {
            ctx: Arc::clone(self),
        };
        let handle = tokio::spawn(async move {
            let _guard = guard;
            f(stream).await;
        });
        Ok(handle)
    }
}

struct HandlerTaskGuard {
    ctx: Arc<StreamContextInner>,
}

impl Drop for HandlerTaskGuard {
    fn drop(&mut self) {
        self.ctx.handler_task_active.store(false, Ordering::Release);
    }
}
