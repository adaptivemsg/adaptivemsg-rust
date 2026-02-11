use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use futures::future::BoxFuture;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::task::JoinHandle;
use tokio::sync::{mpsc, Mutex as AsyncMutex, Notify};
use tracing::{debug, warn};

use crate::error::Error;
use crate::message::Message;
use crate::registry::{Handler, Registry, ContextStream};
use crate::wire::{Envelope, Meta};
use crate::worker::{WorkerConfig, WorkerPool};

const STREAM_QUEUE_SIZE: usize = 1024;
const NEW_STREAM_QUEUE_SIZE: usize = 128;

struct DispatchItem {
    msg: Box<dyn Message>,
    handler: Arc<dyn Handler>,
    stream: Stream,
    meta: Meta,
}

pub struct Connection {
    inner: Arc<ConnectionInner>,
    new_stream_rx: AsyncMutex<mpsc::Receiver<Stream>>,
    next_stream_id: AtomicU64,
    default_stream: Stream,
}

#[derive(Clone)]
pub struct Stream {
    inner: Arc<StreamInner>,
}

struct StreamInner {
    id: u64,
    conn: Arc<ConnectionInner>,
    rx: AsyncMutex<mpsc::Receiver<Envelope>>,
    tx: mpsc::Sender<Envelope>,
    context: Mutex<Option<Arc<dyn Any + Send + Sync>>>,
    recv_timeout: Mutex<Option<Duration>>,
}

static CONN_SEQ: AtomicU64 = AtomicU64::new(0);

struct ConnectionInner {
    id: u64,
    outbound: mpsc::Sender<Envelope>,
    streams: Mutex<HashMap<u64, Stream>>,
    new_stream_tx: mpsc::Sender<Stream>,
    registry: Option<Registry>,
    worker: Option<WorkerPool<DispatchItem>>,
    peer_addr: Option<String>,
    closed: AtomicBool,
    reader_task: Mutex<Option<JoinHandle<()>>>,
    writer_task: Mutex<Option<JoinHandle<()>>>,
    closed_notify: Notify,
}

impl Connection {
    pub fn new<RW>(
        io: RW,
        registry: Option<Registry>,
        worker_cfg: Option<WorkerConfig>,
    ) -> Self
    where
        RW: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        Self::new_with_peer_addr(io, None, registry, worker_cfg)
    }

    pub fn new_with_peer_addr<RW>(
        io: RW,
        peer_addr: Option<String>,
        registry: Option<Registry>,
        worker_cfg: Option<WorkerConfig>,
    ) -> Self
    where
        RW: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (read_half, write_half) = tokio::io::split(io);
        Self::from_split(read_half, write_half, peer_addr, registry, worker_cfg)
    }

    pub fn from_split<R, W>(
        reader: R,
        writer: W,
        peer_addr: Option<String>,
        registry: Option<Registry>,
        worker_cfg: Option<WorkerConfig>,
    ) -> Self
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (outbound_tx, outbound_rx) = mpsc::channel::<Envelope>(STREAM_QUEUE_SIZE);
        let (new_stream_tx, new_stream_rx) = mpsc::channel::<Stream>(NEW_STREAM_QUEUE_SIZE);

        let worker = registry.as_ref().and_then(|_| {
            let cfg = worker_cfg.unwrap_or_default();
            Some(WorkerPool::new(cfg, |item: DispatchItem| -> BoxFuture<'static, ()> {
                Box::pin(async move {
                    let stream = ContextStream::new(item.stream.clone(), item.meta.clone());
                    match item.handler.handle(item.msg, stream).await {
                        Ok(Some(reply)) => {
                            let _ = item.stream.send_boxed(reply, Meta::default()).await;
                        }
                        Ok(None) => {
                            let _ = item
                                .stream
                                .send_boxed(Box::new(crate::message::OkReply), Meta::default())
                                .await;
                        }
                        Err(err) => {
                            warn!("handler error: {err}");
                            let _ = item
                                .stream
                                .send_boxed(
                                    Box::new(crate::message::ErrorReply::new(
                                        "handler_error",
                                        err.to_string(),
                                    )),
                                    Meta::default(),
                                )
                                .await;
                        }
                    }
                })
            }))
        });

        let inner = Arc::new(ConnectionInner {
            id: CONN_SEQ.fetch_add(1, Ordering::Relaxed) + 1,
            outbound: outbound_tx,
            streams: Mutex::new(HashMap::new()),
            new_stream_tx,
            registry,
            worker,
            peer_addr,
            closed: AtomicBool::new(false),
            reader_task: Mutex::new(None),
            writer_task: Mutex::new(None),
            closed_notify: Notify::new(),
        });

        let writer_task = spawn_writer(inner.clone(), writer, outbound_rx);
        let reader_task = spawn_reader(inner.clone(), reader);
        *inner.writer_task.lock().unwrap() = Some(writer_task);
        *inner.reader_task.lock().unwrap() = Some(reader_task);

        let default_stream = {
            let (tx, rx) = mpsc::channel::<Envelope>(STREAM_QUEUE_SIZE);
            let stream = Stream {
                inner: Arc::new(StreamInner {
                    id: 0,
                    conn: inner.clone(),
                    rx: AsyncMutex::new(rx),
                    tx,
                    context: Mutex::new(None),
                    recv_timeout: Mutex::new(None),
                }),
            };
            inner.streams.lock().unwrap().insert(0, stream.clone());
            stream
        };

        Self {
            inner,
            new_stream_rx: AsyncMutex::new(new_stream_rx),
            next_stream_id: AtomicU64::new(1),
            default_stream,
        }
    }

    pub fn stream(&self) -> Stream {
        self.default_stream.clone()
    }

    pub fn id(&self) -> u64 {
        self.inner.id
    }

    pub fn peer_addr(&self) -> Option<String> {
        self.inner.peer_addr.clone()
    }

    pub fn close(&self) {
        self.inner.closed.store(true, Ordering::Relaxed);
        self.inner.closed_notify.notify_waiters();
        if let Some(task) = self.inner.reader_task.lock().unwrap().take() {
            task.abort();
        }
        if let Some(task) = self.inner.writer_task.lock().unwrap().take() {
            task.abort();
        }
    }

    pub fn close_all_streams(&self) {
        let streams: Vec<Stream> = self.inner.streams.lock().unwrap().values().cloned().collect();
        for stream in streams {
            stream.close();
        }
    }

    pub async fn wait_closed(&self) {
        if self.inner.closed.load(Ordering::Relaxed) {
            return;
        }
        self.inner.closed_notify.notified().await;
    }

    pub async fn send<M: Message>(&self, msg: M) -> Result<(), Error> {
        self.default_stream.send(msg).await
    }

    pub async fn recv<T: Message + 'static>(&self) -> Result<T, Error> {
        self.default_stream.recv::<T>().await
    }

    pub async fn send_recv<TReq: Message, TResp: Message + 'static>(
        &self,
        msg: TReq,
    ) -> Result<TResp, Error> {
        self.default_stream.send_recv::<TReq, TResp>(msg).await
    }

    pub fn new_stream(&self) -> Stream {
        let stream_id = self.next_stream_id.fetch_add(1, Ordering::Relaxed);
        self.make_stream(stream_id)
    }

    pub async fn accept_stream(&self) -> Option<Stream> {
        let mut rx = self.new_stream_rx.lock().await;
        rx.recv().await
    }

    fn make_stream(&self, stream_id: u64) -> Stream {
        let (tx, rx) = mpsc::channel::<Envelope>(STREAM_QUEUE_SIZE);
        let stream = Stream {
            inner: Arc::new(StreamInner {
                id: stream_id,
                conn: self.inner.clone(),
                rx: AsyncMutex::new(rx),
                tx,
                context: Mutex::new(None),
                recv_timeout: Mutex::new(None),
            }),
        };
        self.inner.streams.lock().unwrap().insert(stream_id, stream.clone());
        stream
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.close();
    }
}

impl Stream {
    pub fn id(&self) -> u64 {
        self.inner.id
    }

    pub fn close(&self) {
        self.inner.conn.streams.lock().unwrap().remove(&self.inner.id);
    }

    pub fn set_context<T>(&self, ctx: Arc<T>)
    where
        T: Any + Send + Sync + 'static,
    {
        *self.inner.context.lock().unwrap() = Some(ctx);
    }

    pub fn get_context<T>(&self) -> Option<Arc<T>>
    where
        T: Any + Send + Sync + 'static,
    {
        self.inner
            .context
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|ctx| Arc::clone(ctx).downcast::<T>().ok())
    }

    pub fn set_recv_timeout(&self, timeout: Duration) {
        let mut guard = self.inner.recv_timeout.lock().unwrap();
        if timeout.is_zero() {
            *guard = None;
        } else {
            *guard = Some(timeout);
        }
    }

    pub async fn send<M: Message>(&self, msg: M) -> Result<(), Error> {
        self.send_boxed(Box::new(msg), Meta::default()).await
    }

    pub async fn send_with_meta<M: Message>(&self, msg: M, meta: Meta) -> Result<(), Error> {
        self.send_boxed(Box::new(msg), meta).await
    }

    pub async fn send_boxed(&self, msg: Box<dyn Message>, meta: Meta) -> Result<(), Error> {
        let env = Envelope::new(self.inner.id, meta, msg);
        self.inner
            .conn
            .outbound
            .send(env)
            .await
            .map_err(|_| Error::Closed)
    }

    pub async fn recv<T: Message + 'static>(&self) -> Result<T, Error> {
        let env = self.recv_raw().await?;
        let expected = std::any::type_name::<T>();
        let got = env.msg().type_name();
        let boxed_any: Box<dyn std::any::Any> = env.into_msg();
        match boxed_any.downcast::<T>() {
            Ok(val) => Ok(*val),
            Err(_) => Err(Error::TypeMismatch { expected, got }),
        }
    }

    pub async fn send_recv<TReq: Message, TResp: Message + 'static>(
        &self,
        msg: TReq,
    ) -> Result<TResp, Error> {
        self.send(msg).await?;
        let env = self.recv_raw().await?;
        let got = env.msg().type_name();
        let boxed_any: Box<dyn std::any::Any> = env.into_msg();
        let boxed_any = match boxed_any.downcast::<crate::message::ErrorReply>() {
            Ok(err) => {
                let (code, message) = err.into_parts();
                return Err(Error::Remote { code, message });
            }
            Err(boxed_any) => boxed_any,
        };
        let expected = std::any::type_name::<TResp>();
        match boxed_any.downcast::<TResp>() {
            Ok(val) => Ok(*val),
            Err(_) => Err(Error::TypeMismatch { expected, got }),
        }
    }

    pub async fn recv_raw(&self) -> Result<Envelope, Error> {
        let timeout = *self.inner.recv_timeout.lock().unwrap();
        let mut rx = self.inner.rx.lock().await;
        if let Some(timeout) = timeout {
            match tokio::time::timeout(timeout, rx.recv()).await {
                Ok(Some(env)) => Ok(env),
                Ok(None) => Err(Error::Closed),
                Err(_) => Err(Error::RecvTimeout),
            }
        } else {
            rx.recv().await.ok_or(Error::Closed)
        }
    }
}

fn spawn_writer<W>(
    inner: Arc<ConnectionInner>,
    mut writer: W,
    mut outbound_rx: mpsc::Receiver<Envelope>,
) where
    W: AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        while let Some(env) = outbound_rx.recv().await {
            if let Err(err) = write_envelope(&mut writer, &env).await {
                warn!("write failed: {err}");
                inner.closed.store(true, Ordering::Relaxed);
                break;
            }
        }
    })
}

fn spawn_reader<R>(inner: Arc<ConnectionInner>, mut reader: R)
where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        loop {
            let env = match read_envelope(&mut reader).await {
                Ok(env) => env,
                Err(err) => {
                    debug!("read loop ended: {err}");
                    inner.closed.store(true, Ordering::Relaxed);
                    break;
                }
            };

            let stream_id = env.stream_id();
            let stream = match inner.streams.lock().unwrap().get(&stream_id).cloned() {
                Some(existing) => existing,
                None => {
                    let (tx, rx) = mpsc::channel::<Envelope>(STREAM_QUEUE_SIZE);
                    let stream = Stream {
                        inner: Arc::new(StreamInner {
                            id: stream_id,
                            conn: inner.clone(),
                            rx: AsyncMutex::new(rx),
                            tx,
                            context: Mutex::new(None),
                            recv_timeout: Mutex::new(None),
                        }),
                    };
                    inner.streams.lock().unwrap().insert(stream_id, stream.clone());
                    let _ = inner.new_stream_tx.send(stream.clone()).await;
                    stream
                }
            };

            if let Some(registry) = inner.registry.as_ref() {
                let type_name = env.msg().type_name();
                if let Some(handler) = registry.handler(type_name) {
                    if let Some(worker) = inner.worker.as_ref() {
                        let (_stream_id, meta, msg) = env.into_parts();
                        let priority = meta.priority();
                        let item = DispatchItem {
                            msg,
                            handler,
                            stream: stream.clone(),
                            meta,
                        };
                        worker.enqueue(item, priority).await;
                        continue;
                    }
                }
            }

            let _ = stream.inner.tx.send(env).await;
        }
    })
}

async fn write_envelope<W>(writer: &mut W, env: &Envelope) -> Result<(), Error>
where
    W: AsyncWrite + Unpin,
{
    let buf = postcard::to_stdvec(env)?;
    if buf.len() > u32::MAX as usize {
        return Err(Error::FrameTooLarge(buf.len()));
    }
    let len = (buf.len() as u32).to_be_bytes();
    writer.write_all(&len).await?;
    writer.write_all(&buf).await?;
    writer.flush().await?;
    Ok(())
}

async fn read_envelope<R>(reader: &mut R) -> Result<Envelope, Error>
where
    R: AsyncRead + Unpin,
{
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await?;
    let env = postcard::from_bytes::<Envelope>(&buf)?;
    Ok(env)
}
