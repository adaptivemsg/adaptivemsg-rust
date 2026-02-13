use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::task::JoinHandle;
use tokio::sync::{mpsc, Mutex as AsyncMutex, Notify};
use tracing::{debug, warn};

use crate::error::Error;
use crate::message::Message;
use crate::registry::{ContextStream, Registry};
use crate::wire::{Envelope, Meta};

const STREAM_QUEUE_SIZE: usize = 1024;
const MAX_FRAME_SIZE: usize = 8 * 1024 * 1024;

pub(crate) struct Connection {
    inner: Arc<ConnectionInner>,
    next_stream_id: AtomicU64,
    default_stream: Stream,
}

#[derive(Clone)]
pub struct Conn {
    inner: Arc<Connection>,
}

pub struct ConnectionBuilder<R, W> {
    conn: Arc<Connection>,
    reader: R,
    writer: W,
    outbound_rx: mpsc::Receiver<Envelope>,
}

#[derive(Clone)]
pub struct Stream {
    inner: Arc<StreamInner>,
}

struct Frame {
    stream_id: u64,
    buf: Vec<u8>,
}

struct StreamInner {
    id: u64,
    conn: Arc<ConnectionInner>,
    rx: AsyncMutex<mpsc::Receiver<Envelope>>,
    tx: mpsc::Sender<Envelope>,
    incoming_tx: mpsc::Sender<Frame>,
    context: Mutex<Option<Arc<dyn Any + Send + Sync>>>,
    recv_timeout: Mutex<Option<Duration>>,
}

static CONN_SEQ: AtomicU64 = AtomicU64::new(0);

struct ConnectionInner {
    id: u64,
    outbound: mpsc::Sender<Envelope>,
    streams: Mutex<HashMap<u64, Stream>>,
    registry: Option<Registry>,
    peer_addr: Option<String>,
    on_new_stream: Option<Arc<dyn Fn(&Stream) + Send + Sync>>,
    on_close_stream: Option<Arc<dyn Fn(&Stream) + Send + Sync>>,
    closed: AtomicBool,
    reader_task: Mutex<Option<JoinHandle<()>>>,
    writer_task: Mutex<Option<JoinHandle<()>>>,
    closed_notify: Notify,
}

impl Connection {
    pub fn new<RW>(
        io: RW,
        peer_addr: Option<String>,
        registry: Option<Registry>,
        on_new_stream: Option<Arc<dyn Fn(&Stream) + Send + Sync>>,
        on_close_stream: Option<Arc<dyn Fn(&Stream) + Send + Sync>>,
    ) -> ConnectionBuilder<ReadHalf<RW>, WriteHalf<RW>>
    where
        RW: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (read_half, write_half) = tokio::io::split(io);
        let (conn, outbound_rx) =
            Self::new_unstarted(peer_addr, registry, on_new_stream, on_close_stream);
        ConnectionBuilder {
            conn,
            reader: read_half,
            writer: write_half,
            outbound_rx,
        }
    }

    pub fn from_split<R, W>(
        reader: R,
        writer: W,
        peer_addr: Option<String>,
        registry: Option<Registry>,
        on_new_stream: Option<Arc<dyn Fn(&Stream) + Send + Sync>>,
        on_close_stream: Option<Arc<dyn Fn(&Stream) + Send + Sync>>,
    ) -> ConnectionBuilder<R, W>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (conn, outbound_rx) =
            Self::new_unstarted(peer_addr, registry, on_new_stream, on_close_stream);
        ConnectionBuilder {
            conn,
            reader,
            writer,
            outbound_rx,
        }
    }

    fn new_unstarted(
        peer_addr: Option<String>,
        registry: Option<Registry>,
        on_new_stream: Option<Arc<dyn Fn(&Stream) + Send + Sync>>,
        on_close_stream: Option<Arc<dyn Fn(&Stream) + Send + Sync>>,
    ) -> (Arc<Connection>, mpsc::Receiver<Envelope>) {
        let (outbound_tx, outbound_rx) = mpsc::channel::<Envelope>(STREAM_QUEUE_SIZE);

        let inner = Arc::new(ConnectionInner {
            id: CONN_SEQ.fetch_add(1, Ordering::Relaxed) + 1,
            outbound: outbound_tx,
            streams: Mutex::new(HashMap::new()),
            registry,
            peer_addr,
            on_new_stream,
            on_close_stream,
            closed: AtomicBool::new(false),
            reader_task: Mutex::new(None),
            writer_task: Mutex::new(None),
            closed_notify: Notify::new(),
        });

        let default_stream = create_stream(inner.clone(), 0);

        let conn = Connection {
            inner,
            next_stream_id: AtomicU64::new(1),
            default_stream,
        };

        (Arc::new(conn), outbound_rx)
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
        create_stream(self.inner.clone(), stream_id)
    }

}

impl Conn {
    pub fn new_stream(&self) -> Stream {
        self.inner.new_stream()
    }

    pub fn close(&self) {
        self.inner.close();
    }

    pub fn peer_addr(&self) -> Option<String> {
        self.inner.peer_addr()
    }

    pub fn id(&self) -> u64 {
        self.inner.id()
    }

    pub fn set_recv_timeout(&self, timeout: Duration) {
        self.inner.stream().set_recv_timeout(timeout);
    }

    pub async fn send<M: Message>(&self, msg: M) -> Result<(), Error> {
        self.inner.send(msg).await
    }

    pub async fn recv<T: Message + 'static>(&self) -> Result<T, Error> {
        self.inner.recv::<T>().await
    }

    pub async fn send_recv<TReq: Message, TResp: Message + 'static>(
        &self,
        msg: TReq,
    ) -> Result<TResp, Error> {
        self.inner.send_recv::<TReq, TResp>(msg).await
    }

    pub(crate) async fn wait_closed(&self) {
        self.inner.wait_closed().await;
    }

    pub(crate) fn close_all_streams(&self) {
        self.inner.close_all_streams();
    }
}

impl<R, W> ConnectionBuilder<R, W>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    pub fn connection(&self) -> Conn {
        Conn {
            inner: Arc::clone(&self.conn),
        }
    }

    pub fn start(self) -> Conn {
        let ConnectionBuilder {
            conn,
            reader,
            writer,
            outbound_rx,
        } = self;
        let inner = conn.inner.clone();
        let writer_task = spawn_writer(inner.clone(), writer, outbound_rx);
        let reader_task = spawn_reader(inner.clone(), reader);
        *inner.writer_task.lock().unwrap() = Some(writer_task);
        *inner.reader_task.lock().unwrap() = Some(reader_task);

        Conn { inner: conn }
    }
}

fn create_stream(inner: Arc<ConnectionInner>, stream_id: u64) -> Stream {
    let (tx, rx) = mpsc::channel::<Envelope>(STREAM_QUEUE_SIZE);
    let (incoming_tx, incoming_rx) = mpsc::channel::<Frame>(STREAM_QUEUE_SIZE);
    let stream = Stream {
        inner: Arc::new(StreamInner {
            id: stream_id,
            conn: inner.clone(),
            rx: AsyncMutex::new(rx),
            tx,
            incoming_tx,
            context: Mutex::new(None),
            recv_timeout: Mutex::new(None),
        }),
    };
    inner.streams.lock().unwrap().insert(stream_id, stream.clone());
    spawn_stream_task(stream.clone(), incoming_rx);
    stream
}

fn spawn_stream_task(stream: Stream, mut rx: mpsc::Receiver<Frame>) {
    let _ = tokio::spawn(async move {
        while let Some(frame) = rx.recv().await {
            let env = match postcard::from_bytes::<Envelope>(&frame.buf) {
                Ok(env) => env,
                Err(err) => {
                    warn!("decode failed: {err}");
                    stream.inner.conn.closed.store(true, Ordering::Relaxed);
                    stream.inner.conn.closed_notify.notify_waiters();
                    break;
                }
            };

            if env.stream_id() != frame.stream_id {
                warn!(
                    "stream id mismatch: header {} payload {}",
                    frame.stream_id,
                    env.stream_id()
                );
                stream.inner.conn.closed.store(true, Ordering::Relaxed);
                stream.inner.conn.closed_notify.notify_waiters();
                break;
            }

            if let Some(registry) = stream.inner.conn.registry.as_ref() {
                let type_name = env.msg().type_name();
                if let Some(handler) = registry.handler(type_name) {
                    let (_stream_id, meta, msg) = env.into_parts();
                    let ctx = ContextStream::new(stream.clone(), meta);
                    match handler.handle(msg, ctx).await {
                        Ok(Some(reply)) => {
                            let _ = stream.send_boxed(reply, Meta::default()).await;
                        }
                        Ok(None) => {
                            let _ = stream
                                .send_boxed(Box::new(crate::message::OkReply), Meta::default())
                                .await;
                        }
                        Err(err) => {
                            warn!("handler error: {err}");
                            let _ = stream
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
                    continue;
                }
            }

            let _ = stream.inner.tx.send(env).await;
        }
    });
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
        let on_close = self.inner.conn.on_close_stream.clone();
        let removed = self
            .inner
            .conn
            .streams
            .lock()
            .unwrap()
            .remove(&self.inner.id)
            .is_some();
        if removed {
            if let Some(ref f) = on_close {
                f(self);
            }
        }
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
) -> JoinHandle<()>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        while let Some(env) = outbound_rx.recv().await {
            if let Err(err) = write_envelope(&mut writer, &env).await {
                warn!("write failed: {err}");
                inner.closed.store(true, Ordering::Relaxed);
                inner.closed_notify.notify_waiters();
                break;
            }
        }
    })
}

fn spawn_reader<R>(inner: Arc<ConnectionInner>, mut reader: R) -> JoinHandle<()>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        loop {
            let frame = match read_frame(&mut reader).await {
                Ok(frame) => frame,
                Err(err) => {
                    debug!("read loop ended: {err}");
                    inner.closed.store(true, Ordering::Relaxed);
                    inner.closed_notify.notify_waiters();
                    break;
                }
            };

            let stream_id = frame.stream_id;
            let stream = match inner.streams.lock().unwrap().get(&stream_id).cloned() {
                Some(existing) => existing,
                None => {
                    let stream = create_stream(inner.clone(), stream_id);
                    if let Some(ref f) = inner.on_new_stream {
                        f(&stream);
                    }
                    stream
                }
            };

            let _ = stream.inner.incoming_tx.send(frame).await;
        }
    })
}

async fn write_envelope<W>(writer: &mut W, env: &Envelope) -> Result<(), Error>
where
    W: AsyncWrite + Unpin,
{
    let buf = postcard::to_stdvec(env)?;
    if buf.len() > MAX_FRAME_SIZE {
        return Err(Error::FrameTooLarge(buf.len()));
    }
    if buf.len() > u32::MAX as usize {
        return Err(Error::FrameTooLarge(buf.len()));
    }
    let mut header = [0u8; 12];
    header[..8].copy_from_slice(&env.stream_id().to_be_bytes());
    header[8..].copy_from_slice(&(buf.len() as u32).to_be_bytes());
    writer.write_all(&header).await?;
    writer.write_all(&buf).await?;
    writer.flush().await?;
    Ok(())
}

async fn read_frame<R>(reader: &mut R) -> Result<Frame, Error>
where
    R: AsyncRead + Unpin,
{
    let mut header = [0u8; 12];
    reader.read_exact(&mut header).await?;
    let stream_id = u64::from_be_bytes(header[..8].try_into().unwrap());
    let len = u32::from_be_bytes(header[8..].try_into().unwrap()) as usize;
    if len > MAX_FRAME_SIZE {
        return Err(Error::FrameTooLarge(len));
    }
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await?;
    Ok(Frame { stream_id, buf })
}
