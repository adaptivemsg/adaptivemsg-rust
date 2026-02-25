use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::sync::{mpsc, Mutex as AsyncMutex, Notify};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::error::Error;
use crate::message::Message;
use crate::wire::{Envelope, Meta};

const STREAM_QUEUE_SIZE: usize = 1024;

type StreamId = u16;

const STREAM_ID_XOR: StreamId = 0xA5A5;
const DEFAULT_STREAM_ID: StreamId = 0;
const DEFAULT_STREAM_ID_ENCODED: u32 =
    ((DEFAULT_STREAM_ID as u32) << 16) | ((DEFAULT_STREAM_ID ^ STREAM_ID_XOR) as u32);

const FRAME_VERSION_MAJOR_V1: u8 = 1;
const FRAME_VERSION_MINOR_V1: u8 = 0;
const FRAME_HEADER_LEN_V1: usize = 12;

pub(crate) type DispatchFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
pub(crate) type DispatchFn = Arc<dyn Fn(Stream, Envelope) -> DispatchFuture + Send + Sync>;

#[doc(hidden)]
pub struct ConnectionInner {
    id: u64,
    outbound_sender: mpsc::Sender<OutboundFrame>,
    streams: Mutex<HashMap<StreamId, Stream>>,
    peer_addr: Option<String>,
    on_new_stream: Option<Arc<dyn Fn(&Stream) + Send + Sync>>,
    on_close_stream: Option<Arc<dyn Fn(&Stream) + Send + Sync>>,
    dispatch: DispatchFn,
    closed: AtomicBool,
    reader_task: Mutex<Option<JoinHandle<()>>>,
    writer_task: Mutex<Option<JoinHandle<()>>>,
    closed_notify: Notify,
    next_stream_id: AtomicU16,
}

pub type Connection = Arc<ConnectionInner>;

pub(crate) type ConnectionParts<R, W> = (Connection, R, W, mpsc::Receiver<OutboundFrame>);

pub type Stream = Arc<StreamInner>;

type InboundFrame = (StreamId, Vec<u8>);
type OutboundFrame = (StreamId, Envelope);

#[doc(hidden)]
pub struct StreamInner {
    id: StreamId,
    connection: Connection,
    inbox_receiver: AsyncMutex<mpsc::Receiver<Envelope>>,
    inbox_sender: mpsc::Sender<Envelope>,
    incoming_bytes_sender: mpsc::Sender<Vec<u8>>,
    context: Mutex<Option<Arc<dyn Any + Send + Sync>>>,
    recv_timeout: Mutex<Option<Duration>>,
}

static CONN_SEQ: AtomicU64 = AtomicU64::new(0);

impl ConnectionInner {
    fn mark_closed(&self) {
        self.closed.store(true, Ordering::Relaxed);
        self.closed_notify.notify_waiters();
    }

    pub(crate) fn new_with_dispatch<RW>(
        io: RW,
        peer_addr: Option<String>,
        dispatch: DispatchFn,
        on_new_stream: Option<Arc<dyn Fn(&Stream) + Send + Sync>>,
        on_close_stream: Option<Arc<dyn Fn(&Stream) + Send + Sync>>,
    ) -> ConnectionParts<ReadHalf<RW>, WriteHalf<RW>>
    where
        RW: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (read_half, write_half) = tokio::io::split(io);
        let (connection, outbound_receiver) =
            Self::new_unstarted(peer_addr, dispatch, on_new_stream, on_close_stream);
        (connection, read_half, write_half, outbound_receiver)
    }

    pub(crate) fn from_split_with_dispatch<R, W>(
        reader: R,
        writer: W,
        peer_addr: Option<String>,
        dispatch: DispatchFn,
        on_new_stream: Option<Arc<dyn Fn(&Stream) + Send + Sync>>,
        on_close_stream: Option<Arc<dyn Fn(&Stream) + Send + Sync>>,
    ) -> ConnectionParts<R, W>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (connection, outbound_receiver) =
            Self::new_unstarted(peer_addr, dispatch, on_new_stream, on_close_stream);
        (connection, reader, writer, outbound_receiver)
    }

    fn new_unstarted(
        peer_addr: Option<String>,
        dispatch: DispatchFn,
        on_new_stream: Option<Arc<dyn Fn(&Stream) + Send + Sync>>,
        on_close_stream: Option<Arc<dyn Fn(&Stream) + Send + Sync>>,
    ) -> (Connection, mpsc::Receiver<OutboundFrame>) {
        let (outbound_sender, outbound_receiver) =
            mpsc::channel::<OutboundFrame>(STREAM_QUEUE_SIZE);

        let connection: Connection = Arc::new(ConnectionInner {
            id: CONN_SEQ.fetch_add(1, Ordering::Relaxed) + 1,
            outbound_sender,
            streams: Mutex::new(HashMap::new()),
            peer_addr,
            on_new_stream,
            on_close_stream,
            dispatch,
            closed: AtomicBool::new(false),
            reader_task: Mutex::new(None),
            writer_task: Mutex::new(None),
            closed_notify: Notify::new(),
            next_stream_id: AtomicU16::new(1),
        });

        let _ = connection.create_stream(DEFAULT_STREAM_ID);

        (connection, outbound_receiver)
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn peer_addr(&self) -> Option<String> {
        self.peer_addr.clone()
    }

    pub fn close(&self) {
        self.mark_closed();
        if let Some(task) = self.reader_task.lock().unwrap().take() {
            task.abort();
        }
        if let Some(task) = self.writer_task.lock().unwrap().take() {
            task.abort();
        }
    }

    pub(crate) fn close_all_streams(&self) {
        let streams: Vec<Stream> = self.streams.lock().unwrap().values().cloned().collect();
        for stream in streams {
            stream.close();
        }
    }

    pub(crate) async fn wait_closed(&self) {
        if self.closed.load(Ordering::Relaxed) {
            return;
        }
        self.closed_notify.notified().await;
    }

    fn default_stream(self: &Arc<Self>) -> Stream {
        self.select_stream(DEFAULT_STREAM_ID)
    }

    pub fn set_recv_timeout(self: &Arc<Self>, timeout: Duration) {
        self.default_stream().set_recv_timeout(timeout);
    }

    pub async fn send<M: Message>(self: &Arc<Self>, msg: M) -> Result<(), Error> {
        self.default_stream().send(msg).await
    }

    pub async fn recv<T: Message + 'static>(self: &Arc<Self>) -> Result<T, Error> {
        self.default_stream().recv::<T>().await
    }

    pub async fn send_recv<TReq: Message, TResp: Message + 'static>(
        self: &Arc<Self>,
        msg: TReq,
    ) -> Result<TResp, Error> {
        self.default_stream().send_recv::<TReq, TResp>(msg).await
    }

    pub fn new_stream(self: &Arc<Self>) -> Stream {
        let stream_id = self.next_stream_id.fetch_add(1, Ordering::Relaxed);
        self.create_stream(stream_id)
    }

    pub(crate) fn start<R, W>(
        self: &Connection,
        reader: R,
        writer: W,
        outbound_receiver: mpsc::Receiver<OutboundFrame>,
    ) -> Connection
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let writer_task = self.spawn_writer(writer, outbound_receiver);
        let reader_task = self.spawn_reader(reader);
        *self.writer_task.lock().unwrap() = Some(writer_task);
        *self.reader_task.lock().unwrap() = Some(reader_task);

        self.clone()
    }
}

impl Drop for ConnectionInner {
    fn drop(&mut self) {
        self.close();
    }
}

impl StreamInner {
    pub fn id(self: &Arc<Self>) -> u16 {
        self.id
    }

    pub fn close(self: &Arc<Self>) {
        let on_close = self.connection.on_close_stream.clone();
        let removed = self
            .connection
            .streams
            .lock()
            .unwrap()
            .remove(&self.id)
            .is_some();
        if removed {
            if let Some(ref f) = on_close {
                f(self);
            }
        }
    }

    pub fn set_context<T>(self: &Arc<Self>, ctx: Arc<T>)
    where
        T: Any + Send + Sync + 'static,
    {
        *self.context.lock().unwrap() = Some(ctx);
    }

    pub fn get_context<T>(self: &Arc<Self>) -> Option<Arc<T>>
    where
        T: Any + Send + Sync + 'static,
    {
        self.context
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|ctx| Arc::clone(ctx).downcast::<T>().ok())
    }

    pub fn set_recv_timeout(self: &Arc<Self>, timeout: Duration) {
        let mut guard = self.recv_timeout.lock().unwrap();
        if timeout.is_zero() {
            *guard = None;
        } else {
            *guard = Some(timeout);
        }
    }

    pub async fn send<M: Message>(self: &Arc<Self>, msg: M) -> Result<(), Error> {
        self.send_boxed(Box::new(msg), Meta::default()).await
    }

    pub async fn send_with_meta<M: Message>(
        self: &Arc<Self>,
        msg: M,
        meta: Meta,
    ) -> Result<(), Error> {
        self.send_boxed(Box::new(msg), meta).await
    }

    pub async fn send_boxed(
        self: &Arc<Self>,
        msg: Box<dyn Message>,
        meta: Meta,
    ) -> Result<(), Error> {
        let env = Envelope::new(meta, msg);
        let frame: OutboundFrame = (self.id, env);
        self.connection
            .outbound_sender
            .send(frame)
            .await
            .map_err(|_| Error::Closed)
    }

    pub(crate) async fn push_env(self: &Arc<Self>, env: Envelope) -> Result<(), Error> {
        self.inbox_sender
            .send(env)
            .await
            .map_err(|_| Error::Closed)
    }

    pub async fn recv<T: Message + 'static>(self: &Arc<Self>) -> Result<T, Error> {
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
        self: &Arc<Self>,
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

    pub async fn recv_raw(self: &Arc<Self>) -> Result<Envelope, Error> {
        let timeout = *self.recv_timeout.lock().unwrap();
        let mut inbox = self.inbox_receiver.lock().await;
        if let Some(timeout) = timeout {
            match tokio::time::timeout(timeout, inbox.recv()).await {
                Ok(Some(env)) => Ok(env),
                Ok(None) => Err(Error::Closed),
                Err(_) => Err(Error::RecvTimeout),
            }
        } else {
            inbox.recv().await.ok_or(Error::Closed)
        }
    }
}

impl ConnectionInner {
    fn create_stream(self: &Connection, stream_id: StreamId) -> Stream {
        let (inbox_sender, inbox_receiver) = mpsc::channel::<Envelope>(STREAM_QUEUE_SIZE);
        let (incoming_bytes_sender, incoming_bytes_receiver) =
            mpsc::channel::<Vec<u8>>(STREAM_QUEUE_SIZE);
        let stream: Stream = Arc::new(StreamInner {
            id: stream_id,
            connection: self.clone(),
            inbox_receiver: AsyncMutex::new(inbox_receiver),
            inbox_sender,
            incoming_bytes_sender,
            context: Mutex::new(None),
            recv_timeout: Mutex::new(None),
        });
        self.streams
            .lock()
            .unwrap()
            .insert(stream_id, stream.clone());
        self.spawn_stream_task(stream.clone(), incoming_bytes_receiver);
        stream
    }

    fn spawn_stream_task(
        self: &Connection,
        stream: Stream,
        mut incoming_bytes_receiver: mpsc::Receiver<Vec<u8>>,
    ) {
        let dispatch = self.dispatch.clone();
        let _ = tokio::spawn(async move {
            while let Some(payload) = incoming_bytes_receiver.recv().await {
                let env = match postcard::from_bytes::<Envelope>(&payload) {
                    Ok(env) => env,
                    Err(err) => {
                        warn!("decode failed: {err}");
                        stream.connection.mark_closed();
                        break;
                    }
                };

                dispatch(stream.clone(), env).await;
            }
        });
    }

    fn select_stream(self: &Connection, stream_id: StreamId) -> Stream {
        if let Some(stream) = self.streams.lock().unwrap().get(&stream_id).cloned() {
            return stream;
        }

        let stream = self.create_stream(stream_id);
        if stream_id != DEFAULT_STREAM_ID {
            if let Some(ref f) = self.on_new_stream {
                f(&stream);
            }
        }
        stream
    }

    fn spawn_writer<W>(
        self: &Connection,
        mut writer: W,
        mut outbound_receiver: mpsc::Receiver<OutboundFrame>,
    ) -> JoinHandle<()>
    where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let connection = self.clone();
        tokio::spawn(async move {
            while let Some((stream_id, env)) = outbound_receiver.recv().await {
                if let Err(err) = Self::write_envelope(&mut writer, stream_id, &env).await {
                    warn!("write failed: {err}");
                    connection.mark_closed();
                    break;
                }
            }
        })
    }

    fn spawn_reader<R>(self: &Connection, mut reader: R) -> JoinHandle<()>
    where
        R: AsyncRead + Unpin + Send + 'static,
    {
        let connection = self.clone();
        tokio::spawn(async move {
            loop {
                let (stream_id, payload) = match Self::read_frame(&mut reader).await {
                    Ok(frame) => frame,
                    Err(err) => {
                        debug!("read loop ended: {err}");
                        connection.mark_closed();
                        break;
                    }
                };

                let stream = connection.select_stream(stream_id);
                let _ = stream.incoming_bytes_sender.send(payload).await;
            }
        })
    }

    fn encode_stream_id(stream_id: StreamId) -> u32 {
        let part_a = stream_id;
        let part_b = stream_id ^ STREAM_ID_XOR;
        ((part_a as u32) << 16) | (part_b as u32)
    }

    fn decode_stream_id(raw: u32) -> Result<StreamId, Error> {
        let part_a = (raw >> 16) as u16;
        let part_b = (raw & 0xFFFF) as u16;
        let id_a = part_a;
        let id_b = part_b ^ STREAM_ID_XOR;
        if id_a != id_b {
            return Err(Error::StreamIdMismatch { a: id_a, b: id_b });
        }
        Ok(id_a)
    }

    // Frame layout (big endian for multi-byte fields):
    // [0]: version major (u8)
    // [1]: version minor (u8)
    // [2..6): stream id word (u16 part A | u16 part B)
    //   - part A: raw id
    //   - part B: id ^ STREAM_ID_XOR
    // [6..8): reserved (u16)
    // [8..12): payload_len (u32)
    // [12..): payload bytes (postcard-encoded Envelope: meta + msg)
    fn build_header(
        stream_id: StreamId,
        payload_len: usize,
    ) -> Result<[u8; FRAME_HEADER_LEN_V1], Error> {
        if payload_len > u32::MAX as usize {
            return Err(Error::FrameTooLarge(payload_len));
        }

        let mut header = [0u8; FRAME_HEADER_LEN_V1];
        let raw_stream_id = if stream_id == DEFAULT_STREAM_ID {
            DEFAULT_STREAM_ID_ENCODED
        } else {
            Self::encode_stream_id(stream_id)
        };

        header[0] = FRAME_VERSION_MAJOR_V1;
        header[1] = FRAME_VERSION_MINOR_V1;
        header[2..6].copy_from_slice(&raw_stream_id.to_be_bytes());
        header[8..12].copy_from_slice(&(payload_len as u32).to_be_bytes());
        Ok(header)
    }

    fn parse_header(header: [u8; FRAME_HEADER_LEN_V1]) -> Result<(StreamId, usize), Error> {
        let major = header[0];
        let minor = header[1];
        if major != FRAME_VERSION_MAJOR_V1 || minor != FRAME_VERSION_MINOR_V1 {
            return Err(Error::UnsupportedFrameVersion { major, minor });
        }

        let raw_stream_id = u32::from_be_bytes(header[2..6].try_into().unwrap());
        let stream_id = if raw_stream_id == DEFAULT_STREAM_ID_ENCODED {
            DEFAULT_STREAM_ID
        } else {
            Self::decode_stream_id(raw_stream_id)?
        };
        let len = u32::from_be_bytes(header[8..12].try_into().unwrap()) as usize;
        Ok((stream_id, len))
    }

    async fn write_envelope<W>(
        writer: &mut W,
        stream_id: StreamId,
        env: &Envelope,
    ) -> Result<(), Error>
    where
        W: AsyncWrite + Unpin,
    {
        let payload = postcard::to_stdvec(env)?;
        let header = Self::build_header(stream_id, payload.len())?;
        writer.write_all(&header).await?;
        writer.write_all(&payload).await?;
        writer.flush().await?;
        Ok(())
    }

    async fn read_frame<R>(reader: &mut R) -> Result<InboundFrame, Error>
    where
        R: AsyncRead + Unpin,
    {
        let mut header = [0u8; FRAME_HEADER_LEN_V1];
        reader.read_exact(&mut header).await?;
        let (stream_id, payload_len) = Self::parse_header(header)?;
        let mut payload = vec![0u8; payload_len];
        reader.read_exact(&mut payload).await?;
        Ok((stream_id, payload))
    }
}
