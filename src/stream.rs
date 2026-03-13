use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, Mutex as AsyncMutex};

use crate::connection::{Connection, HandlerJob};
use crate::error::Error;
use crate::message::{ErrorReply, Message, MessageDecode};
use crate::type_info::expected_wire_name;
use crate::raw_message::{decode_raw_as, RawMessage};

pub type Stream = Arc<StreamInner>;

const RECV_TIMEOUT_NONE: u64 = 0;

pub struct StreamInner {
    pub(crate) id: u32,
    pub(crate) connection: Connection,
    pub(crate) handler_rx: Option<AsyncMutex<mpsc::Receiver<HandlerJob>>>,
    pub(crate) handler_tx: Option<mpsc::Sender<HandlerJob>>,
    pub(crate) inbox_rx: AsyncMutex<mpsc::Receiver<RawMessage>>,
    pub(crate) inbox_tx: mpsc::Sender<RawMessage>,
    pub(crate) incoming_tx: mpsc::Sender<Vec<u8>>,
    recv_timeout_nanos: AtomicU64,
    recv_active: AtomicBool,
    peeked: Mutex<Option<RawMessage>>,
}

impl StreamInner {
    pub(crate) fn new(
        id: u32,
        connection: Connection,
        inbox_rx: mpsc::Receiver<RawMessage>,
        inbox_tx: mpsc::Sender<RawMessage>,
        incoming_tx: mpsc::Sender<Vec<u8>>,
        handler_rx: Option<AsyncMutex<mpsc::Receiver<HandlerJob>>>,
        handler_tx: Option<mpsc::Sender<HandlerJob>>,
    ) -> Self {
        Self {
            id,
            connection,
            handler_rx,
            handler_tx,
            inbox_rx: AsyncMutex::new(inbox_rx),
            inbox_tx,
            incoming_tx,
            recv_timeout_nanos: AtomicU64::new(RECV_TIMEOUT_NONE),
            recv_active: AtomicBool::new(false),
            peeked: Mutex::new(None),
        }
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn close(self: &Arc<Self>) {
        if let Some(stream_ctx) = self.connection.remove_stream(self.id) {
            self.connection.notify_close(&stream_ctx);
        }
    }

    pub fn set_recv_timeout(&self, timeout: Duration) {
        let nanos = if timeout.is_zero() {
            RECV_TIMEOUT_NONE
        } else {
            timeout.as_nanos().min(u64::MAX as u128) as u64
        };
        self.recv_timeout_nanos.store(nanos, Ordering::Relaxed);
    }

    pub async fn send<M: Message>(&self, msg: M) -> Result<(), Error> {
        self.send_boxed(Box::new(msg)).await
    }

    pub async fn recv<T: MessageDecode + 'static>(&self) -> Result<T, Error> {
        let raw = self.recv_raw().await?;
        let result = decode_raw_as::<T>(raw);
        if let Err(err) = result.as_ref() {
            self.protocol_error_for(err).await;
        }
        result
    }

    pub async fn send_recv<TReq: Message, TResp: MessageDecode + 'static>(
        &self,
        msg: TReq,
    ) -> Result<TResp, Error> {
        self.send(msg).await?;
        let raw = self.recv_raw().await?;
        if raw.wire == error_reply_wire_name() {
            match decode_raw_as::<ErrorReply>(raw) {
                Ok(reply) => {
                    let (code, message) = reply.into_parts();
                    return Err(Error::Remote { code, message });
                }
                Err(err) => {
                    self.protocol_error("codec_error", err.to_string()).await;
                    return Err(err);
                }
            }
        }
        let result = decode_raw_as::<TResp>(raw);
        if let Err(err) = result.as_ref() {
            self.protocol_error_for(err).await;
        }
        result
    }

    pub async fn peek_wire(&self) -> Result<String, Error> {
        let _guard = self.recv_guard()?;
        if let Some(msg) = self.peeked.lock().unwrap().as_ref() {
            return Ok(msg.wire.clone());
        }
        let msg = self.read_raw().await?;
        let wire = msg.wire.clone();
        *self.peeked.lock().unwrap() = Some(msg);
        Ok(wire)
    }

    pub(crate) async fn send_boxed(&self, msg: Box<dyn Message>) -> Result<(), Error> {
        let payload = self.connection.encode_message(msg.as_ref())?;
        let frame = (self.id, payload);
        self.connection.enqueue_frame(frame).await
    }

    pub(crate) async fn inbox_q(&self, msg: RawMessage) -> Result<(), Error> {
        self.inbox_tx.send(msg).await.map_err(|_| Error::Closed)
    }

    pub(crate) async fn handler_q(
        &self,
        handler: Arc<dyn crate::registry::Handler>,
        raw: RawMessage,
    ) -> Result<(), Error> {
        match self.handler_tx.as_ref() {
            Some(tx) => tx.send((handler, raw)).await.map_err(|_| Error::Closed),
            None => Err(Error::Closed),
        }
    }

    pub(crate) async fn recv_handler_job(&self) -> Result<HandlerJob, Error> {
        let Some(handler_rx) = self.handler_rx.as_ref() else {
            return Err(Error::Closed);
        };
        let mut handler_rx = handler_rx.lock().await;
        handler_rx.recv().await.ok_or(Error::Closed)
    }

    fn recv_guard(&self) -> Result<RecvGuard<'_>, Error> {
        if self
            .recv_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(Error::ConcurrentRecv);
        }
        Ok(RecvGuard {
            flag: &self.recv_active,
        })
    }

    async fn recv_raw(&self) -> Result<RawMessage, Error> {
        let _guard = self.recv_guard()?;
        if let Some(msg) = self.peeked.lock().unwrap().take() {
            return Ok(msg);
        }
        self.read_raw().await
    }

    async fn read_raw(&self) -> Result<RawMessage, Error> {
        let timeout_nanos = self.recv_timeout_nanos.load(Ordering::Relaxed);
        let mut inbox = self.inbox_rx.lock().await;
        if timeout_nanos == RECV_TIMEOUT_NONE {
            tokio::select! {
                msg = inbox.recv() => msg.ok_or(Error::Closed),
                _ = self.connection.wait_closed() => Err(Error::Closed),
            }
        } else {
            let timeout = Duration::from_nanos(timeout_nanos);
            tokio::select! {
                msg = inbox.recv() => msg.ok_or(Error::Closed),
                _ = self.connection.wait_closed() => Err(Error::Closed),
                _ = tokio::time::sleep(timeout) => Err(Error::RecvTimeout),
            }
        }
    }

    pub(crate) async fn protocol_error(&self, code: &str, message: String) {
        let _ = self
            .send_boxed(Box::new(ErrorReply::new(code, message)))
            .await;
        self.connection.remove_stream(self.id);
    }

    async fn protocol_error_for(&self, err: &Error) {
        match err {
            Error::TypeMismatch { expected, got } => {
                self.protocol_error(
                    "protocol_error",
                    format!("expected {expected} got {got}"),
                )
                .await;
            }
            _ => {
                self.protocol_error("codec_error", err.to_string()).await;
            }
        }
    }
}

struct RecvGuard<'a> {
    flag: &'a AtomicBool,
}

impl Drop for RecvGuard<'_> {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::Release);
    }
}

fn error_reply_wire_name() -> &'static str {
    expected_wire_name::<ErrorReply>()
}
