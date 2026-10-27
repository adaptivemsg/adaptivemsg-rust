use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, Mutex as AsyncMutex};
use tokio::sync::Notify;

use crate::connection::{ConnectionRef, HandlerJob, Owner};
use crate::debug::{
    DebugFailureCode, MessageCounters, StreamDebugCounters, StreamDebugState,
    FAILURE_STREAM_DECODE, FAILURE_STREAM_ENCODE, FAILURE_STREAM_ENQUEUE,
    FAILURE_STREAM_PROTOCOL, FAILURE_STREAM_PROTOCOL_REPLY_SEND, FAILURE_STREAM_RECV_TIMEOUT,
};
use crate::error::Error;
use crate::message::{ErrorReply, Message, MessageDecode};
use crate::type_info::expected_wire_name;
use crate::raw_message::{decode_raw_as, RawMessage};

pub(crate) type StreamRef = Arc<StreamInner>;

/// Handle to a logical stream within a connection.
///
/// Each stream has independent send/recv channels, its own recv timeout, and
/// carries one ordered conversation. `send` may be called from several tasks
/// at once, but `recv`, `peek_wire` and `send_recv` must not run concurrently
/// on the same stream: the extra call returns [`Error::ConcurrentRecv`], and a
/// rejected `send_recv` sends nothing. Use one stream per concurrent
/// conversation. Obtained via `Connection::new_stream()` or implicitly through
/// the connection's default stream methods. Clones share the stream, and a
/// handle keeps its connection open.
#[derive(Clone)]
pub struct Stream {
    inner: StreamRef,
    _owner: Option<Arc<Owner>>,
}

impl Stream {
    pub(crate) fn new(inner: StreamRef, owner: Option<Arc<Owner>>) -> Self {
        Self {
            inner,
            _owner: owner,
        }
    }
}

impl std::ops::Deref for Stream {
    type Target = Arc<StreamInner>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

const RECV_TIMEOUT_NONE: u64 = 0;

/// A logical stream for sending and receiving messages.
pub struct StreamInner {
    pub(crate) id: u32,
    pub(crate) connection: ConnectionRef,
    pub(crate) handler_tx: Option<mpsc::Sender<HandlerJob>>,
    pub(crate) inbox_rx: AsyncMutex<mpsc::Receiver<RawMessage>>,
    pub(crate) inbox_tx: mpsc::Sender<RawMessage>,
    pub(crate) incoming_tx: mpsc::Sender<Vec<u8>>,
    recv_timeout_nanos: AtomicU64,
    recv_active: AtomicBool,
    peeked: Mutex<Option<RawMessage>>,
    stale_replies: AtomicUsize,
    closed: AtomicBool,
    fin_started: AtomicBool,
    send_lock: AsyncMutex<()>,
    closed_notify: Notify,
    pub(crate) debug: StreamDebugCounters,
}

impl StreamInner {
    pub(crate) fn new(
        id: u32,
        connection: ConnectionRef,
        inbox_rx: mpsc::Receiver<RawMessage>,
        inbox_tx: mpsc::Sender<RawMessage>,
        incoming_tx: mpsc::Sender<Vec<u8>>,
        handler_tx: Option<mpsc::Sender<HandlerJob>>,
    ) -> Self {
        Self {
            id,
            connection,
            handler_tx,
            inbox_rx: AsyncMutex::new(inbox_rx),
            inbox_tx,
            incoming_tx,
            recv_timeout_nanos: AtomicU64::new(RECV_TIMEOUT_NONE),
            recv_active: AtomicBool::new(false),
            peeked: Mutex::new(None),
            stale_replies: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
            fin_started: AtomicBool::new(false),
            send_lock: AsyncMutex::new(()),
            closed_notify: Notify::new(),
            debug: StreamDebugCounters::new(),
        }
    }

    /// A stream that is not registered on `connection` and returns
    /// [`Error::Closed`] from every operation. Its ID is the control stream ID
    /// rather than 0 because closing stream 0 would remove the default stream.
    pub(crate) fn new_closed(connection: ConnectionRef) -> Self {
        let (inbox_tx, inbox_rx) = mpsc::channel(1);
        let (incoming_tx, _) = mpsc::channel(1);
        let stream = Self::new(
            crate::recovery_protocol::CONTROL_STREAM_ID,
            connection,
            inbox_rx,
            inbox_tx,
            incoming_tx,
            None,
        );
        stream.closed.store(true, Ordering::Release);
        stream.fin_started.store(true, Ordering::Release);
        stream
    }

    /// Stream identifier within the connection.
    pub fn id(&self) -> u32 {
        self.id
    }

    /// Returns a point-in-time diagnostic snapshot of this stream.
    pub fn debug_state(&self) -> StreamDebugState {
        let recv_nanos = self.recv_timeout_nanos.load(Ordering::Relaxed);
        let recv_timeout = if recv_nanos == RECV_TIMEOUT_NONE {
            Duration::ZERO
        } else {
            Duration::from_nanos(recv_nanos)
        };
        let handler_q_depth = self
            .handler_tx
            .as_ref()
            .map(|tx| tx.max_capacity() - tx.capacity())
            .unwrap_or(0);
        self.debug.build_state(
            self.id,
            self.closed.load(Ordering::Relaxed),
            recv_timeout,
            self.inbox_tx.max_capacity() - self.inbox_tx.capacity(),
            self.incoming_tx.max_capacity() - self.incoming_tx.capacity(),
            handler_q_depth,
        )
    }

    /// Close this stream and notify close callbacks.
    ///
    /// If this side opened the stream with `new_stream`, it first queues a FIN
    /// frame; the peer delivers the messages sent before it and then releases
    /// its copy. Only the opener sends FIN, so a stream opened by the peer is
    /// removed locally, as is stream 0, whose lifetime follows the connection;
    /// frames the peer still sends on such a stream are dropped.
    ///
    /// `close` does not wait: a task on the connection's runtime queues the
    /// FIN, so `close` can be called from any thread, including one outside
    /// the runtime.
    pub fn close(self: &Arc<Self>) {
        if !self.connection.opened_stream(self.id) {
            if let Some(stream_ctx) = self.connection.remove_peer_stream(self.id) {
                self.connection.notify_close(&stream_ctx);
            }
            return;
        }
        if self.fin_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let stream = Arc::clone(self);
        self.connection.spawn(async move {
            stream.send_fin().await;
            if let Some(stream_ctx) = stream.connection.remove_stream(stream.id) {
                stream.connection.notify_close(&stream_ctx);
            }
        });
    }

    async fn send_fin(&self) {
        let _guard = self.send_lock.lock().await;
        let frame = crate::connection::OutboundFrame::Plain {
            stream_id: self.id,
            flags: crate::frame::FRAME_FLAG_FIN,
            payload: Vec::new(),
        };
        if let Err(err) = self.connection.enqueue_frame(frame).await {
            self.debug
                .note_failure(FAILURE_STREAM_ENQUEUE, format!("fin enqueue failed: {err}"));
        }
    }

    /// Queue the peer's FIN as an empty payload behind the frames already
    /// received, so the decode task closes the stream only after delivering
    /// them.
    pub(crate) async fn queue_fin(&self) {
        let _ = self.incoming_tx.send(Vec::new()).await;
    }

    /// Marks the stream closed; returns false if it already was.
    pub(crate) fn close_channels(&self) -> bool {
        if self.closed.swap(true, Ordering::AcqRel) {
            return false;
        }
        self.closed_notify.notify_waiters();
        true
    }

    pub(crate) async fn wait_closed(&self) {
        // notify_waiters only wakes futures that already exist, so create the
        // future before checking the flag.
        let notified = self.closed_notify.notified();
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        notified.await;
    }

    /// Set the receive timeout for `recv` and `send_recv`.
    ///
    /// Use `Duration::ZERO` to disable timeouts.
    pub fn set_recv_timeout(&self, timeout: Duration) {
        let nanos = if timeout.is_zero() {
            RECV_TIMEOUT_NONE
        } else {
            timeout.as_nanos().min(u64::MAX as u128) as u64
        };
        self.recv_timeout_nanos.store(nanos, Ordering::Relaxed);
    }

    /// Send a message without waiting for a reply.
    ///
    /// The message is encoded with the connection's negotiated codec and
    /// enqueued for transmission on this stream. With recovery, it waits
    /// while `max_replay_bytes` of messages await acknowledgement. Returns
    /// [`Error::Closed`] if the stream or connection has been closed, or an
    /// [`Error::Codec`] if encoding fails.
    pub async fn send<M: Message>(&self, msg: M) -> Result<(), Error> {
        self.send_boxed(Box::new(msg)).await
    }

    /// Receive the next message and decode it as `T`.
    ///
    /// Blocks until a message arrives, the recv timeout expires
    /// ([`Error::RecvTimeout`]), or the stream/connection closes
    /// ([`Error::Closed`]). Returns [`Error::TypeMismatch`] if the received
    /// wire name does not match `T`, or [`Error::Codec`] on decode failure.
    pub async fn recv<T: MessageDecode + 'static>(&self) -> Result<T, Error> {
        let raw = self.recv_raw().await?;
        self.decode_reply(raw).await
    }

    /// Send a request and wait for a response of type `TResp`.
    ///
    /// If the remote sends `ErrorReply`, this returns `Error::Remote`. After
    /// [`Error::RecvTimeout`] the stream discards the late reply when it
    /// arrives, so the next request on this stream receives its own reply.
    /// Returns [`Error::ConcurrentRecv`] without sending `msg` if a `recv`,
    /// `peek_wire` or `send_recv` is already in progress on this stream.
    pub async fn send_recv<TResp: MessageDecode + 'static>(
        &self,
        msg: impl Message,
    ) -> Result<TResp, Error> {
        let _guard = self.recv_guard()?;
        self.send(msg).await?;
        let raw = match self.take_raw().await {
            Err(Error::RecvTimeout) => {
                // The recv guard serializes stale_replies, as it does peeked.
                self.stale_replies.fetch_add(1, Ordering::Relaxed);
                return Err(Error::RecvTimeout);
            }
            result => result?,
        };
        if raw.wire == error_reply_wire_name() {
            match decode_raw_as::<ErrorReply>(raw) {
                Ok(reply) => {
                    self.count(|c| &c.remote_errors);
                    let (code, message) = reply.into_parts();
                    return Err(Error::Remote { code, message });
                }
                Err(err) => {
                    self.note_decode_error(&err);
                    self.protocol_error("codec_error", err.to_string()).await;
                    return Err(err);
                }
            }
        }
        self.decode_reply(raw).await
    }

    /// Decodes a received message; a failure is answered with a protocol
    /// error, which closes the stream.
    async fn decode_reply<T: MessageDecode + 'static>(&self, raw: RawMessage) -> Result<T, Error> {
        let result = decode_raw_as::<T>(raw);
        if let Err(err) = result.as_ref() {
            self.note_decode_error(err);
            self.protocol_error_for(err).await;
        }
        result
    }

    /// Peek the wire name of the next message without consuming it.
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
        let _guard = self.send_lock.lock().await;
        if self.fin_started.load(Ordering::Acquire) || self.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let payload = match self.connection.encode_message(msg.as_ref()) {
            Ok(payload) => payload,
            Err(err) => {
                self.note_failure(FAILURE_STREAM_ENCODE, format!("encode failed: {err}"));
                return Err(err);
            }
        };
        let frame = crate::connection::OutboundFrame::Plain {
            stream_id: self.id,
            flags: 0,
            payload,
        };
        if let Err(err) = self.connection.enqueue_frame(frame).await {
            self.note_failure(FAILURE_STREAM_ENQUEUE, format!("enqueue failed: {err}"));
            return Err(err);
        }
        self.count(|c| &c.data_messages_sent);
        Ok(())
    }

    /// Queues a received message for `recv`. The stream owns the receiver, so
    /// the send alone would wait for room in the inbox of a closed stream
    /// forever, and the reader with it.
    pub(crate) async fn inbox_q(&self, msg: RawMessage) -> Result<(), Error> {
        tokio::select! {
            biased;
            _ = self.wait_closed() => Err(Error::Closed),
            result = self.inbox_tx.send(msg) => {
                result.map_err(|_| Error::Closed)?;
                self.count(|c| &c.data_messages_received);
                Ok(())
            }
        }
    }

    pub(crate) async fn handler_q(
        &self,
        handler: Arc<dyn crate::registry::Handler>,
        msg: Box<dyn crate::message::Message>,
    ) -> Result<(), Error> {
        let Some(tx) = self.handler_tx.as_ref() else {
            return Err(Error::Closed);
        };
        tokio::select! {
            biased;
            _ = self.wait_closed() => Err(Error::Closed),
            result = tx.send((handler, msg)) => {
                result.map_err(|_| Error::Closed)?;
                self.count(|c| &c.data_messages_received);
                Ok(())
            }
        }
    }

    /// Adds one to a message counter of the stream and to the same counter of
    /// its connection.
    pub(crate) fn count(&self, counter: impl Fn(&MessageCounters) -> &AtomicU64) {
        counter(&self.debug.messages).fetch_add(1, Ordering::Relaxed);
        counter(&self.connection.debug.messages).fetch_add(1, Ordering::Relaxed);
    }

    /// Notes a failure on the stream and on its connection.
    pub(crate) fn note_failure(&self, code: DebugFailureCode, reason: String) {
        self.connection.debug.note_failure(code, reason.clone());
        self.debug.note_failure(code, reason);
    }

    pub(crate) fn note_decode_error(&self, err: &Error) {
        self.count(|c| &c.decode_errors);
        self.note_failure(FAILURE_STREAM_DECODE, format!("decode failed: {err}"));
    }

    #[cfg(test)]
    pub(crate) fn recv_in_progress(&self) -> bool {
        self.recv_active.load(Ordering::Acquire)
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
        self.take_raw().await
    }

    /// Returns the next message. The caller holds the recv guard.
    async fn take_raw(&self) -> Result<RawMessage, Error> {
        if let Some(msg) = self.peeked.lock().unwrap().take() {
            return Ok(msg);
        }
        self.read_raw().await
    }

    async fn read_raw(&self) -> Result<RawMessage, Error> {
        let timeout_nanos = self.recv_timeout_nanos.load(Ordering::Relaxed);
        let deadline = (timeout_nanos != RECV_TIMEOUT_NONE)
            .then(|| tokio::time::Instant::now() + Duration::from_nanos(timeout_nanos));
        let mut inbox = self.inbox_rx.lock().await;
        loop {
            let msg = self.next_inbox(&mut inbox, deadline).await?;
            if self.stale_replies.load(Ordering::Relaxed) > 0 {
                self.stale_replies.fetch_sub(1, Ordering::Relaxed);
                continue;
            }
            return Ok(msg);
        }
    }

    async fn next_inbox(
        &self,
        inbox: &mut mpsc::Receiver<RawMessage>,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<RawMessage, Error> {
        if let Ok(msg) = inbox.try_recv() {
            return Ok(msg);
        }
        // Messages delivered before the stream closed are still returned.
        if self.closed.load(Ordering::Acquire) {
            return inbox.try_recv().map_err(|_| Error::Closed);
        }
        let timeout = async {
            match deadline {
                Some(deadline) => tokio::time::sleep_until(deadline).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            biased;
            msg = inbox.recv() => msg.ok_or(Error::Closed),
            _ = self.connection.wait_closed() => Err(Error::Closed),
            _ = self.wait_closed() => Err(Error::Closed),
            _ = timeout => {
                self.debug
                    .note_failure(FAILURE_STREAM_RECV_TIMEOUT, "recv timeout".to_string());
                self.connection
                    .debug
                    .note_failure(FAILURE_STREAM_RECV_TIMEOUT, "stream recv timeout".to_string());
                Err(Error::RecvTimeout)
            }
        }
    }

    pub(crate) async fn protocol_error(&self, code: &str, message: String) {
        let reason = if message.is_empty() {
            code.to_string()
        } else {
            format!("{code}: {message}")
        };
        self.connection
            .debug
            .note_failure(FAILURE_STREAM_PROTOCOL, format!("stream protocol error: {reason}"));
        self.debug.note_failure(FAILURE_STREAM_PROTOCOL, reason);
        self.count(|c| &c.protocol_errors);
        if let Err(err) = self
            .send_boxed(Box::new(ErrorReply::new(code, message)))
            .await
        {
            self.note_failure(
                FAILURE_STREAM_PROTOCOL_REPLY_SEND,
                format!("protocol error reply send failed: {err}"),
            );
            self.count(|c| &c.protocol_error_reply_send_failure);
        }
        if !self.connection.opened_stream(self.id) {
            self.connection.remove_peer_stream(self.id);
            return;
        }
        if !self.fin_started.swap(true, Ordering::AcqRel) {
            self.send_fin().await;
        }
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
