//! Client on `std::net` and threads, for programs that do not run tokio.
//!
//! The API follows the async `Client`, `Connection` and `Stream`, with
//! blocking calls. A connection runs a reader
//! thread, which sorts the frames it reads into per-stream queues, and
//! `recv` decodes on the calling thread. Without recovery `send` writes its
//! frame on the calling thread.
//!
//! With [`Client::with_recovery`] the client negotiates protocol v3. `send`
//! then keeps the frame in the replay buffer and returns, and a writer thread
//! writes it, acknowledges what was received, sends heartbeats, and after a
//! transport failure reconnects and replays what the server has not
//! received.
//!
//! The client runs no handlers: every message the server sends is delivered
//! to `recv`. With `default-features = false, features = ["tcp", "uds",
//! "blocking"]` the crate does not depend on tokio.
//!
//! ```rust,ignore
//! use adaptivemsg::blocking::Client;
//!
//! let conn = Client::new().connect("uds://@hello")?;
//! let reply: HelloReply = conn.send_recv(HelloRequest { who: "alice".into() })?;
//! conn.close();
//! ```

mod recovery;
mod socket;

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{BufReader, BufWriter, Write};
use std::net::Shutdown;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use tracing::{debug, warn};

use crate::codec::{CodecID, Envelope};
use crate::codec_msgpack::{CodecMsgpackCompact, CodecMsgpackMap};
use crate::codec_postcard::CodecPostcard;
use crate::debug::{
    ConnectionDebugCounters, ConnectionDebugState, DebugFailureCode, MessageCounters,
    StreamDebugCounters, StreamDebugState, FAILURE_CONNECTION_READER, FAILURE_CONNECTION_WRITER,
    FAILURE_STREAM_DECODE, FAILURE_STREAM_ENCODE, FAILURE_STREAM_ENQUEUE,
    FAILURE_STREAM_IDS_EXHAUSTED, FAILURE_STREAM_PROTOCOL, FAILURE_STREAM_PROTOCOL_REPLY_SEND,
    FAILURE_STREAM_RECV_TIMEOUT,
};
use crate::error::Error;
use crate::frame::{build_header, CONTROL_STREAM_ID, FRAME_FLAG_FIN};
use crate::message::{ErrorReply, Message, MessageDecode};
use crate::protocol::{DEFAULT_MAX_FRAME, PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3};
use crate::raw_message::{decode_raw_as, RawMessage};
use crate::recovery::ClientRecoveryOptions;
use crate::transport::READ_BUFFER_SIZE;

use self::recovery::RecoveryLink;
use self::socket::{
    connect_timeout, dial, handshake, is_dial_error, limit_io, read_frame, time_left, Config,
    Socket,
};

const DEFAULT_STREAM_ID: u32 = 0;
/// Last ID `new_stream` hands out; the only larger value is the control stream.
const MAX_STREAM_ID: u32 = CONTROL_STREAM_ID - 1;
const STREAM_QUEUE_SIZE: usize = 1024;
/// Bounds how long `close` waits for queued frames to be written and for the
/// peer to close its side.
const CLOSE_FLUSH_TIMEOUT: Duration = Duration::from_secs(5);
const CONNECT_RETRY_MIN_BACKOFF: Duration = Duration::from_millis(100);
const CONNECT_RETRY_MAX_BACKOFF: Duration = Duration::from_secs(2);

/// Client configuration, as for the async `Client`, with blocking calls.
#[derive(Clone)]
pub struct Client {
    connect_timeout: Option<Duration>,
    recv_timeout: Option<Duration>,
    max_frame: u32,
    stream_queue_size: usize,
    codecs: Vec<CodecID>,
    recovery: ClientRecoveryOptions,
}

impl Default for Client {
    fn default() -> Self {
        Self {
            connect_timeout: None,
            recv_timeout: None,
            max_frame: DEFAULT_MAX_FRAME,
            stream_queue_size: STREAM_QUEUE_SIZE,
            codecs: vec![CodecPostcard, CodecMsgpackCompact, CodecMsgpackMap],
            recovery: ClientRecoveryOptions::default(),
        }
    }
}

impl Client {
    /// Create a client with default codecs, no timeouts and no recovery.
    pub fn new() -> Self {
        Self::default()
    }

    /// Limit how long [`connect`](Self::connect) and
    /// [`OnceConn::send_recv`] may take to establish a connection.
    ///
    /// Until then, failed dials such as connection refused are retried,
    /// waiting 100ms between attempts and doubling up to 2s, so the client
    /// can start before its server. Handshake failures are returned at once.
    /// With recovery enabled the timeout also limits each reconnect attempt.
    /// When the timeout is reached the call returns
    /// [`Error::ConnectTimeout`]. `Duration::ZERO` means a single attempt
    /// with no time limit.
    pub fn with_connect_timeout(mut self, d: Duration) -> Self {
        self.connect_timeout = (!d.is_zero()).then_some(d);
        self
    }

    /// Set the initial receive timeout of every stream on connections made by
    /// this client. `Duration::ZERO` means no timeout.
    pub fn with_recv_timeout(mut self, d: Duration) -> Self {
        self.recv_timeout = (!d.is_zero()).then_some(d);
        self
    }

    /// Override the codec preference list.
    pub fn with_codecs(mut self, codecs: &[CodecID]) -> Self {
        self.codecs = codecs.to_vec();
        self
    }

    /// Set the maximum payload size per frame that this client advertises.
    /// The effective limit is the smaller of both sides' values. The default
    /// is 16 MiB.
    pub fn with_max_frame(mut self, max_frame: u32) -> Self {
        self.max_frame = max_frame;
        self
    }

    /// Set how many received messages each stream buffers; 0 keeps the
    /// default of 1024. While a stream's buffer is full the reader waits,
    /// which holds up the other streams too.
    pub fn with_stream_queue_size(mut self, size: usize) -> Self {
        if size > 0 {
            self.stream_queue_size = size;
        }
        self
    }

    /// Configure recovery: with `enable`, the client negotiates protocol v3,
    /// falling back to v2 if the server does not support it.
    pub fn with_recovery(mut self, recovery: ClientRecoveryOptions) -> Self {
        self.recovery = recovery.normalized();
        self
    }

    /// Connect to a server at `addr` using `tcp://`, `uds://`, or `unix://`.
    pub fn connect(&self, addr: &str) -> Result<Connection, Error> {
        Ok(Connection::new(
            self.connect_with(addr, self.recovery.enable)?,
        ))
    }

    /// Prepare a connection for exactly one request-reply exchange with
    /// `addr`; [`OnceConn::send_recv`] connects, exchanges, and closes. It
    /// never uses recovery.
    pub fn connect_once<'a>(&'a self, addr: &'a str) -> OnceConn<'a> {
        OnceConn { client: self, addr }
    }

    fn connect_with(&self, addr: &str, use_recovery: bool) -> Result<Arc<Shared>, Error> {
        debug!("blocking client connect: {addr}");
        let deadline = self.connect_timeout.map(|d| Instant::now() + d);
        let mut backoff = CONNECT_RETRY_MIN_BACKOFF;
        let mut last_dial_err = None;
        loop {
            match (self.connect_attempt(addr, use_recovery, deadline), deadline) {
                (Ok(shared), _) => return Ok(shared),
                (Err(ConnectError::Dial(err)), Some(deadline)) => {
                    debug!("connect to {addr} failed, retrying: {err}");
                    thread::sleep(backoff.min(deadline.saturating_duration_since(Instant::now())));
                    backoff = (backoff * 2).min(CONNECT_RETRY_MAX_BACKOFF);
                    last_dial_err = Some(err);
                }
                (Err(ConnectError::Other(Error::ConnectTimeout(None))), _)
                    if last_dial_err.is_some() =>
                {
                    return Err(Error::ConnectTimeout(last_dial_err.map(Box::new)));
                }
                (Err(ConnectError::Dial(err) | ConnectError::Other(err)), _) => return Err(err),
            }
        }
    }

    /// Connects with v3 when recovery is enabled, falling back to v2 if the
    /// server answers that it only speaks v2.
    fn connect_attempt(
        &self,
        addr: &str,
        use_recovery: bool,
        deadline: Option<Instant>,
    ) -> Result<Arc<Shared>, ConnectError> {
        if use_recovery {
            match self.connect_version(addr, PROTOCOL_VERSION_V3, deadline) {
                Err(ConnectError::Other(Error::UnsupportedFrameVersion(PROTOCOL_VERSION_V2))) => {}
                result => return result,
            }
        }
        self.connect_version(addr, PROTOCOL_VERSION_V2, deadline)
    }

    fn connect_version(
        &self,
        addr: &str,
        version: u8,
        deadline: Option<Instant>,
    ) -> Result<Arc<Shared>, ConnectError> {
        let mut socket = dial(addr, deadline).map_err(|err| {
            if is_dial_error(&err) {
                ConnectError::Dial(err)
            } else {
                ConnectError::Other(err)
            }
        })?;
        let other = |err| ConnectError::Other(connect_timeout(err));
        limit_io(&socket, deadline).map_err(other)?;
        let negotiated =
            handshake(&mut socket, &self.codecs, self.max_frame, version).map_err(other)?;
        let config = Config::new(negotiated).map_err(other)?;
        if config.version != PROTOCOL_VERSION_V3 {
            limit_io(&socket, None).map_err(other)?;
            return Shared::start_plain(socket, config, self).map_err(ConnectError::Other);
        }
        let response = recovery::attach_new(&mut socket).map_err(other)?;
        Shared::start_recovery(socket, config, self, addr, response).map_err(ConnectError::Other)
    }
}

enum ConnectError {
    /// The server could not be reached; worth retrying.
    Dial(Error),
    Other(Error),
}

/// A connection for exactly one request-reply exchange, created by
/// [`Client::connect_once`].
pub struct OnceConn<'a> {
    client: &'a Client,
    addr: &'a str,
}

impl OnceConn<'_> {
    /// Connect, send `msg` on the default stream, receive one reply, and close
    /// the connection. The connect timeout limits connecting and the recv
    /// timeout limits waiting for the reply.
    pub fn send_recv<Rep: MessageDecode + 'static>(self, msg: impl Message) -> Result<Rep, Error> {
        let conn = Connection::new(self.client.connect_with(self.addr, false)?);
        let result = conn.send_recv(msg);
        // Nothing is left to deliver, so close at once instead of waiting
        // for a peer that may not be responding.
        conn.shared.close_now();
        result
    }
}

/// Handle to a connection, as the async `Connection` with blocking calls.
///
/// The methods without a stream act on the default stream, which, like any
/// [`Stream`], runs one `recv`, `peek_wire` or `send_recv` at a time; use
/// [`new_stream`](Self::new_stream) for concurrent request-reply. Clones
/// share the connection; dropping the last handle, counting the streams
/// opened from it, closes it in the background as `close` would.
#[derive(Clone)]
pub struct Connection {
    shared: Arc<Shared>,
    default_stream: Stream,
}

impl Connection {
    fn new(shared: Arc<Shared>) -> Self {
        let owner = Arc::new(Owner(Arc::clone(&shared)));
        let default_stream = Stream {
            inner: shared.lookup(DEFAULT_STREAM_ID),
            _owner: owner,
        };
        Self {
            shared,
            default_stream,
        }
    }

    /// Send a message on the default stream without waiting for a reply.
    ///
    /// Without recovery it returns once the frame is written to the
    /// transport. With recovery it returns once the frame is in the replay
    /// buffer, also while the transport is down, and waits while
    /// `max_replay_bytes` of messages await acknowledgement.
    pub fn send<M: Message>(&self, msg: M) -> Result<(), Error> {
        self.default_stream.send(msg)
    }

    /// Receive the next message on the default stream and decode it as `T`.
    pub fn recv<T: MessageDecode + 'static>(&self) -> Result<T, Error> {
        self.default_stream.recv()
    }

    /// Send a request on the default stream and wait for the reply.
    pub fn send_recv<T: MessageDecode + 'static>(&self, msg: impl Message) -> Result<T, Error> {
        self.default_stream.send_recv(msg)
    }

    /// Peek the wire name of the next message on the default stream.
    pub fn peek_wire(&self) -> Result<String, Error> {
        self.default_stream.peek_wire()
    }

    /// Set the receive timeout of the default stream; `Duration::ZERO`
    /// disables it.
    pub fn set_recv_timeout(&self, timeout: Duration) {
        self.default_stream.set_recv_timeout(timeout);
    }

    /// Open a new logical stream. On a closed connection, or once stream IDs
    /// are exhausted, the stream's operations return [`Error::Closed`].
    pub fn new_stream(&self) -> Stream {
        Stream {
            inner: self.shared.new_stream(),
            _owner: Arc::clone(&self.default_stream._owner),
        }
    }

    /// Close the connection.
    ///
    /// The messages already sent are delivered first: they are written, the
    /// peer is told that nothing more follows, and `close` waits for the peer
    /// to close its side, at most 5 seconds in total. With recovery the server
    /// then releases the connection at once instead of keeping it for a
    /// reconnect. Sends fail with [`Error::Closed`] once `close` has begun;
    /// messages received before it can still be read.
    pub fn close(&self) {
        self.shared.close();
    }

    /// Block until the connection is closed, by either side.
    pub fn wait_closed(&self) {
        self.shared.wait_closed_until(None);
    }

    /// Whether the connection is closed.
    pub fn is_closed(&self) -> bool {
        self.shared.closed.load(Ordering::Acquire)
    }

    /// Returns a point-in-time diagnostic snapshot of this connection.
    pub fn debug_state(&self) -> ConnectionDebugState {
        self.shared.debug_state()
    }

    /// Ends the current transport as a network failure would.
    #[cfg(test)]
    pub(crate) fn break_transport_for_test(&self) {
        if let Link::Recovery(link) = &self.shared.link {
            link.break_transport();
        }
    }

    #[cfg(test)]
    pub(crate) fn transport_generation_for_test(&self) -> u64 {
        match &self.shared.link {
            Link::Recovery(link) => link.generation(),
            Link::Plain(_) => 0,
        }
    }
}

/// A logical stream within a connection, as the async `Stream` with blocking
/// calls.
///
/// `send` may be called from several threads at once, but `recv`,
/// `peek_wire` and `send_recv` must not run concurrently on the same stream:
/// the extra call returns [`Error::ConcurrentRecv`]. Clones share the
/// stream, and a handle keeps its connection open.
#[derive(Clone)]
pub struct Stream {
    inner: Arc<StreamInner>,
    _owner: Arc<Owner>,
}

impl Stream {
    /// Stream identifier within the connection.
    pub fn id(&self) -> u32 {
        self.inner.id
    }

    /// Send a message without waiting for a reply; see
    /// [`Connection::send`].
    pub fn send<M: Message>(&self, msg: M) -> Result<(), Error> {
        self.inner.send(&msg)
    }

    /// Receive the next message and decode it as `T`.
    ///
    /// Blocks until a message arrives, the recv timeout expires
    /// ([`Error::RecvTimeout`]), or the stream closes ([`Error::Closed`]).
    /// With recovery it keeps waiting while the client reconnects.
    pub fn recv<T: MessageDecode + 'static>(&self) -> Result<T, Error> {
        let _guard = self.inner.recv_guard()?;
        let raw = self.inner.take_raw()?;
        self.inner.decode_reply(raw)
    }

    /// Send a request and wait for a reply of type `T`.
    ///
    /// A remote `ErrorReply` returns [`Error::Remote`]. After
    /// [`Error::RecvTimeout`] the late reply is discarded when it arrives, so
    /// the next request on this stream receives its own reply.
    pub fn send_recv<T: MessageDecode + 'static>(&self, msg: impl Message) -> Result<T, Error> {
        self.inner.send_recv(&msg)
    }

    /// Peek the wire name of the next message without consuming it.
    pub fn peek_wire(&self) -> Result<String, Error> {
        let _guard = self.inner.recv_guard()?;
        if let Some(raw) = self.inner.queue.lock().unwrap().peeked.as_ref() {
            return Ok(raw.wire.clone());
        }
        let raw = self.inner.read_raw()?;
        let wire = raw.wire.clone();
        self.inner.queue.lock().unwrap().peeked = Some(raw);
        Ok(wire)
    }

    /// Set the receive timeout for `recv` and `send_recv`; `Duration::ZERO`
    /// disables it.
    pub fn set_recv_timeout(&self, timeout: Duration) {
        self.inner.set_recv_timeout(timeout);
    }

    /// Close this stream. If this side opened it, a FIN tells the peer to
    /// release its copy after the messages sent before it. Messages already
    /// received can still be read.
    pub fn close(&self) {
        self.inner.close();
    }

    /// Returns a point-in-time diagnostic snapshot of this stream.
    pub fn debug_state(&self) -> StreamDebugState {
        self.inner.debug_state()
    }
}

/// Held by every handle to a connection, including the handles to its
/// streams; dropping the last one closes the connection.
struct Owner(Arc<Shared>);

impl Drop for Owner {
    fn drop(&mut self) {
        if self.0.closing.load(Ordering::Acquire) || self.0.closed.load(Ordering::Acquire) {
            return;
        }
        // close waits for the peer, so it gets a thread of its own.
        let shared = Arc::clone(&self.0);
        let spawned = thread::Builder::new()
            .name("adaptivemsg-close".to_string())
            .spawn(move || shared.close());
        if spawned.is_err() {
            self.0.close_now();
        }
    }
}

/// How frames reach the transport.
enum Link {
    /// v2: senders write their frames themselves.
    Plain(PlainLink),
    /// v3: senders stage frames, and a writer thread writes them.
    Recovery(RecoveryLink),
}

struct PlainLink {
    /// Used to shut the transport down; the reader and the writer hold
    /// clones of it.
    socket: Socket,
    writer: WriterCell,
    reader: Mutex<Option<JoinHandle<()>>>,
}

/// The state of one connection, shared by its handles, its streams and its
/// threads.
struct Shared {
    config: Config,
    link: Link,
    state: Mutex<State>,
    closed_cv: Condvar,
    /// Mirrors `State::closed` for checks that take no lock.
    closed: AtomicBool,
    /// Set by `close`; sends fail from then on.
    closing: AtomicBool,
    next_stream_id: AtomicU64,
    stream_queue_size: usize,
    recv_timeout: Option<Duration>,
    debug: ConnectionDebugCounters,
}

#[derive(Default)]
struct State {
    streams: HashMap<u32, Arc<StreamInner>>,
    /// Streams the peer opened and this side released. Their IDs stay
    /// reserved until the peer's FIN, so later frames on them are dropped.
    closed_peer_streams: HashSet<u32>,
    closed: bool,
}

impl Shared {
    fn new(config: Config, link: Link, client: &Client) -> Arc<Self> {
        let shared = Arc::new(Self {
            config,
            link,
            state: Mutex::new(State::default()),
            closed_cv: Condvar::new(),
            closed: AtomicBool::new(false),
            closing: AtomicBool::new(false),
            next_stream_id: AtomicU64::new(1),
            stream_queue_size: client.stream_queue_size,
            recv_timeout: client.recv_timeout,
            debug: ConnectionDebugCounters::new(),
        });
        shared.register_stream(DEFAULT_STREAM_ID);
        shared
    }

    fn start_plain(socket: Socket, config: Config, client: &Client) -> Result<Arc<Self>, Error> {
        let reader = BufReader::with_capacity(READ_BUFFER_SIZE, socket.try_clone()?);
        let writer = BufWriter::new(socket.try_clone()?);
        let link = Link::Plain(PlainLink {
            socket,
            writer: WriterCell::new(writer),
            reader: Mutex::new(None),
        });
        let shared = Self::new(config, link, client);
        shared
            .debug
            .transport_attaches
            .fetch_add(1, Ordering::Relaxed);
        let conn = Arc::clone(&shared);
        let spawned = thread::Builder::new()
            .name("adaptivemsg-reader".to_string())
            .spawn(move || conn.plain_read_loop(reader));
        let handle = match spawned {
            Ok(handle) => handle,
            Err(err) => {
                // The default stream refers back to the connection.
                shared.mark_closed();
                return Err(err.into());
            }
        };
        if let Link::Plain(plain) = &shared.link {
            *plain.reader.lock().unwrap() = Some(handle);
        }
        Ok(shared)
    }

    fn plain_read_loop(self: Arc<Self>, mut reader: BufReader<Socket>) {
        loop {
            match read_frame(&self.config, &mut reader) {
                // The peer closed the transport at a frame boundary.
                Ok(None) => break,
                Ok(Some((stream_id, _, flags, payload))) => {
                    self.debug
                        .note_frame_read(stream_id, self.config.header_len + payload.len());
                    self.accept_frame(stream_id, flags, payload);
                }
                Err(err) => {
                    if !self.closed.load(Ordering::Acquire) {
                        self.debug
                            .note_failure(FAILURE_CONNECTION_READER, err.to_string());
                        debug!("read loop ended: {err}");
                    }
                    break;
                }
            }
        }
        // The frames already queued can still be read.
        self.mark_closed();
    }

    /// Queues a received frame for its stream, opening the stream if the peer
    /// opened it. A FIN closes the stream after the frames before it.
    fn accept_frame(self: &Arc<Self>, stream_id: u32, flags: u8, payload: Vec<u8>) {
        let fin = flags & FRAME_FLAG_FIN != 0
            && stream_id != DEFAULT_STREAM_ID
            && stream_id != CONTROL_STREAM_ID;
        let stream = {
            let mut state = self.state.lock().unwrap();
            if state.closed {
                return;
            }
            let stream = match state.streams.get(&stream_id) {
                Some(stream) => Some(Arc::clone(stream)),
                // Stream IDs are never reused, so a missing stream that this
                // side opened was closed here, as was one in
                // closed_peer_streams.
                None if payload.is_empty()
                    || self.opened_stream(stream_id)
                    || state.closed_peer_streams.contains(&stream_id) =>
                {
                    None
                }
                None => Some(self.insert_stream(&mut state, stream_id)),
            };
            if fin {
                // The peer is done with the stream, so its ID need not stay
                // reserved.
                state.closed_peer_streams.remove(&stream_id);
                state.streams.remove(&stream_id);
            }
            stream
        };
        let Some(stream) = stream else {
            return;
        };
        if !payload.is_empty() {
            stream.push(payload);
        }
        if fin {
            stream.mark_closed();
        }
    }

    fn lookup(self: &Arc<Self>, stream_id: u32) -> Arc<StreamInner> {
        match self.state.lock().unwrap().streams.get(&stream_id) {
            Some(stream) => Arc::clone(stream),
            None => Arc::new(StreamInner::closed(Arc::clone(self))),
        }
    }

    fn insert_stream(self: &Arc<Self>, state: &mut State, stream_id: u32) -> Arc<StreamInner> {
        let stream = Arc::new(StreamInner::new(stream_id, Arc::clone(self)));
        state.streams.insert(stream_id, Arc::clone(&stream));
        self.debug.streams_opened.fetch_add(1, Ordering::Relaxed);
        stream
    }

    /// Registers a stream, or returns a closed one if the connection is
    /// closed.
    fn register_stream(self: &Arc<Self>, stream_id: u32) -> Arc<StreamInner> {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return Arc::new(StreamInner::closed(Arc::clone(self)));
        }
        self.insert_stream(&mut state, stream_id)
    }

    fn new_stream(self: &Arc<Self>) -> Arc<StreamInner> {
        let stream_id = self.next_stream_id.fetch_add(1, Ordering::Relaxed);
        if stream_id > MAX_STREAM_ID as u64 {
            self.debug.note_failure(
                FAILURE_STREAM_IDS_EXHAUSTED,
                "stream IDs exhausted".to_string(),
            );
            return Arc::new(StreamInner::closed(Arc::clone(self)));
        }
        self.register_stream(stream_id as u32)
    }

    /// Whether `new_stream` on this side allocated `stream_id`.
    fn opened_stream(&self, stream_id: u32) -> bool {
        stream_id != DEFAULT_STREAM_ID
            && (stream_id as u64) < self.next_stream_id.load(Ordering::Relaxed)
    }

    fn remove_stream(&self, stream_id: u32) {
        let removed = self.state.lock().unwrap().streams.remove(&stream_id);
        if let Some(stream) = removed {
            // Promote stream failure to connection so it survives stream removal.
            stream.debug.promote_failure_to(&self.debug);
        }
    }

    /// Releases a stream that the peer opened. The peer is not told, so the
    /// ID stays reserved until the peer's FIN.
    fn remove_peer_stream(&self, stream_id: u32) {
        let mut state = self.state.lock().unwrap();
        if let Some(stream) = state.streams.remove(&stream_id) {
            state.closed_peer_streams.insert(stream_id);
            stream.debug.promote_failure_to(&self.debug);
        }
    }

    /// Encodes `msg`, rejecting a payload above the negotiated max frame.
    fn encode(&self, msg: &dyn Message) -> Result<Vec<u8>, Error> {
        let payload = self.config.codec.encode(msg)?;
        if payload.len() > self.config.max_frame as usize {
            return Err(Error::FrameTooLarge(payload.len()));
        }
        Ok(payload)
    }

    fn send_frame(&self, stream: &StreamInner, flags: u8, payload: Vec<u8>) -> Result<(), Error> {
        match &self.link {
            Link::Plain(plain) => self.write_plain(plain, stream, flags, &payload),
            Link::Recovery(link) => self.stage(link, stream, flags, payload),
        }
    }

    /// Writes and flushes one frame on the caller's thread.
    fn write_plain(
        &self,
        plain: &PlainLink,
        stream: &StreamInner,
        flags: u8,
        payload: &[u8],
    ) -> Result<(), Error> {
        let header = build_header(
            self.config.version,
            stream.id,
            0,
            flags,
            payload.len(),
            self.config.max_frame,
        )?;
        let mut writer = plain.writer.take(None).ok_or(Error::Closed)?;
        // Checked while holding the writer, so that no frame follows the
        // shutdown in close or the stream's FIN.
        if self.closing.load(Ordering::Acquire)
            || self.closed.load(Ordering::Acquire)
            || stream.fin_sent.load(Ordering::Acquire)
            || stream.is_closed()
        {
            return Err(Error::Closed);
        }
        if flags & FRAME_FLAG_FIN != 0 {
            stream.fin_sent.store(true, Ordering::Release);
        }
        let written = writer
            .write_all(&header[..self.config.header_len])
            .and_then(|()| writer.write_all(payload))
            .and_then(|()| writer.flush());
        if let Err(err) = written {
            drop(writer);
            self.debug
                .note_failure(FAILURE_CONNECTION_WRITER, err.to_string());
            warn!("write failed: {err}");
            self.mark_closed();
            return Err(err.into());
        }
        self.debug
            .note_frame_written(stream.id, self.config.header_len + payload.len());
        Ok(())
    }

    fn close(&self) {
        if !self.closing.swap(true, Ordering::AcqRel) {
            match &self.link {
                Link::Plain(plain) => self.close_plain(plain),
                Link::Recovery(link) => self.close_recovery(link),
            }
        }
        self.close_now();
    }

    fn close_plain(&self, plain: &PlainLink) {
        let deadline = Instant::now() + CLOSE_FLUSH_TIMEOUT;
        // Every send flushes, so only the shutdown is left to write. A send
        // that the peer holds up keeps the writer past the deadline.
        if let Some(writer) = plain.writer.take(Some(deadline)) {
            let _ = plain.socket.shutdown(Shutdown::Write);
            drop(writer);
            // The reader sees the peer close its side and closes the
            // connection.
            self.wait_closed_until(Some(deadline));
        }
    }

    /// Closes at once and waits for the reader thread to end.
    fn close_now(&self) {
        self.mark_closed();
        let reader = match &self.link {
            Link::Plain(plain) => plain.reader.lock().unwrap().take(),
            Link::Recovery(link) => link.take_reader(),
        };
        if let Some(reader) = reader {
            if reader.thread().id() != thread::current().id() {
                let _ = reader.join();
            }
        }
    }

    fn mark_closed(&self) {
        let streams = {
            let mut state = self.state.lock().unwrap();
            if state.closed {
                return;
            }
            state.closed = true;
            self.closed.store(true, Ordering::Release);
            std::mem::take(&mut state.streams)
        };
        self.closed_cv.notify_all();
        // Ends a read the reader thread is blocked in, and a write a sender
        // is blocked in.
        match &self.link {
            Link::Plain(plain) => {
                let _ = plain.socket.shutdown(Shutdown::Both);
            }
            Link::Recovery(link) => link.shut_down(),
        }
        for stream in streams.values() {
            stream.mark_closed();
        }
    }

    fn wait_closed_until(&self, deadline: Option<Instant>) {
        let mut state = self.state.lock().unwrap();
        while !state.closed {
            state = match deadline {
                None => self.closed_cv.wait(state).unwrap(),
                Some(deadline) => match time_left(deadline) {
                    Some(left) => self.closed_cv.wait_timeout(state, left).unwrap().0,
                    None => return,
                },
            };
        }
    }

    fn debug_state(&self) -> ConnectionDebugState {
        let streams: Vec<Arc<StreamInner>> = self
            .state
            .lock()
            .unwrap()
            .streams
            .values()
            .cloned()
            .collect();
        let stream_states = streams.iter().map(|stream| stream.debug_state()).collect();
        let (next_send_seq, recovery) = match &self.link {
            Link::Plain(_) => (0, None),
            Link::Recovery(link) => {
                let (next_send_seq, state) = link.debug_state();
                (next_send_seq, Some(state))
            }
        };
        self.debug.build_state(
            self.closed.load(Ordering::Relaxed),
            self.config.version,
            self.config.codec_id.0,
            self.config.codec_id.name().to_string(),
            self.config.max_frame,
            streams.len(),
            next_send_seq,
            stream_states,
            recovery,
        )
    }
}

/// Received frames waiting for `recv`, and what `recv` keeps between calls.
#[derive(Default)]
struct Queue {
    frames: VecDeque<Vec<u8>>,
    /// Set when the stream or the connection closes; the frames received
    /// before are still returned.
    closed: bool,
    peeked: Option<RawMessage>,
    /// Replies to requests whose `send_recv` timed out, discarded as they
    /// arrive.
    stale_replies: usize,
}

struct StreamInner {
    id: u32,
    connection: Arc<Shared>,
    queue: Mutex<Queue>,
    /// Signals a frame or the close to `recv`, and room to the reader.
    changed: Condvar,
    recv_timeout_nanos: AtomicU64,
    recv_active: AtomicBool,
    fin_sent: AtomicBool,
    debug: StreamDebugCounters,
}

impl StreamInner {
    fn new(id: u32, connection: Arc<Shared>) -> Self {
        let stream = Self {
            id,
            queue: Mutex::new(Queue::default()),
            changed: Condvar::new(),
            recv_timeout_nanos: AtomicU64::new(0),
            recv_active: AtomicBool::new(false),
            fin_sent: AtomicBool::new(false),
            debug: StreamDebugCounters::new(),
            connection,
        };
        if let Some(timeout) = stream.connection.recv_timeout {
            stream.set_recv_timeout(timeout);
        }
        stream
    }

    /// A stream that is not registered and returns [`Error::Closed`] from
    /// every operation. Its ID is the control stream ID rather than 0 because
    /// closing stream 0 would close the default stream.
    fn closed(connection: Arc<Shared>) -> Self {
        let stream = Self::new(CONTROL_STREAM_ID, connection);
        stream.queue.lock().unwrap().closed = true;
        stream.fin_sent.store(true, Ordering::Release);
        stream
    }

    fn set_recv_timeout(&self, timeout: Duration) {
        let nanos = timeout.as_nanos().min(u64::MAX as u128) as u64;
        self.recv_timeout_nanos.store(nanos, Ordering::Relaxed);
    }

    fn recv_timeout(&self) -> Option<Duration> {
        match self.recv_timeout_nanos.load(Ordering::Relaxed) {
            0 => None,
            nanos => Some(Duration::from_nanos(nanos)),
        }
    }

    fn is_closed(&self) -> bool {
        self.queue.lock().unwrap().closed
    }

    /// Queues a frame for `recv`. While the queue is full the reader waits,
    /// as the async client's reader does.
    fn push(&self, payload: Vec<u8>) {
        let mut queue = self.queue.lock().unwrap();
        while queue.frames.len() >= self.connection.stream_queue_size && !queue.closed {
            queue = self.changed.wait(queue).unwrap();
        }
        if queue.closed {
            return;
        }
        queue.frames.push_back(payload);
        drop(queue);
        self.changed.notify_all();
        self.count(|c| &c.data_messages_received);
    }

    fn mark_closed(&self) {
        let mut queue = self.queue.lock().unwrap();
        if queue.closed {
            return;
        }
        queue.closed = true;
        drop(queue);
        self.changed.notify_all();
        self.connection
            .debug
            .streams_closed
            .fetch_add(1, Ordering::Relaxed);
    }

    fn send(&self, msg: &dyn Message) -> Result<(), Error> {
        if self.fin_sent.load(Ordering::Acquire) || self.is_closed() {
            return Err(Error::Closed);
        }
        let payload = match self.connection.encode(msg) {
            Ok(payload) => payload,
            Err(err) => {
                self.note_failure(FAILURE_STREAM_ENCODE, format!("encode failed: {err}"));
                return Err(err);
            }
        };
        if let Err(err) = self.connection.send_frame(self, 0, payload) {
            self.note_failure(FAILURE_STREAM_ENQUEUE, format!("enqueue failed: {err}"));
            return Err(err);
        }
        self.count(|c| &c.data_messages_sent);
        Ok(())
    }

    fn send_recv<T: MessageDecode + 'static>(&self, msg: &dyn Message) -> Result<T, Error> {
        let _guard = self.recv_guard()?;
        self.send(msg)?;
        let raw = match self.take_raw() {
            Err(Error::RecvTimeout) => {
                // The recv guard serializes stale_replies, as it does peeked.
                self.queue.lock().unwrap().stale_replies += 1;
                return Err(Error::RecvTimeout);
            }
            result => result?,
        };
        if raw.wire == ErrorReply::wire_name_static() {
            return match decode_raw_as::<ErrorReply>(raw) {
                Ok(reply) => {
                    self.count(|c| &c.remote_errors);
                    let (code, message) = reply.into_parts();
                    Err(Error::Remote { code, message })
                }
                Err(err) => {
                    self.note_decode_error(&err);
                    self.protocol_error("codec_error", err.to_string());
                    Err(err)
                }
            };
        }
        self.decode_reply(raw)
    }

    fn recv_guard(&self) -> Result<RecvGuard<'_>, Error> {
        self.recv_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| Error::ConcurrentRecv)?;
        Ok(RecvGuard(&self.recv_active))
    }

    /// Returns the next message. The caller holds the recv guard.
    fn take_raw(&self) -> Result<RawMessage, Error> {
        if let Some(raw) = self.queue.lock().unwrap().peeked.take() {
            return Ok(raw);
        }
        self.read_raw()
    }

    fn read_raw(&self) -> Result<RawMessage, Error> {
        let deadline = self.recv_timeout().map(|timeout| Instant::now() + timeout);
        let payload = {
            let mut queue = self.queue.lock().unwrap();
            loop {
                if let Some(payload) = queue.frames.pop_front() {
                    self.changed.notify_all();
                    if queue.stale_replies > 0 {
                        queue.stale_replies -= 1;
                        continue;
                    }
                    break payload;
                }
                if queue.closed {
                    return Err(Error::Closed);
                }
                queue = match deadline {
                    None => self.changed.wait(queue).unwrap(),
                    Some(deadline) => match time_left(deadline) {
                        Some(left) => self.changed.wait_timeout(queue, left).unwrap().0,
                        None => {
                            drop(queue);
                            self.debug.note_failure(
                                FAILURE_STREAM_RECV_TIMEOUT,
                                "recv timeout".to_string(),
                            );
                            self.connection.debug.note_failure(
                                FAILURE_STREAM_RECV_TIMEOUT,
                                "stream recv timeout".to_string(),
                            );
                            return Err(Error::RecvTimeout);
                        }
                    },
                };
            }
        };
        let codec = self.connection.config.codec;
        match codec.decode_envelope(&payload) {
            Ok(Envelope { wire, body }) => Ok(RawMessage { wire, codec, body }),
            Err(err) => {
                self.note_decode_error(&err);
                warn!("decode failed: {err}");
                self.protocol_error("codec_error", err.to_string());
                Err(err)
            }
        }
    }

    /// Decodes a received message; a failure is answered with a protocol
    /// error, which closes the stream.
    fn decode_reply<T: MessageDecode + 'static>(&self, raw: RawMessage) -> Result<T, Error> {
        let result = decode_raw_as::<T>(raw);
        match &result {
            Err(err @ Error::TypeMismatch { expected, got }) => {
                self.note_decode_error(err);
                self.protocol_error("protocol_error", format!("expected {expected} got {got}"));
            }
            Err(err) => {
                self.note_decode_error(err);
                self.protocol_error("codec_error", err.to_string());
            }
            Ok(_) => {}
        }
        result
    }

    /// Tells the peer why the stream ends, then closes it.
    fn protocol_error(&self, code: &str, message: String) {
        let reason = if message.is_empty() {
            code.to_string()
        } else {
            format!("{code}: {message}")
        };
        self.connection.debug.note_failure(
            FAILURE_STREAM_PROTOCOL,
            format!("stream protocol error: {reason}"),
        );
        self.debug.note_failure(FAILURE_STREAM_PROTOCOL, reason);
        self.count(|c| &c.protocol_errors);
        if let Err(err) = self.send(&ErrorReply::new(code, message)) {
            self.note_failure(
                FAILURE_STREAM_PROTOCOL_REPLY_SEND,
                format!("protocol error reply send failed: {err}"),
            );
            self.count(|c| &c.protocol_error_reply_send_failure);
        }
        self.close();
    }

    fn close(&self) {
        if self.connection.opened_stream(self.id) {
            if let Err(err) = self.connection.send_frame(self, FRAME_FLAG_FIN, Vec::new()) {
                if !matches!(err, Error::Closed) {
                    self.debug
                        .note_failure(FAILURE_STREAM_ENQUEUE, format!("fin enqueue failed: {err}"));
                }
            }
            self.connection.remove_stream(self.id);
        } else if self.id != DEFAULT_STREAM_ID {
            self.connection.remove_peer_stream(self.id);
        }
        // Stream 0 stays registered, closed, so that its frames are dropped.
        self.mark_closed();
    }

    /// Adds one to a message counter of the stream and to the same counter of
    /// its connection.
    fn count(&self, counter: impl Fn(&MessageCounters) -> &AtomicU64) {
        counter(&self.debug.messages).fetch_add(1, Ordering::Relaxed);
        counter(&self.connection.debug.messages).fetch_add(1, Ordering::Relaxed);
    }

    /// Notes a failure on the stream and on its connection.
    fn note_failure(&self, code: DebugFailureCode, reason: String) {
        self.connection.debug.note_failure(code, reason.clone());
        self.debug.note_failure(code, reason);
    }

    fn note_decode_error(&self, err: &Error) {
        self.count(|c| &c.decode_errors);
        self.note_failure(FAILURE_STREAM_DECODE, format!("decode failed: {err}"));
    }

    fn debug_state(&self) -> StreamDebugState {
        let (closed, depth) = {
            let queue = self.queue.lock().unwrap();
            (
                queue.closed,
                queue.frames.len() + queue.peeked.is_some() as usize,
            )
        };
        let recv_timeout = self.recv_timeout().unwrap_or(Duration::ZERO);
        self.debug
            .build_state(self.id, closed, recv_timeout, depth, 0, 0)
    }
}

struct RecvGuard<'a>(&'a AtomicBool);

impl Drop for RecvGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// The write half of a v2 transport. A sender takes it out for one frame, so
/// `close` can stop waiting for a sender that the peer holds up.
struct WriterCell {
    writer: Mutex<Option<BufWriter<Socket>>>,
    returned: Condvar,
}

impl WriterCell {
    fn new(writer: BufWriter<Socket>) -> Self {
        Self {
            writer: Mutex::new(Some(writer)),
            returned: Condvar::new(),
        }
    }

    /// Takes the writer, waiting while another thread has it, until
    /// `deadline` if one is given.
    fn take(&self, deadline: Option<Instant>) -> Option<WriterGuard<'_>> {
        let mut slot = self.writer.lock().unwrap();
        loop {
            if let Some(writer) = slot.take() {
                return Some(WriterGuard {
                    cell: self,
                    writer: Some(writer),
                });
            }
            slot = match deadline {
                None => self.returned.wait(slot).unwrap(),
                Some(deadline) => {
                    self.returned
                        .wait_timeout(slot, time_left(deadline)?)
                        .unwrap()
                        .0
                }
            };
        }
    }
}

struct WriterGuard<'a> {
    cell: &'a WriterCell,
    writer: Option<BufWriter<Socket>>,
}

impl Deref for WriterGuard<'_> {
    type Target = BufWriter<Socket>;

    fn deref(&self) -> &Self::Target {
        self.writer.as_ref().expect("writer taken")
    }
}

impl DerefMut for WriterGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.writer.as_mut().expect("writer taken")
    }
}

impl Drop for WriterGuard<'_> {
    fn drop(&mut self) {
        *self.cell.writer.lock().unwrap() = self.writer.take();
        self.cell.returned.notify_one();
    }
}
