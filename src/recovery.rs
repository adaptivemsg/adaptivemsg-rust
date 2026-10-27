use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::connection::ConnectionRef;
use crate::error::Error;
use crate::protocol::DEFAULT_MAX_FRAME;
use crate::recovery_protocol::RecoveryToken;
use crate::replay::{FrameRecord, ReplayBuffer};

pub type ResumeFuture =
    Pin<Box<dyn Future<Output = Result<crate::connection::TransportParts, Error>> + Send>>;
pub type ResumeConnector = Arc<dyn Fn() -> ResumeFuture + Send + Sync>;
pub type RecoveryRegistry = Arc<Mutex<std::collections::HashMap<RecoveryToken, ConnectionRef>>>;

/// Client-side recovery configuration for automatic reconnect and replay.
///
/// When `enable` is true, the client negotiates protocol v3 and will
/// automatically reconnect and replay unacknowledged frames on transport failure.
#[derive(Clone, Debug)]
pub struct ClientRecoveryOptions {
    /// Enable recovery mode. Default: `false`.
    pub enable: bool,
    /// Minimum backoff between reconnect attempts. Default: 100 ms.
    pub reconnect_min_backoff: Duration,
    /// Maximum backoff between reconnect attempts. Default: 2 s.
    pub reconnect_max_backoff: Duration,
    /// Maximum bytes of unacknowledged messages kept for replay; while it is
    /// reached, sends wait for the peer to acknowledge earlier ones. Default:
    /// 32 MiB, twice the default max frame, so that a frame of the largest
    /// size fits while another is still unacknowledged. Raise it together
    /// with [`Client::with_max_frame`](crate::Client::with_max_frame).
    pub max_replay_bytes: i64,
}

/// Server-side recovery configuration for detached connection retention and ACK policy.
///
/// When `enable` is true, the server supports v3 protocol with attach/resume,
/// cumulative ACKs, and heartbeat-based liveness detection.
#[derive(Clone, Debug)]
pub struct ServerRecoveryOptions {
    /// Enable recovery mode. Default: `false`.
    pub enable: bool,
    /// How long to retain a detached connection before discarding it. Default: 30 s.
    pub detached_ttl: Duration,
    /// Maximum bytes of unacknowledged messages kept for replay; while it is
    /// reached, sends wait for the peer to acknowledge earlier ones. Default:
    /// 32 MiB, twice the default max frame, so that a frame of the largest
    /// size fits while another is still unacknowledged. Raise it together
    /// with [`Server::with_max_frame`](crate::Server::with_max_frame).
    pub max_replay_bytes: i64,
    /// Send a cumulative ACK every N received data frames. Default: 64.
    pub ack_every: u32,
    /// Delay before flushing a pending ACK. Default: 20 ms.
    pub ack_delay: Duration,
    /// Interval between heartbeat pings when idle. Default: 30 s.
    pub heartbeat_interval: Duration,
    /// Close connection if no inbound frame received within this duration. Default: 90 s.
    pub heartbeat_timeout: Duration,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum RecoveryRole {
    Client,
    Server,
}

#[derive(Clone, Debug)]
pub(crate) struct NegotiatedRecoveryOptions {
    pub ack_every: u32,
    pub ack_delay: Duration,
    pub heartbeat_interval: Duration,
    pub heartbeat_timeout: Duration,
}

impl Default for NegotiatedRecoveryOptions {
    fn default() -> Self {
        ServerRecoveryOptions::default().negotiated()
    }
}

struct RecoveryStateFields {
    shared: NegotiatedRecoveryOptions,
    last_recv_seq: u64,
    last_ack_sent: u64,
    ack_pending: u32,
}

pub(crate) struct RecoveryState {
    pub role: RecoveryRole,
    pub connection_id: RecoveryToken,
    pub resume_secret: RecoveryToken,
    pub replay: Arc<ReplayBuffer>,
    /// How many frames the writer has left to replay.
    pub resume_depth: AtomicUsize,
    pub reconnect_min_backoff: Duration,
    pub reconnect_max_backoff: Duration,
    /// Limit for each client reconnect attempt, from the client's connect
    /// timeout.
    pub reconnect_timeout: Option<Duration>,
    pub detached_ttl: Duration,
    pub reconnect_active: AtomicBool,
    pub ack_every: AtomicU32,
    // ack_due and ack_due_at change under `fields`; the writer reads them
    // without it to skip the lock while no ACK is due. ack_due_at is in
    // nanoseconds since `epoch`, zero while no ACK is scheduled.
    ack_due: AtomicBool,
    ack_due_at: AtomicU64,
    epoch: Instant,
    pub ack_delay_nanos: AtomicI64,
    pub heartbeat_interval_nanos: AtomicI64,
    pub heartbeat_timeout_nanos: AtomicI64,
    pub resume_connector: Option<ResumeConnector>,
    pub registry: Option<RecoveryRegistry>,
    fields: Mutex<RecoveryStateFields>,
}

impl Default for ClientRecoveryOptions {
    fn default() -> Self {
        Self {
            enable: false,
            reconnect_min_backoff: Duration::from_millis(100),
            reconnect_max_backoff: Duration::from_secs(2),
            max_replay_bytes: 2 * DEFAULT_MAX_FRAME as i64,
        }
    }
}

impl Default for ServerRecoveryOptions {
    fn default() -> Self {
        Self {
            enable: false,
            detached_ttl: Duration::from_secs(30),
            max_replay_bytes: 2 * DEFAULT_MAX_FRAME as i64,
            ack_every: 64,
            ack_delay: Duration::from_millis(20),
            heartbeat_interval: Duration::from_secs(30),
            heartbeat_timeout: Duration::from_secs(90),
        }
    }
}

impl ClientRecoveryOptions {
    pub fn normalized(&self) -> Self {
        let mut opts = self.clone();
        let defaults = Self::default();
        if opts.reconnect_min_backoff.is_zero() {
            opts.reconnect_min_backoff = defaults.reconnect_min_backoff;
        }
        if opts.reconnect_max_backoff.is_zero() {
            opts.reconnect_max_backoff = defaults.reconnect_max_backoff;
        }
        if opts.reconnect_max_backoff < opts.reconnect_min_backoff {
            opts.reconnect_max_backoff = opts.reconnect_min_backoff;
        }
        if opts.max_replay_bytes <= 0 {
            opts.max_replay_bytes = defaults.max_replay_bytes;
        }
        opts
    }
}

impl ServerRecoveryOptions {
    pub fn normalized(&self) -> Self {
        let mut opts = self.clone();
        let defaults = Self::default();
        if opts.detached_ttl.is_zero() {
            opts.detached_ttl = defaults.detached_ttl;
        }
        if opts.max_replay_bytes <= 0 {
            opts.max_replay_bytes = defaults.max_replay_bytes;
        }
        if opts.ack_every == 0 {
            opts.ack_every = defaults.ack_every;
        }
        if opts.ack_delay.is_zero() {
            opts.ack_delay = defaults.ack_delay;
        }
        if opts.heartbeat_interval.is_zero() {
            opts.heartbeat_interval = defaults.heartbeat_interval;
        }
        if opts.heartbeat_timeout.is_zero() {
            opts.heartbeat_timeout = defaults.heartbeat_timeout;
        }
        let min_timeout = opts.heartbeat_interval.saturating_mul(2);
        if opts.heartbeat_timeout < min_timeout {
            opts.heartbeat_timeout = min_timeout;
        }
        opts
    }

    pub(crate) fn negotiated(&self) -> NegotiatedRecoveryOptions {
        let opts = self.normalized();
        NegotiatedRecoveryOptions {
            ack_every: opts.ack_every,
            ack_delay: opts.ack_delay,
            heartbeat_interval: opts.heartbeat_interval,
            heartbeat_timeout: opts.heartbeat_timeout,
        }
    }
}

impl NegotiatedRecoveryOptions {
    pub(crate) fn normalized(&self) -> Self {
        let defaults = ServerRecoveryOptions::default().negotiated();
        let mut opts = self.clone();
        if opts.ack_every == 0 {
            opts.ack_every = defaults.ack_every;
        }
        if opts.ack_delay.is_zero() {
            opts.ack_delay = defaults.ack_delay;
        }
        if opts.heartbeat_interval.is_zero() {
            opts.heartbeat_interval = defaults.heartbeat_interval;
        }
        if opts.heartbeat_timeout.is_zero() {
            opts.heartbeat_timeout = defaults.heartbeat_timeout;
        }
        let min_timeout = opts.heartbeat_interval.saturating_mul(2);
        if opts.heartbeat_timeout < min_timeout {
            opts.heartbeat_timeout = min_timeout;
        }
        opts
    }
}

impl RecoveryState {
    pub(crate) fn new_client(
        opts: ClientRecoveryOptions,
        negotiated: NegotiatedRecoveryOptions,
        connection_id: RecoveryToken,
        resume_secret: RecoveryToken,
        resume_connector: ResumeConnector,
        reconnect_timeout: Option<Duration>,
    ) -> Arc<Self> {
        let normalized = opts.normalized();
        let neg = negotiated.normalized();
        Arc::new(Self {
            role: RecoveryRole::Client,
            connection_id,
            resume_secret,
            replay: Arc::new(ReplayBuffer::new(
                crate::protocol::PROTOCOL_VERSION_V3,
                normalized.max_replay_bytes,
            )),
            resume_depth: AtomicUsize::new(0),
            reconnect_min_backoff: normalized.reconnect_min_backoff,
            reconnect_max_backoff: normalized.reconnect_max_backoff,
            reconnect_timeout,
            detached_ttl: Duration::ZERO,
            reconnect_active: AtomicBool::new(false),
            ack_every: AtomicU32::new(neg.ack_every),
            ack_due: AtomicBool::new(false),
            ack_due_at: AtomicU64::new(0),
            epoch: Instant::now(),
            ack_delay_nanos: AtomicI64::new(neg.ack_delay.as_nanos() as i64),
            heartbeat_interval_nanos: AtomicI64::new(neg.heartbeat_interval.as_nanos() as i64),
            heartbeat_timeout_nanos: AtomicI64::new(neg.heartbeat_timeout.as_nanos() as i64),
            resume_connector: Some(resume_connector),
            registry: None,
            fields: Mutex::new(RecoveryStateFields {
                shared: neg,
                last_recv_seq: 0,
                last_ack_sent: 0,
                ack_pending: 0,
            }),
        })
    }

    pub(crate) fn new_server(
        opts: ServerRecoveryOptions,
        connection_id: RecoveryToken,
        resume_secret: RecoveryToken,
        registry: RecoveryRegistry,
    ) -> Arc<Self> {
        let normalized = opts.normalized();
        let negotiated = normalized.negotiated();
        Arc::new(Self {
            role: RecoveryRole::Server,
            connection_id,
            resume_secret,
            replay: Arc::new(ReplayBuffer::new(
                crate::protocol::PROTOCOL_VERSION_V3,
                normalized.max_replay_bytes,
            )),
            resume_depth: AtomicUsize::new(0),
            reconnect_min_backoff: Duration::ZERO,
            reconnect_max_backoff: Duration::ZERO,
            reconnect_timeout: None,
            detached_ttl: normalized.detached_ttl,
            reconnect_active: AtomicBool::new(false),
            ack_every: AtomicU32::new(negotiated.ack_every),
            ack_due: AtomicBool::new(false),
            ack_due_at: AtomicU64::new(0),
            epoch: Instant::now(),
            ack_delay_nanos: AtomicI64::new(negotiated.ack_delay.as_nanos() as i64),
            heartbeat_interval_nanos: AtomicI64::new(
                negotiated.heartbeat_interval.as_nanos() as i64
            ),
            heartbeat_timeout_nanos: AtomicI64::new(negotiated.heartbeat_timeout.as_nanos() as i64),
            resume_connector: None,
            registry: Some(registry),
            fields: Mutex::new(RecoveryStateFields {
                shared: negotiated,
                last_recv_seq: 0,
                last_ack_sent: 0,
                ack_pending: 0,
            }),
        })
    }

    pub(crate) fn last_received(&self) -> u64 {
        self.fields.lock().unwrap().last_recv_seq
    }

    pub(crate) fn ack_received(&self, last_seq: u64) {
        self.replay.ack(last_seq);
    }

    pub(crate) fn negotiated(&self) -> NegotiatedRecoveryOptions {
        self.fields.lock().unwrap().shared.clone()
    }

    pub(crate) fn set_negotiated(&self, opts: NegotiatedRecoveryOptions) {
        let normalized = opts.normalized();
        {
            let mut fields = self.fields.lock().unwrap();
            fields.shared = normalized.clone();
        }
        self.ack_every
            .store(normalized.ack_every, Ordering::Relaxed);
        self.ack_delay_nanos
            .store(normalized.ack_delay.as_nanos() as i64, Ordering::Relaxed);
        self.heartbeat_interval_nanos.store(
            normalized.heartbeat_interval.as_nanos() as i64,
            Ordering::Relaxed,
        );
        self.heartbeat_timeout_nanos.store(
            normalized.heartbeat_timeout.as_nanos() as i64,
            Ordering::Relaxed,
        );
    }

    /// Returns the frames after `last_seq`, the last one the peer received,
    /// for the writer to replay. That also acknowledges the frames up to
    /// `last_seq`: the ACK the peer sent for them may have been lost with the
    /// old transport.
    pub(crate) fn prepare_resume(&self, last_seq: u64) -> VecDeque<Arc<FrameRecord>> {
        self.replay.ack(last_seq);
        let frames = self.replay.snapshot_from(last_seq);
        self.resume_depth.store(frames.len(), Ordering::Relaxed);
        frames
    }

    fn elapsed_nanos(&self) -> u64 {
        self.epoch.elapsed().as_nanos() as u64
    }

    /// Notes a received data frame and reports whether the writer should be
    /// woken for an ACK.
    pub(crate) fn note_received(&self, seq: u64) -> bool {
        let mut fields = self.fields.lock().unwrap();
        if seq <= fields.last_recv_seq {
            if fields.last_ack_sent < fields.last_recv_seq {
                self.ack_due.store(true, Ordering::Release);
            }
            return true;
        }
        fields.last_recv_seq = seq;
        fields.ack_pending += 1;
        let ack_every = self.ack_every.load(Ordering::Relaxed);
        let ack_delay = self.ack_delay_nanos.load(Ordering::Relaxed).max(0) as u64;
        if ack_every <= 1 || fields.ack_pending >= ack_every || ack_delay == 0 {
            self.ack_due.store(true, Ordering::Release);
            self.ack_due_at.store(0, Ordering::Release);
            return true;
        }
        if self.ack_due_at.load(Ordering::Acquire) == 0 {
            self.ack_due_at
                .store(self.elapsed_nanos() + ack_delay, Ordering::Release);
            return true;
        }
        false
    }

    pub(crate) fn next_ack_wait(&self) -> Duration {
        if self.ack_due.load(Ordering::Acquire) {
            return Duration::ZERO;
        }
        match self.ack_due_at.load(Ordering::Acquire) {
            0 => Duration::ZERO,
            due_at => Duration::from_nanos(due_at.saturating_sub(self.elapsed_nanos())),
        }
    }

    /// Whether an ACK is due now, judged without the lock.
    fn ack_may_be_due(&self) -> bool {
        if self.ack_due.load(Ordering::Acquire) {
            return true;
        }
        let due_at = self.ack_due_at.load(Ordering::Acquire);
        due_at != 0 && self.elapsed_nanos() >= due_at
    }

    pub(crate) fn take_pending_ack(&self) -> Option<u64> {
        if !self.ack_may_be_due() {
            return None;
        }
        let mut fields = self.fields.lock().unwrap();
        if fields.last_ack_sent >= fields.last_recv_seq {
            self.ack_due.store(false, Ordering::Release);
            self.ack_due_at.store(0, Ordering::Release);
            return None;
        }
        if !self.ack_may_be_due() {
            return None;
        }
        let seq = fields.last_recv_seq;
        fields.last_ack_sent = seq;
        fields.ack_pending = 0;
        self.ack_due.store(false, Ordering::Release);
        self.ack_due_at.store(0, Ordering::Release);
        Some(seq)
    }

    pub(crate) fn on_closed(&self, connection: &ConnectionRef) {
        if let Some(registry) = &self.registry {
            let mut registry = registry.lock().unwrap();
            if let Some(current) = registry.get(&self.connection_id) {
                if Arc::ptr_eq(current, connection) {
                    registry.remove(&self.connection_id);
                }
            }
        }
    }

    pub(crate) fn heartbeat_interval(&self) -> Duration {
        Duration::from_nanos(self.heartbeat_interval_nanos.load(Ordering::Relaxed).max(0) as u64)
    }

    pub(crate) fn heartbeat_timeout(&self) -> Duration {
        Duration::from_nanos(self.heartbeat_timeout_nanos.load(Ordering::Relaxed).max(0) as u64)
    }

    pub(crate) fn detached_ttl(&self) -> Duration {
        self.detached_ttl
    }

    pub(crate) fn debug_state(
        &self,
        transport_gen: u64,
        transport_attached: bool,
        live_queue_depth: usize,
    ) -> crate::debug::RecoveryDebugState {
        let fields = self.fields.lock().unwrap();
        let role = match self.role {
            RecoveryRole::Client => "client",
            RecoveryRole::Server => "server",
        };
        crate::debug::RecoveryDebugState {
            role: role.to_string(),
            connection_id: self.connection_id.iter().map(|b| format!("{b:02x}")).collect::<String>(),
            transport_attached,
            transport_gen,
            reconnect_active: self.reconnect_active.load(Ordering::Relaxed),
            last_recv_seq: fields.last_recv_seq,
            last_acked_seq: self.replay.last_acked_seq(),
            ack_pending: fields.ack_pending,
            ack_due: self.ack_due.load(Ordering::Relaxed),
            ack_every: self.ack_every.load(Ordering::Relaxed),
            ack_delay: Duration::from_nanos(
                self.ack_delay_nanos.load(Ordering::Relaxed).max(0) as u64
            ),
            heartbeat_interval: self.heartbeat_interval(),
            heartbeat_timeout: self.heartbeat_timeout(),
            replay_queued: self.replay.queued_count(),
            replay_bytes: self.replay.used_bytes(),
            live_queue_depth,
            resume_queue_depth: self.resume_depth.load(Ordering::Relaxed),
        }
    }
}
