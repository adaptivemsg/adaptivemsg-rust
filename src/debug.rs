//! Diagnostics, counters, and failure tracking for connections and streams.
//!
//! Matches the Go runtime's `debug.go` surface: per-connection and per-stream
//! atomic counters, last-failure tracking, and snapshot structs for inspection.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------------------
// Failure codes — shared vocabulary with Go runtime
// ---------------------------------------------------------------------------

/// Machine-readable failure code for diagnostics, dashboards, and alerts.
pub type DebugFailureCode = &'static str;

// Stream path
pub const FAILURE_STREAM_RECV_TIMEOUT: DebugFailureCode = "stream.recv_timeout";
pub const FAILURE_STREAM_ENCODE: DebugFailureCode = "stream.encode";
pub const FAILURE_STREAM_ENQUEUE: DebugFailureCode = "stream.enqueue";
pub const FAILURE_STREAM_DECODE: DebugFailureCode = "stream.decode";
pub const FAILURE_STREAM_PROTOCOL: DebugFailureCode = "stream.protocol";
pub const FAILURE_STREAM_PROTOCOL_REPLY_SEND: DebugFailureCode = "stream.protocol_reply_send";

// Connection path
pub const FAILURE_CONNECTION_READER: DebugFailureCode = "connection.reader";
pub const FAILURE_CONNECTION_WRITER: DebugFailureCode = "connection.writer";
pub const FAILURE_CONNECTION_READER_ENQUEUE: DebugFailureCode = "connection.reader_enqueue";
pub const FAILURE_HANDLER_ERROR: DebugFailureCode = "handler.error";

// Recovery path
pub const FAILURE_RECOVERY_RESUME: DebugFailureCode = "recovery.resume";
pub const FAILURE_RECOVERY_RECONNECT_TERMINAL: DebugFailureCode = "recovery.reconnect_terminal";
pub const FAILURE_RECOVERY_READ: DebugFailureCode = "recovery.read";
pub const FAILURE_RECOVERY_CONTROL: DebugFailureCode = "recovery.control";
pub const FAILURE_RECOVERY_DATA: DebugFailureCode = "recovery.data";
pub const FAILURE_RECOVERY_ACK_WRITE: DebugFailureCode = "recovery.ack_write";
pub const FAILURE_RECOVERY_RESUME_WRITE: DebugFailureCode = "recovery.resume_write";
pub const FAILURE_RECOVERY_LIVE_WRITE: DebugFailureCode = "recovery.live_write";
pub const FAILURE_RECOVERY_PING_WRITE: DebugFailureCode = "recovery.ping_write";

// ---------------------------------------------------------------------------
// Internal atomic counters (per-connection)
// ---------------------------------------------------------------------------

pub(crate) struct ConnectionDebugCounters {
    pub streams_opened: AtomicU64,
    pub streams_closed: AtomicU64,
    pub data_messages_sent: AtomicU64,
    pub data_messages_received: AtomicU64,
    pub frames_written: AtomicU64,
    pub frames_read: AtomicU64,
    pub bytes_written: AtomicU64,
    pub bytes_read: AtomicU64,
    pub control_frames_written: AtomicU64,
    pub control_frames_read: AtomicU64,
    pub protocol_errors: AtomicU64,
    pub protocol_error_reply_send_failure: AtomicU64,
    pub remote_errors: AtomicU64,
    pub decode_errors: AtomicU64,
    pub handler_calls: AtomicU64,
    pub handler_errors: AtomicU64,
    pub reconnect_attempts: AtomicU64,
    pub reconnect_successes: AtomicU64,
    pub reconnect_failures: AtomicU64,
    pub transport_attaches: AtomicU64,
    pub transport_detaches: AtomicU64,
    last_failure_code: Mutex<&'static str>,
    last_failure_reason: Mutex<String>,
    last_failure_nanos: AtomicI64,
}

impl ConnectionDebugCounters {
    pub(crate) fn new() -> Self {
        Self {
            streams_opened: AtomicU64::new(0),
            streams_closed: AtomicU64::new(0),
            data_messages_sent: AtomicU64::new(0),
            data_messages_received: AtomicU64::new(0),
            frames_written: AtomicU64::new(0),
            frames_read: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
            control_frames_written: AtomicU64::new(0),
            control_frames_read: AtomicU64::new(0),
            protocol_errors: AtomicU64::new(0),
            protocol_error_reply_send_failure: AtomicU64::new(0),
            remote_errors: AtomicU64::new(0),
            decode_errors: AtomicU64::new(0),
            handler_calls: AtomicU64::new(0),
            handler_errors: AtomicU64::new(0),
            reconnect_attempts: AtomicU64::new(0),
            reconnect_successes: AtomicU64::new(0),
            reconnect_failures: AtomicU64::new(0),
            transport_attaches: AtomicU64::new(0),
            transport_detaches: AtomicU64::new(0),
            last_failure_code: Mutex::new(""),
            last_failure_reason: Mutex::new(String::new()),
            last_failure_nanos: AtomicI64::new(0),
        }
    }

    pub(crate) fn note_failure(&self, code: DebugFailureCode, reason: String) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;
        *self.last_failure_code.lock().unwrap() = code;
        *self.last_failure_reason.lock().unwrap() = reason;
        self.last_failure_nanos.store(nanos, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> ConnectionCounters {
        ConnectionCounters {
            streams_opened: self.streams_opened.load(Ordering::Relaxed),
            streams_closed: self.streams_closed.load(Ordering::Relaxed),
            data_messages_sent: self.data_messages_sent.load(Ordering::Relaxed),
            data_messages_received: self.data_messages_received.load(Ordering::Relaxed),
            frames_written: self.frames_written.load(Ordering::Relaxed),
            frames_read: self.frames_read.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            control_frames_written: self.control_frames_written.load(Ordering::Relaxed),
            control_frames_read: self.control_frames_read.load(Ordering::Relaxed),
            protocol_errors: self.protocol_errors.load(Ordering::Relaxed),
            protocol_error_reply_send_failure: self
                .protocol_error_reply_send_failure
                .load(Ordering::Relaxed),
            remote_errors: self.remote_errors.load(Ordering::Relaxed),
            decode_errors: self.decode_errors.load(Ordering::Relaxed),
            handler_calls: self.handler_calls.load(Ordering::Relaxed),
            handler_errors: self.handler_errors.load(Ordering::Relaxed),
            reconnect_attempts: self.reconnect_attempts.load(Ordering::Relaxed),
            reconnect_successes: self.reconnect_successes.load(Ordering::Relaxed),
            reconnect_failures: self.reconnect_failures.load(Ordering::Relaxed),
            transport_attaches: self.transport_attaches.load(Ordering::Relaxed),
            transport_detaches: self.transport_detaches.load(Ordering::Relaxed),
        }
    }

    fn last_failure(&self) -> (String, String, Option<SystemTime>) {
        let code = self.last_failure_code.lock().unwrap().to_string();
        let reason = self.last_failure_reason.lock().unwrap().clone();
        let nanos = self.last_failure_nanos.load(Ordering::Relaxed);
        let time = if nanos > 0 {
            Some(UNIX_EPOCH + Duration::from_nanos(nanos as u64))
        } else {
            None
        };
        (code, reason, time)
    }
}

// ---------------------------------------------------------------------------
// Internal atomic counters (per-stream)
// ---------------------------------------------------------------------------

pub(crate) struct StreamDebugCounters {
    pub data_messages_sent: AtomicU64,
    pub data_messages_received: AtomicU64,
    pub protocol_errors: AtomicU64,
    pub protocol_error_reply_send_failure: AtomicU64,
    pub remote_errors: AtomicU64,
    pub decode_errors: AtomicU64,
    pub handler_calls: AtomicU64,
    pub handler_errors: AtomicU64,
    last_failure_code: Mutex<&'static str>,
    last_failure_reason: Mutex<String>,
    last_failure_nanos: AtomicI64,
}

impl StreamDebugCounters {
    pub(crate) fn new() -> Self {
        Self {
            data_messages_sent: AtomicU64::new(0),
            data_messages_received: AtomicU64::new(0),
            protocol_errors: AtomicU64::new(0),
            protocol_error_reply_send_failure: AtomicU64::new(0),
            remote_errors: AtomicU64::new(0),
            decode_errors: AtomicU64::new(0),
            handler_calls: AtomicU64::new(0),
            handler_errors: AtomicU64::new(0),
            last_failure_code: Mutex::new(""),
            last_failure_reason: Mutex::new(String::new()),
            last_failure_nanos: AtomicI64::new(0),
        }
    }

    pub(crate) fn note_failure(&self, code: DebugFailureCode, reason: String) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;
        *self.last_failure_code.lock().unwrap() = code;
        *self.last_failure_reason.lock().unwrap() = reason;
        self.last_failure_nanos.store(nanos, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> StreamCounters {
        StreamCounters {
            data_messages_sent: self.data_messages_sent.load(Ordering::Relaxed),
            data_messages_received: self.data_messages_received.load(Ordering::Relaxed),
            protocol_errors: self.protocol_errors.load(Ordering::Relaxed),
            protocol_error_reply_send_failure: self
                .protocol_error_reply_send_failure
                .load(Ordering::Relaxed),
            remote_errors: self.remote_errors.load(Ordering::Relaxed),
            decode_errors: self.decode_errors.load(Ordering::Relaxed),
            handler_calls: self.handler_calls.load(Ordering::Relaxed),
            handler_errors: self.handler_errors.load(Ordering::Relaxed),
        }
    }

    fn last_failure(&self) -> (String, String, Option<SystemTime>) {
        let code = self.last_failure_code.lock().unwrap().to_string();
        let reason = self.last_failure_reason.lock().unwrap().clone();
        let nanos = self.last_failure_nanos.load(Ordering::Relaxed);
        let time = if nanos > 0 {
            Some(UNIX_EPOCH + Duration::from_nanos(nanos as u64))
        } else {
            None
        };
        (code, reason, time)
    }

    /// Copy this stream's last failure to the connection counters so it
    /// survives stream removal.
    pub(crate) fn promote_failure_to(&self, conn: &ConnectionDebugCounters) {
        let code = *self.last_failure_code.lock().unwrap();
        if code.is_empty() {
            return;
        }
        let reason = self.last_failure_reason.lock().unwrap().clone();
        conn.note_failure(code, reason);
    }
}

// ---------------------------------------------------------------------------
// Public snapshot structs
// ---------------------------------------------------------------------------

/// Point-in-time counters for a connection.
#[derive(Debug, Clone, Default)]
pub struct ConnectionCounters {
    pub streams_opened: u64,
    pub streams_closed: u64,
    pub data_messages_sent: u64,
    pub data_messages_received: u64,
    pub frames_written: u64,
    pub frames_read: u64,
    pub bytes_written: u64,
    pub bytes_read: u64,
    pub control_frames_written: u64,
    pub control_frames_read: u64,
    pub protocol_errors: u64,
    pub protocol_error_reply_send_failure: u64,
    pub remote_errors: u64,
    pub decode_errors: u64,
    pub handler_calls: u64,
    pub handler_errors: u64,
    pub reconnect_attempts: u64,
    pub reconnect_successes: u64,
    pub reconnect_failures: u64,
    pub transport_attaches: u64,
    pub transport_detaches: u64,
}

/// Point-in-time counters for a stream.
#[derive(Debug, Clone, Default)]
pub struct StreamCounters {
    pub data_messages_sent: u64,
    pub data_messages_received: u64,
    pub protocol_errors: u64,
    pub protocol_error_reply_send_failure: u64,
    pub remote_errors: u64,
    pub decode_errors: u64,
    pub handler_calls: u64,
    pub handler_errors: u64,
}

/// Point-in-time diagnostic snapshot of a stream.
#[derive(Debug, Clone)]
pub struct StreamDebugState {
    pub id: u32,
    pub closed: bool,
    pub last_failure_code: String,
    pub last_failure: String,
    pub last_failure_at: Option<SystemTime>,
    pub recv_timeout: Duration,
    pub inbox_depth: usize,
    pub incoming_depth: usize,
    pub handler_q_depth: usize,
    pub counters: StreamCounters,
}

/// Point-in-time diagnostic snapshot of recovery state.
#[derive(Debug, Clone)]
pub struct RecoveryDebugState {
    pub role: String,
    pub connection_id: String,
    pub transport_attached: bool,
    pub transport_gen: u64,
    pub reconnect_active: bool,
    pub last_recv_seq: u64,
    pub last_acked_seq: u64,
    pub ack_pending: u32,
    pub ack_due: bool,
    pub ack_every: u32,
    pub ack_delay: Duration,
    pub heartbeat_interval: Duration,
    pub heartbeat_timeout: Duration,
    pub replay_queued: usize,
    pub replay_bytes: i64,
    pub live_queue_depth: usize,
    pub resume_queue_depth: usize,
}

/// Point-in-time diagnostic snapshot of a connection.
#[derive(Debug, Clone)]
pub struct ConnectionDebugState {
    pub closed: bool,
    pub last_failure_code: String,
    pub last_failure: String,
    pub last_failure_at: Option<SystemTime>,
    pub protocol: u8,
    pub codec_id: u8,
    pub codec_name: String,
    pub max_frame: u32,
    pub stream_count: usize,
    pub next_send_seq: u64,
    pub counters: ConnectionCounters,
    pub streams: Vec<StreamDebugState>,
    pub recovery: Option<RecoveryDebugState>,
}

// ---------------------------------------------------------------------------
// Snapshot builders (called from Connection and Stream)
// ---------------------------------------------------------------------------

impl ConnectionDebugCounters {
    pub(crate) fn build_state(
        &self,
        closed: bool,
        protocol: u8,
        codec_id: u8,
        codec_name: String,
        max_frame: u32,
        stream_count: usize,
        next_send_seq: u64,
        streams: Vec<StreamDebugState>,
        recovery: Option<RecoveryDebugState>,
    ) -> ConnectionDebugState {
        let (code, reason, time) = self.last_failure();
        ConnectionDebugState {
            closed,
            last_failure_code: code,
            last_failure: reason,
            last_failure_at: time,
            protocol,
            codec_id,
            codec_name,
            max_frame,
            stream_count,
            next_send_seq,
            counters: self.snapshot(),
            streams,
            recovery,
        }
    }
}

impl StreamDebugCounters {
    pub(crate) fn build_state(
        &self,
        id: u32,
        closed: bool,
        recv_timeout: Duration,
        inbox_depth: usize,
        incoming_depth: usize,
        handler_q_depth: usize,
    ) -> StreamDebugState {
        let (code, reason, time) = self.last_failure();
        StreamDebugState {
            id,
            closed,
            last_failure_code: code,
            last_failure: reason,
            last_failure_at: time,
            recv_timeout,
            inbox_depth,
            incoming_depth,
            handler_q_depth,
            counters: self.snapshot(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_codes_are_nonempty() {
        let codes = [
            FAILURE_STREAM_RECV_TIMEOUT,
            FAILURE_STREAM_ENCODE,
            FAILURE_STREAM_ENQUEUE,
            FAILURE_STREAM_DECODE,
            FAILURE_STREAM_PROTOCOL,
            FAILURE_STREAM_PROTOCOL_REPLY_SEND,
            FAILURE_CONNECTION_READER,
            FAILURE_CONNECTION_WRITER,
            FAILURE_CONNECTION_READER_ENQUEUE,
            FAILURE_HANDLER_ERROR,
            FAILURE_RECOVERY_RESUME,
            FAILURE_RECOVERY_RECONNECT_TERMINAL,
            FAILURE_RECOVERY_READ,
            FAILURE_RECOVERY_CONTROL,
            FAILURE_RECOVERY_DATA,
            FAILURE_RECOVERY_ACK_WRITE,
            FAILURE_RECOVERY_RESUME_WRITE,
            FAILURE_RECOVERY_LIVE_WRITE,
            FAILURE_RECOVERY_PING_WRITE,
        ];
        for code in &codes {
            assert!(!code.is_empty(), "failure code should not be empty");
            assert!(
                code.contains('.'),
                "failure code {code} should contain a dot separator"
            );
        }
    }

    #[test]
    fn connection_counters_start_at_zero() {
        let c = ConnectionDebugCounters::new();
        let snap = c.snapshot();
        assert_eq!(snap.streams_opened, 0);
        assert_eq!(snap.data_messages_sent, 0);
        assert_eq!(snap.frames_written, 0);
        assert_eq!(snap.bytes_read, 0);
        assert_eq!(snap.handler_calls, 0);
        assert_eq!(snap.reconnect_attempts, 0);
    }

    #[test]
    fn stream_counters_start_at_zero() {
        let c = StreamDebugCounters::new();
        let snap = c.snapshot();
        assert_eq!(snap.data_messages_sent, 0);
        assert_eq!(snap.data_messages_received, 0);
        assert_eq!(snap.protocol_errors, 0);
        assert_eq!(snap.handler_calls, 0);
    }

    #[test]
    fn note_failure_records_code_and_reason() {
        let c = ConnectionDebugCounters::new();
        c.note_failure(FAILURE_STREAM_DECODE, "bad data".to_string());
        let (code, reason, at) = c.last_failure();
        assert_eq!(code, FAILURE_STREAM_DECODE);
        assert_eq!(reason, "bad data");
        assert!(at.is_some());
    }

    #[test]
    fn stream_failure_promotes_to_connection() {
        let stream = StreamDebugCounters::new();
        let conn = ConnectionDebugCounters::new();
        stream.note_failure(FAILURE_STREAM_ENCODE, "encode failed".to_string());
        stream.promote_failure_to(&conn);
        let (code, reason, _) = conn.last_failure();
        assert_eq!(code, FAILURE_STREAM_ENCODE);
        assert_eq!(reason, "encode failed");
    }

    #[test]
    fn connection_build_state_populates_fields() {
        let c = ConnectionDebugCounters::new();
        let state = c.build_state(
            false,
            2,
            1,
            "compact".to_string(),
            65535,
            3,
            0,
            vec![],
            None,
        );
        assert!(!state.closed);
        assert_eq!(state.protocol, 2);
        assert_eq!(state.codec_id, 1);
        assert_eq!(state.codec_name, "compact");
        assert_eq!(state.max_frame, 65535);
        assert_eq!(state.stream_count, 3);
        assert!(state.recovery.is_none());
    }

    #[test]
    fn stream_build_state_populates_fields() {
        let c = StreamDebugCounters::new();
        let state = c.build_state(7, false, Duration::from_secs(5), 10, 20, 5);
        assert_eq!(state.id, 7);
        assert!(!state.closed);
        assert_eq!(state.recv_timeout, Duration::from_secs(5));
        assert_eq!(state.inbox_depth, 10);
        assert_eq!(state.incoming_depth, 20);
        assert_eq!(state.handler_q_depth, 5);
    }
}
