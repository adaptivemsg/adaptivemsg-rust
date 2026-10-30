//! Protocol v3 for the blocking client: replay, acknowledgements, heartbeats
//! and reconnects.
//!
//! A sender keeps each frame in the replay buffer under the next sequence
//! number. One writer thread per connection writes the buffered frames that
//! the attached transport has not carried yet, so the frames replayed after a
//! reconnect and new frames go out in one order; it also writes ACKs,
//! heartbeats and CLOSE, and reconnects when the transport is lost. Each
//! transport has its own reader thread.

use std::collections::VecDeque;
use std::io::{BufReader, BufWriter, Read, Write};
use std::net::Shutdown;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use tracing::{debug, warn};

use crate::codec::CodecID;
use crate::debug::{
    DebugFailureCode, RecoveryDebugState, FAILURE_RECOVERY_ACK_WRITE, FAILURE_RECOVERY_CONTROL,
    FAILURE_RECOVERY_DATA, FAILURE_RECOVERY_LIVE_WRITE, FAILURE_RECOVERY_PING_WRITE,
    FAILURE_RECOVERY_READ, FAILURE_RECOVERY_RECONNECT_TERMINAL, FAILURE_RECOVERY_RESUME,
    FAILURE_RECOVERY_RESUME_WRITE,
};
use crate::error::Error;
use crate::frame::{CONTROL_STREAM_ID, FRAME_FLAG_FIN};
use crate::protocol::PROTOCOL_VERSION_V3;
use crate::recovery::RecoveryState;
use crate::recovery_protocol::{
    build_ack_control_payload, build_close_control_payload, build_ping_control_payload,
    decode_attach_response, encode_attach_request, parse_control_payload, AttachRequest,
    AttachResponse, ATTACH_MODE_NEW, ATTACH_MODE_RESUME, ATTACH_RESPONSE_LEN, ATTACH_STATUS_OK,
    CONTROL_TYPE_ACK, CONTROL_TYPE_CLOSE, CONTROL_TYPE_PING,
};
use crate::replay::{FrameRecord, Staged};
use crate::transport::READ_BUFFER_SIZE;

use super::socket::{
    as_recv_timeout, connect_timeout, dial, handshake, limit_io, read_frame, time_left,
    write_frame, Config, Socket,
};
use super::{Client, Link, Shared, StreamInner, CLOSE_FLUSH_TIMEOUT};

pub(super) struct RecoveryLink {
    state: RecoveryState,
    /// The server to reconnect to.
    addr: String,
    codec_id: CodecID,
    transport: Mutex<Transport>,
    /// Signals a change to `transport`, to the replay buffer or to what is due
    /// to the writer, to senders waiting for room, and to `close`.
    changed: Condvar,
    /// Held by a sender while it stages a frame, so that a frame waiting for
    /// room in the replay buffer is not overtaken.
    send_order: Mutex<()>,
    /// Held by a reader while it delivers a frame. The reader of a replaced
    /// transport may still deliver one; checking its sequence number under
    /// this lock delivers each frame once.
    deliver: Mutex<()>,
    /// The last sequence number written to a transport, for diagnostics.
    written_seq: AtomicU64,
}

struct Transport {
    /// Changes on every attach and detach, so that a reader can tell whether
    /// its transport is still the attached one.
    gen: u64,
    /// The attached transport; `None` while detached.
    socket: Option<Socket>,
    reader: Option<JoinHandle<()>>,
    last_staged_seq: u64,
    /// Set once the writer wrote CLOSE: whether that worked.
    close_written: Option<bool>,
}

/// The transport the writer writes to.
struct Attached {
    gen: u64,
    writer: BufWriter<Socket>,
    /// The last sequence number this transport carried, or that the peer had
    /// when it was attached.
    last_sent: u64,
    /// The frames up to this sequence number were staged before the attach;
    /// writing them is the replay.
    replay_until: u64,
    /// When the writer last wrote, for the heartbeat.
    last_write: Instant,
}

/// What the writer does next.
enum Work {
    Write {
        ack: Option<u64>,
        frames: VecDeque<Arc<FrameRecord>>,
        close: bool,
    },
    Ping,
    /// The transport was detached; reconnect.
    Detached,
    /// The connection is closed.
    Exit,
}

/// How a write ended for the writer.
enum Written {
    More,
    /// CLOSE was written: the writer is done.
    Closed,
    /// The transport was lost; reconnect.
    Lost,
}

impl RecoveryLink {
    fn new(state: RecoveryState, addr: &str, codec_id: CodecID) -> Self {
        Self {
            state,
            addr: addr.to_string(),
            codec_id,
            transport: Mutex::new(Transport {
                gen: 0,
                socket: None,
                reader: None,
                last_staged_seq: 0,
                close_written: None,
            }),
            changed: Condvar::new(),
            send_order: Mutex::new(()),
            deliver: Mutex::new(()),
            written_seq: AtomicU64::new(0),
        }
    }

    /// Wakes the threads waiting on `changed`. Taking the lock first means a
    /// waiter cannot miss the change between checking and waiting.
    fn wake(&self) {
        drop(self.transport.lock().unwrap());
        self.changed.notify_all();
    }

    fn is_current(&self, gen: u64) -> bool {
        self.transport.lock().unwrap().gen == gen
    }

    /// Shuts the attached transport down for good.
    pub(super) fn shut_down(&self) {
        let mut transport = self.transport.lock().unwrap();
        transport.gen += 1;
        if let Some(socket) = transport.socket.take() {
            let _ = socket.shutdown(Shutdown::Both);
        }
        drop(transport);
        self.changed.notify_all();
    }

    pub(super) fn take_reader(&self) -> Option<JoinHandle<()>> {
        self.transport.lock().unwrap().reader.take()
    }

    /// Returns the last sequence number staged and the recovery state.
    pub(super) fn debug_state(&self) -> (u64, RecoveryDebugState) {
        let transport = self.transport.lock().unwrap();
        let staged = transport.last_staged_seq;
        let unsent = staged.saturating_sub(self.written_seq.load(Ordering::Relaxed)) as usize;
        let replaying = self.state.resume_depth.load(Ordering::Relaxed);
        let state = self.state.debug_state(
            transport.gen,
            transport.socket.is_some(),
            unsent.saturating_sub(replaying),
        );
        (staged, state)
    }

    #[cfg(test)]
    pub(super) fn break_transport(&self) {
        if let Some(socket) = self.transport.lock().unwrap().socket.as_ref() {
            let _ = socket.shutdown(Shutdown::Both);
        }
    }

    #[cfg(test)]
    pub(super) fn generation(&self) -> u64 {
        self.transport.lock().unwrap().gen
    }
}

/// Asks the server for a new recovery connection on a transport that
/// completed the v3 handshake.
pub(super) fn attach_new(socket: &mut Socket) -> Result<AttachResponse, Error> {
    let request = AttachRequest {
        mode: ATTACH_MODE_NEW,
        ..AttachRequest::default()
    };
    let response = exchange_attach(socket, &request)?;
    if response.status != ATTACH_STATUS_OK {
        return Err(Error::ResumeRejected("initial attach rejected".to_string()));
    }
    Ok(response)
}

fn exchange_attach(socket: &mut Socket, request: &AttachRequest) -> Result<AttachResponse, Error> {
    socket.write_all(&encode_attach_request(request))?;
    let mut response = [0u8; ATTACH_RESPONSE_LEN];
    socket.read_exact(&mut response)?;
    decode_attach_response(&response)
}

impl Shared {
    fn recovery(&self) -> Option<&RecoveryLink> {
        match &self.link {
            Link::Recovery(link) => Some(link),
            Link::Plain(_) => None,
        }
    }

    pub(super) fn start_recovery(
        socket: Socket,
        config: Config,
        client: &Client,
        addr: &str,
        response: AttachResponse,
    ) -> Result<Arc<Self>, Error> {
        let state = RecoveryState::client(
            client.recovery.clone(),
            response.negotiated,
            response.connection_id,
            response.resume_secret,
            client.connect_timeout,
        );
        let link = RecoveryLink::new(state, addr, config.codec_id);
        let shared = Self::new(config, Link::Recovery(link), client);
        let link = shared.recovery().expect("recovery link");
        let attached = match shared.attach(link, socket, response.last_recv_seq) {
            Ok(attached) => attached,
            Err(err) => {
                // The default stream refers back to the connection.
                shared.mark_closed();
                return Err(err);
            }
        };
        let writer = Arc::clone(&shared);
        let spawned = thread::Builder::new()
            .name("adaptivemsg-writer".to_string())
            .spawn(move || writer.drive(attached));
        if let Err(err) = spawned {
            shared.mark_closed();
            return Err(err.into());
        }
        Ok(shared)
    }

    /// Makes `socket` the attached transport and starts its reader. The peer
    /// has received the frames up to `peer_last_recv_seq`.
    fn attach(
        self: &Arc<Self>,
        link: &RecoveryLink,
        socket: Socket,
        peer_last_recv_seq: u64,
    ) -> Result<Attached, Error> {
        // A transport that carries no frame for this long is dead.
        socket.set_read_timeout(Some(link.state.heartbeat_timeout()))?;
        socket.set_write_timeout(None)?;
        let reader = BufReader::with_capacity(READ_BUFFER_SIZE, socket.try_clone()?);
        let writer = BufWriter::new(socket.try_clone()?);
        let mut transport = link.transport.lock().unwrap();
        if self.closed.load(Ordering::Acquire) {
            let _ = socket.shutdown(Shutdown::Both);
            return Err(Error::Closed);
        }
        let gen = transport.gen + 1;
        let conn = Arc::clone(self);
        let reader = thread::Builder::new()
            .name("adaptivemsg-reader".to_string())
            .spawn(move || conn.recovery_read_loop(gen, reader))?;
        transport.gen = gen;
        transport.socket = Some(socket);
        // A reader of an earlier transport ends on its own.
        transport.reader = Some(reader);
        // The peer cannot have received more than was sent.
        let last_sent = peer_last_recv_seq.min(transport.last_staged_seq);
        let replay_until = transport.last_staged_seq;
        // That also acknowledges the frames the peer has: the ACK it sent for
        // them may have been lost with the old transport.
        link.state.prepare_resume(last_sent);
        drop(transport);
        link.written_seq.store(last_sent, Ordering::Relaxed);
        // The acknowledgement may free room for a waiting sender.
        link.changed.notify_all();
        self.debug
            .transport_attaches
            .fetch_add(1, Ordering::Relaxed);
        Ok(Attached {
            gen,
            writer,
            last_sent,
            replay_until,
            last_write: Instant::now(),
        })
    }

    /// Detaches the transport attached as generation `gen`, if it still is,
    /// so that the writer reconnects.
    fn detach(&self, link: &RecoveryLink, gen: u64) {
        let mut transport = link.transport.lock().unwrap();
        if transport.gen != gen || self.closed.load(Ordering::Acquire) {
            return;
        }
        transport.gen += 1;
        // Ends a read or write still in progress on it.
        if let Some(socket) = transport.socket.take() {
            let _ = socket.shutdown(Shutdown::Both);
        }
        drop(transport);
        link.changed.notify_all();
        self.debug
            .transport_detaches
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Notes a failure the connection cannot recover from and closes it.
    fn fail(&self, code: DebugFailureCode, what: &str, err: &Error) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        self.debug.note_failure(code, err.to_string());
        warn!("{what}: {err}");
        self.mark_closed();
    }

    /// Keeps a frame in the replay buffer under the next sequence number,
    /// waiting while the buffer is full. The writer sends it, after a
    /// reconnect if the transport is down.
    pub(super) fn stage(
        &self,
        link: &RecoveryLink,
        stream: &StreamInner,
        flags: u8,
        mut payload: Vec<u8>,
    ) -> Result<(), Error> {
        let _order = link.send_order.lock().unwrap();
        let mut transport = link.transport.lock().unwrap();
        loop {
            if self.closing.load(Ordering::Acquire)
                || self.closed.load(Ordering::Acquire)
                || stream.fin_sent.load(Ordering::Acquire)
                || stream.is_closed()
            {
                return Err(Error::Closed);
            }
            let seq = transport.last_staged_seq + 1;
            match link.state.replay.stage(stream.id, seq, flags, payload)? {
                Staged::Kept(_) => {
                    transport.last_staged_seq = seq;
                    if flags & FRAME_FLAG_FIN != 0 {
                        stream.fin_sent.store(true, Ordering::Release);
                    }
                    drop(transport);
                    link.changed.notify_all();
                    return Ok(());
                }
                // An ACK frees room.
                Staged::Full(returned) => {
                    payload = returned;
                    transport = link.changed.wait(transport).unwrap();
                }
            }
        }
    }

    pub(super) fn close_recovery(&self, link: &RecoveryLink) {
        let deadline = Instant::now() + CLOSE_FLUSH_TIMEOUT;
        let written = {
            let mut transport = link.transport.lock().unwrap();
            // The writer writes CLOSE after the frames staged so far.
            link.changed.notify_all();
            loop {
                // While detached there is no transport to write CLOSE on.
                if self.closed.load(Ordering::Acquire) || transport.socket.is_none() {
                    break false;
                }
                if let Some(written) = transport.close_written {
                    break written;
                }
                match time_left(deadline) {
                    Some(left) => transport = link.changed.wait_timeout(transport, left).unwrap().0,
                    None => break false,
                }
            }
        };
        if written {
            // The reader sees the peer close its side and closes the
            // connection.
            self.wait_closed_until(Some(deadline));
        }
    }

    /// The reader thread of the transport attached as generation `gen`.
    fn recovery_read_loop(self: Arc<Self>, gen: u64, mut reader: BufReader<Socket>) {
        let Some(link) = self.recovery() else {
            return;
        };
        loop {
            let (stream_id, seq, flags, payload) = match read_frame(&self.config, &mut reader) {
                Ok(Some(frame)) => frame,
                Ok(None) => return self.transport_ended(link, gen, None),
                // Resuming would replay the same frame, so a malformed frame
                // is terminal.
                Err(
                    err @ (Error::InvalidMessage(_)
                    | Error::UnsupportedFrameVersion(_)
                    | Error::FrameTooLarge(_)),
                ) => return self.fail(FAILURE_RECOVERY_DATA, "recovery frame rejected", &err),
                Err(err) => return self.transport_ended(link, gen, Some(err)),
            };
            self.debug
                .note_frame_read(stream_id, self.config.header_len + payload.len());
            if stream_id == CONTROL_STREAM_ID {
                match self.handle_control_frame(link, &payload) {
                    Ok(false) => continue,
                    // The frames received before CLOSE can still be read.
                    Ok(true) => return self.mark_closed(),
                    Err(err) => {
                        return self.fail(
                            FAILURE_RECOVERY_CONTROL,
                            "recovery control frame failed",
                            &err,
                        )
                    }
                }
            }
            if let Err(err) = self.deliver(link, stream_id, seq, flags, payload) {
                return self.fail(FAILURE_RECOVERY_DATA, "recovery data frame failed", &err);
            }
        }
    }

    /// Handles the end of the transport attached as generation `gen`: the
    /// writer reconnects, unless the connection is closing.
    fn transport_ended(&self, link: &RecoveryLink, gen: u64, err: Option<Error>) {
        if self.closed.load(Ordering::Acquire) || !link.is_current(gen) {
            return;
        }
        if self.closing.load(Ordering::Acquire) {
            // close sent CLOSE and waits for the peer to close its side.
            return self.mark_closed();
        }
        match err {
            None => debug!("recovery reader ended: peer closed the transport"),
            Some(err) => {
                let err = as_recv_timeout(err);
                self.debug
                    .note_failure(FAILURE_RECOVERY_READ, err.to_string());
                debug!("recovery reader ended: {err}");
            }
        }
        self.detach(link, gen);
    }

    /// Applies a control frame and reports whether it was CLOSE.
    fn handle_control_frame(&self, link: &RecoveryLink, payload: &[u8]) -> Result<bool, Error> {
        let (control_type, value) = parse_control_payload(payload)?;
        match control_type {
            CONTROL_TYPE_ACK => {
                link.state.ack_received(value);
                // A sender may wait for the room this frees.
                link.wake();
                Ok(false)
            }
            CONTROL_TYPE_PING => Ok(false),
            CONTROL_TYPE_CLOSE => Ok(true),
            _ => Err(Error::InvalidMessage(
                "unknown control frame type".to_string(),
            )),
        }
    }

    /// Delivers a data frame unless it was delivered already, and notes it
    /// for the ACK.
    fn deliver(
        self: &Arc<Self>,
        link: &RecoveryLink,
        stream_id: u32,
        seq: u64,
        flags: u8,
        payload: Vec<u8>,
    ) -> Result<(), Error> {
        if seq == 0 {
            return Err(Error::InvalidMessage(
                "recovery data frame missing seq".to_string(),
            ));
        }
        let _deliver = link.deliver.lock().unwrap();
        let last_recv_seq = link.state.last_received();
        if seq > last_recv_seq + 1 {
            return Err(Error::InvalidMessage(
                "recovery frame sequence gap".to_string(),
            ));
        }
        if seq == last_recv_seq + 1 {
            self.accept_frame(stream_id, flags, payload);
        }
        if link.state.note_received(seq) {
            link.wake();
        }
        Ok(())
    }

    /// The writer thread: writes what is staged, ACKs, heartbeats and CLOSE to
    /// the attached transport, and reconnects when it is lost.
    fn drive(self: Arc<Self>, attached: Attached) {
        let Some(link) = self.recovery() else {
            return;
        };
        let mut attached = Some(attached);
        loop {
            let Some(transport) = attached.as_mut() else {
                attached = self.reconnect(link);
                if attached.is_none() {
                    return;
                }
                continue;
            };
            let written = match self.next_work(link, transport) {
                Work::Exit => return,
                Work::Detached => Written::Lost,
                Work::Ping => self.write_ping(link, transport),
                Work::Write { ack, frames, close } => {
                    self.write_batch(link, transport, ack, &frames, close)
                }
            };
            match written {
                Written::More => {}
                Written::Closed => return,
                Written::Lost => attached = None,
            }
        }
    }

    /// Waits for something to write: an ACK, a staged frame, the close, or the
    /// heartbeat interval passing without a write.
    fn next_work(&self, link: &RecoveryLink, transport: &Attached) -> Work {
        let state = &link.state;
        let mut guard = link.transport.lock().unwrap();
        loop {
            if self.closed.load(Ordering::Acquire) {
                return Work::Exit;
            }
            if guard.gen != transport.gen {
                return Work::Detached;
            }
            let close = self.closing.load(Ordering::Acquire);
            let ack = state.take_pending_ack();
            let frames = state.replay.snapshot_from(transport.last_sent);
            if ack.is_some() || !frames.is_empty() || close {
                return Work::Write { ack, frames, close };
            }
            let interval = state.heartbeat_interval();
            let idle = transport.last_write.elapsed();
            if idle >= interval {
                return Work::Ping;
            }
            let mut wait = interval - idle;
            let ack_wait = state.next_ack_wait();
            if !ack_wait.is_zero() {
                wait = wait.min(ack_wait);
            }
            guard = link.changed.wait_timeout(guard, wait).unwrap().0;
        }
    }

    /// Writes a pending ACK, then the staged frames the transport has not
    /// carried, then CLOSE if the connection is closing.
    fn write_batch(
        &self,
        link: &RecoveryLink,
        transport: &mut Attached,
        ack: Option<u64>,
        frames: &VecDeque<Arc<FrameRecord>>,
        close: bool,
    ) -> Written {
        if let Some(seq) = ack {
            if let Err(err) = self.write_control(transport, &build_ack_control_payload(seq)) {
                return self.write_failed(link, transport, FAILURE_RECOVERY_ACK_WRITE, "ack", &err);
            }
        }
        for frame in frames {
            let written = write_frame(
                &self.config,
                &mut transport.writer,
                frame.stream_id,
                frame.seq,
                frame.flags,
                &frame.payload,
            );
            if let Err(err) = written {
                let (failure, kind) = if frame.seq <= transport.replay_until {
                    (FAILURE_RECOVERY_RESUME_WRITE, "resume")
                } else {
                    (FAILURE_RECOVERY_LIVE_WRITE, "live")
                };
                return self.write_failed(link, transport, failure, kind, &err);
            }
            self.debug.note_frame_written(
                frame.stream_id,
                self.config.header_len + frame.payload.len(),
            );
            transport.last_sent = frame.seq;
        }
        link.written_seq
            .store(transport.last_sent, Ordering::Relaxed);
        link.state.resume_depth.store(
            transport.replay_until.saturating_sub(transport.last_sent) as usize,
            Ordering::Relaxed,
        );
        if close {
            return self.write_close(link, transport);
        }
        if let Err(err) = transport.writer.flush() {
            return self.write_failed(
                link,
                transport,
                FAILURE_RECOVERY_LIVE_WRITE,
                "flush",
                &err.into(),
            );
        }
        transport.last_write = Instant::now();
        Written::More
    }

    fn write_ping(&self, link: &RecoveryLink, transport: &mut Attached) -> Written {
        let written = self
            .write_control(transport, &build_ping_control_payload())
            .and_then(|()| transport.writer.flush().map_err(Error::from));
        match written {
            Ok(()) => {
                transport.last_write = Instant::now();
                Written::More
            }
            Err(err) => {
                self.write_failed(link, transport, FAILURE_RECOVERY_PING_WRITE, "ping", &err)
            }
        }
    }

    /// Writes CLOSE after the frames written so far and shuts down the sending
    /// side of the transport, so that the server releases the connection
    /// instead of keeping it for a reconnect.
    fn write_close(&self, link: &RecoveryLink, transport: &mut Attached) -> Written {
        let written = self
            .write_control(transport, &build_close_control_payload())
            .and_then(|()| transport.writer.flush().map_err(Error::from))
            .and_then(|()| {
                transport
                    .writer
                    .get_ref()
                    .shutdown(Shutdown::Write)
                    .map_err(Error::from)
            });
        if let Err(err) = &written {
            self.debug.note_failure(
                FAILURE_RECOVERY_CONTROL,
                format!("recovery close write failed: {err}"),
            );
        }
        link.transport.lock().unwrap().close_written = Some(written.is_ok());
        link.changed.notify_all();
        Written::Closed
    }

    fn write_control(&self, transport: &mut Attached, payload: &[u8]) -> Result<(), Error> {
        write_frame(
            &self.config,
            &mut transport.writer,
            CONTROL_STREAM_ID,
            0,
            0,
            payload,
        )?;
        self.debug
            .note_frame_written(CONTROL_STREAM_ID, self.config.header_len + payload.len());
        Ok(())
    }

    /// Detaches the transport after a failed write, so that the writer
    /// reconnects. A write fails also when the reader detached the transport
    /// first; the reader noted why.
    fn write_failed(
        &self,
        link: &RecoveryLink,
        transport: &Attached,
        failure: DebugFailureCode,
        kind: &str,
        err: &Error,
    ) -> Written {
        if !self.closed.load(Ordering::Acquire) && link.is_current(transport.gen) {
            self.debug.note_failure(failure, err.to_string());
            warn!("recovery {kind} write failed: {err}");
            self.detach(link, transport.gen);
        }
        Written::Lost
    }

    /// Resumes the connection on a new transport, backing off between
    /// attempts. Returns `None` once the connection is closing or closed, or
    /// after the resume failed for good, which closes it.
    fn reconnect(self: &Arc<Self>, link: &RecoveryLink) -> Option<Attached> {
        let state = &link.state;
        state.reconnect_active.store(true, Ordering::Relaxed);
        let mut backoff = state.reconnect_min_backoff;
        let max_backoff = state.reconnect_max_backoff.max(backoff);
        let attached = loop {
            if self.closing.load(Ordering::Acquire) || self.closed.load(Ordering::Acquire) {
                break None;
            }
            self.debug
                .reconnect_attempts
                .fetch_add(1, Ordering::Relaxed);
            let err = match self.resume(link) {
                Ok(attached) => {
                    self.debug
                        .reconnect_successes
                        .fetch_add(1, Ordering::Relaxed);
                    break Some(attached);
                }
                Err(err) => err,
            };
            if self.closed.load(Ordering::Acquire) {
                break None;
            }
            self.debug
                .note_failure(FAILURE_RECOVERY_RESUME, format!("resume failed: {err}"));
            self.debug
                .reconnect_failures
                .fetch_add(1, Ordering::Relaxed);
            warn!("resume failed: {err}");
            if matches!(
                err,
                Error::ResumeRejected(_) | Error::UnsupportedFrameVersion(_)
            ) {
                self.debug.note_failure(
                    FAILURE_RECOVERY_RECONNECT_TERMINAL,
                    format!("reconnect terminal failure: {err}"),
                );
                self.mark_closed();
                break None;
            }
            if !self.pause(link, backoff) {
                break None;
            }
            backoff = (backoff * 2).min(max_backoff);
        };
        state.reconnect_active.store(false, Ordering::Relaxed);
        attached
    }

    /// Waits `backoff` before the next attempt. Returns false if the
    /// connection started closing meanwhile.
    fn pause(&self, link: &RecoveryLink, backoff: Duration) -> bool {
        let deadline = Instant::now() + backoff;
        let mut guard = link.transport.lock().unwrap();
        loop {
            if self.closing.load(Ordering::Acquire) || self.closed.load(Ordering::Acquire) {
                return false;
            }
            let Some(left) = time_left(deadline) else {
                return true;
            };
            guard = link.changed.wait_timeout(guard, left).unwrap().0;
        }
    }

    /// Dials the server again and resumes the connection on the new
    /// transport. The connect timeout limits each attempt.
    fn resume(self: &Arc<Self>, link: &RecoveryLink) -> Result<Attached, Error> {
        let deadline = link
            .state
            .reconnect_timeout
            .map(|limit| Instant::now() + limit);
        let mut socket = dial(&link.addr, deadline)?;
        limit_io(&socket, deadline)?;
        let response = self
            .resume_handshake(link, &mut socket)
            .map_err(connect_timeout)?;
        if response.status != ATTACH_STATUS_OK {
            return Err(Error::ResumeRejected("resume rejected".to_string()));
        }
        link.state.set_negotiated(response.negotiated);
        self.attach(link, socket, response.last_recv_seq)
    }

    fn resume_handshake(
        &self,
        link: &RecoveryLink,
        socket: &mut Socket,
    ) -> Result<AttachResponse, Error> {
        let negotiated = handshake(
            socket,
            &[link.codec_id],
            self.config.max_frame,
            PROTOCOL_VERSION_V3,
        )?;
        if negotiated.codec_id != link.codec_id {
            return Err(Error::ResumeRejected("codec changed on resume".to_string()));
        }
        let state = &link.state;
        let request = AttachRequest {
            mode: ATTACH_MODE_RESUME,
            connection_id: state.connection_id,
            resume_secret: state.resume_secret,
            last_recv_seq: state.last_received(),
        };
        exchange_attach(socket, &request)
    }
}
