use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Sleep;
use tracing::{debug, warn};

use crate::debug::{
    DebugFailureCode, FAILURE_RECOVERY_ACK_WRITE, FAILURE_RECOVERY_CONTROL, FAILURE_RECOVERY_DATA,
    FAILURE_RECOVERY_LIVE_WRITE, FAILURE_RECOVERY_PING_WRITE, FAILURE_RECOVERY_READ,
    FAILURE_RECOVERY_RESUME_WRITE,
};
use crate::error::Error;
use crate::protocol::{handshake_client, PROTOCOL_VERSION_V3};
use crate::recovery::{RecoveryRole, RecoveryState};
use crate::recovery_protocol::{
    build_ack_control_payload, build_close_control_payload, build_ping_control_payload,
    parse_control_payload, AttachRequest, CONTROL_STREAM_ID, CONTROL_TYPE_ACK, CONTROL_TYPE_CLOSE,
    CONTROL_TYPE_PING,
};

use super::{
    read_frame, write_frame_counted, ConnConfig, ConnectionInner, ConnectionRef, InboundFrame,
    OutboundFrame, TransportParts, TransportReader, TransportWriter, WriterCommand,
};
use crate::replay::{FrameRecord, Staged};

impl ConnectionInner {
    pub(crate) fn attach_transport_parts(
        self: &ConnectionRef,
        parts: TransportParts,
        peer_last_recv_seq: u64,
    ) {
        if self.closed.load(Ordering::Relaxed) {
            return;
        }
        let gen = self.transport_gen.fetch_add(1, Ordering::AcqRel) + 1;
        if let Some((_, handle)) = self.current_reader_abort.lock().unwrap().take() {
            handle.abort();
        }
        let _ = self.writer_cmd_tx.send(WriterCommand::Attach {
            gen,
            writer: parts.writer,
            resume_seq: peer_last_recv_seq,
        });
        self.transport_attached.store(true, Ordering::Release);
        self.signal_send();
        let reader_task = if self.is_recovery_enabled() {
            self.spawn_recovery_reader(gen, parts.reader)
        } else {
            self.spawn_plain_reader(parts.reader)
        };
        *self.current_reader_abort.lock().unwrap() = Some((gen, reader_task.abort_handle()));
        self.debug
            .transport_attaches
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn spawn_recovery_writer(
        self: &ConnectionRef,
        writer_cmd_rx: mpsc::UnboundedReceiver<WriterCommand>,
        outbound_rx: mpsc::Receiver<OutboundFrame>,
    ) -> JoinHandle<()> {
        let connection = self.clone();
        tokio::spawn(async move {
            let Some(recovery) = connection.recovery().cloned() else {
                return;
            };
            RecoveryWriter {
                connection,
                recovery,
                writer_cmd_rx,
                outbound_rx,
                writer: None,
                gen: 0,
                last_sent_seq: 0,
                resume: VecDeque::new(),
                held: None,
                dirty: false,
            }
            .run()
            .await;
        })
    }

    pub(super) fn spawn_recovery_reader(
        self: &ConnectionRef,
        gen: u64,
        mut reader: TransportReader,
    ) -> JoinHandle<()> {
        let connection = self.clone();
        tokio::spawn(async move {
            let Some(recovery) = connection.recovery().cloned() else {
                return;
            };
            let timeout = recovery.heartbeat_timeout();
            let deadline = tokio::time::sleep(timeout);
            tokio::pin!(deadline);
            // Only this task advances the last received sequence number.
            let mut last_recv_seq = recovery.last_received();
            loop {
                let read =
                    read_frame_within(connection.config(), &mut reader, timeout, deadline.as_mut());
                let (stream_id, seq, flags, payload) = match read.await {
                    Ok(Some(frame)) => frame,
                    // Resuming would replay the same frame, so a malformed
                    // frame is terminal.
                    Err(
                        err @ (Error::InvalidMessage(_)
                        | Error::UnsupportedFrameVersion(_)
                        | Error::FrameTooLarge(_)),
                    ) => {
                        return connection.fail(
                            FAILURE_RECOVERY_DATA,
                            "recovery frame rejected",
                            &err,
                        )
                    }
                    // close sent CLOSE and waits for the peer to close its side.
                    _ if connection.closing.load(Ordering::Acquire) => {
                        return connection.mark_closed()
                    }
                    Ok(None) => {
                        debug!("recovery reader ended: peer closed the transport");
                        return connection.detach_transport(gen, false);
                    }
                    Err(err) => {
                        connection
                            .debug
                            .note_failure(FAILURE_RECOVERY_READ, err.to_string());
                        debug!("recovery reader ended: {err}");
                        return connection.detach_transport(gen, false);
                    }
                };
                let wire_bytes = connection.config().header_len + payload.len();
                connection.debug.note_frame_read(stream_id, wire_bytes);
                if stream_id == CONTROL_STREAM_ID {
                    match connection.handle_control_frame(&payload) {
                        Ok(false) => continue,
                        Ok(true) => return connection.close_by_peer().await,
                        Err(err) => {
                            return connection.fail(
                                FAILURE_RECOVERY_CONTROL,
                                "recovery control frame failed",
                                &err,
                            )
                        }
                    }
                }
                let delivered = connection
                    .handle_recovery_data_frame(
                        &recovery,
                        last_recv_seq,
                        stream_id,
                        seq,
                        flags,
                        payload,
                    )
                    .await;
                match delivered {
                    Ok(seq) => last_recv_seq = seq,
                    Err(err) => {
                        return connection.fail(
                            FAILURE_RECOVERY_DATA,
                            "recovery data frame failed",
                            &err,
                        )
                    }
                }
            }
        })
    }

    /// Notes a failure the connection cannot recover from and closes it.
    fn fail(&self, code: DebugFailureCode, what: &str, err: &Error) {
        self.debug.note_failure(code, err.to_string());
        warn!("{what}: {err}");
        self.mark_closed();
    }

    pub(super) fn detach_transport(&self, gen: u64, abort_reader: bool) {
        if self.closed.load(Ordering::Relaxed) {
            return;
        }
        if self
            .transport_gen
            .compare_exchange(gen, gen + 1, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        self.transport_attached.store(false, Ordering::Release);
        self.debug
            .transport_detaches
            .fetch_add(1, Ordering::Relaxed);
        if abort_reader {
            if let Some((current_gen, handle)) = self.current_reader_abort.lock().unwrap().take() {
                if current_gen == gen {
                    handle.abort();
                }
            }
        }
        let _ = self.writer_cmd_tx.send(WriterCommand::Detach { gen });
        if let Some(recovery) = self.recovery() {
            match recovery.role {
                RecoveryRole::Client => self.spawn_reconnect_loop(),
                RecoveryRole::Server => self.spawn_server_expiry(gen + 1),
            }
        }
    }

    pub(crate) fn spawn_server_expiry(&self, detached_gen: u64) {
        let Some(recovery) = self.recovery() else {
            return;
        };
        let ttl = recovery.detached_ttl();
        if ttl.is_zero() {
            self.mark_closed();
            return;
        }
        let connection = self.shared();
        tokio::spawn(async move {
            tokio::time::sleep(ttl).await;
            if !connection.closed.load(Ordering::Relaxed)
                && connection.transport_gen.load(Ordering::Acquire) == detached_gen
            {
                connection.mark_closed();
            }
        });
    }

    pub(crate) fn spawn_reconnect_loop(&self) {
        let Some(recovery) = self.recovery() else {
            return;
        };
        if !matches!(recovery.role, RecoveryRole::Client) {
            return;
        }
        if recovery
            .reconnect_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let connection = self.shared();
        tokio::spawn(async move {
            let recovery = match connection.recovery() {
                Some(recovery) => recovery.clone(),
                None => return,
            };
            loop {
                connection.reconnect_until_attached(&recovery).await;
                recovery.reconnect_active.store(false, Ordering::Release);
                // A detach while the flag was set started no loop of its own.
                if connection.is_closed()
                    || connection.transport_attached.load(Ordering::Acquire)
                    || recovery
                        .reconnect_active
                        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                        .is_err()
                {
                    break;
                }
            }
        });
    }

    /// Retries the resume, backing off between attempts, until a transport is
    /// attached, the resume fails for good, or the connection closes.
    async fn reconnect_until_attached(
        self: &ConnectionRef,
        recovery: &crate::recovery::RecoveryState,
    ) {
        let mut backoff = recovery.reconnect_min_backoff;
        let max_backoff = recovery.reconnect_max_backoff.max(backoff);
        loop {
            if self.is_closed() {
                return;
            }
            self.debug
                .reconnect_attempts
                .fetch_add(1, Ordering::Relaxed);
            match self.resume_client_transport().await {
                Ok(()) => {
                    self.debug
                        .reconnect_successes
                        .fetch_add(1, Ordering::Relaxed);
                    #[cfg(test)]
                    if let Some(hook) = self.reconnected_hook.lock().unwrap().take() {
                        hook(self);
                    }
                    return;
                }
                Err(err) => {
                    self.debug.note_failure(
                        crate::debug::FAILURE_RECOVERY_RESUME,
                        format!("resume failed: {err}"),
                    );
                    self.debug
                        .reconnect_failures
                        .fetch_add(1, Ordering::Relaxed);
                    warn!("resume failed: {err}");
                    if matches!(
                        err,
                        Error::ResumeRejected(_) | Error::UnsupportedFrameVersion(_)
                    ) {
                        self.debug.note_failure(
                            crate::debug::FAILURE_RECOVERY_RECONNECT_TERMINAL,
                            format!("reconnect terminal failure: {err}"),
                        );
                        self.mark_closed();
                        return;
                    }
                    tokio::select! {
                        _ = self.wait_closed() => return,
                        _ = tokio::time::sleep(backoff) => {}
                    }
                    backoff = (backoff * 2).min(max_backoff);
                }
            }
        }
    }

    pub(crate) async fn resume_client_transport(&self) -> Result<(), Error> {
        let recovery = self.recovery().cloned().ok_or(Error::Closed)?;
        let connector = recovery
            .resume_connector
            .clone()
            .ok_or_else(|| Error::ResumeRejected("resume connector missing".to_string()))?;
        let attempt = async {
            let mut parts = connector().await?;
            let config = handshake_client(
                &mut parts.reader,
                &mut parts.writer,
                &[self.config().codec_id],
                self.config().max_frame,
                PROTOCOL_VERSION_V3,
            )
            .await?;
            if config.codec_id != self.config().codec_id {
                return Err(Error::ResumeRejected("codec changed on resume".to_string()));
            }
            let request = AttachRequest {
                mode: crate::recovery_protocol::ATTACH_MODE_RESUME,
                connection_id: recovery.connection_id,
                resume_secret: recovery.resume_secret,
                last_recv_seq: recovery.last_received(),
            };
            crate::recovery_protocol::write_attach_request(&mut parts.writer, &request).await?;
            let response =
                crate::recovery_protocol::read_attach_response(&mut parts.reader).await?;
            Ok::<_, Error>((parts, response))
        };
        let (parts, response) = match recovery.reconnect_timeout {
            Some(limit) => tokio::time::timeout(limit, attempt)
                .await
                .map_err(|_| Error::ConnectTimeout(None))??,
            None => attempt.await?,
        };
        if response.status != crate::recovery_protocol::ATTACH_STATUS_OK {
            return Err(Error::ResumeRejected("resume rejected".to_string()));
        }
        recovery.set_negotiated(response.negotiated);
        self.shared()
            .attach_transport_parts(parts, response.last_recv_seq);
        Ok(())
    }

    /// Applies a control frame and reports whether it was CLOSE.
    pub(crate) fn handle_control_frame(&self, payload: &[u8]) -> Result<bool, Error> {
        let (control_type, value) = parse_control_payload(payload)?;
        match control_type {
            CONTROL_TYPE_ACK => {
                if let Some(recovery) = self.recovery() {
                    recovery.ack_received(value);
                    // The writer may hold a frame waiting for the room this
                    // frees.
                    self.signal_send();
                }
                Ok(false)
            }
            CONTROL_TYPE_PING => Ok(false),
            CONTROL_TYPE_CLOSE => Ok(true),
            _ => Err(Error::InvalidMessage(
                "unknown control frame type".to_string(),
            )),
        }
    }

    /// Delivers a data frame that follows `last_recv_seq`, the last one
    /// received, and returns the new last received sequence number.
    pub(crate) async fn handle_recovery_data_frame(
        self: &ConnectionRef,
        recovery: &crate::recovery::RecoveryState,
        last_recv_seq: u64,
        stream_id: u32,
        seq: u64,
        flags: u8,
        payload: Vec<u8>,
    ) -> Result<u64, Error> {
        if seq == 0 {
            return Err(Error::InvalidMessage(
                "recovery data frame missing seq".to_string(),
            ));
        }
        if seq <= last_recv_seq {
            if recovery.note_received(seq) {
                self.signal_send();
            }
            return Ok(last_recv_seq);
        }
        if seq != last_recv_seq + 1 {
            return Err(Error::InvalidMessage(
                "recovery frame sequence gap".to_string(),
            ));
        }
        self.accept_frame(stream_id, flags, payload).await?;
        if recovery.note_received(seq) {
            self.signal_send();
        }
        Ok(seq)
    }
}

/// Reads the next frame, which must arrive within `timeout` of starting to
/// read it. `deadline` is re-armed only when it fires, rather than for every
/// frame.
async fn read_frame_within(
    config: &ConnConfig,
    reader: &mut TransportReader,
    timeout: Duration,
    mut deadline: Pin<&mut Sleep>,
) -> Result<Option<InboundFrame>, Error> {
    let read_started = tokio::time::Instant::now();
    let read = read_frame(config, reader);
    tokio::pin!(read);
    loop {
        tokio::select! {
            biased;
            frame = &mut read => return frame,
            _ = &mut deadline, if !timeout.is_zero() => {
                let expires = read_started + timeout;
                if tokio::time::Instant::now() >= expires {
                    return Err(Error::RecvTimeout);
                }
                deadline.as_mut().reset(expires);
            }
        }
    }
}

/// Ends the writer task: the connection is closed.
struct Done;

/// The state of the v3 writer task.
struct RecoveryWriter {
    connection: ConnectionRef,
    recovery: Arc<RecoveryState>,
    writer_cmd_rx: mpsc::UnboundedReceiver<WriterCommand>,
    outbound_rx: mpsc::Receiver<OutboundFrame>,
    /// The transport being written to, if one is attached.
    writer: Option<TransportWriter>,
    gen: u64,
    last_sent_seq: u64,
    /// The frames left to replay after the last attach.
    resume: VecDeque<Arc<FrameRecord>>,
    /// A frame that does not fit in the replay buffer yet. While it is held
    /// no other frame is taken, so its sender, and the senders queued behind
    /// it, wait until an ACK frees room.
    held: Option<OutboundFrame>,
    /// Whether frames were written since the last flush.
    dirty: bool,
}

impl RecoveryWriter {
    async fn run(mut self) {
        while self.step().await.is_ok() {}
    }

    /// Waits for a transport, or writes what is ready and waits for more.
    async fn step(&mut self) -> Result<(), Done> {
        if self.connection.closed.load(Ordering::Relaxed) {
            return Err(Done);
        }
        if self.writer.is_none() {
            return self.wait_detached().await;
        }
        while let Ok(cmd) = self.writer_cmd_rx.try_recv() {
            self.apply(cmd);
        }
        if self.writer.is_some() && !self.write_next().await? {
            self.flush_and_wait().await?;
        }
        Ok(())
    }

    /// Waits while no transport is attached. Frames sent meanwhile are staged,
    /// and the resume replays them.
    async fn wait_detached(&mut self) -> Result<(), Done> {
        // A pending Attach goes first, so that a close marker queued right
        // after connecting still gets written.
        tokio::select! {
            biased;
            _ = self.connection.closed_notify.notified() => return Err(Done),
            cmd = self.writer_cmd_rx.recv() => self.apply(cmd.ok_or(Done)?),
            frame = self.outbound_rx.recv(), if self.held.is_none() => {
                let frame = self.finish_on_close(frame.ok_or(Done)?).await?;
                self.stage(frame);
            }
            // The close marker is queued behind the held frame, and nothing
            // can be written while detached: stop, so close does not wait for
            // it.
            _ = self.connection.send_notify.notified(), if self.held.is_some() => {
                if self.connection.closing.load(Ordering::Acquire) {
                    return Err(Done);
                }
            }
        }
        Ok(())
    }

    /// Writes the next frame that is ready: a pending ACK first, then the
    /// frames to replay, which go before live frames because live frames take
    /// later sequence numbers. Returns whether there may be more to write.
    async fn write_next(&mut self) -> Result<bool, Done> {
        if let Some(ack_seq) = self.recovery.take_pending_ack() {
            let payload = build_ack_control_payload(ack_seq);
            self.write(
                CONTROL_STREAM_ID,
                0,
                0,
                &payload,
                FAILURE_RECOVERY_ACK_WRITE,
                "ack",
            )
            .await;
            return Ok(true);
        }
        if let Some(frame) = self.resume.pop_front() {
            self.recovery
                .resume_depth
                .store(self.resume.len(), Ordering::Relaxed);
            if frame.seq > self.last_sent_seq && frame.seq > self.recovery.replay.last_acked_seq() {
                self.write_record(&frame, FAILURE_RECOVERY_RESUME_WRITE, "resume")
                    .await;
            }
            return Ok(true);
        }
        let Some(frame) = self
            .held
            .take()
            .or_else(|| self.outbound_rx.try_recv().ok())
        else {
            return Ok(false);
        };
        self.write_live(frame).await?;
        // A held frame leaves nothing to write until an ACK frees room.
        Ok(self.held.is_none())
    }

    /// Flushes the frames written so far, then waits for something to do: a
    /// new frame, a signal that a frame or an ACK is ready, a transport
    /// change, the ACK delay, or the heartbeat interval.
    async fn flush_and_wait(&mut self) -> Result<(), Done> {
        if self.dirty {
            self.dirty = false;
            let writer = self.writer.as_mut().expect("writer missing");
            if let Err(err) = writer.flush().await {
                self.connection
                    .debug
                    .note_failure(FAILURE_RECOVERY_LIVE_WRITE, err.to_string());
                warn!("recovery flush failed: {err}");
                self.detach();
                return Ok(());
            }
        }
        let (wait, heartbeat_tick) = min_wait(
            self.recovery.next_ack_wait(),
            self.recovery.heartbeat_interval(),
        );
        tokio::select! {
            _ = self.connection.closed_notify.notified() => return Err(Done),
            _ = self.connection.send_notify.notified() => {}
            cmd = self.writer_cmd_rx.recv() => self.apply(cmd.ok_or(Done)?),
            Some(frame) = self.outbound_rx.recv(), if self.held.is_none() => {
                self.write_live(frame).await?;
            }
            _ = tokio::time::sleep(wait), if !wait.is_zero() => {
                if heartbeat_tick {
                    let payload = build_ping_control_payload();
                    self.write(CONTROL_STREAM_ID, 0, 0, &payload, FAILURE_RECOVERY_PING_WRITE, "ping")
                        .await;
                }
            }
        }
        Ok(())
    }

    /// Attaches a new transport or detaches the current one.
    fn apply(&mut self, cmd: WriterCommand) {
        match cmd {
            WriterCommand::Attach {
                gen,
                writer,
                resume_seq,
            } => {
                self.gen = gen;
                // The peer cannot have received more than was sent.
                let sent = self.connection.next_send_seq.load(Ordering::Acquire);
                self.last_sent_seq = resume_seq.min(sent);
                self.resume = self.recovery.prepare_resume(self.last_sent_seq);
                self.writer = Some(writer);
                self.dirty = false;
            }
            WriterCommand::Detach { gen } if gen == self.gen => self.writer = None,
            WriterCommand::Detach { .. } => {}
        }
    }

    /// Passes a data frame through. For the close marker it writes CLOSE after
    /// the frames already written, shuts down the transport and stops the
    /// writer, so the peer releases the connection instead of keeping it for a
    /// reconnect.
    async fn finish_on_close(&mut self, frame: OutboundFrame) -> Result<OutboundFrame, Done> {
        let written = match frame {
            OutboundFrame::Close { written } => written,
            frame => return Ok(frame),
        };
        let mut ok = false;
        if let Some(writer) = self.writer.as_mut() {
            let payload = build_close_control_payload();
            // shutdown flushes, then sends FIN.
            ok = write_frame_counted(&self.connection, writer, CONTROL_STREAM_ID, 0, 0, &payload)
                .await
                .is_ok()
                && writer.shutdown().await.is_ok();
        }
        let _ = written.send(ok);
        Err(Done)
    }

    /// Stages a new frame and writes it, unless the peer has it.
    async fn write_live(&mut self, frame: OutboundFrame) -> Result<(), Done> {
        let frame = self.finish_on_close(frame).await?;
        if let Some(record) = self.stage(frame) {
            if record.seq > self.last_sent_seq {
                self.write_record(&record, FAILURE_RECOVERY_LIVE_WRITE, "live")
                    .await;
            }
        }
        Ok(())
    }

    /// Keeps a new frame in the replay buffer under the next sequence number
    /// and tells its sender the outcome. Returns the kept record, or `None`
    /// if the frame is held or failed.
    fn stage(&mut self, frame: OutboundFrame) -> Option<Arc<FrameRecord>> {
        let (stream_id, flags, payload, queued_tx) = match frame {
            OutboundFrame::Plain {
                stream_id,
                flags,
                payload,
            } => (stream_id, flags, payload, None),
            OutboundFrame::Recovery {
                stream_id,
                flags,
                payload,
                queued_tx,
            } => (stream_id, flags, payload, Some(queued_tx)),
            OutboundFrame::Close { .. } => return None,
        };
        // The number is taken only once the frame is kept: the peer closes the
        // connection when a number is skipped. Only this task takes numbers.
        let seq = self.connection.next_send_seq.load(Ordering::Acquire) + 1;
        match self.recovery.replay.stage(stream_id, seq, flags, payload) {
            Ok(Staged::Kept(record)) => {
                self.connection.next_send_seq.store(seq, Ordering::Release);
                if let Some(queued_tx) = queued_tx {
                    let _ = queued_tx.send(Ok(()));
                }
                Some(record)
            }
            Ok(Staged::Full(payload)) => {
                self.held = Some(match queued_tx {
                    Some(queued_tx) => OutboundFrame::Recovery {
                        stream_id,
                        flags,
                        payload,
                        queued_tx,
                    },
                    None => OutboundFrame::Plain {
                        stream_id,
                        flags,
                        payload,
                    },
                });
                None
            }
            Err(err) => {
                if let Some(queued_tx) = queued_tx {
                    let _ = queued_tx.send(Err(err));
                }
                None
            }
        }
    }

    /// Writes a kept frame and records it as sent.
    async fn write_record(&mut self, frame: &FrameRecord, failure: DebugFailureCode, kind: &str) {
        let (stream_id, seq, flags) = (frame.stream_id, frame.seq, frame.flags);
        if self
            .write(stream_id, seq, flags, &frame.payload, failure, kind)
            .await
        {
            self.last_sent_seq = seq;
        }
    }

    /// Buffers a frame and reports whether that worked. On failure it
    /// detaches the transport.
    async fn write(
        &mut self,
        stream_id: u32,
        seq: u64,
        flags: u8,
        payload: &[u8],
        failure: DebugFailureCode,
        kind: &str,
    ) -> bool {
        let writer = self.writer.as_mut().expect("writer missing");
        match write_frame_counted(&self.connection, writer, stream_id, seq, flags, payload).await {
            Ok(()) => {
                self.dirty = true;
                true
            }
            Err(err) => {
                self.connection.debug.note_failure(failure, err.to_string());
                warn!("recovery {kind} write failed: {err}");
                self.detach();
                false
            }
        }
    }

    fn detach(&mut self) {
        self.connection.detach_transport(self.gen, true);
        self.writer = None;
    }
}

fn min_wait(ack_wait: Duration, heartbeat_wait: Duration) -> (Duration, bool) {
    if heartbeat_wait.is_zero() {
        return (ack_wait, false);
    }
    if ack_wait.is_zero() || heartbeat_wait < ack_wait {
        return (heartbeat_wait, true);
    }
    (ack_wait, false)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tokio::io::DuplexStream;
    use tokio::sync::oneshot;

    use super::*;
    use crate::codec_msgpack::CodecMsgpackCompact;
    use crate::connection::PendingConnection;
    use crate::frame::FRAME_HEADER_LEN_V3;
    use crate::protocol::{HandshakeConfig, DEFAULT_MAX_FRAME};
    use crate::recovery::{RecoveryState, ServerRecoveryOptions};
    use crate::recovery_protocol::RECOVERY_TOKEN_LEN;
    use crate::registry::Registry;
    use crate::replay::must_stage;

    /// A v3 connection over an in-memory pipe, set up but not started, with
    /// the peer end for the test to read what the writer sends.
    fn recovery_pipe(
        max_replay_bytes: i64,
    ) -> (PendingConnection, Arc<RecoveryState>, DuplexStream) {
        let (local, peer) = tokio::io::duplex(1 << 20);
        let pending = ConnectionInner::new_pending(local, Registry::new(), None, None);
        let recovery = RecoveryState::new_server(
            ServerRecoveryOptions {
                enable: true,
                detached_ttl: Duration::from_secs(60),
                max_replay_bytes,
                ack_every: 1,
                ack_delay: Duration::from_millis(1),
                heartbeat_interval: Duration::from_secs(3600),
                heartbeat_timeout: Duration::from_secs(7200),
            },
            [1; RECOVERY_TOKEN_LEN],
            [2; RECOVERY_TOKEN_LEN],
            Arc::new(std::sync::Mutex::new(HashMap::new())),
        );
        (pending, recovery, peer)
    }

    fn v3_config() -> HandshakeConfig {
        HandshakeConfig {
            version: PROTOCOL_VERSION_V3,
            codec_id: CodecMsgpackCompact,
            max_frame: DEFAULT_MAX_FRAME,
        }
    }

    fn live_frame(payload: Vec<u8>) -> (OutboundFrame, oneshot::Receiver<Result<(), Error>>) {
        let (queued_tx, queued_rx) = oneshot::channel();
        let frame = OutboundFrame::Recovery {
            stream_id: 1,
            flags: 0,
            payload,
            queued_tx,
        };
        (frame, queued_rx)
    }

    /// Returns the stream ID and sequence number of the next frame the
    /// connection writes.
    async fn next_frame(connection: &ConnectionRef, peer: &mut DuplexStream) -> (u32, u64) {
        let frame = tokio::time::timeout(
            Duration::from_secs(2),
            read_frame(connection.config(), peer),
        )
        .await
        .expect("timed out waiting for a frame")
        .expect("read frame")
        .expect("transport closed");
        (frame.0, frame.1)
    }

    /// A live frame must not overtake the frames waiting to be replayed. The
    /// ACK due at resume used to carry a live frame ahead of them, and the
    /// peer then closed the connection on the sequence gap.
    #[tokio::test]
    async fn resume_replays_before_live_frame() {
        let (pending, recovery, mut peer) = recovery_pipe(1 << 20);
        for seq in 1..=5u64 {
            must_stage(&recovery.replay, seq, vec![seq as u8]);
        }
        pending
            .connection()
            .next_send_seq
            .store(5, Ordering::Relaxed);
        recovery.note_received(1);
        let (frame, queued) = live_frame(vec![6]);
        assert!(pending.connection().outbound_tx.try_send(frame).is_ok());

        let connection = pending
            .start_with_config(v3_config(), Some(recovery), 0)
            .expect("start");
        let mut controls = 0;
        let mut want = 1;
        while want <= 6 {
            let (stream_id, seq) = next_frame(&connection, &mut peer).await;
            if stream_id == CONTROL_STREAM_ID {
                controls += 1;
                continue;
            }
            assert_eq!(
                seq, want,
                "data frame {seq} arrived while {want} was expected"
            );
            want += 1;
        }
        assert_eq!(controls, 1, "want the one ACK");
        queued.await.expect("queued result").expect("live frame");
        connection.close_now();
    }

    /// The peer's last received sequence number, exchanged at attach,
    /// acknowledges the frames up to it. The ACK the peer sent for them may
    /// have been lost with the old transport, and a send waiting for room in
    /// the replay buffer then waited forever.
    #[tokio::test]
    async fn attach_acknowledges_frames_the_peer_has() {
        const PAYLOAD_LEN: usize = 30;
        let frame_size = (FRAME_HEADER_LEN_V3 + PAYLOAD_LEN) as i64;
        let (pending, recovery, mut peer) = recovery_pipe(2 * frame_size + 10);
        for seq in 1..=2u64 {
            must_stage(&recovery.replay, seq, vec![0; PAYLOAD_LEN]);
        }
        pending
            .connection()
            .next_send_seq
            .store(2, Ordering::Relaxed);
        let (frame, queued) = live_frame(vec![0; PAYLOAD_LEN]);
        assert!(pending.connection().outbound_tx.try_send(frame).is_ok());
        let replay = Arc::clone(&recovery.replay);

        let connection = pending
            .start_with_config(v3_config(), Some(recovery), 2)
            .expect("start");
        let (stream_id, seq) = next_frame(&connection, &mut peer).await;
        assert_ne!(stream_id, CONTROL_STREAM_ID, "want a data frame");
        assert_eq!(seq, 3);
        queued.await.expect("queued result").expect("held frame");
        assert_eq!(replay.queued_count(), 1);
        connection.close_now();
    }
}
