use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::error::Error;
use crate::protocol::{handshake_client, PROTOCOL_VERSION_V3};
use crate::recovery::RecoveryRole;
use crate::recovery_protocol::{
    build_ack_control_payload, build_close_control_payload, build_ping_control_payload,
    parse_control_payload, AttachRequest, CONTROL_STREAM_ID, CONTROL_TYPE_ACK,
    CONTROL_TYPE_CLOSE, CONTROL_TYPE_PING,
};

use super::{
    read_frame, write_frame, write_frame_no_flush, ConnectionRef, ConnectionInner, InboundFrame,
    OutboundFrame, TransportParts, TransportReader, TransportWriter, WriterCommand,
};
use std::sync::Arc;
use crate::replay::FrameRecord;

impl ConnectionInner {
    pub(crate) fn attach_transport_parts(
        self: &ConnectionRef,
        parts: TransportParts,
        peer_last_recv_seq: u64,
    ) {
        if self.closed.load(std::sync::atomic::Ordering::Relaxed) {
            return;
        }
        let gen = self
            .transport_gen
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            + 1;
        if let Some((_, handle)) = self.current_reader_abort.lock().unwrap().take() {
            handle.abort();
        }
        let _ = self.writer_cmd_tx.send(WriterCommand::Attach {
            gen,
            writer: parts.writer,
            resume_seq: peer_last_recv_seq,
        });
        if let Some(recovery) = self.recovery() {
            recovery.on_attached();
        }
        self.signal_send();
        let reader_task = if self.is_recovery_enabled() {
            self.spawn_recovery_reader(gen, parts.reader)
        } else {
            self.spawn_plain_reader(gen, parts.reader)
        };
        *self.current_reader_abort.lock().unwrap() = Some((gen, reader_task.abort_handle()));
        self.debug.transport_attaches.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub(super) fn spawn_recovery_writer(
        self: &ConnectionRef,
        mut writer_cmd_rx: mpsc::UnboundedReceiver<WriterCommand>,
        mut outbound_rx: mpsc::Receiver<OutboundFrame>,
    ) -> JoinHandle<()> {
        let connection = self.clone();
        tokio::spawn(async move {
            let recovery = match connection.recovery() {
                Some(recovery) => recovery.clone(),
                None => return,
            };
            let mut writer: Option<TransportWriter> = None;
            let mut current_gen = 0;
            let mut last_sent_seq = 0;
            // A frame that does not fit in the replay buffer yet. While it is
            // held no other frame is taken, so its sender, and the senders
            // queued behind it, wait until an ACK frees room.
            let mut held: Option<OutboundFrame> = None;

            let stage_live = |frame: OutboundFrame,
                              held: &mut Option<OutboundFrame>|
             -> Option<Arc<FrameRecord>> {
                let len = match &frame {
                    OutboundFrame::Plain { payload, .. }
                    | OutboundFrame::Recovery { payload, .. } => payload.len(),
                    OutboundFrame::Close { .. } => return None,
                };
                if let (false, true) = recovery.replay.fits(len) {
                    *held = Some(frame);
                    return None;
                }
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
                let seq = connection.next_outbound_seq();
                match recovery.replay.add(stream_id, seq, flags, payload) {
                    Ok(record) => {
                        if let Some(queued_tx) = queued_tx {
                            let _ = queued_tx.send(Ok(()));
                        }
                        Some(record)
                    }
                    Err(err) => {
                        // Give the unused sequence number back: the peer closes
                        // the connection when a number is skipped. Only this
                        // task takes numbers.
                        connection
                            .next_send_seq
                            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                        if let Some(queued_tx) = queued_tx {
                            let _ = queued_tx.send(Err(err));
                        }
                        None
                    }
                }
            };

            // Passes a data frame through. For the close marker it writes CLOSE
            // after the frames already written, shuts down the transport and
            // stops, so the peer releases the connection instead of keeping it
            // for a reconnect.
            macro_rules! live_or_finish {
                ($frame:expr) => {{
                    match $frame {
                        OutboundFrame::Close { written } => {
                            let mut ok = false;
                            if let Some(w) = writer.as_mut() {
                                let payload = build_close_control_payload();
                                ok = write_frame(connection.config(), w, CONTROL_STREAM_ID, 0, 0, &payload)
                                    .await
                                    .is_ok()
                                    && w.shutdown().await.is_ok();
                            }
                            let _ = written.send(ok);
                            return;
                        }
                        frame => frame,
                    }
                }};
            }

            // Write a live frame; returns true if writer was lost.
            macro_rules! write_live {
                ($frame:expr) => {{
                    let frame = $frame;
                    if frame.seq > last_sent_seq {
                        let result = {
                            let w = writer.as_mut().expect("writer missing");
                            write_frame(connection.config(), w, frame.stream_id, frame.seq, frame.flags, &frame.payload).await
                        };
                        if let Err(err) = result {
                            connection.debug.note_failure(crate::debug::FAILURE_RECOVERY_LIVE_WRITE, err.to_string());
                            warn!("recovery live write failed: {err}");
                            connection.detach_transport(current_gen, true);
                            writer = None;
                        } else {
                            connection.debug.frames_written.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            connection.debug.bytes_written.fetch_add(frame.payload.len() as u64, std::sync::atomic::Ordering::Relaxed);
                            last_sent_seq = frame.seq;
                        }
                    }
                }};
            }

            loop {
                if connection.closed.load(std::sync::atomic::Ordering::Relaxed) {
                    return;
                }
                if writer.is_none() {
                    // A pending Attach goes first, so that a close marker
                    // queued right after connecting still gets written.
                    tokio::select! {
                        biased;
                        _ = connection.closed_notify.notified() => return,
                        cmd = writer_cmd_rx.recv() => match cmd {
                            Some(WriterCommand::Attach { gen, writer: next_writer, resume_seq }) => {
                                current_gen = gen;
                                last_sent_seq = resume_seq;
                                recovery.prepare_resume(resume_seq);
                                writer = Some(next_writer);
                            }
                            Some(WriterCommand::Detach { .. }) => {}
                            None => return,
                        },
                        maybe_frame = outbound_rx.recv(), if held.is_none() => match maybe_frame {
                            Some(frame) => {
                                let _ = stage_live(live_or_finish!(frame), &mut held);
                            }
                            None => return,
                        },
                        // The close marker is queued behind the held frame,
                        // and nothing can be written while detached: stop, so
                        // close does not wait for it.
                        _ = connection.send_notify.notified(), if held.is_some() => {
                            if connection.closing.load(std::sync::atomic::Ordering::Acquire) {
                                return;
                            }
                        }
                    }
                    continue;
                }

                while let Ok(cmd) = writer_cmd_rx.try_recv() {
                    match cmd {
                        WriterCommand::Attach {
                            gen,
                            writer: next_writer,
                            resume_seq,
                        } => {
                            current_gen = gen;
                            last_sent_seq = resume_seq;
                            recovery.prepare_resume(resume_seq);
                            writer = Some(next_writer);
                        }
                        WriterCommand::Detach { gen } if gen == current_gen => writer = None,
                        WriterCommand::Detach { .. } => {}
                    }
                }
                if writer.is_none() {
                    continue;
                }

                if let Some(ack_seq) = recovery.take_pending_ack() {
                    let ack_payload = build_ack_control_payload(ack_seq);
                    let writer_ref = writer.as_mut().expect("writer missing");
                    // Buffer ACK frame (no flush yet)
                    let result = write_frame_no_flush(
                        connection.config(),
                        writer_ref,
                        CONTROL_STREAM_ID,
                        0,
                        0,
                        &ack_payload,
                    )
                    .await;
                    if let Err(err) = result {
                        connection.debug.note_failure(crate::debug::FAILURE_RECOVERY_ACK_WRITE, err.to_string());
                        warn!("recovery ack write failed: {err}");
                        connection.detach_transport(current_gen, true);
                        writer = None;
                        continue;
                    }
                    connection.debug.control_frames_written.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    // Batch: also write one pending live frame before flushing.
                    let next = if held.is_none() {
                        outbound_rx.try_recv().ok()
                    } else {
                        None
                    };
                    if let Some(frame) = next {
                        if let Some(frame) = stage_live(live_or_finish!(frame), &mut held) {
                            if frame.seq > last_sent_seq {
                                let writer_ref = writer.as_mut().expect("writer missing");
                                let result = write_frame_no_flush(
                                    connection.config(),
                                    writer_ref,
                                    frame.stream_id,
                                    frame.seq,
                                    frame.flags,
                                    &frame.payload,
                                )
                                .await;
                                if let Err(err) = result {
                                    connection.debug.note_failure(crate::debug::FAILURE_RECOVERY_LIVE_WRITE, err.to_string());
                                    warn!("recovery live write failed: {err}");
                                    connection.detach_transport(current_gen, true);
                                    writer = None;
                                    continue;
                                }
                                connection.debug.frames_written.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                connection.debug.bytes_written.fetch_add(frame.payload.len() as u64, std::sync::atomic::Ordering::Relaxed);
                                last_sent_seq = frame.seq;
                            }
                        }
                    }
                    // Single flush for ACK + optional data
                    if let Err(err) = writer.as_mut().expect("writer missing").flush().await {
                        warn!("recovery flush failed: {err}");
                        connection.detach_transport(current_gen, true);
                        writer = None;
                    }
                    continue;
                }

                if recovery
                    .resume_active
                    .load(std::sync::atomic::Ordering::Acquire)
                {
                    if let Some(frame) = recovery.take_resume_frame() {
                        if frame.seq <= last_sent_seq
                            || frame.seq <= recovery.replay.last_acked_seq()
                        {
                            continue;
                        }
                        let result = {
                            let writer_ref = writer.as_mut().expect("writer missing");
                            write_frame(
                                connection.config(),
                                writer_ref,
                                frame.stream_id,
                                frame.seq,
                                frame.flags,
                                &frame.payload,
                            )
                            .await
                        };
                        if let Err(err) = result {
                            connection.debug.note_failure(crate::debug::FAILURE_RECOVERY_RESUME_WRITE, err.to_string());
                            warn!("recovery resume write failed: {err}");
                            connection.detach_transport(current_gen, true);
                            writer = None;
                        } else {
                            connection.debug.frames_written.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            last_sent_seq = frame.seq;
                        }
                        continue;
                    }
                }

                let next = held.take().or_else(|| outbound_rx.try_recv().ok());
                if let Some(frame) = next {
                    if let Some(frame) = stage_live(live_or_finish!(frame), &mut held) {
                        write_live!(frame);
                    }
                    if held.is_none() {
                        continue;
                    }
                }

                let ack_wait = recovery.next_ack_wait();
                let heartbeat_wait = recovery.heartbeat_interval();
                let (wait, heartbeat_tick) = min_wait(ack_wait, heartbeat_wait);

                if wait.is_zero() {
                    tokio::select! {
                        _ = connection.closed_notify.notified() => return,
                        _ = connection.send_notify.notified() => {},
                        cmd = writer_cmd_rx.recv() => match cmd {
                            Some(WriterCommand::Attach { gen, writer: next_writer, resume_seq }) => {
                                current_gen = gen;
                                last_sent_seq = resume_seq;
                                recovery.prepare_resume(resume_seq);
                                writer = Some(next_writer);
                            }
                            Some(WriterCommand::Detach { gen }) if gen == current_gen => writer = None,
                            _ => {}
                        },
                        Some(frame) = outbound_rx.recv(), if held.is_none() => {
                            if let Some(frame) = stage_live(live_or_finish!(frame), &mut held) {
                                write_live!(frame);
                            }
                        },
                    }
                    continue;
                }

                let sleep = tokio::time::sleep(wait);
                tokio::pin!(sleep);
                tokio::select! {
                    _ = connection.closed_notify.notified() => return,
                    _ = connection.send_notify.notified() => {},
                    cmd = writer_cmd_rx.recv() => match cmd {
                        Some(WriterCommand::Attach { gen, writer: next_writer, resume_seq }) => {
                            current_gen = gen;
                            last_sent_seq = resume_seq;
                            recovery.prepare_resume(resume_seq);
                            writer = Some(next_writer);
                        }
                        Some(WriterCommand::Detach { gen }) if gen == current_gen => writer = None,
                        _ => {}
                    },
                    Some(frame) = outbound_rx.recv(), if held.is_none() => {
                        if let Some(frame) = stage_live(live_or_finish!(frame), &mut held) {
                            write_live!(frame);
                        }
                    },
                    _ = &mut sleep => {
                        if heartbeat_tick {
                            let payload = build_ping_control_payload();
                            let result = {
                                let w = writer.as_mut().expect("writer missing");
                                write_frame(connection.config(), w, CONTROL_STREAM_ID, 0, 0, &payload).await
                            };
                            if let Err(err) = result {
                                connection.debug.note_failure(crate::debug::FAILURE_RECOVERY_PING_WRITE, err.to_string());
                                warn!("recovery ping write failed: {err}");
                                connection.detach_transport(current_gen, true);
                                writer = None;
                            } else {
                                connection.debug.control_frames_written.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                        }
                    }
                }
            }
        })
    }

    pub(super) fn spawn_recovery_reader(
        self: &ConnectionRef,
        gen: u64,
        mut reader: TransportReader,
    ) -> JoinHandle<()> {
        let connection = self.clone();
        tokio::spawn(async move {
            let recovery = match connection.recovery() {
                Some(recovery) => recovery.clone(),
                None => return,
            };
            loop {
                let timeout = recovery.heartbeat_timeout();
                let frame: Result<Option<InboundFrame>, Error> = if timeout.is_zero() {
                    read_frame(connection.config(), &mut reader).await
                } else {
                    match tokio::time::timeout(
                        timeout,
                        read_frame(connection.config(), &mut reader),
                    )
                    .await
                    {
                        Ok(result) => result,
                        Err(_) => Err(Error::RecvTimeout),
                    }
                };
                match frame {
                    Ok(Some((stream_id, seq, flags, payload))) => {
                        connection.debug.frames_read.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        connection.debug.bytes_read.fetch_add(payload.len() as u64, std::sync::atomic::Ordering::Relaxed);
                        recovery.touch_activity();
                        if stream_id == CONTROL_STREAM_ID {
                            connection.debug.control_frames_read.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            match connection.handle_control_frame(&payload) {
                                Ok(false) => continue,
                                Ok(true) => {
                                    connection.close_by_peer().await;
                                    return;
                                }
                                Err(err) => {
                                    connection.debug.note_failure(crate::debug::FAILURE_RECOVERY_CONTROL, err.to_string());
                                    warn!("recovery control frame failed: {err}");
                                    connection.mark_closed();
                                    return;
                                }
                            }
                        }
                        if let Err(err) = connection
                            .handle_recovery_data_frame(stream_id, seq, flags, payload)
                            .await
                        {
                            connection.debug.note_failure(crate::debug::FAILURE_RECOVERY_DATA, err.to_string());
                            warn!("recovery data frame failed: {err}");
                            connection.mark_closed();
                            return;
                        }
                    }
                    // Resuming would replay the same frame, so a malformed frame is terminal.
                    Err(
                        err @ (Error::InvalidMessage(_)
                        | Error::UnsupportedFrameVersion(_)
                        | Error::FrameTooLarge(_)),
                    ) => {
                        connection.debug.note_failure(crate::debug::FAILURE_RECOVERY_DATA, err.to_string());
                        warn!("recovery frame rejected: {err}");
                        connection.mark_closed();
                        return;
                    }
                    // close sent CLOSE and waits for the peer to close its side.
                    _ if connection.closing.load(std::sync::atomic::Ordering::Acquire) => {
                        connection.mark_closed();
                        return;
                    }
                    Ok(None) => {
                        debug!("recovery reader ended: peer closed the transport");
                        connection.detach_transport(gen, false);
                        return;
                    }
                    Err(err) => {
                        connection.debug.note_failure(crate::debug::FAILURE_RECOVERY_READ, err.to_string());
                        debug!("recovery reader ended: {err}");
                        connection.detach_transport(gen, false);
                        return;
                    }
                }
            }
        })
    }

    pub(super) fn detach_transport(&self, gen: u64, abort_reader: bool) {
        if self.closed.load(std::sync::atomic::Ordering::Relaxed) {
            return;
        }
        if self
            .transport_gen
            .compare_exchange(
                gen,
                gen + 1,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_err()
        {
            return;
        }
        self.debug.transport_detaches.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
            if !connection.closed.load(std::sync::atomic::Ordering::Relaxed)
                && connection
                    .transport_gen
                    .load(std::sync::atomic::Ordering::Acquire)
                    == detached_gen
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
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
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
            let mut backoff = recovery.reconnect_min_backoff;
            let max_backoff = recovery.reconnect_max_backoff.max(backoff);
            loop {
                if connection.closed.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
                connection.debug.reconnect_attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                match connection.resume_client_transport().await {
                    Ok(()) => {
                        connection.debug.reconnect_successes.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        break;
                    }
                    Err(err) => {
                        connection.debug.reconnect_failures.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        warn!("resume failed: {err}");
                        if matches!(
                            err,
                            Error::ResumeRejected(_) | Error::UnsupportedFrameVersion(_)
                        ) {
                            connection.debug.note_failure(crate::debug::FAILURE_RECOVERY_RECONNECT_TERMINAL, err.to_string());
                            connection.mark_closed();
                            break;
                        }
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(max_backoff);
                    }
                }
            }
            recovery
                .reconnect_active
                .store(false, std::sync::atomic::Ordering::Release);
        });
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

    pub(crate) async fn handle_recovery_data_frame(
        &self,
        stream_id: u32,
        seq: u64,
        flags: u8,
        payload: Vec<u8>,
    ) -> Result<(), Error> {
        let Some(recovery) = self.recovery() else {
            return Err(Error::Closed);
        };
        if seq == 0 {
            return Err(Error::InvalidMessage(
                "recovery data frame missing seq".to_string(),
            ));
        }
        let last_recv_seq = recovery.last_received();
        if seq <= last_recv_seq {
            if recovery.note_received(seq) {
                self.signal_send();
            }
            return Ok(());
        }
        if seq != last_recv_seq + 1 {
            return Err(Error::InvalidMessage(
                "recovery frame sequence gap".to_string(),
            ));
        }
        self.shared()
            .accept_frame(stream_id, flags, payload)
            .await?;
        if recovery.note_received(seq) {
            self.signal_send();
        }
        Ok(())
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
