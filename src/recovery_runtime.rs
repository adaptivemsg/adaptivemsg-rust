use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::error::Error;
use crate::protocol::{handshake_client, PROTOCOL_VERSION_V3};
use crate::recovery::RecoveryRole;
use crate::recovery_protocol::{
    build_ack_control_payload, build_ping_control_payload, parse_control_payload, AttachRequest,
    CONTROL_STREAM_ID, CONTROL_TYPE_ACK, CONTROL_TYPE_PING,
};

use super::{
    read_frame, write_frame, write_frame_no_flush, Connection, ConnectionInner, InboundFrame,
    OutboundFrame, TransportParts, TransportReader, TransportWriter, WriterCommand,
};
use std::sync::Arc;
use crate::replay::FrameRecord;

impl ConnectionInner {
    pub(crate) fn attach_transport_parts(
        self: &Connection,
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
        self: &Connection,
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

            let stage_live = |frame: OutboundFrame| -> Option<Arc<FrameRecord>> {
                match frame {
                    OutboundFrame::Plain { stream_id, payload } => {
                        let seq = connection.next_outbound_seq();
                        recovery.replay.add(stream_id, seq, payload).ok()
                    }
                    OutboundFrame::Recovery {
                        stream_id,
                        payload,
                        queued_tx,
                    } => {
                        let seq = connection.next_outbound_seq();
                        match recovery.replay.add(stream_id, seq, payload) {
                            Ok(record) => {
                                let _ = queued_tx.send(Ok(()));
                                Some(record)
                            }
                            Err(err) => {
                                let _ = queued_tx.send(Err(err));
                                None
                            }
                        }
                    }
                }
            };

            // Write a live frame; returns true if writer was lost.
            macro_rules! write_live {
                ($frame:expr) => {{
                    let frame = $frame;
                    if frame.seq > last_sent_seq {
                        let result = {
                            let w = writer.as_mut().expect("writer missing");
                            write_frame(connection.config(), w, frame.stream_id, frame.seq, &frame.payload).await
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
                    tokio::select! {
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
                        maybe_frame = outbound_rx.recv() => match maybe_frame {
                            Some(frame) => {
                                let _ = stage_live(frame);
                            }
                            None => return,
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
                    if let Ok(frame) = outbound_rx.try_recv() {
                        if let Some(frame) = stage_live(frame) {
                            if frame.seq > last_sent_seq {
                                let writer_ref = writer.as_mut().expect("writer missing");
                                let result = write_frame_no_flush(
                                    connection.config(),
                                    writer_ref,
                                    frame.stream_id,
                                    frame.seq,
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

                if let Ok(frame) = outbound_rx.try_recv() {
                    if let Some(frame) = stage_live(frame) {
                        write_live!(frame);
                    }
                    continue;
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
                        Some(frame) = outbound_rx.recv() => {
                            if let Some(frame) = stage_live(frame) {
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
                    Some(frame) = outbound_rx.recv() => {
                        if let Some(frame) = stage_live(frame) {
                            write_live!(frame);
                        }
                    },
                    _ = &mut sleep => {
                        if heartbeat_tick {
                            let payload = build_ping_control_payload();
                            let result = {
                                let w = writer.as_mut().expect("writer missing");
                                write_frame(connection.config(), w, CONTROL_STREAM_ID, 0, &payload).await
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
        self: &Connection,
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
                let frame: Result<InboundFrame, Error> = if timeout.is_zero() {
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
                    Ok((stream_id, seq, payload)) => {
                        connection.debug.frames_read.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        connection.debug.bytes_read.fetch_add(payload.len() as u64, std::sync::atomic::Ordering::Relaxed);
                        recovery.touch_activity();
                        if stream_id == CONTROL_STREAM_ID {
                            connection.debug.control_frames_read.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            if let Err(err) = connection.handle_control_frame(&payload) {
                                connection.debug.note_failure(crate::debug::FAILURE_RECOVERY_CONTROL, err.to_string());
                                warn!("recovery control frame failed: {err}");
                                connection.mark_closed();
                                return;
                            }
                            continue;
                        }
                        if let Err(err) = connection
                            .handle_recovery_data_frame(stream_id, seq, payload)
                            .await
                        {
                            connection.debug.note_failure(crate::debug::FAILURE_RECOVERY_DATA, err.to_string());
                            warn!("recovery data frame failed: {err}");
                            connection.mark_closed();
                            return;
                        }
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
        let response = crate::recovery_protocol::read_attach_response(&mut parts.reader).await?;
        if response.status != crate::recovery_protocol::ATTACH_STATUS_OK {
            return Err(Error::ResumeRejected("resume rejected".to_string()));
        }
        recovery.set_negotiated(response.negotiated);
        self.shared()
            .attach_transport_parts(parts, response.last_recv_seq);
        Ok(())
    }

    pub(crate) fn handle_control_frame(&self, payload: &[u8]) -> Result<(), Error> {
        let (control_type, value) = parse_control_payload(payload)?;
        match control_type {
            CONTROL_TYPE_ACK => {
                if let Some(recovery) = self.recovery() {
                    recovery.ack_received(value);
                }
                Ok(())
            }
            CONTROL_TYPE_PING => Ok(()),
            _ => Err(Error::InvalidMessage(
                "unknown control frame type".to_string(),
            )),
        }
    }

    pub(crate) async fn handle_recovery_data_frame(
        &self,
        stream_id: u32,
        seq: u64,
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
        let stream = self.shared().get_stream(stream_id);
        stream
            .incoming_tx
            .send(payload)
            .await
            .map_err(|_| Error::Closed)?;
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
