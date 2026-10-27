use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::error::Error;
use crate::frame::frame_header_len_for_version;
use crate::frame::FRAME_HEADER_LEN_V3;

#[derive(Debug)]
pub(crate) struct FrameRecord {
    pub stream_id: u32,
    pub seq: u64,
    pub flags: u8,
    pub payload: Vec<u8>,
    pub size: i64,
}

/// What [`ReplayBuffer::stage`] did with a frame.
pub(crate) enum Staged {
    Kept(Arc<FrameRecord>),
    /// The frame does not fit next to the frames kept now, but will once they
    /// are acknowledged. The payload is handed back.
    Full(Vec<u8>),
}

#[derive(Default)]
struct ReplayInner {
    used_bytes: i64,
    replay_queue: VecDeque<Arc<FrameRecord>>,
}

pub(crate) struct ReplayBuffer {
    header_len: usize,
    max_bytes: i64,
    last_acked: AtomicU64,
    inner: Mutex<ReplayInner>,
}

impl ReplayBuffer {
    pub(crate) fn new(version: u8, max_bytes: i64) -> Self {
        Self {
            header_len: frame_header_len_for_version(version).unwrap_or(FRAME_HEADER_LEN_V3),
            max_bytes,
            last_acked: AtomicU64::new(0),
            inner: Mutex::new(ReplayInner::default()),
        }
    }

    /// Keeps a frame under sequence number `seq` unless it has to wait for
    /// room.
    pub(crate) fn stage(
        &self,
        stream_id: u32,
        seq: u64,
        flags: u8,
        payload: Vec<u8>,
    ) -> Result<Staged, Error> {
        let size = (self.header_len + payload.len()) as i64;
        let mut inner = self.inner.lock().unwrap();
        if self.max_bytes > 0 && inner.used_bytes + size > self.max_bytes {
            if size <= self.max_bytes {
                return Ok(Staged::Full(payload));
            }
            return Err(Error::FrameTooLarge(payload.len()));
        }
        if seq <= self.last_acked.load(Ordering::Relaxed) {
            return Err(Error::InvalidMessage(
                "replay frame seq already acknowledged".to_string(),
            ));
        }
        if inner
            .replay_queue
            .back()
            .is_some_and(|last| seq <= last.seq)
        {
            return Err(Error::InvalidMessage(
                "replay frame seq must increase monotonically".to_string(),
            ));
        }

        let record = Arc::new(FrameRecord {
            stream_id,
            seq,
            flags,
            payload,
            size,
        });
        inner.replay_queue.push_back(Arc::clone(&record));
        inner.used_bytes += size;
        Ok(Staged::Kept(record))
    }

    pub(crate) fn ack(&self, last_seq: u64) -> i64 {
        if last_seq == 0 {
            return 0;
        }
        let mut inner = self.inner.lock().unwrap();
        if last_seq <= self.last_acked.load(Ordering::Relaxed) {
            return 0;
        }
        self.last_acked.store(last_seq, Ordering::Relaxed);
        let mut dropped = 0;
        while let Some(front) = inner.replay_queue.front() {
            if front.seq > last_seq {
                break;
            }
            dropped += front.size;
            inner.replay_queue.pop_front();
        }
        inner.used_bytes -= dropped;
        dropped
    }

    /// Returns the frames after `last_seq`.
    pub(crate) fn snapshot_from(&self, last_seq: u64) -> VecDeque<Arc<FrameRecord>> {
        let inner = self.inner.lock().unwrap();
        let start = inner
            .replay_queue
            .partition_point(|frame| frame.seq <= last_seq);
        inner.replay_queue.range(start..).cloned().collect()
    }

    pub(crate) fn last_acked_seq(&self) -> u64 {
        self.last_acked.load(Ordering::Relaxed)
    }

    pub(crate) fn queued_count(&self) -> usize {
        self.inner.lock().unwrap().replay_queue.len()
    }

    pub(crate) fn used_bytes(&self) -> i64 {
        self.inner.lock().unwrap().used_bytes
    }
}

#[cfg(test)]
pub(crate) fn must_stage(buffer: &ReplayBuffer, seq: u64, payload: Vec<u8>) -> Arc<FrameRecord> {
    match buffer.stage(1, seq, 0, payload) {
        Ok(Staged::Kept(record)) => record,
        Ok(Staged::Full(_)) => panic!("frame {seq} has to wait for room"),
        Err(err) => panic!("stage frame {seq}: {err}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::PROTOCOL_VERSION_V3;

    fn size(payload_len: usize) -> i64 {
        (FRAME_HEADER_LEN_V3 + payload_len) as i64
    }

    #[test]
    fn replay_ack_drops_acked_frames() {
        let buffer = ReplayBuffer::new(PROTOCOL_VERSION_V3, 1024);
        must_stage(&buffer, 1, vec![1, 2, 3]);
        must_stage(&buffer, 2, vec![4, 5, 6]);

        let snapshot = buffer.snapshot_from(0);
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot[0].seq, 1);
        assert_eq!(snapshot[1].seq, 2);

        assert_eq!(buffer.ack(1), size(3));
        let snapshot = buffer.snapshot_from(0);
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].seq, 2);
    }

    #[test]
    fn stage_and_snapshot() {
        let buf = ReplayBuffer::new(PROTOCOL_VERSION_V3, 1_000_000);
        must_stage(&buf, 1, vec![1, 2, 3]);
        must_stage(&buf, 2, vec![4, 5]);
        must_stage(&buf, 3, vec![6]);
        assert_eq!(buf.queued_count(), 3);
        assert_eq!(buf.used_bytes(), size(3) + size(2) + size(1));

        let snap = buf.snapshot_from(0);
        assert_eq!(snap.len(), 3);
        assert_eq!(snap[0].seq, 1);
        assert_eq!(snap[2].seq, 3);

        let snap2 = buf.snapshot_from(2);
        assert_eq!(snap2.len(), 1);
        assert_eq!(snap2[0].seq, 3);
        assert!(buf.snapshot_from(3).is_empty());
    }

    #[test]
    fn byte_limit_rejects_frame_larger_than_buffer() {
        let buf = ReplayBuffer::new(PROTOCOL_VERSION_V3, 30);
        must_stage(&buf, 1, vec![0; 10]);
        let result = buf.stage(1, 2, 0, vec![0; 100]);
        assert!(matches!(result, Err(Error::FrameTooLarge(100))));
    }

    #[test]
    fn full_until_ack() {
        let buf = ReplayBuffer::new(PROTOCOL_VERSION_V3, 30);
        must_stage(&buf, 1, vec![0; 10]);
        let payload = match buf.stage(1, 2, 0, vec![0; 10]) {
            Ok(Staged::Full(payload)) => payload,
            _ => panic!("want the frame to wait for room"),
        };
        buf.ack(1);
        must_stage(&buf, 2, payload);
    }

    #[test]
    fn rejects_stale_seq() {
        let buf = ReplayBuffer::new(PROTOCOL_VERSION_V3, 1024);
        must_stage(&buf, 5, vec![1]);
        assert!(buf.stage(1, 5, 0, vec![2]).is_err());
        buf.ack(5);
        assert!(buf.stage(1, 4, 0, vec![3]).is_err());
    }

    #[test]
    fn ack_frees_bytes() {
        let buf = ReplayBuffer::new(PROTOCOL_VERSION_V3, 1_000_000);
        must_stage(&buf, 1, vec![0; 100]);
        must_stage(&buf, 2, vec![0; 100]);
        let bytes_before = buf.used_bytes();
        buf.ack(1);
        assert!(buf.used_bytes() < bytes_before);
        assert_eq!(buf.queued_count(), 1);
        assert_eq!(buf.last_acked_seq(), 1);
    }

    #[test]
    fn snapshot_from_empty() {
        let buf = ReplayBuffer::new(PROTOCOL_VERSION_V3, 1_000_000);
        let snap = buf.snapshot_from(0);
        assert!(snap.is_empty());
    }
}
