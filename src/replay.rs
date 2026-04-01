use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::error::Error;
use crate::frame::frame_header_len_for_version;

#[derive(Debug)]
pub(crate) struct FrameRecord {
    pub stream_id: u32,
    pub seq: u64,
    pub payload: Vec<u8>,
    pub size: i64,
}

#[derive(Default)]
struct ReplayInner {
    used_bytes: i64,
    replay_queue: VecDeque<Arc<FrameRecord>>,
}

pub(crate) struct ReplayBuffer {
    version: u8,
    max_bytes: i64,
    last_acked: AtomicU64,
    inner: Mutex<ReplayInner>,
}

impl ReplayBuffer {
    pub(crate) fn new(version: u8, max_bytes: i64) -> Self {
        Self {
            version,
            max_bytes,
            last_acked: AtomicU64::new(0),
            inner: Mutex::new(ReplayInner::default()),
        }
    }

    pub(crate) fn add(
        &self,
        stream_id: u32,
        seq: u64,
        payload: Vec<u8>,
    ) -> Result<Arc<FrameRecord>, Error> {
        if seq == 0 {
            return Ok(Arc::new(FrameRecord {
                stream_id,
                seq,
                size: payload.len() as i64,
                payload,
            }));
        }

        let size = replay_entry_size(self.version, payload.len())?;
        let mut inner = self.inner.lock().unwrap();
        let last_acked = self.last_acked.load(Ordering::Relaxed);
        if seq <= last_acked {
            return Err(Error::InvalidMessage(
                "replay frame seq already acknowledged".to_string(),
            ));
        }
        if let Some(last) = inner.replay_queue.back() {
            if seq <= last.seq {
                return Err(Error::InvalidMessage(
                    "replay frame seq must increase monotonically".to_string(),
                ));
            }
        }

        let next_used = inner.used_bytes + size;
        if self.max_bytes > 0 && next_used > self.max_bytes {
            return Err(Error::ReplayBufferFull {
                limit: self.max_bytes,
                size: next_used,
            });
        }

        let record = Arc::new(FrameRecord {
            stream_id,
            seq,
            payload,
            size,
        });
        inner.replay_queue.push_back(record.clone());
        inner.used_bytes = next_used;
        Ok(record)
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

    pub(crate) fn snapshot_from(&self, last_seq: u64) -> Vec<Arc<FrameRecord>> {
        let inner = self.inner.lock().unwrap();
        inner
            .replay_queue
            .iter()
            .filter(|frame| frame.seq > last_seq)
            .cloned()
            .collect()
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

fn replay_entry_size(version: u8, payload_len: usize) -> Result<i64, Error> {
    Ok((frame_header_len_for_version(version)? + payload_len) as i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::PROTOCOL_VERSION_V3;

    #[test]
    fn replay_ack_drops_acked_frames() {
        let buffer = ReplayBuffer::new(crate::protocol::PROTOCOL_VERSION_V3, 1024);
        let first = buffer.add(1, 1, vec![1, 2, 3]).expect("first replay add");
        let second = buffer.add(1, 2, vec![4, 5, 6]).expect("second replay add");

        let snapshot = buffer.snapshot_from(0);
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot[0].seq, first.seq);
        assert_eq!(snapshot[1].seq, second.seq);

        let dropped = buffer.ack(1);
        assert!(dropped > 0);

        let snapshot = buffer.snapshot_from(0);
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].seq, 2);
    }

    #[test]
    fn add_and_snapshot() {
        let buf = ReplayBuffer::new(PROTOCOL_VERSION_V3, 1_000_000);
        buf.add(0, 1, vec![1, 2, 3]).unwrap();
        buf.add(0, 2, vec![4, 5]).unwrap();
        buf.add(0, 3, vec![6]).unwrap();
        assert_eq!(buf.queued_count(), 3);
        assert!(buf.used_bytes() > 0);

        let snap = buf.snapshot_from(0);
        assert_eq!(snap.len(), 3);
        assert_eq!(snap[0].seq, 1);
        assert_eq!(snap[2].seq, 3);

        let snap2 = buf.snapshot_from(2);
        assert_eq!(snap2.len(), 1);
        assert_eq!(snap2[0].seq, 3);
    }

    #[test]
    fn byte_limit_rejects_overflow() {
        let buf = ReplayBuffer::new(PROTOCOL_VERSION_V3, 30);
        buf.add(0, 1, vec![0; 10]).unwrap();
        let result = buf.add(0, 2, vec![0; 100]);
        assert!(result.is_err());
    }

    #[test]
    fn ack_frees_bytes() {
        let buf = ReplayBuffer::new(PROTOCOL_VERSION_V3, 1_000_000);
        buf.add(0, 1, vec![0; 100]).unwrap();
        buf.add(0, 2, vec![0; 100]).unwrap();
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
