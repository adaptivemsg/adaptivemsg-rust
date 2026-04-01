use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::replay::FrameRecord;

#[derive(Default)]
pub(crate) struct FrameDeque {
    inner: Mutex<VecDeque<Arc<FrameRecord>>>,
}

impl FrameDeque {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn pop(&self) -> Option<Arc<FrameRecord>> {
        self.inner.lock().unwrap().pop_front()
    }

    pub(crate) fn reset(&self, frames: Vec<Arc<FrameRecord>>) {
        let mut inner = self.inner.lock().unwrap();
        inner.clear();
        inner.extend(frames);
    }

    pub(crate) fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    pub(crate) fn push(&self, frame: Arc<FrameRecord>) {
        self.inner.lock().unwrap().push_back(frame);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replay::FrameRecord;

    #[test]
    fn push_pop_order_and_growth() {
        let dq = FrameDeque::default();
        for i in 1..=5u64 {
            dq.push(Arc::new(FrameRecord { stream_id: 0, seq: i, payload: vec![i as u8], size: 10 }));
        }
        assert_eq!(dq.len(), 5);
        for i in 1..=5u64 {
            let f = dq.pop().unwrap();
            assert_eq!(f.seq, i);
        }
        assert_eq!(dq.len(), 0);
        assert!(dq.pop().is_none());
    }

    #[test]
    fn reset_replaces_contents() {
        let dq = FrameDeque::default();
        dq.push(Arc::new(FrameRecord { stream_id: 0, seq: 1, payload: vec![1], size: 10 }));
        dq.push(Arc::new(FrameRecord { stream_id: 0, seq: 2, payload: vec![2], size: 10 }));
        assert_eq!(dq.len(), 2);

        let replacements = vec![
            Arc::new(FrameRecord { stream_id: 0, seq: 10, payload: vec![10], size: 10 }),
            Arc::new(FrameRecord { stream_id: 0, seq: 11, payload: vec![11], size: 10 }),
            Arc::new(FrameRecord { stream_id: 0, seq: 12, payload: vec![12], size: 10 }),
        ];
        dq.reset(replacements);
        assert_eq!(dq.len(), 3);
        assert_eq!(dq.pop().unwrap().seq, 10);
    }
}
