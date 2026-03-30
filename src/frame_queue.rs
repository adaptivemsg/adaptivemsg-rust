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
}
