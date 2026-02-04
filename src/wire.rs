use serde::{Deserialize, Serialize};

use crate::message::Message;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub stream_id: u64,
    pub meta: Meta,
    pub msg: Box<dyn Message>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Meta {
    pub priority: Priority,
    pub trace: Option<TraceCtx>,
    pub version: u16,
}

impl Default for Meta {
    fn default() -> Self {
        Self {
            priority: Priority::Normal,
            trace: None,
            version: 1,
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum Priority {
    High,
    Normal,
    Low,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceCtx {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
}
