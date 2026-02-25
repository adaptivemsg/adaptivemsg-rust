use serde::{Deserialize, Serialize};

use crate::message::Message;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    meta: Meta,
    msg: Box<dyn Message>,
}

impl Envelope {
    pub fn new(meta: Meta, msg: Box<dyn Message>) -> Self {
        Self {
            meta,
            msg,
        }
    }

    pub fn meta(&self) -> &Meta {
        &self.meta
    }

    pub fn meta_mut(&mut self) -> &mut Meta {
        &mut self.meta
    }

    pub fn msg(&self) -> &dyn Message {
        self.msg.as_ref()
    }

    pub fn into_msg(self) -> Box<dyn Message> {
        self.msg
    }

    pub fn into_parts(self) -> (Meta, Box<dyn Message>) {
        (self.meta, self.msg)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Meta {
    trace: Option<TraceCtx>,
    version: u16,
}

impl Default for Meta {
    fn default() -> Self {
        Self {
            trace: None,
            version: 1,
        }
    }
}

impl Meta {
    pub fn trace(&self) -> Option<&TraceCtx> {
        self.trace.as_ref()
    }

    pub fn set_trace(&mut self, trace: Option<TraceCtx>) {
        self.trace = trace;
    }

    pub fn version(&self) -> u16 {
        self.version
    }

    pub fn set_version(&mut self, version: u16) {
        self.version = version;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceCtx {
    trace_id: [u8; 16],
    span_id: [u8; 8],
}

impl TraceCtx {
    pub fn new(trace_id: [u8; 16], span_id: [u8; 8]) -> Self {
        Self { trace_id, span_id }
    }

    pub fn trace_id(&self) -> &[u8; 16] {
        &self.trace_id
    }

    pub fn span_id(&self) -> &[u8; 8] {
        &self.span_id
    }
}
