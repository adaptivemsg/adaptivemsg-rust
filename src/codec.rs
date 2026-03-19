use std::any::Any;
use std::fmt;

use rmpv::Value;

use crate::error::Error;
use crate::message::Message;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
/// Identifier for a negotiated codec.
pub struct CodecID(pub u8);

impl CodecID {
    /// Returns the registered codec name, or "unknown" if not registered.
    pub fn name(self) -> &'static str {
        if let Some(codec) = crate::codec_registry::codec_by_id(self) {
            codec.name()
        } else {
            "unknown"
        }
    }
}

impl fmt::Display for CodecID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

pub struct Envelope {
    pub wire: String,
    pub body: Box<dyn Any + Send + Sync>,
}

pub trait DecodeTarget {
    fn decode_map(&mut self, value: Value) -> Result<(), Error>;
    fn decode_compact(&mut self, values: Vec<Value>) -> Result<(), Error>;
    fn decode_postcard(&mut self, payload: &[u8]) -> Result<(), Error>;
}

/// Codec implementation used for message encode/decode.
pub trait CodecImpl: Send + Sync + 'static {
    /// Unique non-zero ID used during handshake.
    fn id(&self) -> CodecID;
    /// Short human-readable name used for debugging.
    fn name(&self) -> &'static str;
    /// Encode a message into a payload buffer.
    fn encode(&self, msg: &dyn Message) -> Result<Vec<u8>, Error>;
    /// Decode a payload into a wire name and opaque body.
    fn decode_envelope(&self, payload: &[u8]) -> Result<Envelope, Error>;
    /// Decode the opaque body into a concrete message via `DecodeTarget`.
    fn decode_into(&self, body: &dyn Any, target: &mut dyn DecodeTarget) -> Result<(), Error>;
}
