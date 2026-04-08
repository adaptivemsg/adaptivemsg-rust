use std::any::Any;

use serde::{Deserialize, Serialize};

use crate::codec::{CodecID, CodecImpl, DecodeTarget, Envelope};
use crate::codec_registry::register_codec;
use crate::error::Error;
use crate::message::Message;

pub const CODEC_POSTCARD: CodecID = CodecID(64);

#[allow(non_upper_case_globals)]
/// Postcard binary codec (`CodecID(64)`).
///
/// Rust-only; fastest encode/decode but not cross-language compatible.
pub const CodecPostcard: CodecID = CODEC_POSTCARD;

struct PostcardCodec;

pub(crate) fn register_builtin_codecs() -> Result<(), Error> {
    register_codec(PostcardCodec)?;
    Ok(())
}

impl CodecImpl for PostcardCodec {
    fn id(&self) -> CodecID {
        CODEC_POSTCARD
    }

    fn name(&self) -> &'static str {
        "postcard"
    }

    fn encode(&self, msg: &dyn Message) -> Result<Vec<u8>, Error> {
        let wire = msg.wire_name();
        let data = msg.encode_postcard()?;
        let env = PostcardEnvelope {
            r#type: wire,
            data: &data,
        };
        Ok(postcard::to_stdvec(&env)?)
    }

    fn decode_envelope(&self, payload: &[u8]) -> Result<Envelope, Error> {
        let env: PostcardEnvelopeOwned = postcard::from_bytes(payload)?;
        if env.r#type.is_empty() {
            return Err(Error::Codec("postcard payload missing type".to_string()));
        }
        Ok(Envelope {
            wire: env.r#type,
            body: Box::new(env.data),
        })
    }

    fn decode_into(&self, body: &dyn Any, target: &mut dyn DecodeTarget) -> Result<(), Error> {
        let data = body
            .downcast_ref::<Vec<u8>>()
            .ok_or_else(|| Error::Codec("postcard body must be byte buffer".to_string()))?;
        target.decode_postcard(data)
    }
}

#[derive(Serialize)]
struct PostcardEnvelope<'a> {
    #[serde(rename = "type")]
    r#type: &'a str,
    data: &'a [u8],
}

#[derive(Deserialize)]
struct PostcardEnvelopeOwned {
    #[serde(rename = "type")]
    r#type: String,
    data: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[crate::message]
    struct PostcardPing {
        value: u32,
    }

    #[test]
    fn postcard_round_trip() {
        let msg = PostcardPing { value: 7 };
        let codec = PostcardCodec;
        let payload = codec.encode(&msg).expect("encode");
        let env = codec.decode_envelope(&payload).expect("decode envelope");
        let raw = crate::raw_message::RawMessage {
            wire: env.wire,
            codec: CODEC_POSTCARD,
            body: env.body,
        };
        let decoded: PostcardPing = crate::raw_message::decode_raw_as(raw).expect("decode");
        assert_eq!(decoded.value, 7);
    }
}
