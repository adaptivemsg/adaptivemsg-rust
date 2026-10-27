use std::any::Any;

use serde::Deserialize;

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
        // This is postcard's encoding of the envelope {type: &str, data:
        // &[u8]}: each field's length as a varint, then its bytes.
        let mut out = Vec::with_capacity(2 * MAX_LEN_VARINT + wire.len() + data.len());
        push_len_varint(&mut out, wire.len());
        out.extend_from_slice(wire.as_bytes());
        push_len_varint(&mut out, data.len());
        out.extend_from_slice(&data);
        Ok(out)
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

    fn decode_into(
        &self,
        body: Box<dyn Any + Send + Sync>,
        target: &mut dyn DecodeTarget,
    ) -> Result<(), Error> {
        let data = body
            .downcast::<Vec<u8>>()
            .map_err(|_| Error::Codec("postcard body must be byte buffer".to_string()))?;
        target.decode_postcard(&data)
    }
}

/// The longest varint of a 64-bit length.
const MAX_LEN_VARINT: usize = 10;

fn push_len_varint(out: &mut Vec<u8>, mut len: usize) {
    while len >= 0x80 {
        out.push(len as u8 | 0x80);
        len >>= 7;
    }
    out.push(len as u8);
}

#[derive(Deserialize)]
struct PostcardEnvelopeOwned {
    #[serde(rename = "type")]
    r#type: String,
    data: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

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
            codec: crate::codec_registry::codec_by_id(CODEC_POSTCARD).expect("postcard registered"),
            body: env.body,
        };
        let decoded: PostcardPing = crate::raw_message::decode_raw_as(raw).expect("decode");
        assert_eq!(decoded.value, 7);
    }

    #[crate::message]
    struct PostcardBlob {
        data: Vec<u8>,
    }

    /// The envelope is written by hand; it must stay what postcard writes.
    #[test]
    fn envelope_matches_postcard() {
        #[derive(Serialize)]
        struct Envelope<'a> {
            r#type: &'a str,
            data: &'a [u8],
        }
        for len in [0, 1, 127, 128, 300, 20_000] {
            let msg = PostcardBlob { data: vec![7; len] };
            let data = msg.encode_postcard().expect("encode message");
            let want = postcard::to_stdvec(&Envelope {
                r#type: msg.wire_name(),
                data: &data,
            })
            .expect("encode envelope");
            assert_eq!(
                PostcardCodec.encode(&msg).expect("encode"),
                want,
                "data length {len}"
            );
        }
    }
}
