use std::any::Any;
use std::borrow::Cow;
use std::io::Cursor;

use rmpv::Value;
use serde::Deserialize;

use crate::codec::{CodecID, CodecImpl, DecodeTarget, Envelope};
use crate::codec_registry::register_codec;
use crate::error::Error;
use crate::message::Message;

pub const CODEC_MSGPACK_COMPACT: CodecID = CodecID(1);
pub const CODEC_MSGPACK_MAP: CodecID = CodecID(2);

#[allow(non_upper_case_globals)]
pub const CodecMsgpackCompact: CodecID = CODEC_MSGPACK_COMPACT;
#[allow(non_upper_case_globals)]
pub const CodecMsgpackMap: CodecID = CODEC_MSGPACK_MAP;

struct MsgpackMapCodec;
struct MsgpackCompactCodec;

pub(crate) fn register_builtin_codecs() -> Result<(), Error> {
    register_codec(MsgpackMapCodec)?;
    register_codec(MsgpackCompactCodec)?;
    Ok(())
}

impl CodecImpl for MsgpackMapCodec {
    fn id(&self) -> CodecID {
        CODEC_MSGPACK_MAP
    }

    fn name(&self) -> &'static str {
        "map"
    }

    fn encode(&self, msg: &dyn Message) -> Result<Vec<u8>, Error> {
        msg.encode_map()
    }

    fn decode_envelope(&self, payload: &[u8]) -> Result<Envelope, Error> {
        decode_map_envelope(payload)
    }

    fn decode_into(&self, body: &dyn Any, target: &mut dyn DecodeTarget) -> Result<(), Error> {
        let value = body
            .downcast_ref::<Value>()
            .ok_or_else(|| Error::Codec("map body must be rmpv value".to_string()))?;
        target.decode_map(value.clone())
    }
}

impl CodecImpl for MsgpackCompactCodec {
    fn id(&self) -> CodecID {
        CODEC_MSGPACK_COMPACT
    }

    fn name(&self) -> &'static str {
        "compact"
    }

    fn encode(&self, msg: &dyn Message) -> Result<Vec<u8>, Error> {
        msg.encode_compact()
    }

    fn decode_envelope(&self, payload: &[u8]) -> Result<Envelope, Error> {
        decode_compact_envelope(payload)
    }

    fn decode_into(&self, body: &dyn Any, target: &mut dyn DecodeTarget) -> Result<(), Error> {
        let values = body
            .downcast_ref::<Vec<Value>>()
            .ok_or_else(|| Error::Codec("compact body must be value array".to_string()))?;
        target.decode_compact(values.clone())
    }
}

#[derive(Deserialize)]
struct MapEnvelope<'a> {
    #[serde(rename = "type")]
    #[serde(borrow)]
    r#type: Cow<'a, str>,
    data: Value,
}

fn decode_map_envelope(payload: &[u8]) -> Result<Envelope, Error> {
    let env: MapEnvelope<'_> = rmp_serde::from_slice(payload)?;
    if env.r#type.is_empty() {
        return Err(Error::Codec("map payload missing type".to_string()));
    }
    Ok(Envelope {
        wire: env.r#type.into_owned(),
        body: Box::new(env.data),
    })
}

fn decode_compact_envelope(payload: &[u8]) -> Result<Envelope, Error> {
    let mut cursor = Cursor::new(payload);
    let value = rmpv::decode::read_value(&mut cursor)?;
    let values = match value {
        Value::Array(values) if !values.is_empty() => values,
        _ => {
            return Err(Error::Codec(
                "compact payload must be a non-empty array".to_string(),
            ))
        }
    };
    let mut iter = values.into_iter();
    let name_value = iter.next().unwrap();
    let name = match &name_value {
        Value::String(s) => s
            .as_str()
            .ok_or_else(|| Error::Codec("compact message name must be utf-8".to_string()))?,
        _ => {
            return Err(Error::Codec(
                "compact message name must be a string".to_string(),
            ))
        }
    };
    let values = iter.collect::<Vec<_>>();
    Ok(Envelope {
        wire: name.to_string(),
        body: Box::new(values),
    })
}
