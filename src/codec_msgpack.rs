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
/// MessagePack compact array codec ID.
pub const CodecMsgpackCompact: CodecID = CODEC_MSGPACK_COMPACT;
#[allow(non_upper_case_globals)]
/// MessagePack map codec ID.
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde::de::{self, MapAccess, Visitor};
    use serde::ser::SerializeMap;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::fmt;

    #[crate::message]
    struct CompactCustom {
        name: String,
        inner: InnerCustom,
    }

    struct InnerCustom {
        value: String,
    }

    impl Serialize for InnerCustom {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let mut map = serializer.serialize_map(Some(1))?;
            map.serialize_entry("value", &self.value)?;
            map.end()
        }
    }

    impl<'de> Deserialize<'de> for InnerCustom {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            struct InnerVisitor;

            impl<'de> Visitor<'de> for InnerVisitor {
                type Value = InnerCustom;

                fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                    formatter.write_str("a map with a value field")
                }

                fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
                where
                    M: MapAccess<'de>,
                {
                    let mut value: Option<String> = None;
                    while let Some(key) = map.next_key::<String>()? {
                        match key.as_str() {
                            "value" => {
                                if value.is_some() {
                                    return Err(de::Error::duplicate_field("value"));
                                }
                                value = Some(map.next_value()?);
                            }
                            _ => {
                                let _: de::IgnoredAny = map.next_value()?;
                            }
                        }
                    }
                    let value = value.ok_or_else(|| de::Error::missing_field("value"))?;
                    Ok(InnerCustom { value })
                }
            }

            deserializer.deserialize_map(InnerVisitor)
        }
    }

    #[test]
    fn compact_nested_custom_fallback() {
        let msg = CompactCustom {
            name: "hello".to_string(),
            inner: InnerCustom {
                value: "ok".to_string(),
            },
        };
        let payload = msg.encode_compact().expect("encode_compact");
        let value = rmpv::decode::read_value(&mut Cursor::new(&payload)).expect("decode value");
        let mut items = match value {
            Value::Array(items) => items,
            other => panic!("expected array, got {other:?}"),
        };
        assert_eq!(items.len(), 3);
        match &items[2] {
            Value::Map(_) => {}
            other => panic!("expected nested map, got {other:?}"),
        }

        let wire_value = items.remove(0);
        let wire = match wire_value {
            Value::String(s) => s
                .as_str()
                .expect("wire name must be utf-8")
                .to_string(),
            other => panic!("expected wire string, got {other:?}"),
        };
        let raw = crate::raw_message::RawMessage {
            wire,
            codec: CODEC_MSGPACK_COMPACT,
            body: Box::new(items),
        };
        let decoded: CompactCustom =
            crate::raw_message::decode_raw_as(raw).expect("decode_raw_as");
        assert_eq!(decoded.name, "hello");
        assert_eq!(decoded.inner.value, "ok");
    }
}
