use std::any::Any;

use rmpv::Value;

use crate::codec::{CodecID, DecodeTarget};
use crate::codec_registry::codec_by_id;
use crate::error::Error;
use crate::message::{Message, MessageDecode};
use crate::registry::{MessageFactory, Registry};

pub struct RawMessage {
    pub wire: String,
    pub codec: CodecID,
    pub body: Box<dyn Any + Send + Sync>,
}

pub fn decode_raw_as<T>(raw: RawMessage) -> Result<T, Error>
where
    T: MessageDecode + 'static,
{
    let expected = T::wire_name_static();
    if raw.wire != expected {
        return Err(Error::TypeMismatch {
            expected: expected.to_string(),
            got: raw.wire,
        });
    }
    let codec = codec_by_id(raw.codec).ok_or(Error::UnsupportedCodec(raw.codec.0))?;
    let mut target = TypedTarget::<T>::new();
    codec.decode_into(raw.body.as_ref(), &mut target)?;
    target.into_value()
}

pub fn decode_raw_with_registry(raw: RawMessage, reg: &Registry) -> Result<Box<dyn Message>, Error> {
    let wire = raw.wire.clone();
    let factory = reg
        .message(&wire)
        .ok_or_else(|| Error::UnknownMessage(wire.clone()))?;
    let codec = codec_by_id(raw.codec).ok_or(Error::UnsupportedCodec(raw.codec.0))?;
    let mut target = FactoryTarget {
        factory: factory.as_ref(),
        msg: None,
    };
    codec.decode_into(raw.body.as_ref(), &mut target)?;
    target.into_message()
}

struct TypedTarget<T> {
    value: Option<T>,
}

impl<T> TypedTarget<T> {
    fn new() -> Self {
        Self { value: None }
    }

    fn into_value(self) -> Result<T, Error> {
        self.value
            .ok_or_else(|| Error::Codec("decode target did not produce value".to_string()))
    }
}

impl<T> DecodeTarget for TypedTarget<T>
where
    T: MessageDecode,
{
    fn decode_map(&mut self, value: Value) -> Result<(), Error> {
        self.value = Some(T::decode_map(value)?);
        Ok(())
    }

    fn decode_compact(&mut self, values: Vec<Value>) -> Result<(), Error> {
        self.value = Some(T::decode_compact(values)?);
        Ok(())
    }

    fn decode_postcard(&mut self, payload: &[u8]) -> Result<(), Error> {
        self.value = Some(T::decode_postcard(payload)?);
        Ok(())
    }
}

struct FactoryTarget<'a> {
    factory: &'a dyn MessageFactory,
    msg: Option<Box<dyn Message>>,
}

impl<'a> FactoryTarget<'a> {
    fn into_message(self) -> Result<Box<dyn Message>, Error> {
        self.msg
            .ok_or_else(|| Error::Codec("decode target did not produce value".to_string()))
    }
}

impl<'a> DecodeTarget for FactoryTarget<'a> {
    fn decode_map(&mut self, value: Value) -> Result<(), Error> {
        self.msg = Some(self.factory.decode_map(value)?);
        Ok(())
    }

    fn decode_compact(&mut self, values: Vec<Value>) -> Result<(), Error> {
        self.msg = Some(self.factory.decode_compact(values)?);
        Ok(())
    }

    fn decode_postcard(&mut self, payload: &[u8]) -> Result<(), Error> {
        self.msg = Some(self.factory.decode_postcard(payload)?);
        Ok(())
    }
}
