use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

use crate::codec::{CodecID, CodecImpl};
use crate::error::Error;

static CODEC_REGISTRY: OnceLock<RwLock<HashMap<CodecID, Arc<dyn CodecImpl>>>> = OnceLock::new();
static BUILTIN_CODECS: OnceLock<()> = OnceLock::new();

fn registry() -> &'static RwLock<HashMap<CodecID, Arc<dyn CodecImpl>>> {
    CODEC_REGISTRY.get_or_init(|| RwLock::new(HashMap::new()))
}

fn ensure_builtin_codecs() {
    BUILTIN_CODECS.get_or_init(|| {
        let _ = crate::codec_msgpack::register_builtin_codecs();
        let _ = crate::codec_postcard::register_builtin_codecs();
    });
}

/// Register a codec implementation for handshake negotiation.
///
/// Returns an error if the codec ID is zero, the name is empty, or the ID is
/// already registered.
pub fn register_codec<C>(codec: C) -> Result<(), Error>
where
    C: CodecImpl,
{
    let id = codec.id();
    if id.0 == 0 {
        return Err(Error::InvalidMessage("codec ID must be non-zero".to_string()));
    }
    if codec.name().is_empty() {
        return Err(Error::InvalidMessage("codec name must be non-empty".to_string()));
    }
    let mut guard = registry().write().unwrap();
    if guard.contains_key(&id) {
        return Err(Error::InvalidMessage("codec already registered".to_string()));
    }
    guard.insert(id, Arc::new(codec));
    Ok(())
}

/// Register a codec and panic if registration fails.
pub fn must_register_codec<C>(codec: C)
where
    C: CodecImpl,
{
    if let Err(err) = register_codec(codec) {
        panic!("{err}");
    }
}

pub fn codec_by_id(id: CodecID) -> Option<Arc<dyn CodecImpl>> {
    ensure_builtin_codecs();
    let guard = registry().read().unwrap();
    guard.get(&id).cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec_msgpack::CodecMsgpackCompact;
    use crate::codec_postcard::CodecPostcard;

    #[test]
    fn builtin_codecs_are_available() {
        assert!(codec_by_id(CodecMsgpackCompact).is_some());
        assert!(codec_by_id(crate::codec_msgpack::CodecMsgpackMap).is_some());
        assert!(codec_by_id(CodecPostcard).is_some());
    }

    #[test]
    fn unknown_codec_returns_none() {
        assert!(codec_by_id(CodecID(200)).is_none());
    }
}
