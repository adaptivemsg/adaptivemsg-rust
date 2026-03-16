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
