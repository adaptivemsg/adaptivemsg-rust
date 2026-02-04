use std::any::Any;

use serde::{Deserialize, Serialize};

#[typetag::serde(tag = "type")]
pub trait Message: Any + Send + Sync + 'static {
    fn type_name(&self) -> &'static str;
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub trait MessageExt {
    fn downcast_ref<T: Any>(&self) -> Option<&T>;
}

impl MessageExt for dyn Message {
    fn downcast_ref<T: Any>(&self) -> Option<&T> {
        self.as_any().downcast_ref::<T>()
    }
}

// Helper to force serde to see trait object implementations.
#[allow(dead_code)]
fn _serde_compile_guard<T: Serialize + for<'de> Deserialize<'de>>() {}
