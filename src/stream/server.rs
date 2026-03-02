use std::sync::Arc;

use tracing::warn;

use crate::registry::Registry;
use crate::stream::core::{DispatchFn, Stream};
use crate::stream::Envelope;

pub(crate) fn dispatch(registry: Option<Registry>) -> DispatchFn {
    Arc::new(move |stream: Stream, envelope: Envelope| {
        let registry = registry.clone();
        Box::pin(async move {
            if let Some(registry) = registry.as_ref() {
                let type_name = envelope.msg().type_name();
                if registry.handler(type_name).is_some() {
                    let _ = stream.handler_q(envelope.into_msg()).await;
                    return;
                }
            }

            let _ = stream.inbox_q(envelope.into_msg()).await;
        })
    })
}
