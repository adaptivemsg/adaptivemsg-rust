use std::sync::Arc;

use crate::stream::core::{DispatchFn, Stream};
use crate::stream::Envelope;

pub(crate) fn dispatch() -> DispatchFn {
    Arc::new(|stream: Stream, envelope: Envelope| {
        Box::pin(async move {
            let _ = stream.inbox_q(envelope.into_msg()).await;
        })
    })
}
