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
                if let Some(handler) = registry.handler(type_name) {
                    let msg = envelope.into_msg();
                    match handler.handle(msg, stream.clone()).await {
                        Ok(Some(reply)) => {
                            let _ = stream.send_boxed(reply).await;
                        }
                        Ok(None) => {
                            let _ = stream
                                .send_boxed(Box::new(crate::message::OkReply))
                                .await;
                        }
                        Err(err) => {
                            warn!("handler error: {err}");
                            let _ = stream
                                .send_boxed(
                                    Box::new(crate::message::ErrorReply::new(
                                        "handler_error",
                                        err.to_string(),
                                    )),
                                )
                                .await;
                        }
                    }
                    return;
                }
            }

            let _ = stream.inbox_q(envelope.into_msg()).await;
        })
    })
}
