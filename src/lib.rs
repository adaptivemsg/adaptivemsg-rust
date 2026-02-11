pub mod error;
pub mod client;
pub mod server;
pub mod message;
pub mod registry;
pub mod stream;
pub mod wire;
pub mod worker;

pub mod transport;

pub use crate::error::{Error, Result};
pub use crate::client::{Client, Transport};
pub use crate::server::Server;
pub use crate::message::{ErrorReply, Message, MessageHandler, OkReply};
pub use crate::registry::{Handler, KnownEntry, Registry, ContextStream};
pub use crate::stream::{Connection, Stream};
pub use crate::wire::{Envelope, Meta, Priority, TraceCtx};
pub use crate::worker::{WorkerConfig, WorkerPool};
pub use async_trait::async_trait;
pub use adaptivemsg_macros::message_handler;
pub use adaptivemsg_macros::message;

#[macro_export]
macro_rules! submit_message_handler {
    ($t:ty) => {
        inventory::submit! {
            $crate::KnownEntry::new(|reg: &mut $crate::Registry| {
                reg.register_known::<$t>();
            })
        }
    };
}
