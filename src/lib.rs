pub mod error;
pub mod client;
pub mod server;
pub mod message;
pub mod registry;
pub mod stream;
pub mod wire;
pub mod worker;

pub mod transport;

pub use crate::error::Error;
pub use crate::client::{Client, Transport};
pub use crate::server::{ConnContext, Server};
pub use crate::message::{Message, MessageHandler};
pub use crate::registry::{Handler, KnownEntry, Registry, StreamContext};
pub use crate::stream::{Connection, Stream};
pub use crate::wire::{Envelope, Meta, Priority, TraceCtx};
pub use crate::worker::{WorkerConfig, WorkerPool};
pub use adaptivemsg_macros::message_handler;
pub use adaptivemsg_macros::message;

#[macro_export]
macro_rules! submit_message_handler {
    ($t:ty) => {
        inventory::submit! {
            $crate::KnownEntry {
                register: |reg: &mut $crate::Registry| {
                    reg.register_known::<$t>();
                }
            }
        }
    };
}
