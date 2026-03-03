pub mod error;
pub mod client;
pub mod server;
pub mod message;
pub mod registry;
pub mod stream;

pub mod transport;

pub use crate::error::{Error, Result};
pub use crate::client::{Client, Transport};
pub use crate::server::Server;
pub use crate::message::{ErrorReply, Message, MessageHandler, OkReply};
pub use crate::registry::{Handler, KnownEntry, Registry};
pub use crate::stream::{Connection, HandlerStream, Stream};
pub use async_trait::async_trait;
pub use adaptivemsg_macros::message_handler;
pub use adaptivemsg_macros::message;

#[macro_export]
macro_rules! submit_message_handler {
    ($t:ty) => {
        const _: () = {
            fn register(reg: &mut $crate::Registry) {
                reg.register::<$t>();
            }
            inventory::submit! {
                $crate::KnownEntry::new(register)
            }
        };
    };
}
