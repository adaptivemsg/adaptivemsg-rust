extern crate self as adaptivemsg;

mod error;
mod client;
mod server;
mod message;
mod registry;
mod stream;
mod transport;

pub use crate::error::{Error, Result};
pub use crate::client::Client;
pub use crate::server::Server;
pub use crate::message::{ErrorReply, Message, MessageHandler, OkReply};
pub use crate::stream::{Codec, Connection, Context, Netconn, Stream, StreamContext};
#[doc(hidden)]
pub use async_trait::async_trait;
#[doc(hidden)]
pub mod __private {
    pub use rmp_serde;
    pub use rmpv;
    pub use crate::message::MessageDecode;
    pub use crate::registry::{KnownEntry, KnownMessageEntry, Registry};
}
pub use adaptivemsg_macros::message_handler;
pub use adaptivemsg_macros::message;

#[doc(hidden)]
#[macro_export]
macro_rules! submit_message_handler {
    ($t:ty) => {
        const _: () = {
            fn register(reg: &mut $crate::__private::Registry) {
                reg.register::<$t>();
            }
            inventory::submit! {
                $crate::__private::KnownEntry::new(register)
            }
        };
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! submit_message {
    ($t:ty) => {
        const _: () = {
            fn register(reg: &mut $crate::__private::Registry) {
                reg.register_message::<$t>();
            }
            inventory::submit! {
                $crate::__private::KnownMessageEntry::new(register)
            }
        };
    };
}
