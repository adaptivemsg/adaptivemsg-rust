//! Adaptive message protocol runtime.
//!
//! Define messages with `#[message]`, optionally attach handlers with
//! `#[message_handler]`, then use `Server` to accept connections and `Client`
//! to connect and exchange messages.
//!
//! Built-in codecs include `CodecMsgpackCompact`, `CodecMsgpackMap`, and
//! `CodecPostcard`; register custom codecs with `RegisterCodec`.

extern crate self as adaptivemsg;

mod codec;
mod codec_msgpack;
mod codec_postcard;
mod codec_registry;
mod connection;
mod context;
mod error;
mod frame;
mod message;
mod protocol;
mod raw_message;
mod registry;
mod server;
mod stream;
mod transport;
mod client;
mod type_info;

pub use crate::codec::{CodecID, CodecImpl};
pub use crate::codec_msgpack::{CodecMsgpackCompact, CodecMsgpackMap};
pub use crate::codec_postcard::CodecPostcard;
pub use crate::codec_registry::{must_register_codec as MustRegisterCodec, register_codec as RegisterCodec};
pub use crate::connection::{Connection, Netconn};
pub use crate::context::{Context, StreamContext};
pub use crate::error::{Error, Result};
pub use crate::message::{ErrorReply, Message, MessageHandler, OkReply};
pub use crate::registry::Registry;
pub use crate::server::Server;
pub use crate::stream::Stream;
pub use crate::client::Client;

#[doc(hidden)]
pub use async_trait::async_trait;

#[doc(hidden)]
pub mod __private {
    pub use rmp_serde;
    pub use rmpv;
    pub use postcard;
    pub use crate::message::MessageDecode;
    pub use crate::registry::{KnownEntry, KnownMessageEntry, Registry};
}

/// Define a message type with encode/decode support and a wire name.
pub use adaptivemsg_macros::message;
/// Define a server-side handler for a message type.
pub use adaptivemsg_macros::message_handler;

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
