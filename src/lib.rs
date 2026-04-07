//! Adaptive message protocol runtime.
//!
//! Define messages with `#[message]`, optionally attach handlers with
//! `#[message_handler]`, then use `Server` to accept connections and `Client`
//! to connect and exchange messages.
//!
//! Built-in codecs include `CodecMsgpackCompact`, `CodecMsgpackMap`, and
//! `CodecPostcard`; register custom codecs with `RegisterCodec`.

extern crate self as adaptivemsg;

mod client;
mod codec;
mod codec_msgpack;
mod codec_postcard;
mod codec_registry;
mod connection;
mod context;
pub mod debug;
mod error;
mod frame;
mod frame_queue;
mod message;
mod once;
mod protocol;
mod raw_message;
mod recovery;
mod recovery_protocol;
mod registry;
mod replay;
mod server;
mod stream;
mod transport;
mod type_info;

#[cfg(test)]
mod protocol_version_bench_test;
#[cfg(test)]
mod recovery_integration_test;
#[cfg(test)]
mod recovery_runtime_bench_test;
#[cfg(test)]
mod scaling_bench_test;

pub use crate::client::Client;
pub use crate::codec::{CodecID, CodecImpl};
pub use crate::codec_msgpack::{CodecMsgpackCompact, CodecMsgpackMap};
pub use crate::codec_postcard::CodecPostcard;
pub use crate::codec_registry::{
    must_register_codec as MustRegisterCodec, register_codec as RegisterCodec,
};
pub use crate::connection::{Connection, Netconn};
pub use crate::context::{Context, StreamContext};
pub use crate::debug::{
    ConnectionCounters, ConnectionDebugState, RecoveryDebugState, StreamCounters, StreamDebugState,
};
pub use crate::error::{Error, Result};
pub use crate::message::{ErrorReply, Message, MessageHandler, OkReply};
pub use crate::once::{once, OnceConn};
pub use crate::recovery::{ClientRecoveryOptions, ServerRecoveryOptions};
pub use crate::registry::Registry;
pub use crate::server::Server;
pub use crate::stream::Stream;

#[doc(hidden)]
pub use async_trait::async_trait;

#[doc(hidden)]
pub mod __private {
    pub use crate::message::MessageDecode;
    pub use crate::registry::{KnownEntry, KnownMessageEntry, Registry};
    pub use postcard;
    pub use rmp_serde;
    pub use rmpv;
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
