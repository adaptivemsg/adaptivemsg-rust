mod core;
pub(crate) mod client;
pub(crate) mod server;

pub use core::{Connection, Stream};
pub(crate) use core::{ConnectionInner, Envelope};
