# adaptivemsg

Typed Rust messages with server-side handler routing over multiplexed streams (no IDL).
Minimal async message library over multiplexed streams, with optional server-side handlers.

- Transport: TCP / UDS / QUIC (feature)
- Framing: v2 handshake + versioned header + length-prefixed payload
- Codec: MessagePack map/compact, postcard (Rust-only), pluggable codecs (compact-first for shared codecs)
- Data model: serde
- Dispatch: registry-driven handlers
- Logs: tracing

## Concepts

- **Message**: a serde struct annotated with `#[am::message]`.
- **Wire name**: derived from module/type or overridden by attributes.
- **Known message**: has a registered handler (server-side dispatch). Clients MUST use `send_recv()` for handled messages.
- **Handler reply**: `Ok(Some(msg))` sends `msg`, `Ok(None)` sends `OkReply`, and `Err(e)` sends `ErrorReply`.
- **Handler context**: handlers get `StreamContext` (context + `new_task`), not full I/O.
- **Unknown message**: delivered to the stream's recv queue and decoded on demand.
- **Registry**: `#[am::message_handler]` registers handler + message. Use `#[am::message(register)]` to opt into dynamic receive.
- **Lazy decode**: envelopes are queued; payload decode happens in `recv()`.
- **Compact codec**: positional arrays for fields; nested structs encode as arrays unless they implement custom serde encoding, in which case their custom representation (often map) is used.

Tip: for brevity in local code, you can alias the crate, e.g. `use adaptivemsg as am;`.

## Minimal usage

```rust
use adaptivemsg as am;
use am::{Message, MessageHandler, Result, StreamContext};

#[am::message]
struct HelloRequest {
    who: String,
}

#[am::message]
struct HelloReply {
    answer: String,
}

#[am::message_handler]
impl MessageHandler for HelloRequest {
    async fn handle(self: Box<Self>, _stream_ctx: StreamContext) -> Result<Option<Box<dyn Message>>> {
        let reply = HelloReply {
            answer: format!("hi, {}", self.who),
        };
        Ok(Some(Box::new(reply)))
    }
}

```

## TCP server/client sketch

```rust
use adaptivemsg as am;

// server
am::Server::new().serve("tcp://0.0.0.0:5555").await?;

// client
let client = am::Client::new();
let conn = client.connect("tcp://127.0.0.1:5555").await?;
let reply: HelloReply = conn.send_recv(HelloRequest { who: "alice".into() }).await?;
```

## Code generation (amgen-rs)

Install:

```bash
cargo install adaptivemsg-amgen
```

Run:

```bash
amgen-rs --in api/<service>/message.rs
```

This writes `<input>.go` alongside the Rust source and a `go.mod` at the repo root.
