# adaptivemsg

Typed Rust messages with server-side handler routing over multiplexed streams (no IDL).
Minimal async message library over multiplexed streams, with optional server-side handlers.

- Transport: TCP / UDS / QUIC (feature)
- Framing: versioned header + length-prefixed payload
- Codec: postcard
- Data model: serde
- Extensibility: typetag
- Dispatch: dyn trait handlers
- Logs: tracing

## Concepts

- **Message**: typetag-enabled trait object serialized via postcard.
- **Known message**: has a registered handler (server-side dispatch). Clients MUST use `send_recv()` for handled messages.
- **Handler reply**: `Ok(Some(msg))` sends `msg`, `Ok(None)` sends `OkReply`, and `Err(e)` sends `ErrorReply`.
- **Handler stream**: handlers get `HandlerStream` (id + context + `new_task`), not full I/O.
- **Unknown message**: delivered to the stream's recv queue.
- **Stream**: logical channel over a single connection (stream_id).

Tip: for brevity in local code, you can alias the crate, e.g. `use adaptivemsg as am;`.

## Minimal usage

```rust
use adaptivemsg as am;
use am::{HandlerStream, Message, MessageHandler, Result};

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
    async fn handle(self: Box<Self>, _stream: HandlerStream) -> Result<Option<Box<dyn Message>>> {
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
