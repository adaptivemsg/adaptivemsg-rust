# adaptivemsg

Minimal message-oriented library over multiplexed streams with an autoscaled worker pool.

- Transport: TCP / QUIC
- Framing: length-prefixed
- Codec: postcard
- Data model: serde
- Extensibility: typetag
- Dispatch: dyn trait handlers
- Logs: tracing

## Concepts

- **Message**: typetag-enabled trait object serialized via postcard.
- **Known message**: has a registered handler (server-side dispatch). Clients MUST use `send_recv()` for handled messages.
- **Handler reply**: `Ok(Some(msg))` sends `msg`, `Ok(None)` sends built-in `OkReply`, and `Err(e)` sends built-in `ErrorReply`.
- **Handler errors**: use `adaptivemsg::Result` (alias of `anyhow::Result`).
- **Unknown message**: delivered to the stream's recv queue.
- **Stream**: logical channel over a single connection (stream_id).

## Minimal usage

`ContextStream` derefs to `Stream`, so you can call stream methods directly (for example, `stream.id()`).

```rust
use adaptivemsg::{ContextStream, Message, MessageHandler, Registry, Result};

#[adaptivemsg::message]
struct HelloRequest {
    who: String,
}

#[adaptivemsg::message]
struct HelloReply {
    answer: String,
}

#[adaptivemsg::message_handler]
impl MessageHandler for HelloRequest {
    async fn handle(
        self: Box<Self>,
        _stream: ContextStream,
    ) -> Result<Option<Box<dyn Message>>> {
        let reply = HelloReply { answer: format!("hi, {}", self.who) };
        Ok(Some(Box::new(reply)))
    }
}

fn registry() -> Registry {
    let mut reg = Registry::new();
    reg.register_known::<HelloRequest>();
    reg
}
```

## TCP server/client sketch

```rust
use adaptivemsg::transport::tcp;

// server
let reg = registry();
let listener = tcp::listen("0.0.0.0:5555").await?;
let conn = tcp::accept(&listener, Some(reg), None).await?;

// client
let conn = tcp::connect("127.0.0.1:5555").await?;
let reply: HelloReply = conn.send_recv(HelloRequest { who: "alice".into() }).await?;

// client (via Client helper with transport prefixes)
let client = adaptivemsg::Client::new();
let conn = client.connect("tcp://127.0.0.1:5555").await?;
let reply: HelloReply = conn.send_recv(HelloRequest { who: "alice".into() }).await?;
```
