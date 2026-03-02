# adaptivemsg

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

## Minimal usage

```rust
use adaptivemsg::{HandlerStream, Message, MessageHandler, Registry, Result};

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
    async fn handle(self: Box<Self>, _stream: HandlerStream) -> Result<Option<Box<dyn Message>>> {
        let reply = HelloReply {
            answer: format!("hi, {}", self.who),
        };
        Ok(Some(Box::new(reply)))
    }
}

fn registry() -> Registry {
    let mut reg = Registry::new();
    reg.register::<HelloRequest>();
    reg
}
```

## TCP server/client sketch

```rust
use adaptivemsg::transport::tcp;

// server
let reg = registry();
let listener = tcp::listen("0.0.0.0:5555").await?;
let conn = tcp::accept(&listener, Some(reg)).await?;

// client
let conn = tcp::connect("127.0.0.1:5555").await?;
let reply: HelloReply = conn.send_recv(HelloRequest { who: "alice".into() }).await?;

// client (via Client helper with transport prefixes)
let client = adaptivemsg::Client::new();
let conn = client.connect("tcp://127.0.0.1:5555").await?;
let reply: HelloReply = conn.send_recv(HelloRequest { who: "alice".into() }).await?;
```
