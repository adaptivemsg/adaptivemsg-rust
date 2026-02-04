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
- **Known message**: has a registered handler (server-side dispatch).
- **Unknown message**: delivered to the stream's recv queue.
- **Stream**: logical channel over a single connection (stream_id).

## Minimal usage

```rust
use std::sync::Arc;

use adaptivemsg::{Handler, Message, Registry, RequestCtx};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct HelloRequest {
    who: String,
}

#[derive(Serialize, Deserialize)]
struct HelloReply {
    answer: String,
}

#[typetag::serde]
impl Message for HelloRequest {
    fn type_name(&self) -> &'static str { "hello.request" }
}

#[typetag::serde]
impl Message for HelloReply {
    fn type_name(&self) -> &'static str { "hello.reply" }
}

struct HelloHandler;
impl Handler for HelloHandler {
    fn handle(
        &self,
        msg: Box<dyn Message>,
        _ctx: RequestCtx,
    ) -> BoxFuture<'static, Result<Option<Box<dyn Message>>, adaptivemsg::Error>> {
        Box::pin(async move {
            let req = msg.as_any().downcast_ref::<HelloRequest>().unwrap();
            let reply = HelloReply { answer: format!("hi, {}", req.who) };
            Ok(Some(Box::new(reply)))
        })
    }
}

fn registry() -> Registry {
    let mut reg = Registry::new();
    reg.register("hello.request", Arc::new(HelloHandler));
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
let stream = conn.accept_stream().await.unwrap();

// client
let conn = tcp::connect("127.0.0.1:5555").await?;
let reply: HelloReply = conn.send_recv(HelloRequest { who: "alice".into() }).await?;
```
