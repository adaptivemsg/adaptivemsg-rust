# adaptivemsg

Typed Rust messages with server-side handler routing over multiplexed streams (no IDL).
Minimal async message library over multiplexed streams, with optional server-side handlers.

- Transport: TCP / UDS / QUIC (feature)
- Runtime: tokio for the server and the async client (`tokio` feature, on by default); a blocking client on threads (`blocking` feature), which needs no tokio
- Framing: v2 handshake + versioned header + length-prefixed payload
- Codec: MessagePack map/compact, postcard (Rust-only); default preference: postcard, compact, map, so a Go peer selects compact. Unlike Go, Rust does not yet support codecs defined outside the crate.
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
conn.close().await;
```

`close().await` delivers the messages already queued by `send`: it writes them,
tells the peer, and waits for the peer to close its side, at most 5 seconds.
Dropping the last handle to a connection, counting the streams opened from
it, closes it the same way in the background. A stream carries one
conversation at a time: `recv`, `peek_wire` and `send_recv` on the same
stream must not run concurrently, so open one stream per concurrent
request-reply. Frames are limited to 16 MiB by default; the limit is the
smaller of `Client::with_max_frame` and `Server::with_max_frame`. With
recovery, messages awaiting acknowledgement are kept in a replay buffer of
`max_replay_bytes` (32 MiB by default, twice the max frame); raise both
together. `send` keeps working while the transport is down: the message
waits in the buffer and is sent after the reconnect. While the buffer is
full, `send` waits; only a message larger than the whole buffer fails, with
`Error::FrameTooLarge`. The server closes an accepted connection that has not
completed the handshake (and, with recovery, the attach) within 2 minutes;
`Server::with_handshake_timeout` changes the limit, and `Duration::ZERO`
disables it.

`Server::serve_with_shutdown(addr, signal)` serves until `signal` completes,
then stops accepting, drops connections still in the handshake, and closes the
established ones as `close()` does, running their `on_disconnect`. A failed
accept, such as `EMFILE` when the process is out of file descriptors, does not
end `serve`; it waits briefly and accepts again. Each stream buffers up to 1024
received messages (`with_stream_queue_size` on the client or server changes
it); when a stream's buffer is full the connection stops reading, which holds
up its other streams too. `Stream::close` and `new_stream` can be called from
any thread, including one outside the tokio runtime.

For a one-shot connection:

```rust
let reply: HelloReply = am::Client::new()
    .with_recv_timeout(std::time::Duration::from_secs(5))
    .connect_once("tcp://127.0.0.1:5555")
    .send_recv(HelloRequest { who: "alice".into() })
    .await?;
```

`connect_once` does not dial; `send_recv` connects, exchanges one request and
reply, and closes the connection. `send_recv` consumes the `OnceConn`, so it
can be used only once. `with_recv_timeout` limits waiting for the reply;
without it `send_recv` waits indefinitely. Set `with_connect_timeout` to wait
for a server that is still starting. One-shot connections never use recovery.

## Blocking client

For programs that do not run tokio, the `blocking` feature adds
`adaptivemsg::blocking`, a client with the same methods as the async one,
without `.await`. With `default-features = false` nothing depends on tokio:

```toml
adaptivemsg = { version = "0.1", default-features = false, features = ["tcp", "uds", "blocking"] }
```

```rust
use adaptivemsg::blocking::Client;

let conn = Client::new().connect("uds://@adaptivemsg-hello")?;
let reply: HelloReply = conn.send_recv(HelloRequest { who: "alice".into() })?;

// One stream per thread for concurrent request-reply.
std::thread::scope(|s| {
    s.spawn(|| {
        let stream = conn.new_stream();
        let reply = stream.send_recv::<HelloReply>(HelloRequest { who: "bob".into() });
        stream.close();
        reply
    });
});
conn.close();
```

A connection runs one reader thread; `send` writes the frame on the calling
thread and `recv` decodes on it. `with_recovery` negotiates v3 as the async
client does: `send` then returns once the frame is in the replay buffer, and a
writer thread sends it, sends ACKs and heartbeats, and after a transport
failure reconnects and replays. Timeouts, `connect_once`, `close`, closing on
drop and `debug_state` behave as in the async client. The blocking client runs
no handlers: every message from the server, also one whose type has a handler,
is delivered to `recv`. It does not support QUIC.

`#[message_handler]` needs the `tokio` feature, so a message crate shared by a
blocking client and a server puts its handlers behind a feature of its own,
as the `server` feature of `examples/hello/server` does.
`examples/hello/client-blocking` is the hello client without tokio.

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
