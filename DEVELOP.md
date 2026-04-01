# DEVELOP

Contributor-oriented design notes for adaptivemsg-rust.

This file is intentionally split into two layers:

- Practical layer: how the system behaves and where to change it.
- Reference layer: exact wire bytes, protocol constraints, and recovery details.

For public API usage, see `README.md`.

## How to Read This Document

- Start with **Quick Mental Model** for architecture and runtime flow.
- Use **Recovery At A Glance** to understand current v3 semantics.
- Use **Wire Reference** only when implementing/validating protocol compatibility.
- Use **Testing Plan** and **Code Pointers** when making changes.

## Quick Mental Model

### Core Concepts

- Message: any serde struct annotated with `#[am::message]`. Implements `Message` trait.
- Wire name: `wire_name_static()` derived from module/type or overridden by `#[am::message(ns="...", name="...")]`.
- Codec: pluggable envelope+payload codec negotiated per connection. Implements `CodecImpl` trait.
- Connection: `Arc<ConnectionInner>` — multiplexed streams over one transport; stream `0` is default.
- Stream: `Arc<StreamInner>` — FIFO per stream; `recv()` is single-consumer (enforced by `recv_active` AtomicBool).
- StreamContext: `Arc<StreamContextInner>` — per-stream state for handlers, typed user context, and task gate.
- Registry: wire-name → type + optional handler map, populated from `inventory` at `Registry::from_inventory()`.

### Runtime Flow

Client:
1. `Client::new()` builds registry from inventory.
2. `connect()` negotiates handshake via async I/O and spawns tokio tasks.

Server:
1. `Server::new()` builds registry from inventory.
2. `serve()` accepts sockets, negotiates handshake, spawns tokio tasks per connection.

Reader task (per connection):
- Reads frames via `AsyncReadExt` and routes payloads into per-stream `incoming_tx` channels.

Decoder task (per stream):
- Parses envelope to get wire name and payload.
- If a handler is registered: decode and enqueue `HandlerJob` via `handler_tx`.
- Otherwise: enqueue `RawMessage` into stream `inbox_tx`.

Handler task (per stream, if handlers exist):
- Calls `handler.handle()` and sends reply or `OkReply`.
- On handler error: sends `ErrorReply` (`handler_error`).

Recv path:
- `recv::<T>()` pulls raw inbox message, validates wire, decodes on demand.
- `recv::<Box<dyn Message>>()` uses registry dynamic decode.
- Unknown wire in dynamic recv returns `Error::UnknownMessage` (no protocol error emitted).
- `peek_wire()` peeks next wire without decoding.

Send path:
- `send()` encodes with negotiated codec into `outbound_tx` channel.
- `send_recv()` is send + wait on the same stream.

### Concurrency Model

Each connection spawns 3–5 tokio tasks:
- **Reader** (1 per connection): reads frames, routes to stream channels.
- **Writer** (1 per connection): pulls from `outbound_rx`, writes to transport.
- **Decoder** (1 per stream): parses envelopes, dispatches to handler or inbox.
- **Handler** (0–1 per stream): executes handlers if `registry.has_handlers()`.
- **Reconnect** (0–1 per client, v3 only): exponential backoff reconnect loop.

All tasks are lightweight async state machines — no OS threads blocked.

Channel hops per inbound message: 2 (reader → decoder, decoder → inbox/handler).

### Ownership Model

- `Connection = Arc<ConnectionInner>` — shared by all streams and tasks.
- `Stream = Arc<StreamInner>` — shared by user code and decoder/handler tasks.
- `StreamContext = Arc<StreamContextInner>` — handlers get context, not full I/O.
- Self-referential via `Arc::new_cyclic()` and `OnceLock<Weak<ConnectionInner>>`.
- Transport ownership: writer holds `TransportWriter`, reader holds `TransportReader`. Both are `Box<dyn AsyncRead/Write>`.

## Payload Encoding

Map codec (MessagePack, built-in, ID=2):
- Envelope: `{type: "<wire>", data: <msgpack object>}`
- `data` uses serde derive struct encoding (msgpack map).

Compact codec (MessagePack, built-in, ID=1):
- Envelope: `["<wire>", field1, field2, ...]`
- Field order is Rust struct field order (as defined in source).
- Nested structs with custom serde (e.g., map) are encoded as maps, not arrays.

Postcard codec (Rust-only, ID=64):
- Envelope: `PostcardEnvelope { type: wire, data: bytes }`
- Smallest payload size; Rust-native binary serialization.
- Not usable for cross-language interop.

## Dispatch And Lazy Decode

- Stream inbox stores `RawMessage` (wire name + codec-specific body, not yet deserialized).
- Handler dispatch is wire-name first.
- If handler exists: decode in handler path via `DecodeTarget` trait.
- Otherwise: decode later in consumer task (`recv()`).

`recv::<T>()` for concrete types does strict wire matching.
- On mismatch it sends `ErrorReply` (`protocol_error`) and closes stream.

## Errors And Protocol Signaling

Local errors (defined in `error.rs`):
- `Error::InvalidMessage`
- `Error::UnknownMessage`
- `Error::UnsupportedCodec`
- `Error::UnsupportedFrameVersion`
- `Error::NoCommonCodec`
- `Error::HandshakeRejected`
- `Error::NoCommonVersion`
- `Error::TooManyCodecs`
- `Error::RecvTimeout`
- `Error::ConcurrentRecv`
- `Error::Closed`

Remote errors:
- `ErrorReply` sent on protocol or handler failures.
- In `send_recv`, remote error maps to `Error::Remote { code, message }`.

Protocol error codes:
- `protocol_error`: wire mismatch, invalid ordering
- `codec_error`: decode/envelope failure
- `handler_error`: handler returned error

Handler errors use `anyhow::Error` (`Result<T> = std::result::Result<T, anyhow::Error>`).

## Concurrency And Lifetimes

- Exactly one in-flight `recv()`/`send_recv()` per stream (guarded by `recv_active` AtomicBool).
- `set_recv_timeout(Duration::ZERO)` disables timeout.
- `stream.close()` is local only (no on-wire close frame).
- Stream context uses `Arc<dyn Any + Send + Sync>` for type-erased storage.
- `new_task()` spawns one background tokio task per stream (guarded by `handler_task_active`).

## Current Design Gaps

- Registry snapshot is fixed at client/server construction time.
- No explicit on-wire stream close.
- `max_frame == 0` rejects all non-empty frames.
- Recovery includes heartbeat knobs, but no OS keepalive integration.
- Only one `unsafe` block: `Box::into_raw`/`Box::from_raw` in message downcast (type-checked).

## Recovery At A Glance

### Scope

Implemented scope is transport-only breakage while both processes stay alive.

In scope:
- TCP/UDS disconnect and reconnect
- logical connection continuity
- replay of unacknowledged frames

Out of scope:
- client process crash/restart
- server process crash/restart
- durable replay after process death

### Public Model

- Public handle remains `Connection` (= `Arc<ConnectionInner>`) from `client.connect()`.
- `Connection` is logical and long-lived.
- Attached transport (`TransportReader`/`TransportWriter`) is replaceable.
- Stream IDs are stream identity, not connection identity.

### Identity And Ownership

- Internal stable identity: `connection_id` (`RecoveryToken = [u8; 16]`).
- Secret for resume authorization: `resume_secret` (`RecoveryToken`).
- Server issues identity+secret; client does not choose them.
- Client owns reconnect attempts.
- Server validates resume and reattaches to existing logical connection.
- Exactly one active transport per logical connection.

### Negotiation Model

- Recovery is opt-in via protocol version.
- `v2`: legacy behavior.
- `v3`: recovery/replay enabled.
- If peer cannot do `v3`, connection continues with `v2`.

### Heartbeat And Liveness (Current)

Current implementation is Ping-only with read-deadline liveness.

Writer side:
- Sends periodic `PING` at negotiated heartbeat interval when idle.

Reader side:
- Arms read deadline to negotiated heartbeat timeout:
  - before each blocking read
  - after each successful read

Timeout behavior:
- If no inbound frame arrives before deadline, transport is treated as dead.
- Dead transport triggers detach + reconnect flow.

Any valid inbound frame refreshes liveness:
- data frame
- `ACK`
- `PING`

`PONG` is intentionally not part of current protocol/runtime behavior.

### Replay Semantics

Minimum replay design in use:
- one sequence space per direction per connection
- cumulative ACK
- replay retention of unacknowledged outbound data frames
- reconnect + resume based on `connection_id`/`resume_secret`

Send acceptance semantics:
- `send()` means local acceptance into logical outbound path, not peer receipt.

### Detach Lifecycle

On transport failure:
- mark detached
- keep streams, contexts, and replay state alive
- wake client reconnect loop

On server side while detached:
- logical connection stored in `RecoveryRegistry` (`Arc<Mutex<HashMap>>`)
- per-connection TTL timer controls expiry

On successful resume:
- new transport replaces old transport
- replay resumes from peer last received sequence

On TTL expiry:
- logical connection is permanently closed
- resources and replay retention are freed

## Wire Reference (v2 and v3)

All integer fields are big-endian unless explicitly stated.

### Shared Handshake Envelope

Client -> server request header (12 bytes):

| Offset | Size | Field |
|---|---:|---|
| 0..1 | 2 | magic = `"AM"` |
| 2 | 1 | version (`2` or `3`) |
| 3 | 1 | codec_count |
| 4 | 1 | flags (currently `0`) |
| 5..7 | 3 | reserved (`0`) |
| 8..11 | 4 | max_frame (`u32`) |

Immediately after header:
- codec list length = `codec_count`
- one-byte `CodecID` entries

Server -> client response header (12 bytes):

| Offset | Size | Field |
|---|---:|---|
| 0..1 | 2 | magic = `"AM"` |
| 2 | 1 | accept (`1` accepted, `0` rejected) |
| 3 | 1 | version |
| 4 | 1 | selected_codec |
| 5 | 1 | flags (currently `0`) |
| 6..7 | 2 | reserved (`0`) |
| 8..11 | 4 | negotiated_max_frame (`u32`) |

### v2 Frame Format

v2 frame header is 10 bytes:

| Offset | Size | Field |
|---|---:|---|
| 0 | 1 | version (`2`) |
| 1 | 1 | flags (currently unused) |
| 2..5 | 4 | stream_id (`u32`) |
| 6..9 | 4 | payload_len (`u32`) |

Payload bytes immediately follow the header.

### v3 Frame Format

v3 frame header is 18 bytes:

| Offset | Size | Field |
|---|---:|---|
| 0 | 1 | version (`3`) |
| 1 | 1 | flags (currently unused) |
| 2..5 | 4 | stream_id (`u32`) |
| 6..9 | 4 | payload_len (`u32`) |
| 10..17 | 8 | seq (`u64`) |

Payload bytes immediately follow the header.

### v3 Attach/Resume Format

Attach request (44 bytes):

| Offset | Size | Field |
|---|---:|---|
| 0 | 1 | mode (`1` new, `2` resume) |
| 1..3 | 3 | reserved (`0`) |
| 4..19 | 16 | connection_id |
| 20..35 | 16 | resume_secret |
| 36..43 | 8 | last_recv_seq (`u64`) |

Attach response (60 bytes):

| Offset | Size | Field |
|---|---:|---|
| 0 | 1 | status (`1` ok, `2` rejected) |
| 1..3 | 3 | reserved (`0`) |
| 4..19 | 16 | connection_id |
| 20..35 | 16 | resume_secret |
| 36..43 | 8 | last_recv_seq (`u64`) |
| 44..47 | 4 | ack_every (`u32`) |
| 48..51 | 4 | ack_delay_ms (`u32`) |
| 52..55 | 4 | heartbeat_interval_ms (`u32`) |
| 56..59 | 4 | heartbeat_timeout_ms (`u32`) |

### v3 Control Stream Format

Reserved stream id:
- `CONTROL_STREAM_ID = u32::MAX` (`0xFFFFFFFF`)

Control payloads:
- ACK (`type=1`, total 9 bytes)
  - byte `0`: `1`
  - bytes `1..8`: `last_recv_seq` (`u64`)
- PING (`type=2`, total 1 byte)
  - byte `0`: `2`

Control frames are not replayed and do not consume sequence numbers.

### Validation Constraints

- handshake version must be supported (`2` or `3`)
- `codec_count` must be in `[1, 16]`
- frame `payload_len` must be <= negotiated `max_frame`
- negotiated recovery values must satisfy:
  - `ack_every > 0`
  - `ack_delay_ms > 0`
  - `heartbeat_interval_ms > 0`
  - `heartbeat_timeout_ms >= 2 * heartbeat_interval_ms`
- unknown control type is invalid

### Interop Checklist (Non-Rust Peer)

- follow exact byte layouts (including reserved bytes)
- encode/decode all integers big-endian
- implement v3 `seq` at bytes `10..17` of frame header
- encode/decode ACK as exact 9-byte payload
- encode/decode PING as exact 1-byte payload
- treat heartbeat timeout as transport failure and trigger resume path
- do not rely on PONG semantics

## Detailed Recovery Implementation Shape

### Internal State Summary

Recovery configuration is role-specific:

- `ClientRecoveryOptions`
  - `enable: bool`
  - `reconnect_min_backoff: Duration`
  - `reconnect_max_backoff: Duration`
  - `max_replay_bytes: i64`
- `ServerRecoveryOptions`
  - `enable: bool`
  - `detached_ttl: Duration`
  - `max_replay_bytes: i64`
  - `ack_delay: Duration`
  - `ack_every: u32`
  - `heartbeat_interval: Duration`
  - `heartbeat_timeout: Duration`

Connection transport state includes:
- `outbound_tx/rx: mpsc::channel<OutboundFrame>` (bounded, `STREAM_QUEUE_SIZE=1024`)
- `writer_cmd_tx/rx: mpsc::unbounded_channel<WriterCommand>` (attach/detach commands)
- `send_notify: Notify` (wake recovery writer on new frame)
- `transport_gen: AtomicU64`
- `recovery: Option<RecoveryState>`

Replay-related state:
- `next_send_seq: AtomicU64`
- `ReplayBuffer` — frame retention keyed by sequence
- `live_tx/rx: mpsc::unbounded_channel` — new frames queued during detach
- resume deque populated on reconnect

`RecoveryState` includes:
- `connection_id: RecoveryToken` (`[u8; 16]`)
- `resume_secret: RecoveryToken`
- negotiated timing/batching (`ack_delay`, `ack_every`, heartbeat values)
- `last_recv_seq: AtomicU64`
- `last_acked_seq: AtomicU64`
- `ack_pending: AtomicU32`
- `ack_due: AtomicBool`
- atomic mirrors for heartbeat/read-deadline hot path
- detached expiry timer
- client reconnect guards/parameters

Server detached table:
- `RecoveryRegistry = Arc<Mutex<HashMap<RecoveryToken, Connection>>>`

### Attach/Resume Rules

Client attach request carries:
- mode (`new`/`resume`)
- `connection_id` (zero for `new`)
- `resume_secret` (zero for `new`)
- client last received server sequence

Server attach response carries:
- status (`ok`/`rejected`)
- assigned or confirmed identity + secret
- server last received client sequence
- authoritative shared recovery parameters

Rules:
- first connect: client sends `new`; server issues identity+secret
- reconnect: client sends `resume`; server validates secret + detached entry
- rejection: logical connection permanently closes

### Delivery, ACK, And Replay Rules

Outbound data send:
- allocate next connection sequence (`next_send_seq.fetch_add`)
- encode frame
- retain frame in replay storage
- enqueue frame for writer

Inbound data receive:
- if `seq == last_recv_seq + 1`: accept and deliver
- if `seq <= last_recv_seq`: drop duplicate
- if `seq > last_recv_seq + 1`: protocol error

ACK policy:
- cumulative ACK, not per-message ACK
- send ACK after `ack_delay` or every `ack_every` accepted data frames

Replay on resume:
- each side reports last fully received sequence
- sender replays retained frames with `seq > peer_last_recv_seq`
- replayed frames preserve original encoded bytes and sequence

### Runtime API Semantics

- `recv()` can continue waiting across reconnect while logical connection survives.
- `send()` still means local acceptance only.
- `send_recv()` continuity comes from frame replay (not API-level resend tricks).
- `wait_closed()` and `Error::Closed` refer to permanent logical closure.

## Protocol v2 vs v3 Performance Snapshot

Measured with Rust 1.75+ on linux/amd64 (Xeon E5-2680 v4 @ 2.40GHz):

```bash
AM_BENCH_ITERS=1000 AM_BENCH_RUNS=3 cargo test --release -- --ignored --nocapture 'benchmark_protocol'
```

Latest measured result (MsgpackCompact codec, for Go comparison):
- v2: `210,293 ns/op`
- v3: `226,314 ns/op`

With native Postcard codec:
- v2: `131,734 ns/op`
- v3: `223,112 ns/op`

Delta vs v2 baseline (MsgpackCompact):
- latency: `+7.6%`

Interpretation:
- v3 has measurable steady-state overhead from replay/ack bookkeeping.
- v3 provides transport-break resilience unavailable in v2.
- Postcard codec provides significant encoding speedup over MsgpackCompact.

## Testing Plan

### Unit Tests

- Frame header roundtrip (v2, v3)
- Codec roundtrip (msgpack compact, map, postcard)
- Handshake negotiation (v2/v3, codec selection, rejection cases)
- Attach/resume encoding/validation
- Replay bookkeeping (monotonic sequence, ACK truncation, byte accounting)
- Registry (inventory registration, handler/message lookup)
- Debug counters and failure tracking
- Transport address parsing (TCP, UDS, abstract sockets)

### Integration Tests

Recovery disabled:
- behavior unchanged on disconnect

Recovery enabled:
- reconnect resumes same logical connection
- `recv()` survives reconnect
- queued sends during detach are delivered after resume
- reply replay works when reply was lost after request processing
- duplicate replay is not redelivered
- detached TTL expiry causes permanent close
- stale transport replaced on successful resume

### Benchmarks

Protocol version benchmarks (`protocol_version_bench_test.rs`):
- V2 SendRecv (Postcard + MsgpackCompact)
- V3 Recovery SendRecv (Postcard + MsgpackCompact)

Recovery micro-benchmarks (`recovery_runtime_bench_test.rs`):
- ACK wait computation
- Control frame extraction
- Combined wait duration selection

### Compatibility And Performance

- compare recovery off vs on:
  - throughput small/medium payloads
  - replay buffer memory growth
- compatibility matrix:
  - new client vs legacy server
  - legacy client vs new server

## Code Pointers

- `connection.rs`: frame I/O, stream lifecycle, reader/writer tasks, transport attach/detach
- `protocol.rs`: handshake and version negotiation
- `frame.rs`: frame header build/parse
- `stream.rs`: send/recv, timeout, inbox, protocol errors
- `codec.rs`: codec trait and envelope contracts
- `codec_registry.rs`: codec registration/lookup
- `codec_msgpack.rs`: map/compact MessagePack codecs
- `codec_postcard.rs`: Rust-native postcard codec
- `registry.rs`: wire registry, handler registration, inventory integration
- `message.rs`: Message trait, wire name, built-in types (OkReply, ErrorReply)
- `raw_message.rs`: lazy envelope decode and type-specific decode
- `context.rs`: per-stream context and handler task gating
- `debug.rs`: observability counters, failure tracking, diagnostic snapshots
- `recovery.rs`: recovery configuration and state
- `recovery_protocol.rs`: attach/resume wire protocol, control frames (ACK, PING)
- `recovery_runtime.rs`: recovery reader/writer task implementations
- `replay.rs`: unacknowledged frame retention and replay
- `frame_queue.rs`: thread-safe frame deque for replay/resume queues
- `type_info.rs`: cached type metadata (wire names)
- `error.rs`: error types (thiserror enum)
- `transport/tcp.rs`: TCP dial/listen/accept
- `transport/uds.rs`: UDS dial/listen/accept (+ abstract sockets on Linux)
- `transport/quic.rs`: QUIC transport (optional feature)
- `lib.rs`: module organization and public re-exports
