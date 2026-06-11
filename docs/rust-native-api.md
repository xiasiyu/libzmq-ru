# Rust Native API

The `libzmq` crate exposes a safe Rust API backed by the same `libzmq-core` implementation used by the C ABI facade.

## Main Types

- `Context`: owns context lifecycle and creates sockets.
- `Socket`: exposes bind/connect, send/recv, options, subscriptions, and draft helpers.
- `Message`: owns message payload, multipart state, routing id, group, and metadata.
- `SocketType`: typed socket kind enum matching libzmq socket constants.

## Basic Example

```rust
use libzmq::{Context, SocketType};

let ctx = Context::new()?;
let server = ctx.socket(SocketType::Pair)?;
let client = ctx.socket(SocketType::Pair)?;

server.bind("inproc://example")?;
client.connect("inproc://example")?;

client.send("hello")?;
let message = server.recv()?;
assert_eq!(message.data(), b"hello");
# Ok::<(), libzmq::Error>(())
```

## Feature Flags

- `curve`: CURVE constants/API surface.
- `gssapi`: platform GSSAPI bindings and real token/wrap code.
- `norm`: sys-level NORM bindings, option constants, and a PUB/SUB single-frame data-object transport smoke path.
- `wss`: TLS-over-WebSocket support through rustls.
- `sodium`: optional libsodium acceleration for CURVE message paths.

## Current Limitations

The native API is not a final stable Rust API contract yet. It is suitable for testing and migration work, but the release candidate must freeze names, error mapping, and documentation after Phase 10 and Phase 11 are complete.
