# Implementation Plan

This project is a full Rust rewrite of `libzmq`. The target is not a partial MVP: stable, deprecated, and draft C APIs remain supported, and a Rust-native API is added on top of the same core.

## Architecture

```text
include/zmq.h compatible C ABI
  -> crates/libzmq-ffi
    -> crates/libzmq-core
    <- crates/libzmq
Rust-native API

crates/libzmq-sys isolates OS and third-party FFI.
```

## Phases

1. Workspace, ABI facade, Rust API facade, baseline docs.
2. Error, errno, version, context, message ABI.
3. Message internals, metadata, options, pipe, ypipe, mailbox.
4. Inproc transport and core socket patterns.
5. Stable sockets: PAIR, PUB, SUB, REQ, REP, DEALER, ROUTER, PULL, PUSH, XPUB, XSUB, STREAM.
6. Draft sockets: SERVER, CLIENT, RADIO, DISH, GATHER, SCATTER, DGRAM, PEER, CHANNEL.
7. Poller, proxy, monitor, timers, atomic counter, thread helpers.
8. TCP and IPC stream transports.
9. ZMTP v1/v2/v3/v3.1, raw engine, encoders, decoders, metadata.
10. NULL, ZAP, PLAIN.
11. CURVE and GSSAPI.
12. UDP, WS, WSS.
13. PGM, NORM, TIPC, VMCI, VSOCK.
14. Full differential testing, interoperability, performance tuning, and unsafe reduction.

## Compatibility Rules

- Public constants must keep the original `libzmq` numeric values.
- `zmq_msg_t` must remain 64 bytes and pointer-size aligned.
- C API return values and errno behavior must match original `libzmq`.
- Rust native API and C ABI must use the same core implementation.
- Feature-gated modules are allowed, but the first release must pass a full-feature validation profile.

## Current State

The current code establishes the workspace and minimum ABI/native boundaries. Socket transport, protocol engines, pollers, and real messaging semantics are not migrated yet. Unimplemented C ABI calls return `ENOTSUP` rather than silently providing incompatible behavior.
