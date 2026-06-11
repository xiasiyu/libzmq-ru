# C ABI Compatibility Notes

The C ABI target is original `libzmq` 4.3.6. Public headers live in `include/` and the exported C ABI facade is implemented by `crates/libzmq-ffi`.

## Compatibility Guarantees

- `zmq_msg_t` remains 64 bytes and pointer-size aligned.
- Stable, deprecated, and draft constants are mirrored in `include/zmq.h`, `include/zmq_utils.h`, and `include/zmq_draft.h`.
- Exported C ABI functions use the same symbol names as original libzmq.
- C ABI functions route through the shared Rust core rather than a separate implementation.
- Unsupported transports return explicit errors instead of fake success.
- `zmq_socket_get_peer_state` supports the current inproc `ROUTER` numeric peer-id path, including `ZMQ_POLLOUT`, `ENOTSUP`, and `EHOSTUNREACH` behavior.
- `zmq_socket_monitor_pipes_stats` matches original precondition errors and publishes v2 queue-stat monitor events for inproc pipes.

## Known Release Gaps

- C++ oracle interop is covered for NULL/PLAIN/CURVE TCP cases, but real GSSAPI interop remains blocked by Kerberos environment setup.
- Same-process `tcp_interop_oracle` and process-isolated `tcp_interop_process` pass covered NULL/PLAIN/CURVE TCP directions against CURVE-capable original C++ oracle builds.
- `norm://` has a feature-gated PUB/SUB single-frame data-object path, but is not a full socket transport yet; `pgm://`, `epgm://`, `tipc://`, `vmci://`, and `vsock://` remain unsupported socket transports.
- Original arbitrary blob routing-id parity is incomplete: `zmq_socket_get_peer_state` currently accepts the rewrite's internal `u32` peer id, while full TCP/IPC `ROUTER` identity framing still needs original blob semantics.
- `zmq_socket_monitor_pipes_stats` does not yet publish original I/O-thread queue stats for TCP/IPC pipes.
- Windows DLL export validation has not been run in this macOS environment.

## Validation Commands

```sh
cargo test -p libzmq-ffi --all-features
cargo build -p libzmq-ffi --all-features
nm -gU target/debug/libzmq.dylib
cargo run -p libzmq-test-harness --bin unsafe-report -- --write docs/unsafe-report.md
```

Use `docs/windows-export-validation.md` for Windows symbol validation.
