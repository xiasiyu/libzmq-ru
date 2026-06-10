# C ABI Compatibility Notes

The C ABI target is original `libzmq` 4.3.6. Public headers live in `include/` and the exported C ABI facade is implemented by `crates/libzmq-ffi`.

## Compatibility Guarantees

- `zmq_msg_t` remains 64 bytes and pointer-size aligned.
- Stable, deprecated, and draft constants are mirrored in `include/zmq.h`, `include/zmq_utils.h`, and `include/zmq_draft.h`.
- Exported C ABI functions use the same symbol names as original libzmq.
- C ABI functions route through the shared Rust core rather than a separate implementation.
- Unsupported transports return explicit errors instead of fake success.

## Known Release Gaps

- C++ oracle interop is covered for NULL/PLAIN/CURVE TCP cases, but real GSSAPI interop remains blocked by Kerberos environment setup.
- Same-process `tcp_interop_oracle` and process-isolated `tcp_interop_process` pass covered NULL/PLAIN/CURVE TCP directions against CURVE-capable original C++ oracle builds.
- `norm://`, `pgm://`, `epgm://`, `tipc://`, `vmci://`, and `vsock://` are not full socket transports yet.
- Windows DLL export validation has not been run in this macOS environment.

## Validation Commands

```sh
cargo test -p libzmq-ffi --all-features
cargo build -p libzmq-ffi --all-features
nm -gU target/debug/libzmq.dylib
cargo run -p libzmq-test-harness --bin unsafe-report -- --write docs/unsafe-report.md
```

Use `docs/windows-export-validation.md` for Windows symbol validation.
