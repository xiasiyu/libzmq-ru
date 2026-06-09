# ru-libzmq

`ru-libzmq` is a Rust rewrite of `libzmq` that keeps the original C ABI while adding a Rust-native API.

The implementation is intentionally split into shared core logic, a safe Rust API, a C ABI facade, and isolated platform/FFI code. Both public interfaces must call the same Rust core so behavior cannot diverge.

## Crates

- `ru-libzmq-core`: shared messaging core.
- `ru-libzmq`: Rust-native safe API.
- `ru-libzmq-ffi`: `zmq.h` compatible C ABI exports.
- `ru-libzmq-sys`: platform syscalls and third-party FFI isolation.

## Non-Negotiable Goals

- Preserve the original `libzmq` C ABI, including stable, deprecated, and draft APIs.
- Add a Rust-native API backed by the same core implementation.
- Cover Linux, macOS, and Windows from the start.
- Reimplement tests in Rust and use differential tests against the original C++ implementation.
- Keep performance regression below 5% versus original C++ `libzmq`.
- Keep handwritten unsafe code below 10%.
