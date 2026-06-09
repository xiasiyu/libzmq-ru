# Unsafe Policy

The handwritten unsafe code target is below 10% for the full project and below 8% for the default feature profile.

## Allowed Unsafe Islands

- `crates/ru-libzmq-ffi`: C ABI raw pointers and `zmq_msg_t` layout.
- `crates/ru-libzmq-sys`: platform syscalls and third-party C libraries.
- Future `platform` modules: fd/socket ownership, poller syscall boundaries, sockaddr casts.
- Future crypto FFI modules: libsodium, GSSAPI, OpenPGM, NORM.

## Rules

- Core business logic must not call `libc`, WinSock, or third-party FFI directly.
- Every unsafe block must document the safety invariant.
- Raw fd/socket handles must be wrapped in RAII types before entering business logic.
- Raw C strings and pointers are validated at the C ABI boundary.
- Generated bindings and third-party crates are measured separately from handwritten code.
- CI should include `cargo geiger` or an equivalent unsafe counter once Rust tooling is available.

## High-Risk Areas

- `zmq_msg_t` ABI: 64 bytes, pointer-size alignment, lifecycle callbacks.
- Zero-copy message data returned to C callers.
- sockaddr casts and platform-specific socket options.
- Windows handle/socket differences.
- GSSAPI, OpenPGM, NORM, and libsodium FFI.
