# Migration Guide

This guide covers migration from original C++ `libzmq` to this Rust implementation.

## C ABI Consumers

Use the same `zmq.h` API and link against the produced `zmq` library from `libzmq-ffi`.

Expected unchanged areas:

- Context lifecycle APIs.
- Message lifecycle APIs and `zmq_msg_t` size/alignment.
- Stable socket constants and socket option constants.
- Main inproc/TCP/IPC socket patterns covered by tests.
- NULL/PLAIN/CURVE security on covered TCP paths.

Known migration caveats:

- Optional transports that are not fully implemented return explicit `ENOTSUP`.
- Same-process C++ oracle CURVE interop still has one blocked direction; validate your exact security topology before migrating CURVE production traffic.
- Real Kerberos/GSSAPI interop requires external Kerberos configuration before validation.
- Windows DLL exports must be validated on a Windows-capable runner before release.

## Rust Consumers

Prefer the `libzmq` crate. It uses the same core as the C ABI implementation and avoids raw pointers.

## Release Candidate Checklist

Before switching production consumers, verify:

- Your socket patterns are covered by native and C ABI tests.
- Your transport is implemented, not explicitly unsupported.
- Your security mechanism has passed the matching interop test.
- `docs/performance-report.md` passes for your relevant transport/security profile.
- `docs/unsafe-report.md` remains below the release unsafe threshold.
