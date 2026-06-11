# Release Hardening

This document tracks release-readiness for the Rust implementation of `libzmq`.

## Release Status

This repository is not ready for a first full-compatible release yet. Phase 14 hardening can validate the completed surface, but release completion depends on the remaining Phase 10 and Phase 11 blockers.

## Required Gates

Run these before any release candidate:

```sh
cargo fmt --all
cargo clippy --workspace --all-features --all-targets
cargo test --workspace
cargo test --workspace --all-features
cargo check --workspace --target x86_64-unknown-linux-gnu
cargo build -p libzmq-ffi --all-features
cargo run -p libzmq-test-harness --bin differential-runner
LIBZMQ_ORACLE=../libzmq/build-ru-oracle-secure/lib/libzmq.dylib cargo run -p libzmq-test-harness --bin tcp_interop_oracle
LIBZMQ_ORACLE=../libzmq/build-ru-oracle-wss/lib/libzmq.dylib cargo run -p libzmq-test-harness --bin tcp_interop_process
cargo run -p libzmq-test-harness --bin unsafe-report -- --write docs/unsafe-report.md
LIBZMQ_ORACLE=../libzmq/build-ru-oracle-wss/lib/libzmq.dylib cargo run --release -p libzmq-test-harness --features wss,sodium --bin performance-gate -- --iterations 1000 --samples 3 --write docs/performance-report.md
```

On macOS, validate local C ABI exports with:

```sh
nm -gU target/debug/libzmq.dylib
```

Windows export validation is documented in `docs/windows-export-validation.md` and must be run on a Windows-capable host.

## Last Host Verification

Last verified on macOS on 2026-06-10:

- PASS: `cargo fmt --all -- --check`
- PASS: `cargo clippy --workspace --all-features --all-targets`
- PASS: `cargo test --workspace`
- PASS: `cargo test --workspace --all-features`
- PASS: `cargo check --workspace --target x86_64-unknown-linux-gnu`
- PASS: `cargo build -p libzmq-ffi --all-features`
- PASS: `cargo run -p libzmq-test-harness --bin differential-runner`
- PASS: `cargo run -p libzmq-test-harness --bin compare-message-oracles`
- PASS: `cargo run -p libzmq-test-harness --bin compare-stable-oracles`
- PASS: `LIBZMQ_ORACLE=../libzmq/build-ru-oracle-curve/lib/libzmq.dylib cargo run -p libzmq-test-harness --bin tcp_interop_oracle`
- PASS: `cargo build -p libzmq-test-harness --bins && LIBZMQ_ORACLE=../libzmq/build-ru-oracle-curve/lib/libzmq.dylib target/debug/tcp_interop_process`
- PASS: `cargo run -p libzmq-test-harness --bin unsafe-report -- --write docs/unsafe-report.md`
- PASS: `LIBZMQ_ORACLE=../libzmq/build-ru-oracle-wss/lib/libzmq.dylib cargo run --release -p libzmq-test-harness --features wss,sodium --bin performance-gate -- --iterations 1000 --samples 3 --write docs/performance-report.md`
- PASS: local macOS `nm -gU target/debug/libzmq.dylib` lists the expected exported `zmq_*` C ABI symbols.

## Optional Host-Dependent Gates

Run these when the relevant dependency/platform exists:

```sh
cargo test -p libzmq-sys --features norm norm_
LIBZMQ_TEST_REAL_GSSAPI=1 cargo test --workspace --features gssapi
cargo check --workspace --all-features --target x86_64-unknown-linux-gnu
```

Fuzz smoke testing is not available yet; `fuzz/` currently contains only the target plan.

## Current Blockers

- Real GSSAPI interop requires a configured Kerberos realm, principal/keytab, or credential cache.
- `norm://` socket transport still needs the original libzmq stream/event-loop semantics. Sys-level NORM data-object send/receive and a feature-gated PUB/SUB single-frame data-object path are proven.
- Full `ROUTER` blob routing-id parity is still incomplete. The C ABI now covers inproc `zmq_socket_get_peer_state` for the current numeric peer-id model, but TCP/IPC blob identity framing remains release-blocking for full compatibility.
- `zmq_socket_monitor_pipes_stats` still needs TCP/IPC I/O-thread queue-stat parity; inproc v2 queue-stat event publication is covered.
- PGM/EPGM requires OpenPGM, which is unavailable in this environment.
- TIPC/VMCI/VSOCK require platform/kernel support that is unavailable on the current macOS host.
- Windows DLL export validation requires a Windows-capable runner.
- Linux all-features cross-check requires a Linux C cross-compiler for WSS crypto provider dependencies.

## Release Decision Rule

Do not mark Phase 14 complete and do not cut a full-compatible release until every required gate passes on the target release matrix and every blocker above is either implemented or explicitly scoped out of the release.
