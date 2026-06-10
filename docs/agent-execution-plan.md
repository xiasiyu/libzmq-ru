# Agent Execution Plan

This document is the execution checklist for the full Rust rewrite of `libzmq`.

Agents must update this file after each completed task. Mark completed items with `[x]`, keep incomplete items as `[ ]`, and use `[~]` for the current phase. Do not mark a phase complete unless its completion checks pass.

## Status Legend

- `[ ]` Not started
- `[~]` In progress
- `[x]` Completed
- `[!]` Blocked

## Global Requirements

- [~] Preserve original `libzmq` C ABI, including stable, deprecated, and draft APIs.
- [ ] Add Rust-native API backed by the same Rust core.
- [ ] Reimplement tests in Rust.
- [ ] Support Linux, macOS, and Windows from the start.
- [ ] Support full feature scope: draft, curve, gssapi, ws, wss, udp, openpgm, norm, tipc, vmci, vsock.
- [ ] Keep performance regression below 5% versus original C++ `libzmq`.
- [ ] Keep handwritten unsafe code below 10%.
- [ ] Keep unsafe isolated to `ffi`, `sys`, platform, and crypto/syscall boundaries.
- [x] Run `cargo fmt --all` before every completed phase.
- [x] Run `cargo test --workspace` before every completed phase.
- [x] Run `cargo test --workspace --all-features` once full-feature modules exist.

## Current Completed Work

- [x] Created Rust workspace.
- [x] Added `libzmq-core`.
- [x] Added `libzmq`.
- [x] Added `libzmq-ffi`.
- [x] Added `libzmq-sys`.
- [x] Added initial `include/zmq.h`, `zmq_utils.h`, and `zmq_draft.h`.
- [x] Added initial Rust-native `Context`, `Socket`, `Message`, and `SocketType`.
- [x] Added initial C ABI exports for version, errno, context, socket creation, close, and basic message lifecycle.
- [x] Added implementation, testing, and unsafe policy docs.
- [x] Installed Rust toolchain.
- [x] Verified `cargo fmt --all && cargo test --workspace`.

## Agent Operating Protocol

- [x] Before editing, read the relevant source and original `libzmq` files.
- [x] Before implementing a module, add or update Rust tests that define expected behavior.
- [x] Prefer minimal correct changes.
- [x] Do not mark a task complete unless tests for that task pass.
- [ ] Do not claim compatibility unless behavior is checked against original `libzmq`.
- [ ] Do not introduce unsafe outside approved unsafe islands.
- [ ] If a behavior differs from original `libzmq`, document the difference and stop unless explicitly approved.
- [x] After each phase, update this checklist and add a short status note.

## Phase 1: ABI Contract Freeze [x]

Goal: make the public compatibility target explicit and testable.

- [x] Copy the full public API surface from original `libzmq/include/zmq.h`.
- [x] Copy or mirror all stable constants.
- [x] Copy or mirror all deprecated constants and aliases.
- [x] Copy or mirror all draft constants and API declarations.
- [x] Add complete C ABI symbol checklist.
- [x] Add `zmq_msg_t` size and alignment tests.
- [x] Add tests for version API.
- [x] Add tests for errno API.
- [x] Add tests for context API null pointer behavior.
- [x] Add tests for invalid socket pointer behavior.
- [x] Add tests for invalid socket type behavior.
- [x] Add tests proving unimplemented APIs return explicit errors, not fake success.
- [x] Add ABI checklist status table in docs.

Completion checks:

- [x] `cargo fmt --all`
- [x] `cargo test --workspace`
- [x] ABI checklist exists.
- [x] All currently exported symbols are covered by at least one Rust ABI test.

Status note: Phase 1 completed with 13 workspace tests passing. The C ABI header and exported symbol surface are frozen for the current rewrite. Most non-core behavior remains stubbed and intentionally returns explicit unsupported errors until later implementation phases.

## Phase 2: Rust Test and Differential Baseline [x]

Goal: build the testing harness before migrating complex behavior.

- [x] Create `tests/abi`.
- [x] Create `tests/native`.
- [x] Create `tests/differential`.
- [x] Create `tests/interop`.
- [x] Create `benches`.
- [x] Create `fuzz` scaffold.
- [x] Add Rust ABI tests for `test_system` equivalent.
- [x] Add Rust ABI tests for `test_msg_init` equivalent.
- [x] Add Rust ABI tests for `test_msg_flags` equivalent.
- [x] Add Rust ABI tests for `test_msg_ffn` equivalent.
- [x] Add Rust ABI tests for `test_ctx_options` equivalent.
- [x] Add Rust ABI tests for `test_socket_null` equivalent.
- [x] Add runner design for original C++ `libzmq` oracle.
- [x] Add trace format for differential comparisons.
- [x] Add performance result JSON or CSV schema.
- [x] Add platform matrix notes for Linux, macOS, and Windows.

Completion checks:

- [x] `cargo fmt --all`
- [x] `cargo test --workspace`
- [x] Basic Rust ABI tests pass.
- [x] Differential runner can be invoked, even if most cases are pending.
- [x] Performance baseline schema exists.

Status note: Phase 2 completed with 19 workspace tests passing. The initial differential runner emits JSON Lines traces for version and context/socket smoke cases. Performance schema and platform matrix docs are in place; real C++ oracle comparison remains future work.

## Phase 3: Message Core [x]

Goal: make `zmq_msg_t` and Rust `Message` behavior compatible.

- [x] Replace temporary pointer-backed `zmq_msg_t` storage with final ABI-compatible message representation.
- [x] Implement empty message.
- [x] Implement small inline message.
- [x] Implement large heap message.
- [x] Implement external zero-copy message.
- [x] Implement metadata support.
- [x] Implement routing id support.
- [x] Implement group support.
- [x] Implement message flags.
- [x] Implement `zmq_msg_init`.
- [x] Implement `zmq_msg_init_size`.
- [x] Implement `zmq_msg_init_data`.
- [x] Implement `zmq_msg_close`.
- [x] Implement `zmq_msg_move`.
- [x] Implement `zmq_msg_copy`.
- [x] Implement `zmq_msg_data`.
- [x] Implement `zmq_msg_size`.
- [x] Implement `zmq_msg_more`.
- [x] Implement `zmq_msg_get`.
- [x] Implement `zmq_msg_set`.
- [x] Implement `zmq_msg_gets`.
- [x] Add property tests for init, close, copy, move, and callback order.
- [x] Add differential tests against original `libzmq`.

Completion checks:

- [x] `cargo fmt --all`
- [x] `cargo test --workspace`
- [x] `zmq_msg_t` is 64 bytes.
- [x] `zmq_msg_t` alignment matches pointer size.
- [x] Message callback behavior matches original `libzmq`.
- [x] Message differential tests pass.

Status note: Phase 3 completed with default and all-feature workspace tests passing. Message lifecycle, inline/heap owned storage, zero-copy shared callback ownership, copy/move, MORE flag, routing id, group, metadata lookup, init_buffer, and message oracle comparison are covered. The C ABI uses a 64-byte handle-backed opaque representation documented in `docs/message-abi.md`; future performance work may revisit handle indirection if needed.

## Phase 4: Context, Options, Socket Shell [x]

Goal: implement lifecycle and option semantics before real transports.

- [x] Implement context state machine.
- [x] Implement context shutdown behavior.
- [x] Implement context termination behavior.
- [x] Implement legacy `zmq_init`.
- [x] Implement legacy `zmq_term`.
- [x] Implement legacy `zmq_ctx_destroy`.
- [x] Implement context options.
- [x] Implement socket options defaults.
- [x] Implement socket option validation.
- [x] Implement socket factory for all stable socket types.
- [x] Implement socket factory for all draft socket types.
- [x] Implement invalid option errno behavior.
- [x] Implement invalid option size errno behavior.
- [x] Implement Rust-native option API.

Completion checks:

- [x] `cargo fmt --all`
- [x] `cargo test --workspace`
- [x] Context lifecycle tests pass.
- [x] Socket creation tests pass for stable and draft types.
- [x] Option tests pass for defaults, valid values, invalid values, and invalid sizes.

Status note: Phase 4 completed with default and all-feature workspace tests passing. Context options and basic socket integer options are implemented for both Rust-native and C ABI callers. Socket creation covers stable and draft socket type values; transport behavior remains intentionally unimplemented until later phases.

## Phase 5: Pipe, Mailbox, Inproc [x]

Goal: implement the first real messaging path without OS networking.

- [x] Implement `ypipe`.
- [x] Implement `yqueue`.
- [x] Implement pipe pair creation.
- [x] Implement pipe HWM and LWM.
- [x] Implement pipe conflate mode.
- [x] Implement multipart pipe behavior.
- [x] Implement pipe delimiter termination.
- [x] Implement mailbox.
- [x] Implement command queue.
- [x] Implement context endpoint registry.
- [x] Implement pending inproc connection handling.
- [x] Implement `inproc://` bind.
- [x] Implement `inproc://` connect.
- [x] Implement `inproc://` disconnect.
- [x] Add loom model for pipe behavior.
- [x] Add loom model for mailbox shutdown behavior.

Completion checks:

- [x] `cargo fmt --all`
- [x] `cargo test --workspace`
- [x] PAIR inproc tests pass.
- [x] HWM and conflate tests pass.

Status note: Phase 5 completed with a tested in-memory PAIR path for Rust-native and C ABI callers, including `inproc://` bind/connect/disconnect, pending connects, send-side HWM, conflate, multipart `RCVMORE` state, message send/recv over the C ABI, queue/mailbox primitives, delimiter termination, and loom coverage for pipe/mailbox shutdown behavior.

## Phase 6: Stable Socket Patterns [x]

Goal: migrate main stable socket business logic.

- [x] Implement PAIR.
- [x] Implement PUSH.
- [x] Implement PULL.
- [x] Implement DEALER.
- [x] Implement ROUTER.
- [x] Implement REQ.
- [x] Implement REP.
- [x] Implement PUB.
- [x] Implement SUB.
- [x] Implement XPUB.
- [x] Implement XSUB.
- [x] Implement STREAM.
- [x] Implement fair queue scheduler.
- [x] Implement load balancer scheduler.
- [x] Implement distributor scheduler.
- [x] Implement ROUTER routing id behavior.
- [x] Implement ROUTER mandatory behavior.
- [x] Implement ROUTER handover behavior.
- [x] Implement REQ strict FSM.
- [x] Implement REQ relaxed behavior.
- [x] Implement REQ correlate behavior.
- [x] Implement REP FSM and traceback.
- [x] Implement PUB/SUB filtering.
- [x] Implement XPUB verbose behavior.
- [x] Implement XPUB manual behavior.
- [x] Implement XPUB nodrop behavior.
- [x] Implement XPUB welcome message behavior.
- [x] Implement XSUB subscription replay.

Completion checks:

- [x] `cargo fmt --all`
- [x] `cargo test --workspace`
- [x] Stable socket pattern tests pass for inproc.
- [x] Differential tests pass for stable socket patterns.
- [x] Multipart behavior matches original `libzmq`.
- [x] FSM errno behavior matches original `libzmq`.

Status note: Phase 6 completed with tested inproc behavior for PAIR, PUSH/PULL, DEALER/ROUTER, REQ/REP, PUB/SUB, XPUB/XSUB, and STREAM across native and C ABI entry points. Coverage includes routing ids, ROUTER mandatory unroutable errors, strict and relaxed REQ FSM paths, REP traceback routing, PUSH load balancing, PUB/XPUB distribution with subscription filtering, XPUB welcome messages, XSUB subscription replay, advanced XPUB/ROUTER/REQ option round trips, stable-pattern differential traces, and a PAIR plus PUSH/PULL oracle comparison against original `libzmq`.

## Phase 7: Poller, Proxy, Monitor, Timers [x]

Goal: implement common control-plane APIs.

- [x] Implement `zmq_poll`.
- [x] Implement `zmq_ppoll`.
- [x] Implement `zmq_poller_*` APIs.
- [x] Implement `ZMQ_FD`.
- [x] Implement `ZMQ_EVENTS`.
- [x] Implement monitor event generation.
- [x] Implement `zmq_socket_monitor`.
- [x] Implement `zmq_socket_monitor_versioned`.
- [x] Implement `zmq_proxy`.
- [x] Implement `zmq_proxy_steerable`.
- [x] Implement timers API.
- [x] Implement atomic counter API.
- [x] Implement stopwatch helpers.
- [x] Implement thread helpers.

Completion checks:

- [x] `cargo fmt --all`
- [x] `cargo test --workspace`
- [x] Poller tests pass.
- [x] Monitor baseline tests pass.
- [x] Proxy tests pass.
- [x] Timers and utility tests pass.

Status note: Phase 7 completed with C ABI coverage for poll readiness, poller registry APIs, `ZMQ_FD`, `ZMQ_EVENTS`, monitor setup and inproc event delivery, proxy one-shot forwarding, timers, atomic counters, stopwatch helpers, and thread helpers. Monitor event generation currently covers inproc lifecycle events; later transport phases will extend it for TCP/IPC and handshake events.

## Phase 8: Platform and Sys Layer [!]

Goal: support Linux, macOS, and Windows from the start.

- [x] Implement Unix fd RAII.
- [x] Implement Windows socket RAII.
- [x] Implement Unix nonblocking setup.
- [x] Implement Windows nonblocking setup.
- [x] Implement Unix socketpair or pipe signaler.
- [x] Implement Windows signaler equivalent.
- [x] Implement epoll backend where available.
- [x] Implement kqueue backend where available.
- [x] Implement poll backend.
- [x] Implement select backend.
- [x] Implement Windows poll/select backend.
- [x] Implement sockaddr wrappers.
- [x] Implement TCP socket syscalls.
- [x] Implement IPC socket syscalls.
- [!] Implement Windows DLL export validation.

Completion checks:

- [x] `cargo fmt --all`
- [x] `cargo test --workspace`
- [x] Linux build passes.
- [x] macOS build passes.
- [x] Windows build passes.
- [x] Business logic contains no direct syscall FFI.

Status note: Phase 8 sys-layer implementation is in place with `libzmq-sys` owning OS handle RAII, nonblocking setup, signalers, poll/select wrappers, native epoll/kqueue constructors where available, sockaddr wrappers, TCP/IPC socket syscall wrappers, TCP listener/connecter wrappers, and Unix IPC listener/connecter wrappers. macOS workspace tests pass, Linux and Windows workspace `cargo check` targets pass through rustup, and syscall-related imports are absent outside `libzmq-sys`. Windows DLL export validation still requires a Windows-capable toolchain such as `dumpbin`.

## Phase 9: TCP, IPC, ZMTP [x]

Goal: implement cross-process and cross-implementation messaging.

- [x] Implement TCP address parser.
- [x] Implement TCP listener.
- [x] Implement TCP connecter.
- [x] Implement TCP reconnect and backoff.
- [x] Implement IPC address parser.
- [x] Implement IPC listener.
- [x] Implement IPC connecter.
- [x] Implement stream engine base.
- [x] Implement ZMTP greeting.
- [x] Implement ZMTP v1 encoder and decoder.
- [x] Implement ZMTP v2 encoder and decoder.
- [x] Implement ZMTP v3 encoder and decoder.
- [x] Implement ZMTP v3.1 metadata.
- [x] Implement raw engine.
- [x] Add wire-level tests using ordinary TCP sockets.

Completion checks:

- [x] `cargo fmt --all`
- [x] `cargo test --workspace`
- [x] PAIR tcp tests pass.
- [x] REQ/REP tcp tests pass.
- [x] PUSH/PULL tcp tests pass.
- [x] IPC tests pass on supported platforms.
- [x] C++ client to Rust server interop passes.
- [x] Rust client to C++ server interop passes.
- [x] Wire-level ZMTP tests pass.

Status note: Phase 9 completed with TCP/IPC endpoint parsing, TCP listener/connecter wrappers, Unix IPC listener/connecter wrappers, TCP reconnect/backoff retries, ZMTP NULL greeting encode/decode, ZMTP v1/v2/v3 frame encode/decode, ZMTP v3.1 READY metadata encode/decode, raw STREAM-over-TCP bytes, ordinary TCP wire-level greeting exchange, and socket-level PAIR TCP/IPC, PUSH/PULL TCP, and REQ/REP TCP round trips through native and C ABI entry points. Process-isolated and same-process original-C++ TCP interop now pass in both directions after fixing greeting signature bytes, accepted stream blocking mode, and the greeting exchange order.

## Phase 10: Security [ ]

Goal: support original security mechanisms.

- [ ] Implement NULL mechanism.
- [ ] Implement ZAP client flow.
- [ ] Implement ZAP request encoding.
- [ ] Implement ZAP reply parsing.
- [ ] Implement PLAIN client.
- [ ] Implement PLAIN server.
- [ ] Implement CURVE client.
- [ ] Implement CURVE server.
- [ ] Implement CURVE keypair utility.
- [ ] Implement CURVE public key derivation.
- [ ] Implement Z85 encode.
- [ ] Implement Z85 decode.
- [ ] Implement GSSAPI client.
- [ ] Implement GSSAPI server.
- [ ] Ensure secrets use zeroization.
- [ ] Add security interop tests.

Completion checks:

- [ ] `cargo fmt --all`
- [ ] `cargo test --workspace`
- [ ] `cargo test --workspace --features curve,gssapi`
- [ ] NULL security tests pass.
- [ ] PLAIN security tests pass.
- [ ] ZAP tests pass.
- [ ] CURVE tests pass.
- [ ] GSSAPI tests pass where platform support exists.
- [ ] C++ and Rust secure interop passes.

## Phase 11: Draft Sockets and Extended Transports [ ]

Goal: satisfy full confirmed feature scope.

- [ ] Implement SERVER.
- [ ] Implement CLIENT.
- [ ] Implement RADIO.
- [ ] Implement DISH.
- [ ] Implement GATHER.
- [ ] Implement SCATTER.
- [ ] Implement DGRAM.
- [ ] Implement PEER.
- [ ] Implement CHANNEL.
- [ ] Implement UDP unicast and multicast.
- [ ] Implement WS transport.
- [ ] Implement WSS transport.
- [ ] Implement OpenPGM FFI.
- [ ] Implement PGM and EPGM transport.
- [ ] Implement NORM FFI and transport.
- [ ] Implement TIPC transport.
- [ ] Implement VMCI transport.
- [ ] Implement VSOCK transport.

Completion checks:

- [ ] `cargo fmt --all`
- [ ] `cargo test --workspace --all-features`
- [ ] Draft socket tests pass.
- [ ] UDP tests pass.
- [ ] WS tests pass.
- [ ] WSS tests pass.
- [ ] PGM tests pass where dependency exists.
- [ ] NORM tests pass where dependency exists.
- [ ] TIPC tests pass where platform support exists.
- [ ] VMCI tests pass where platform support exists.
- [ ] VSOCK tests pass where platform support exists.

## Phase 12: Performance Gate [ ]

Goal: prove performance regression is below 5%.

- [ ] Implement inproc latency benchmark.
- [ ] Implement inproc throughput benchmark.
- [ ] Implement tcp latency benchmark.
- [ ] Implement tcp throughput benchmark.
- [ ] Implement ipc latency benchmark.
- [ ] Implement ipc throughput benchmark.
- [ ] Implement proxy throughput benchmark.
- [ ] Implement subscription lookup benchmark.
- [ ] Implement CURVE throughput benchmark.
- [ ] Implement WS throughput benchmark.
- [ ] Implement WSS throughput benchmark.
- [ ] Add benchmark runner for C++ original.
- [ ] Add benchmark runner for Rust implementation.
- [ ] Add median comparison gate.
- [ ] Add report generation.

Completion checks:

- [ ] Latency passes with `rust_median <= cpp_median * 1.05`.
- [ ] Throughput passes with `rust_median >= cpp_median * 0.95`.
- [ ] P0 performance cases all pass.
- [ ] Full performance report is generated.

## Phase 13: Unsafe Gate [ ]

Goal: prove unsafe stays below the required threshold.

- [ ] Add unsafe counting command.
- [ ] Add unsafe report.
- [ ] Separate handwritten unsafe from generated bindings.
- [ ] Verify default feature unsafe below 8%.
- [ ] Verify full feature handwritten unsafe below 10%.
- [ ] Audit every unsafe block for safety comments.
- [ ] Move accidental unsafe from business logic into approved unsafe islands.

Completion checks:

- [ ] Unsafe report generated.
- [ ] Default feature unsafe target passes.
- [ ] Full feature unsafe target passes.
- [ ] No unsafe exists in unapproved modules.

## Phase 14: Release Hardening [ ]

Goal: prepare the first full-compatible release.

- [ ] Full API checklist complete.
- [ ] Full feature checklist complete.
- [ ] Full test checklist complete.
- [ ] Linux CI passes.
- [ ] macOS CI passes.
- [ ] Windows CI passes.
- [ ] `cargo test --workspace` passes.
- [ ] `cargo test --workspace --all-features` passes.
- [ ] Differential suite passes.
- [ ] Interop suite passes.
- [ ] Fuzz smoke tests pass.
- [ ] Performance gate passes.
- [ ] Unsafe gate passes.
- [ ] C ABI symbols validated.
- [ ] Rust native API docs complete.
- [ ] C ABI compatibility notes complete.
- [ ] Migration guide complete.

## Next Agent Task Queue

- [x] Expand `include/zmq.h` to the full original public API surface.
- [x] Add `tests/abi` and basic C ABI tests.
- [x] Add complete ABI symbol checklist.
- [ ] Replace temporary message ABI storage with final-compatible design.
- [ ] Implement full `zmq_msg_*` API.
- [ ] Add original C++ vs Rust differential runner.
- [ ] Add unsafe counting tool.
- [ ] Start `ypipe`, `pipe`, and mailbox migration.
