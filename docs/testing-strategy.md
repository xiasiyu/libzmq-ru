# Testing Strategy

All new tests are written in Rust. Original C++ tests from `libzmq` are used as behavior references and differential oracles, not as the final test implementation.

## Layers

- Unit tests: Rust `#[test]` for core data structures.
- Parameterized tests: `rstest` for socket pattern matrices.
- Property tests: `proptest` for endpoint parsing, options, subscriptions, multipart frames, and ZMTP codecs.
- Fuzz tests: `cargo-fuzz` for bind/connect endpoints, Z85, socket options, ZMTP decoder, and WS handshake.
- Benchmarks: `criterion` plus ports of original `perf/` scenarios.
- Concurrency model tests: `loom` for bounded pipe/mailbox/context shutdown models.

## Compatibility Coverage

- C ABI behavior through Rust `extern "C"` tests.
- Rust native API behavior through integration tests.
- Stable, deprecated, and draft API surfaces.
- Linux, macOS, and Windows.
- Default feature profile and full feature profile.
- C++ original vs Rust implementation differential tests.
- Cross-implementation TCP interoperability:
  - C++ server to Rust client.
  - Rust server to C++ client.
  - C++ pub to Rust sub.
  - Rust pub to C++ sub.
  - C++ router to Rust dealer.
  - Rust router to C++ dealer.

## Main Path Coverage

- Context lifecycle: new, shutdown, term, destroy.
- Message lifecycle: init, init_size, init_data, copy, move, close, data, size, more.
- Socket options: set/get defaults, valid values, invalid values, invalid sizes.
- Bind/connect/unbind/disconnect.
- Blocking, nonblocking, timeout, context termination wakeup.
- Multipart atomicity.
- HWM, LWM, conflate.
- PAIR, REQ/REP, DEALER/ROUTER, PUSH/PULL, PUB/SUB, XPUB/XSUB, STREAM.
- Draft sockets and draft APIs.
- inproc, tcp, ipc, udp, ws, wss, pgm, norm, tipc, vmci, vsock.
- NULL, PLAIN, ZAP, CURVE, GSSAPI.
- Poller, proxy, monitor, timers.

## Performance Gate

- Latency passes when `rust_median <= cpp_median * 1.05`.
- Throughput passes when `rust_median >= cpp_median * 0.95`.
- Each benchmark warms up 3 times and records at least 10 measured samples.
- Median is the primary decision metric.
- Cases with sample variance above 5% are rerun or marked unstable.
