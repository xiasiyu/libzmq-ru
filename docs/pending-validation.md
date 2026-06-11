# Pending Validation

This document tracks release blockers and validation items that cannot be truthfully marked complete in the current local environment.

## Environment-Dependent Validation

- Real GSSAPI original-C++ interop: requires a configured Kerberos realm, principal/keytab, or credential cache.
- Windows DLL export validation: requires a Windows-capable runner.
- Linux all-features cross-check: requires the Linux C toolchain needed by WSS crypto provider dependencies.

## Incomplete Feature Scope

- `norm://` socket transport: sys-level NORM data-object send/receive and a feature-gated PUB/SUB single-frame data-object path are proven, but full socket transport still needs original libzmq stream/event-loop semantics.
- `pgm://` and `epgm://` transports: require OpenPGM availability and implementation validation.
- `tipc://`, `vmci://`, and `vsock://` transports: require corresponding platform/kernel support and implementation validation.
- UDP `RADIO`/`DISH` transport parity needs oracle coverage for original group and endpoint semantics beyond the current local round trips.
- Custom socket routing identity behavior still needs full original parity: C ABI option storage/validation and inproc `ZMQ_PROBE_ROUTER` smoke behavior are covered, but routing paths still use the current internal numeric peer-id model and do not yet match original TCP/IPC identity framing.
- Draft hello/disconnect/hiccup message options now have original set/clear surface behavior, but full pipe/session delivery semantics are not complete yet.
- XPUB/XSUB draft option surface is partially covered, including `ZMQ_TOPICS_COUNT`, but full manual-last and only-first-subscribe forwarding semantics still need parity work.

## Test Infrastructure Gaps

- Fuzz smoke targets: `fuzz/` currently contains only the target plan and needs executable fuzz targets before release validation can pass.

## Recently Resolved

- NULL/PLAIN/CURVE TCP original-C++ interop now passes in same-process and process-isolated harnesses against CURVE-capable oracle builds.
- `zmq_has` no longer returns a blanket `0`; it reports implemented capabilities and keeps unimplemented transports disabled until their socket-level parity is complete.
- Deprecated `zmq_device` now behaves like original libzmq by forwarding through `zmq_proxy`.
- `zmq_sendiov` and `zmq_recviov` now implement original multipart iovec semantics for the C ABI.
- `zmq_disconnect_peer` now supports inproc `SERVER`/`PEER` routing-id disconnection and original `ENOTSUP`/`EHOSTUNREACH` errors.
- `zmq_msg_set` and unsupported `zmq_msg_get` properties now return `EINVAL`, matching original libzmq semantics.
- Stable C ABI socket options now cover original defaults, round-trips, and validation for common transport/control settings such as affinity, max message size, rate, buffers, reconnect/backlog, multicast, keepalive, handshake, heartbeat, IPv6, immediate, and use-fd.
- `ZMQ_LAST_ENDPOINT` now tracks successful bind/connect endpoints and returns the original null-terminated string `getsockopt` shape.
- Stable C ABI string socket options now cover original null-terminated `getsockopt` behavior for SOCKS proxy credentials and bind-to-device settings.
- Feature-gated `norm://` PUB/SUB single-frame data-object round trips now pass through the native API, C ABI, and sys-layer tests under `--all-features`.
- `ZMQ_ROUTING_ID` and `ZMQ_CONNECT_ROUTING_ID` C ABI option validation now matches original raw-byte shape for valid, empty, oversized, and wrong-socket cases.
- `ZMQ_PROBE_ROUTER` now validates like original libzmq for `DEALER`/`ROUTER` and sends an empty inproc probe message through native and C ABI paths.
- `ZMQ_MULTICAST_LOOP` now has original default and relaxed boolean set/get behavior, and UDP multicast setup uses the configured value.
- Draft/control integer socket options now cover original defaults and validation for reconnect-stop, priority, in/out batch sizes, loopback fastpath, and set-only busy-poll behavior.
- `ZMQ_HELLO_MSG`, `ZMQ_DISCONNECT_MSG`, and `ZMQ_HICCUP_MSG` now support original set/clear C ABI option shape and remain non-gettable like original libzmq.
- `ZMQ_TOPICS_COUNT`, `ZMQ_XPUB_MANUAL_LAST_VALUE`, `ZMQ_ONLY_FIRST_SUBSCRIBE`, and `ZMQ_XSUB_VERBOSE_UNSUBSCRIBE` now have native/C ABI option-surface coverage for local XPUB/XSUB cases.
