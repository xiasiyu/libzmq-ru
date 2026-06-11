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
- Custom socket routing identity behavior still needs full original parity: C ABI option storage/validation, inproc `ZMQ_PROBE_ROUTER`, inproc `zmq_socket_get_peer_state` numeric plus exposed decimal-blob peer-id behavior, and TCP/IPC ZMTP READY `Identity` metadata exposure for UTF-8 routing ids are covered. Routing paths still use the current internal numeric peer-id model and do not yet match original TCP/IPC arbitrary binary blob identity framing.
- `ZMQ_ROUTER_RAW` and `ZMQ_STREAM_NOTIFY` now have original option-level validation coverage, but full raw ROUTER/STREAM notification delivery semantics are not complete yet.
- Draft hello/disconnect message options now deliver over the inproc pipe path, and hello/disconnect/hiccup options have original set/clear surface behavior, but full TCP/IPC session delivery and hiccup semantics are not complete yet.
- XPUB/XSUB draft option surface is partially covered, including `ZMQ_TOPICS_COUNT` and `ZMQ_ONLY_FIRST_SUBSCRIBE` validation. Inproc subscription notifications now suppress duplicate subscribes, aggregate topics across XSUB peers, use refcounted duplicate-subscribe unsubscribe behavior, forward final unsubscribes, and emit final unsubscribe notifications when an XSUB disconnects. `ZMQ_XPUB_VERBOSE`/`ZMQ_XPUB_VERBOSER` forward duplicate subscribes, `ZMQ_XPUB_VERBOSER` forwards non-final multi-peer unsubscribes, `ZMQ_XSUB_VERBOSE_UNSUBSCRIBE` forwards unmatched local unsubscribes, `ZMQ_XPUB_MANUAL` supports inproc last-peer accept/revoke, `ZMQ_XPUB_MANUAL_LAST_VALUE` covers same-topic inproc delivery to the last subscribing peer, and `ZMQ_ONLY_FIRST_SUBSCRIBE` covers raw multipart user-frame forwarding. Proxy/multipart manual-last edge cases still need parity work.
- `zmq_socket_monitor_pipes_stats` now covers original precondition errors plus inproc and established TCP/IPC synchronous stream v2 queue-stat event publication, but full original I/O-thread queue-depth oracle parity remains incomplete.

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
- `ZMQ_HELLO_MSG` and `ZMQ_DISCONNECT_MSG` now deliver configured lifecycle messages over the inproc pipe path.
- `ZMQ_TOPICS_COUNT`, `ZMQ_XPUB_MANUAL_LAST_VALUE`, `ZMQ_ONLY_FIRST_SUBSCRIBE`, and `ZMQ_XSUB_VERBOSE_UNSUBSCRIBE` now have native/C ABI option-surface coverage for local XPUB/XSUB cases.
- Inproc XPUB/XSUB subscription forwarding now matches original first-subscribe, multi-peer duplicate suppression, refcounted final-unsubscribe notification behavior, disconnect unsubscribe cleanup, XPUB verbose/verboser duplicate-subscribe behavior, XPUB verboser non-final unsubscribe behavior, XSUB verbose unmatched-unsubscribe behavior, XPUB manual last-peer accept/revoke, same-topic XPUB manual-last value delivery, raw XSUB subscription forwarding, and ONLY_FIRST multipart user-frame forwarding for local inproc subscription changes.
- `zmq_socket_get_peer_state` now reports `ZMQ_POLLOUT`, HWM-full `0`, `ENOTSUP`, and `EHOSTUNREACH` for the current inproc `ROUTER` numeric peer-id path and accepts the received message's decimal `Routing-Id` blob property.
- `ZMQ_ROUTER_RAW` and `ZMQ_STREAM_NOTIFY` now cover original C ABI set-option validation for ROUTER and STREAM sockets.
- `zmq_socket_monitor` and `zmq_socket_monitor_versioned` now return original `EPROTONOSUPPORT` for non-`inproc://` monitor endpoints.
- `zmq_socket_monitor` and `zmq_socket_monitor_versioned` now support original null-endpoint monitor deregistration behavior.
- `zmq_socket_monitor_versioned` now binds monitor endpoints with the requested `PAIR`, `PUB`, or `PUSH` socket type instead of only validating the type argument.
- `zmq_socket_monitor_pipes_stats` now returns original `ENOTSOCK`, `EINVAL`, and `EAGAIN` precondition errors and publishes v2 queue-stat events for inproc pipes plus established TCP/IPC synchronous streams instead of blanket `ENOTSUP`.
- `zmq_msg_gets` now returns `EINVAL` for missing metadata properties, matching original libzmq.
- `zmq_strerror(EHOSTUNREACH)` now returns original `Host unreachable` text for the libzmq custom errno.
- `zmq_ctx_set_ext` and `zmq_ctx_get_ext` now support original `ZMQ_THREAD_NAME_PREFIX` string round-trips and validation.
- `ZMQ_THREAD_PRIORITY` and `ZMQ_THREAD_SCHED_POLICY` context options now reject negative values like original libzmq.
- `ZMQ_MAX_MSGSZ` and `ZMQ_ZERO_COPY_RECV` context options now reject negative values like original libzmq.
- `ZMQ_THREAD_AFFINITY_CPU_ADD` and `ZMQ_THREAD_AFFINITY_CPU_REMOVE` now validate negative values and missing removals like original libzmq.
- `ZMQ_IPV6`, `ZMQ_BLOCKY`, and `ZMQ_ZERO_COPY_RECV` context defaults now match original libzmq, and new sockets inherit context IPv6/blocky defaults.
- `ZMQ_MAX_SOCKETS` now rejects zero, and `ZMQ_SOCKET_LIMIT` reports the process socket ceiling instead of the current max-sockets setting.
- Poller C ABI helpers now match original errno behavior for direct null pollers, null socket registration/removal, invalid event masks, duplicate socket/fd registration, and `zmq_poller_wait` success return shape.
