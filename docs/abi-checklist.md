# ABI Checklist

This checklist tracks the public C ABI contract inherited from original `libzmq/include/zmq.h`.

Status values:

- `implemented`: exported and has compatible behavior for the currently covered tests.
- `stub`: exported but intentionally returns `ENOTSUP` or a null pointer until its implementation phase.
- `declared`: present in `include/zmq.h`; export status must be verified by tests or symbol checks.

## Header Contract

- [x] Version macros match original `libzmq` 4.3.6.
- [x] Native error constants match original values.
- [x] Context option constants are present.
- [x] Stable socket type constants are present.
- [x] Stable socket option constants are present.
- [x] Deprecated aliases are present.
- [x] Monitor event constants are present.
- [x] Poll constants and `zmq_pollitem_t` are present.
- [x] Timer API declarations are present.
- [x] Utility API declarations are present.
- [x] Draft socket type constants are present under `ZMQ_BUILD_DRAFT_API`.
- [x] Draft option constants are present under `ZMQ_BUILD_DRAFT_API`.
- [x] Draft API declarations are present under `ZMQ_BUILD_DRAFT_API`.
- [x] `zmq_msg_t` remains 64 bytes and pointer-size aligned.

## Implemented Exports

- [x] `zmq_errno`
- [x] `zmq_strerror`
- [x] `zmq_version`
- [x] `zmq_ctx_new`
- [x] `zmq_ctx_term`
- [x] `zmq_ctx_shutdown`
- [x] `zmq_init`
- [x] `zmq_term`
- [x] `zmq_ctx_destroy`
- [x] `zmq_socket`
- [x] `zmq_close`
- [x] `zmq_msg_init`
- [x] `zmq_msg_init_size`
- [x] `zmq_msg_init_data`
- [x] `zmq_msg_close`
- [x] `zmq_msg_move`
- [x] `zmq_msg_data`
- [x] `zmq_msg_size`
- [x] `zmq_msg_more`
- [x] `zmq_has`
- [x] `zmq_sleep`

## Stubbed Stable and Deprecated Exports

- [x] `zmq_ctx_set`
- [x] `zmq_ctx_get`
- [x] `zmq_msg_send`
- [x] `zmq_msg_recv`
- [x] `zmq_msg_copy`
- [x] `zmq_msg_get`
- [x] `zmq_msg_set`
- [x] `zmq_msg_gets`
- [x] `zmq_setsockopt`
- [x] `zmq_getsockopt`
- [x] `zmq_bind`
- [x] `zmq_connect`
- [x] `zmq_unbind`
- [x] `zmq_disconnect`
- [x] `zmq_send`
- [x] `zmq_send_const`
- [x] `zmq_recv`
- [x] `zmq_socket_monitor`
- [x] `zmq_poll`
- [x] `zmq_proxy`
- [x] `zmq_proxy_steerable`
- [x] `zmq_device`
- [x] `zmq_sendmsg`
- [x] `zmq_recvmsg`
- [x] `zmq_sendiov`
- [x] `zmq_recviov`
- [x] `zmq_z85_encode`
- [x] `zmq_z85_decode`
- [x] `zmq_curve_keypair`
- [x] `zmq_curve_public`
- [x] `zmq_atomic_counter_new`
- [x] `zmq_atomic_counter_set`
- [x] `zmq_atomic_counter_inc`
- [x] `zmq_atomic_counter_dec`
- [x] `zmq_atomic_counter_value`
- [x] `zmq_atomic_counter_destroy`
- [x] `zmq_timers_new`
- [x] `zmq_timers_destroy`
- [x] `zmq_timers_add`
- [x] `zmq_timers_cancel`
- [x] `zmq_timers_set_interval`
- [x] `zmq_timers_reset`
- [x] `zmq_timers_timeout`
- [x] `zmq_timers_execute`
- [x] `zmq_stopwatch_start`
- [x] `zmq_stopwatch_intermediate`
- [x] `zmq_stopwatch_stop`
- [x] `zmq_threadstart`
- [x] `zmq_threadclose`

## Stubbed Draft Exports

- [x] `zmq_ctx_set_ext`
- [x] `zmq_ctx_get_ext`
- [x] `zmq_join`
- [x] `zmq_leave`
- [x] `zmq_connect_peer`
- [x] `zmq_disconnect_peer`
- [x] `zmq_msg_set_routing_id`
- [x] `zmq_msg_routing_id`
- [x] `zmq_msg_set_group`
- [x] `zmq_msg_group`
- [x] `zmq_msg_init_buffer`
- [x] `zmq_poller_new`
- [x] `zmq_poller_destroy`
- [x] `zmq_poller_size`
- [x] `zmq_poller_add`
- [x] `zmq_poller_modify`
- [x] `zmq_poller_remove`
- [x] `zmq_poller_wait`
- [x] `zmq_poller_wait_all`
- [x] `zmq_poller_fd`
- [x] `zmq_poller_add_fd`
- [x] `zmq_poller_modify_fd`
- [x] `zmq_poller_remove_fd`
- [x] `zmq_socket_get_peer_state`
- [x] `zmq_socket_monitor_versioned`
- [x] `zmq_socket_monitor_pipes_stats`
- [x] `zmq_ppoll`

## Next Compatibility Work

- [ ] Replace stub behavior with original semantics phase by phase.
- [ ] Add symbol export verification for `libzmq` dynamic library builds.
- [ ] Add C header compile tests with and without `ZMQ_BUILD_DRAFT_API`.
- [ ] Add differential tests against original C++ `libzmq` for every API family.
