# Message ABI Design

`zmq_msg_t` remains the public C ABI message type. It is opaque to applications and must stay 64 bytes with pointer-size alignment.

## Current Rust Representation

The C ABI layer stores an internal handle inside the 64-byte `zmq_msg_t` buffer. The handle points to Rust-owned message state containing:

- owned inline or heap data,
- shared external zero-copy data with reference-counted callback ownership,
- the `MORE` flag,
- draft routing id,
- draft group,
- string metadata used by `zmq_msg_gets`.

This is ABI-compatible at the public C boundary because callers may allocate, pass, and close `zmq_msg_t` exactly as before. The internal byte layout is intentionally not exposed.

## Compatibility Rules

- `sizeof(zmq_msg_t) == 64`.
- `alignof(zmq_msg_t) == sizeof(void *)`.
- `zmq_msg_init*` initializes the opaque storage.
- `zmq_msg_close` releases owned resources.
- `zmq_msg_move` transfers the handle and resets the source to an empty message.
- `zmq_msg_copy` clones owned data and shares external zero-copy data.
- External zero-copy free callbacks run exactly once after the last shared message closes.

## Follow-Up Work

- Expand original C++ oracle traces beyond `init_size`, `size`, and `close`.
- Compare callback timing with original `libzmq` across copy and move cases.
- Revisit the handle indirection during performance tuning if it contributes to the 5% regression budget.
