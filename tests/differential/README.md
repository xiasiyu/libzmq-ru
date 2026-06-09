# Differential Tests

The `differential-runner` binary in `crates/libzmq-test-harness` emits trace lines that can be compared against an original C++ `libzmq` oracle.

Current invocation:

```sh
cargo run -p libzmq-test-harness --bin differential-runner
```
