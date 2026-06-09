# Differential Tests

The `differential-runner` binary in `crates/ru-libzmq-test-harness` emits trace lines that can be compared against an original C++ `libzmq` oracle.

Current invocation:

```sh
cargo run -p ru-libzmq-test-harness --bin differential-runner
```
