# Differential Trace Format

Differential tests compare observable behavior from original C++ `libzmq` and Rust `ru-libzmq`.

The trace format is JSON Lines. Each line contains one event and can be streamed from a runner.

## Event Types

Case header:

```json
{"case":"version"}
```

Operation event:

```json
{"operation":{"type":"context_new"}}
```

Observation event:

```json
{"observation":{"type":"return_code","rc":0}}
```

## Required Observation Fields

- Return code.
- `zmq_errno()` value when return code indicates failure.
- Message frame bytes for send/recv cases.
- Multipart flags.
- Monitor event ids and values.
- Timeout class rather than exact wall-clock time.

## Current Runner

The initial Rust runner is intentionally small and only emits version/context/socket traces:

```sh
cargo run -p ru-libzmq-test-harness --bin differential-runner
```

Original C++ message oracle invocation after building `../libzmq/build-ru-oracle/lib/libzmq.dylib`:

```sh
cargo run -p ru-libzmq-test-harness --bin cpp-message-oracle
```

Set `LIBZMQ_ORACLE=/path/to/libzmq.dylib` to use a different original library. Future work will compare normalized trace files automatically.

Rust message oracle invocation:

```sh
cargo run -p ru-libzmq-test-harness --bin rust-message-oracle
```

Automated message oracle comparison:

```sh
cargo build -p ru-libzmq-test-harness --bins
cargo run -p ru-libzmq-test-harness --bin compare-message-oracles
```
