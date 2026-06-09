# Platform Matrix

Platform support starts from the first implementation phases. Feature support may be skipped only when the original transport is unavailable on that platform, and skips must be explicit.

## Required Platforms

| Platform | Default Features | Full Features | Notes |
| --- | --- | --- | --- |
| Linux | required | required where system dependencies exist | TIPC, VSOCK, OpenPGM, NORM, GSSAPI are validated here first. |
| macOS | required | required where system dependencies exist | kqueue and Unix domain sockets are required. |
| Windows | required | required where system dependencies exist | WinSock, DLL exports, and AF_UNIX support are required. |

## Build Profiles

Default validation:

```sh
cargo test --workspace
```

Full feature validation once modules exist:

```sh
cargo test --workspace --all-features
```

Differential runner smoke test:

```sh
cargo run -p libzmq-test-harness --bin differential-runner
```
