# Windows Export Validation

Phase 8 requires validating that the Windows DLL exports the `zmq.h` C ABI symbols.

This environment cannot run the validation because the Windows Rust target is not installed and `rustup` is unavailable. On a Windows-capable runner, use:

```sh
cargo build -p libzmq-ffi --target x86_64-pc-windows-msvc
dumpbin /exports target\x86_64-pc-windows-msvc\debug\zmq.dll
```

The exported names must match `include/zmq.h`, including stable, deprecated, and draft symbols enabled for the build.
