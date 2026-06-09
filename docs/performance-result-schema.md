# Performance Result Schema

Performance gates compare this Rust implementation against original C++ `libzmq`.

Latency passes when:

```text
rust_median <= cpp_median * 1.05
```

Throughput passes when:

```text
rust_median >= cpp_median * 0.95
```

## JSON Schema Shape

```json
{
  "impl": "cpp|rust",
  "git_rev": "unknown",
  "build_type": "release",
  "compiler": "unknown",
  "os": "macos|linux|windows",
  "arch": "x86_64|aarch64|unknown",
  "test": "inproc_lat|inproc_thr|local_lat|remote_lat|local_thr|remote_thr|proxy_thr|subscription_lookup",
  "transport": "inproc|tcp|ipc|ws|wss|udp",
  "socket_pattern": "reqrep|pushpull|pubsub|xpubxsub|dealerrouter|pair",
  "message_size": 64,
  "message_count": 1000000,
  "unit": "usec|msg_per_sec|mbit_per_sec|ns",
  "samples": [1.0, 2.0, 3.0],
  "median": 2.0,
  "p90": 3.0,
  "stddev_percent": 1.2
}
```

## P0 Cases

- inproc latency: 64 B and 1024 B.
- inproc throughput: 64 B and 1024 B.
- tcp latency: 64 B and 1024 B.
- tcp throughput: 64 B, 1024 B, and 65536 B.
- proxy throughput: 64 B and 1024 B.
- subscription lookup.
