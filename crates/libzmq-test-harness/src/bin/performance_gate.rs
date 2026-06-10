#![allow(clippy::undocumented_unsafe_blocks)]

// This benchmark runner dynamically loads the original C++ libzmq C ABI, then
// compares it with the Rust implementation through the same C ABI surface.

use std::env;
use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::fs;
use std::mem;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::ptr;
use std::time::{Duration, Instant};

const RTLD_NOW: c_int = 2;
const ZMQ_PAIR: c_int = 0;
const ZMQ_PUB: c_int = 1;
const ZMQ_SUB: c_int = 2;
const ZMQ_PULL: c_int = 7;
const ZMQ_PUSH: c_int = 8;
const ZMQ_SUBSCRIBE: c_int = 6;
const ZMQ_LINGER: c_int = 17;
const ZMQ_RCVTIMEO: c_int = 27;
const ZMQ_SNDTIMEO: c_int = 28;
const ZMQ_CURVE_SERVER: c_int = 47;
const ZMQ_CURVE_PUBLICKEY: c_int = 48;
const ZMQ_CURVE_SECRETKEY: c_int = 49;
const ZMQ_CURVE_SERVERKEY: c_int = 50;
const ZMQ_WSS_KEY_PEM: c_int = 103;
const ZMQ_WSS_CERT_PEM: c_int = 104;
const ZMQ_WSS_TRUST_PEM: c_int = 105;
const ZMQ_WSS_HOSTNAME: c_int = 106;
const ZMQ_DONTWAIT: c_int = 1;
const EAGAIN: c_int = 11;
const OS_EAGAIN: c_int = 35;

const WSS_HOSTNAME: &[u8] = b"zeromq.org";
const WSS_KEY_PEM: &[u8] = br#"-----BEGIN PRIVATE KEY-----
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
lMwbKi/uEdrUT3ei3nAxnuQolXgClZk=
-----END PRIVATE KEY-----"#;

const WSS_CERT_PEM: &[u8] = br#"-----BEGIN CERTIFICATE-----
MIIFkTCCA3mgAwIBAgIUWazS3jRxgV/9TgdybZ9ch7nYsQIwDQYJKoZIhvcNAQEL
BQAwVzELMAkGA1UEBhMCWFgxFTATBgNVBAcMDERlZmF1bHQgQ2l0eTEcMBoGA1UE
CgwTRGVmYXVsdCBDb21wYW55IEx0ZDETMBEGA1UEAwwKemVyb21xLm9yZzAgFw0x
OTExMTAwODMzMThaGA8yMTE5MTAxNzA4MzMxOFowVzELMAkGA1UEBhMCWFgxFTAT
BgNVBAcMDERlZmF1bHQgQ2l0eTEcMBoGA1UECgwTRGVmYXVsdCBDb21wYW55IEx0
ZDETMBEGA1UEAwwKemVyb21xLm9yZzCCAiIwDQYJKoZIhvcNAQEBBQADggIPADCC
AgoCggIBAOfOLOaorV7SJFGVjvnP2rdNZUHMVhfTRGXXwZPaWJboBjM4H+prnkSG
NN/iyeELYVpSiiy7gxhF2+jyoE6kW+dANJwnqjTovKtbIvXmSQFgSxOSOYMRd79P
FRUTYgFv33WbUX3VIgSMrmY1EfZpGOs3mYv1/z4Kuu/3pjizEYIOSrA5qtWIVsNg
XF1hrtau9kiamSeuYzko/KA+1T2CqeUiABT1s9MZQkZ1B5D4BYo6DDq6dXwiIoRf
yPIcGgqICpOylEIJX3IM4rF5VQaCRwAyl5K/wdVug1t+h/wMj55PwS1g2xZbh0g2
CJhgQZkiHLvhfZ0quJrqEJwBh1L7/3Sd/REDLKaeIO2BMaswhcWaOVikueCp8Cj7
POXbV0aHdWdFQVtNqTwIfiDBgh58kV6vAk/WKbmMCJQtYdqI46Tavl8d3FsEt5up
xpYFFIYY0T1yPhuP+pi4cB9Ws1bn4I3ePSHmyz9/yU6gNtutvoa1Il8jXp+5Bl3a
Fbzi7fwS5V3X+bNtUy/sGKnLONsQBw6CfQyTz3Nl7CijPxLGvZ7VHjv/kIIcmh1Q
WpzmGZyc6pF3tiakzrxtLtUpuc5jfD0uXDq2RcyK2a1SzsiIpnVWJcDdTMxK6a8Q
koyo6d80RQf2mSph7aVVtrLUdKT+HPsCSmtU8yWqUXXbH/v3h+D3AgMBAAGjUzBR
MB0GA1UdDgQWBBTVHR+4lBIBcr2rEEMdideTAkwDZDAfBgNVHSMEGDAWgBTVHR+4
lBIBcr2rEEMdideTAkwDZDAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUA
A4ICAQB9M5p9z92UhVXg2baUj9QBN2YFGAeveFRpZ9Y/wktEssTqMKkc+39UtfJS
UclZnzMEhLyTieNqf+8GCgLLI0YSTIJpWwzvQBcXPoo+8IcexANvxR22KZrE51y4
/YfCKh8Q0ZbG8oc5Br8YHwipzGcmWjWtznfMpuaELezv/r381QV1Sbmpw2a0jvwp
uA/bc+4IZ9yvrhC9KkOUnivnCV71U2Wy8zOvrBEJicuGOc+lbWJRKyjbMDi1IybG
VnemtkQEyXFh6f1h8AdaN+Xj7qXX/YKmNk20Siu4qDNo8nozVpOL2DHjoKLa4N2c
ULG3kXScxVxWqCuPVNeypV2TZ9uSVFeKK/VJ5iGvFeifDsqVVo6WC4Pdz0WYes8H
3VqEPSwmNJ1FswLpYpGgCEFnkGRPFFB5dmwr0fuubkgaJPatxrImFac+nukfqZ8N
x/d4t72u1yIs0HnrkAj96ZIUXH5jFGPXbD8eGO0hzw+wbY9KRLXGBBl2B4EAaBdt
Cmp8R8xus3FGDZ5RVftZvTQO2CiTC4yn9Wab/ADDwcXDs6ntHctx4xQpm0tLqMoz
BTH8+ftqyzklar35gJluD84oh1kynEojrPkUvb75NlzxikBSF3pRrOx30Myy7DBx
rhUIqDFxqlYFx9InEzHlvI7cWWdMNqAmSxpz4SXMrd/7PJG+Ag==
-----END CERTIFICATE-----"#;

const LATENCY_LIMIT: f64 = 1.05;
const THROUGHPUT_LIMIT: f64 = 0.95;

type ZmqCtxNew = unsafe extern "C" fn() -> *mut c_void;
type ZmqCtxTerm = unsafe extern "C" fn(*mut c_void) -> c_int;
type ZmqSocket = unsafe extern "C" fn(*mut c_void, c_int) -> *mut c_void;
type ZmqClose = unsafe extern "C" fn(*mut c_void) -> c_int;
type ZmqBind = unsafe extern "C" fn(*mut c_void, *const c_char) -> c_int;
type ZmqConnect = unsafe extern "C" fn(*mut c_void, *const c_char) -> c_int;
type ZmqSend = unsafe extern "C" fn(*mut c_void, *const c_void, usize, c_int) -> c_int;
type ZmqRecv = unsafe extern "C" fn(*mut c_void, *mut c_void, usize, c_int) -> c_int;
type ZmqSetsockopt = unsafe extern "C" fn(*mut c_void, c_int, *const c_void, usize) -> c_int;
type ZmqErrno = unsafe extern "C" fn() -> c_int;

unsafe extern "C" {
    fn dlopen(filename: *const c_char, flags: c_int) -> *mut c_void;
    fn dlsym(handle: *mut c_void, symbol: *const c_char) -> *mut c_void;
    fn dlclose(handle: *mut c_void) -> c_int;
}

#[derive(Debug, Clone, Copy)]
enum Transport {
    Inproc,
    Tcp,
    #[cfg(unix)]
    Ipc,
}

#[derive(Debug, Clone, Copy)]
enum Metric {
    Latency,
    Throughput,
}

#[derive(Debug, Clone, Copy)]
enum Case {
    PairLatency(Transport),
    PushPullThroughput(Transport),
    ProxyThroughput,
    SubscriptionLookup,
    CurveThroughput,
    WsThroughput,
    WssThroughput,
}

#[derive(Debug)]
enum CaseStatus {
    Pass,
    Fail,
    Skipped(String),
}

#[derive(Debug)]
struct Args {
    oracle: PathBuf,
    write_path: Option<PathBuf>,
    iterations: usize,
    samples: usize,
}

#[derive(Debug, Clone, Copy)]
struct CppZmq {
    ctx_new: ZmqCtxNew,
    ctx_term: ZmqCtxTerm,
    socket: ZmqSocket,
    close: ZmqClose,
    bind: ZmqBind,
    connect: ZmqConnect,
    send: ZmqSend,
    recv: ZmqRecv,
    setsockopt: ZmqSetsockopt,
    zmq_errno: ZmqErrno,
}

#[derive(Debug)]
struct CaseResult {
    name: String,
    metric: Metric,
    cpp_median: Option<f64>,
    rust_median: Option<f64>,
    status: CaseStatus,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("performance-gate: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let args = parse_args()?;
    let oracle = CString::new(args.oracle.to_string_lossy().as_bytes())
        .map_err(|_| "oracle path contains interior nul".to_string())?;
    let handle = unsafe { dlopen(oracle.as_ptr(), RTLD_NOW) };
    if handle.is_null() {
        return Err(format!(
            "failed to load oracle at {}",
            args.oracle.display()
        ));
    }

    let result = unsafe { run_with_oracle(handle, &args) };
    unsafe {
        dlclose(handle);
    }
    result
}

unsafe fn run_with_oracle(handle: *mut c_void, args: &Args) -> Result<(), String> {
    let cpp = CppZmq {
        ctx_new: unsafe { load_symbol(handle, "zmq_ctx_new")? },
        ctx_term: unsafe { load_symbol(handle, "zmq_ctx_term")? },
        socket: unsafe { load_symbol(handle, "zmq_socket")? },
        close: unsafe { load_symbol(handle, "zmq_close")? },
        bind: unsafe { load_symbol(handle, "zmq_bind")? },
        connect: unsafe { load_symbol(handle, "zmq_connect")? },
        send: unsafe { load_symbol(handle, "zmq_send")? },
        recv: unsafe { load_symbol(handle, "zmq_recv")? },
        setsockopt: unsafe { load_symbol(handle, "zmq_setsockopt")? },
        zmq_errno: unsafe { load_symbol(handle, "zmq_errno")? },
    };

    let mut results = Vec::new();
    for case in cases() {
        results.push(run_case(&cpp, case, args)?);
    }

    let report = render_report(args, &results);
    if let Some(path) = &args.write_path {
        let path = workspace_root()?.join(path);
        fs::write(&path, report).map_err(|error| format!("write {}: {error}", path.display()))?;
    } else {
        print!("{report}");
    }

    let failures = results
        .iter()
        .filter(|result| matches!(result.status, CaseStatus::Fail))
        .count();
    if failures == 0 {
        Ok(())
    } else {
        Err(format!("{failures} performance cases failed the 5% gate"))
    }
}

fn parse_args() -> Result<Args, String> {
    let mut oracle = env::var_os("LIBZMQ_ORACLE")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("../libzmq/build-ru-oracle/lib/libzmq.dylib"));
    let mut write_path = None;
    let mut iterations = 1_000usize;
    let mut samples = 5usize;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--oracle" => {
                oracle = PathBuf::from(args.next().ok_or("--oracle requires a path")?);
            }
            "--write" => {
                write_path = Some(PathBuf::from(args.next().ok_or("--write requires a path")?));
            }
            "--iterations" => {
                iterations = args
                    .next()
                    .ok_or("--iterations requires a value")?
                    .parse()
                    .map_err(|_| "invalid --iterations value".to_string())?;
            }
            "--samples" => {
                samples = args
                    .next()
                    .ok_or("--samples requires a value")?
                    .parse()
                    .map_err(|_| "invalid --samples value".to_string())?;
            }
            _ => {
                return Err(
                    "usage: performance-gate [--oracle <path>] [--write <path>] [--iterations <n>] [--samples <n>]"
                        .to_string(),
                );
            }
        }
    }
    if iterations == 0 || samples == 0 {
        return Err("iterations and samples must be nonzero".to_string());
    }
    Ok(Args {
        oracle,
        write_path,
        iterations,
        samples,
    })
}

fn cases() -> Vec<Case> {
    let mut transports = vec![Transport::Inproc, Transport::Tcp];
    #[cfg(unix)]
    transports.push(Transport::Ipc);
    let mut cases = Vec::new();
    for transport in transports {
        cases.push(Case::PairLatency(transport));
        cases.push(Case::PushPullThroughput(transport));
    }
    cases.extend([
        Case::ProxyThroughput,
        Case::SubscriptionLookup,
        Case::CurveThroughput,
        Case::WsThroughput,
        Case::WssThroughput,
    ]);
    cases
}

fn run_case(cpp: &CppZmq, case: Case, args: &Args) -> Result<CaseResult, String> {
    let metric = case.metric();
    let mut cpp_samples = Vec::with_capacity(args.samples);
    let mut rust_samples = Vec::with_capacity(args.samples);
    for sample in 0..args.samples {
        let seed = format!("{}-{sample}-{}", std::process::id(), monotonic_suffix());
        match unsafe { run_cpp_sample(cpp, case, args.iterations, &seed) } {
            Ok(sample) => cpp_samples.push(sample),
            Err(error) if case.optional() => return Ok(case.skipped(error)),
            Err(error) => return Err(error),
        }
        match run_rust_sample(case, args.iterations, &seed) {
            Ok(sample) => rust_samples.push(sample),
            Err(error) if case.optional() => return Ok(case.skipped(error)),
            Err(error) => return Err(error),
        }
    }
    let cpp_median = median(&mut cpp_samples);
    let rust_median = median(&mut rust_samples);
    let pass = match metric {
        Metric::Latency => rust_median <= cpp_median * LATENCY_LIMIT,
        Metric::Throughput => rust_median >= cpp_median * THROUGHPUT_LIMIT,
    };
    Ok(CaseResult {
        name: case.name(),
        metric,
        cpp_median: Some(cpp_median),
        rust_median: Some(rust_median),
        status: if pass {
            CaseStatus::Pass
        } else {
            CaseStatus::Fail
        },
    })
}

impl Case {
    fn name(self) -> String {
        match self {
            Self::PairLatency(transport) => format!("{:?} Latency", transport),
            Self::PushPullThroughput(transport) => format!("{:?} Throughput", transport),
            Self::ProxyThroughput => "Proxy Throughput".to_string(),
            Self::SubscriptionLookup => "Subscription Lookup".to_string(),
            Self::CurveThroughput => "CURVE Throughput".to_string(),
            Self::WsThroughput => "WS Throughput".to_string(),
            Self::WssThroughput => "WSS Throughput".to_string(),
        }
    }

    fn metric(self) -> Metric {
        match self {
            Self::PairLatency(_) => Metric::Latency,
            Self::PushPullThroughput(_)
            | Self::ProxyThroughput
            | Self::SubscriptionLookup
            | Self::CurveThroughput
            | Self::WsThroughput
            | Self::WssThroughput => Metric::Throughput,
        }
    }

    fn optional(self) -> bool {
        !matches!(self, Self::PairLatency(_) | Self::PushPullThroughput(_))
    }

    fn skipped(self, reason: String) -> CaseResult {
        CaseResult {
            name: self.name(),
            metric: self.metric(),
            cpp_median: None,
            rust_median: None,
            status: CaseStatus::Skipped(reason),
        }
    }
}

fn run_rust_sample(case: Case, iterations: usize, seed: &str) -> Result<f64, String> {
    match case {
        Case::PairLatency(transport) => run_rust_pair_latency(transport, iterations, seed),
        Case::PushPullThroughput(transport) => {
            run_rust_push_pull_throughput(transport, iterations, seed)
        }
        Case::ProxyThroughput => run_rust_proxy_throughput(iterations, seed),
        Case::SubscriptionLookup => run_rust_subscription_lookup(iterations, seed),
        Case::CurveThroughput => run_rust_curve_throughput(iterations, seed),
        Case::WsThroughput => run_rust_pair_throughput_endpoint(
            &CString::new(format!("ws://127.0.0.1:{}/zmq", unused_tcp_port()))
                .map_err(|_| "ws endpoint contains interior nul".to_string())?,
            iterations,
            Transport::Tcp,
        ),
        Case::WssThroughput => run_rust_wss_throughput(
            &CString::new(format!("wss://127.0.0.1:{}/zmq", unused_tcp_port()))
                .map_err(|_| "wss endpoint contains interior nul".to_string())?,
            iterations,
        ),
    }
}

fn run_rust_pair_latency(
    transport: Transport,
    iterations: usize,
    seed: &str,
) -> Result<f64, String> {
    let ctx = zmq::zmq_ctx_new();
    if ctx.is_null() {
        return Err("rust ctx_new failed".to_string());
    }
    let server = zmq::zmq_socket(ctx, ZMQ_PAIR);
    let client = zmq::zmq_socket(ctx, ZMQ_PAIR);
    configure_rust_socket(server)?;
    configure_rust_socket(client)?;
    let endpoint = endpoint(transport, seed)?;
    bind_connect_rust(server, client, &endpoint, transport)?;
    rust_send(client, b"warmup")?;
    rust_recv(server)?;
    rust_send(server, b"warmup")?;
    rust_recv(client)?;
    let started = Instant::now();
    for _ in 0..iterations {
        rust_send(client, b"ping")?;
        rust_recv(server)?;
        rust_send(server, b"pong")?;
        rust_recv(client)?;
    }
    let elapsed = started.elapsed();
    close_rust(ctx, &[client, server]);
    Ok(elapsed.as_secs_f64() * 1_000_000_000.0 / (iterations as f64 * 2.0))
}

fn run_rust_push_pull_throughput(
    transport: Transport,
    iterations: usize,
    seed: &str,
) -> Result<f64, String> {
    let ctx = zmq::zmq_ctx_new();
    if ctx.is_null() {
        return Err("rust ctx_new failed".to_string());
    }
    let pull = zmq::zmq_socket(ctx, ZMQ_PULL);
    let push = zmq::zmq_socket(ctx, ZMQ_PUSH);
    configure_rust_socket(pull)?;
    configure_rust_socket(push)?;
    let endpoint = endpoint(transport, seed)?;
    bind_connect_rust(pull, push, &endpoint, transport)?;
    rust_send(push, b"warmup")?;
    rust_recv(pull)?;
    let started = Instant::now();
    for _ in 0..iterations {
        rust_send(push, b"payload")?;
        rust_recv(pull)?;
    }
    let elapsed = started.elapsed();
    close_rust(ctx, &[push, pull]);
    Ok(iterations as f64 / elapsed.as_secs_f64())
}

fn run_rust_pair_throughput_endpoint(
    endpoint: &CString,
    iterations: usize,
    transport: Transport,
) -> Result<f64, String> {
    let ctx = zmq::zmq_ctx_new();
    if ctx.is_null() {
        return Err("rust ctx_new failed".to_string());
    }
    let server = zmq::zmq_socket(ctx, ZMQ_PAIR);
    let client = zmq::zmq_socket(ctx, ZMQ_PAIR);
    configure_rust_socket(server)?;
    configure_rust_socket(client)?;
    bind_connect_rust(server, client, endpoint, transport)?;
    rust_send(client, b"warmup")?;
    rust_recv(server)?;
    let started = Instant::now();
    for _ in 0..iterations {
        rust_send(client, b"payload")?;
        rust_recv(server)?;
    }
    let elapsed = started.elapsed();
    close_rust(ctx, &[client, server]);
    Ok(iterations as f64 / elapsed.as_secs_f64())
}

fn run_rust_wss_throughput(endpoint: &CString, iterations: usize) -> Result<f64, String> {
    let ctx = zmq::zmq_ctx_new();
    if ctx.is_null() {
        return Err("rust ctx_new failed".to_string());
    }
    let server = zmq::zmq_socket(ctx, ZMQ_PAIR);
    let client = zmq::zmq_socket(ctx, ZMQ_PAIR);
    configure_rust_socket(server)?;
    configure_rust_socket(client)?;
    bind_connect_rust(server, client, endpoint, Transport::Tcp)?;
    let server_addr = server as usize;
    let receiver = std::thread::spawn(move || {
        let server = server_addr as *mut c_void;
        rust_recv(server)?;
        Ok::<(), String>(())
    });
    rust_send(client, b"warmup")?;
    receiver
        .join()
        .map_err(|_| "rust wss receiver panicked".to_string())??;
    let started = Instant::now();
    for _ in 0..iterations {
        rust_send(client, b"payload")?;
        rust_recv(server)?;
    }
    let elapsed = started.elapsed();
    close_rust(ctx, &[client, server]);
    Ok(iterations as f64 / elapsed.as_secs_f64())
}

fn run_rust_proxy_throughput(iterations: usize, seed: &str) -> Result<f64, String> {
    let ctx = zmq::zmq_ctx_new();
    if ctx.is_null() {
        return Err("rust ctx_new failed".to_string());
    }
    let source = zmq::zmq_socket(ctx, ZMQ_PUSH);
    let frontend = zmq::zmq_socket(ctx, ZMQ_PULL);
    let backend = zmq::zmq_socket(ctx, ZMQ_PUSH);
    let sink = zmq::zmq_socket(ctx, ZMQ_PULL);
    for socket in [source, frontend, backend, sink] {
        configure_rust_socket(socket)?;
    }
    let frontend_endpoint = CString::new(format!("inproc://perf-proxy-front-{seed}"))
        .map_err(|_| "proxy frontend endpoint contains interior nul".to_string())?;
    let backend_endpoint = CString::new(format!("inproc://perf-proxy-back-{seed}"))
        .map_err(|_| "proxy backend endpoint contains interior nul".to_string())?;
    bind_connect_rust(frontend, source, &frontend_endpoint, Transport::Inproc)?;
    bind_connect_rust(sink, backend, &backend_endpoint, Transport::Inproc)?;
    rust_proxy_once(source, frontend, backend, sink)?;
    let started = Instant::now();
    for _ in 0..iterations {
        rust_proxy_once(source, frontend, backend, sink)?;
    }
    let elapsed = started.elapsed();
    close_rust(ctx, &[source, frontend, backend, sink]);
    Ok(iterations as f64 / elapsed.as_secs_f64())
}

fn rust_proxy_once(
    source: *mut c_void,
    frontend: *mut c_void,
    backend: *mut c_void,
    sink: *mut c_void,
) -> Result<(), String> {
    let mut buffer = [0u8; 256];
    rust_send(source, b"payload")?;
    let len = rust_recv_into(frontend, &mut buffer)?;
    rust_send(backend, &buffer[..len])?;
    rust_recv(sink)?;
    Ok(())
}

fn run_rust_subscription_lookup(iterations: usize, seed: &str) -> Result<f64, String> {
    let ctx = zmq::zmq_ctx_new();
    if ctx.is_null() {
        return Err("rust ctx_new failed".to_string());
    }
    let publisher = zmq::zmq_socket(ctx, ZMQ_PUB);
    let subscriber = zmq::zmq_socket(ctx, ZMQ_SUB);
    configure_rust_socket(publisher)?;
    configure_rust_socket(subscriber)?;
    for prefix in subscription_prefixes() {
        rust_set_bytes(subscriber, ZMQ_SUBSCRIBE, prefix.as_bytes())?;
    }
    let endpoint = CString::new(format!("inproc://perf-sub-{seed}"))
        .map_err(|_| "subscription endpoint contains interior nul".to_string())?;
    bind_connect_rust(publisher, subscriber, &endpoint, Transport::Inproc)?;
    std::thread::sleep(Duration::from_millis(50));
    wait_rust_subscription(publisher, subscriber)?;
    let started = Instant::now();
    for _ in 0..iterations {
        rust_send(publisher, b"topic-127 payload")?;
        rust_recv(subscriber)?;
    }
    let elapsed = started.elapsed();
    close_rust(ctx, &[publisher, subscriber]);
    Ok(iterations as f64 / elapsed.as_secs_f64())
}

fn wait_rust_subscription(publisher: *mut c_void, subscriber: *mut c_void) -> Result<(), String> {
    for _ in 0..1_000 {
        rust_send(publisher, b"topic-127 warmup")?;
        match rust_recv_dontwait(subscriber)? {
            Some(_) => return Ok(()),
            None => std::thread::sleep(Duration::from_millis(1)),
        }
    }
    Err("rust subscription warmup timed out".to_string())
}

fn run_rust_curve_throughput(iterations: usize, seed: &str) -> Result<f64, String> {
    let (server_public, server_secret) = rust_curve_keypair()?;
    let (client_public, client_secret) = rust_curve_keypair()?;
    let ctx = zmq::zmq_ctx_new();
    if ctx.is_null() {
        return Err("rust ctx_new failed".to_string());
    }
    let server = zmq::zmq_socket(ctx, ZMQ_PAIR);
    let client = zmq::zmq_socket(ctx, ZMQ_PAIR);
    configure_rust_socket(server)?;
    configure_rust_socket(client)?;
    configure_rust_curve_server(server, &server_secret, &client_public)?;
    configure_rust_curve_client(client, &client_public, &client_secret, &server_public)?;
    let endpoint = endpoint(Transport::Tcp, seed)?;
    bind_connect_rust(server, client, &endpoint, Transport::Tcp)?;
    let server_addr = server as usize;
    let receiver = std::thread::spawn(move || {
        let server = server_addr as *mut c_void;
        rust_recv(server)?;
        Ok::<(), String>(())
    });
    rust_send(client, b"warmup")?;
    receiver
        .join()
        .map_err(|_| "rust curve receiver panicked".to_string())??;
    let started = Instant::now();
    for _ in 0..iterations {
        rust_send(client, b"payload")?;
        rust_recv(server)?;
    }
    let elapsed = started.elapsed();
    close_rust(ctx, &[client, server]);
    Ok(iterations as f64 / elapsed.as_secs_f64())
}

unsafe fn run_cpp_sample(
    cpp: &CppZmq,
    case: Case,
    iterations: usize,
    seed: &str,
) -> Result<f64, String> {
    match case {
        Case::PairLatency(transport) => unsafe {
            run_cpp_pair_latency(cpp, transport, iterations, seed)
        },
        Case::PushPullThroughput(transport) => unsafe {
            run_cpp_push_pull_throughput(cpp, transport, iterations, seed)
        },
        Case::ProxyThroughput => unsafe { run_cpp_proxy_throughput(cpp, iterations, seed) },
        Case::SubscriptionLookup => unsafe { run_cpp_subscription_lookup(cpp, iterations, seed) },
        Case::CurveThroughput => unsafe { run_cpp_curve_throughput(cpp, iterations, seed) },
        Case::WsThroughput => unsafe {
            run_cpp_pair_throughput_endpoint(
                cpp,
                &CString::new(format!("ws://127.0.0.1:{}/zmq", unused_tcp_port()))
                    .map_err(|_| "ws endpoint contains interior nul".to_string())?,
                iterations,
                Transport::Tcp,
            )
        },
        Case::WssThroughput => unsafe {
            run_cpp_wss_throughput(
                cpp,
                &CString::new(format!("wss://127.0.0.1:{}/zmq", unused_tcp_port()))
                    .map_err(|_| "wss endpoint contains interior nul".to_string())?,
                iterations,
            )
        },
    }
}

unsafe fn run_cpp_pair_latency(
    cpp: &CppZmq,
    transport: Transport,
    iterations: usize,
    seed: &str,
) -> Result<f64, String> {
    let ctx = unsafe { (cpp.ctx_new)() };
    let server = unsafe { (cpp.socket)(ctx, ZMQ_PAIR) };
    let client = unsafe { (cpp.socket)(ctx, ZMQ_PAIR) };
    unsafe {
        configure_cpp_socket(cpp, server)?;
        configure_cpp_socket(cpp, client)?;
    }
    let endpoint = endpoint(transport, seed)?;
    unsafe { bind_connect_cpp(cpp, server, client, &endpoint, transport)? };
    unsafe { cpp_send(cpp, client, b"warmup")? };
    unsafe { cpp_recv(cpp, server)? };
    unsafe { cpp_send(cpp, server, b"warmup")? };
    unsafe { cpp_recv(cpp, client)? };
    let started = Instant::now();
    for _ in 0..iterations {
        unsafe { cpp_send(cpp, client, b"ping")? };
        unsafe { cpp_recv(cpp, server)? };
        unsafe { cpp_send(cpp, server, b"pong")? };
        unsafe { cpp_recv(cpp, client)? };
    }
    let elapsed = started.elapsed();
    unsafe { close_cpp(cpp, ctx, &[client, server]) };
    Ok(elapsed.as_secs_f64() * 1_000_000_000.0 / (iterations as f64 * 2.0))
}

unsafe fn run_cpp_push_pull_throughput(
    cpp: &CppZmq,
    transport: Transport,
    iterations: usize,
    seed: &str,
) -> Result<f64, String> {
    let ctx = unsafe { (cpp.ctx_new)() };
    let pull = unsafe { (cpp.socket)(ctx, ZMQ_PULL) };
    let push = unsafe { (cpp.socket)(ctx, ZMQ_PUSH) };
    unsafe {
        configure_cpp_socket(cpp, pull)?;
        configure_cpp_socket(cpp, push)?;
    }
    let endpoint = endpoint(transport, seed)?;
    unsafe { bind_connect_cpp(cpp, pull, push, &endpoint, transport)? };
    unsafe { cpp_send(cpp, push, b"warmup")? };
    unsafe { cpp_recv(cpp, pull)? };
    let started = Instant::now();
    for _ in 0..iterations {
        unsafe { cpp_send(cpp, push, b"payload")? };
        unsafe { cpp_recv(cpp, pull)? };
    }
    let elapsed = started.elapsed();
    unsafe { close_cpp(cpp, ctx, &[push, pull]) };
    Ok(iterations as f64 / elapsed.as_secs_f64())
}

unsafe fn run_cpp_pair_throughput_endpoint(
    cpp: &CppZmq,
    endpoint: &CString,
    iterations: usize,
    transport: Transport,
) -> Result<f64, String> {
    let ctx = unsafe { (cpp.ctx_new)() };
    let server = unsafe { (cpp.socket)(ctx, ZMQ_PAIR) };
    let client = unsafe { (cpp.socket)(ctx, ZMQ_PAIR) };
    unsafe {
        configure_cpp_socket(cpp, server)?;
        configure_cpp_socket(cpp, client)?;
        bind_connect_cpp(cpp, server, client, endpoint, transport)?;
        cpp_send(cpp, client, b"warmup")?;
        cpp_recv(cpp, server)?;
    }
    let started = Instant::now();
    for _ in 0..iterations {
        unsafe {
            cpp_send(cpp, client, b"payload")?;
            cpp_recv(cpp, server)?;
        }
    }
    let elapsed = started.elapsed();
    unsafe { close_cpp(cpp, ctx, &[client, server]) };
    Ok(iterations as f64 / elapsed.as_secs_f64())
}

unsafe fn run_cpp_wss_throughput(
    cpp: &CppZmq,
    endpoint: &CString,
    iterations: usize,
) -> Result<f64, String> {
    let ctx = unsafe { (cpp.ctx_new)() };
    let server = unsafe { (cpp.socket)(ctx, ZMQ_PAIR) };
    let client = unsafe { (cpp.socket)(ctx, ZMQ_PAIR) };
    unsafe {
        configure_cpp_socket(cpp, server)?;
        configure_cpp_socket(cpp, client)?;
        configure_cpp_wss_server(cpp, server)?;
        configure_cpp_wss_client(cpp, client)?;
        bind_connect_cpp(cpp, server, client, endpoint, Transport::Tcp)?;
        cpp_send(cpp, client, b"warmup")?;
        cpp_recv(cpp, server)?;
    }
    let started = Instant::now();
    for _ in 0..iterations {
        unsafe {
            cpp_send(cpp, client, b"payload")?;
            cpp_recv(cpp, server)?;
        }
    }
    let elapsed = started.elapsed();
    unsafe { close_cpp(cpp, ctx, &[client, server]) };
    Ok(iterations as f64 / elapsed.as_secs_f64())
}

unsafe fn run_cpp_proxy_throughput(
    cpp: &CppZmq,
    iterations: usize,
    seed: &str,
) -> Result<f64, String> {
    let ctx = unsafe { (cpp.ctx_new)() };
    let source = unsafe { (cpp.socket)(ctx, ZMQ_PUSH) };
    let frontend = unsafe { (cpp.socket)(ctx, ZMQ_PULL) };
    let backend = unsafe { (cpp.socket)(ctx, ZMQ_PUSH) };
    let sink = unsafe { (cpp.socket)(ctx, ZMQ_PULL) };
    for socket in [source, frontend, backend, sink] {
        unsafe { configure_cpp_socket(cpp, socket)? };
    }
    let frontend_endpoint = CString::new(format!("inproc://perf-proxy-front-{seed}"))
        .map_err(|_| "proxy frontend endpoint contains interior nul".to_string())?;
    let backend_endpoint = CString::new(format!("inproc://perf-proxy-back-{seed}"))
        .map_err(|_| "proxy backend endpoint contains interior nul".to_string())?;
    unsafe {
        bind_connect_cpp(cpp, frontend, source, &frontend_endpoint, Transport::Inproc)?;
        bind_connect_cpp(cpp, sink, backend, &backend_endpoint, Transport::Inproc)?;
        cpp_proxy_once(cpp, source, frontend, backend, sink)?;
    }
    let started = Instant::now();
    for _ in 0..iterations {
        unsafe { cpp_proxy_once(cpp, source, frontend, backend, sink)? };
    }
    let elapsed = started.elapsed();
    unsafe { close_cpp(cpp, ctx, &[source, frontend, backend, sink]) };
    Ok(iterations as f64 / elapsed.as_secs_f64())
}

unsafe fn cpp_proxy_once(
    cpp: &CppZmq,
    source: *mut c_void,
    frontend: *mut c_void,
    backend: *mut c_void,
    sink: *mut c_void,
) -> Result<(), String> {
    let mut buffer = [0u8; 256];
    unsafe {
        cpp_send(cpp, source, b"payload")?;
        let len = cpp_recv_into(cpp, frontend, &mut buffer)?;
        cpp_send(cpp, backend, &buffer[..len])?;
        cpp_recv(cpp, sink)?;
    }
    Ok(())
}

unsafe fn run_cpp_subscription_lookup(
    cpp: &CppZmq,
    iterations: usize,
    seed: &str,
) -> Result<f64, String> {
    let ctx = unsafe { (cpp.ctx_new)() };
    let publisher = unsafe { (cpp.socket)(ctx, ZMQ_PUB) };
    let subscriber = unsafe { (cpp.socket)(ctx, ZMQ_SUB) };
    unsafe {
        configure_cpp_socket(cpp, publisher)?;
        configure_cpp_socket(cpp, subscriber)?;
    }
    for prefix in subscription_prefixes() {
        unsafe { cpp_set_bytes(cpp, subscriber, ZMQ_SUBSCRIBE, prefix.as_bytes())? };
    }
    let endpoint = CString::new(format!("inproc://perf-sub-{seed}"))
        .map_err(|_| "subscription endpoint contains interior nul".to_string())?;
    unsafe { bind_connect_cpp(cpp, publisher, subscriber, &endpoint, Transport::Inproc)? };
    std::thread::sleep(Duration::from_millis(100));
    unsafe {
        wait_cpp_subscription(cpp, publisher, subscriber)?;
    }
    let started = Instant::now();
    for _ in 0..iterations {
        unsafe {
            cpp_send(cpp, publisher, b"topic-127 payload")?;
            cpp_recv(cpp, subscriber)?;
        }
    }
    let elapsed = started.elapsed();
    unsafe { close_cpp(cpp, ctx, &[publisher, subscriber]) };
    Ok(iterations as f64 / elapsed.as_secs_f64())
}

unsafe fn wait_cpp_subscription(
    cpp: &CppZmq,
    publisher: *mut c_void,
    subscriber: *mut c_void,
) -> Result<(), String> {
    for _ in 0..1_000 {
        unsafe { cpp_send(cpp, publisher, b"topic-127 warmup")? };
        match unsafe { cpp_recv_dontwait(cpp, subscriber)? } {
            Some(_) => return Ok(()),
            None => std::thread::sleep(Duration::from_millis(1)),
        }
    }
    Err("cpp subscription warmup timed out".to_string())
}

unsafe fn run_cpp_curve_throughput(
    cpp: &CppZmq,
    iterations: usize,
    seed: &str,
) -> Result<f64, String> {
    let (server_public, server_secret) = rust_curve_keypair()?;
    let (client_public, client_secret) = rust_curve_keypair()?;
    let ctx = unsafe { (cpp.ctx_new)() };
    let server = unsafe { (cpp.socket)(ctx, ZMQ_PAIR) };
    let client = unsafe { (cpp.socket)(ctx, ZMQ_PAIR) };
    unsafe {
        configure_cpp_socket(cpp, server)?;
        configure_cpp_socket(cpp, client)?;
        configure_cpp_curve_server(cpp, server, &server_secret, &client_public)?;
        configure_cpp_curve_client(cpp, client, &client_public, &client_secret, &server_public)?;
    }
    let endpoint = endpoint(Transport::Tcp, seed)?;
    unsafe { bind_connect_cpp(cpp, server, client, &endpoint, Transport::Tcp)? };
    let cpp_for_thread = *cpp;
    let server_addr = server as usize;
    let receiver = std::thread::spawn(move || {
        let server = server_addr as *mut c_void;
        unsafe { cpp_recv(&cpp_for_thread, server)? };
        Ok::<(), String>(())
    });
    unsafe {
        cpp_send(cpp, client, b"warmup")?;
    }
    receiver
        .join()
        .map_err(|_| "cpp curve receiver panicked".to_string())??;
    let started = Instant::now();
    for _ in 0..iterations {
        unsafe {
            cpp_send(cpp, client, b"payload")?;
            cpp_recv(cpp, server)?;
        }
    }
    let elapsed = started.elapsed();
    unsafe { close_cpp(cpp, ctx, &[client, server]) };
    Ok(iterations as f64 / elapsed.as_secs_f64())
}

fn bind_connect_rust(
    bind_socket: *mut c_void,
    connect_socket: *mut c_void,
    endpoint: &CString,
    transport: Transport,
) -> Result<(), String> {
    if zmq::zmq_bind(bind_socket, endpoint.as_ptr()) != 0 {
        return Err(format!("rust bind failed errno {}", zmq::zmq_errno()));
    }
    if zmq::zmq_connect(connect_socket, endpoint.as_ptr()) != 0 {
        return Err(format!("rust connect failed errno {}", zmq::zmq_errno()));
    }
    settle_transport(transport);
    Ok(())
}

unsafe fn bind_connect_cpp(
    cpp: &CppZmq,
    bind_socket: *mut c_void,
    connect_socket: *mut c_void,
    endpoint: &CString,
    transport: Transport,
) -> Result<(), String> {
    if unsafe { (cpp.bind)(bind_socket, endpoint.as_ptr()) } != 0 {
        return Err(format!("cpp bind failed errno {}", unsafe {
            (cpp.zmq_errno)()
        }));
    }
    if unsafe { (cpp.connect)(connect_socket, endpoint.as_ptr()) } != 0 {
        return Err(format!("cpp connect failed errno {}", unsafe {
            (cpp.zmq_errno)()
        }));
    }
    settle_transport(transport);
    Ok(())
}

fn configure_rust_socket(socket: *mut c_void) -> Result<(), String> {
    if socket.is_null() {
        return Err("rust socket creation failed".to_string());
    }
    let linger = 0i32;
    let timeout = 2_000i32;
    for (option, value) in [
        (ZMQ_LINGER, linger),
        (ZMQ_RCVTIMEO, timeout),
        (ZMQ_SNDTIMEO, timeout),
    ] {
        if zmq::zmq_setsockopt(
            socket,
            option,
            (&value as *const i32).cast(),
            std::mem::size_of_val(&value),
        ) != 0
        {
            return Err(format!(
                "rust setsockopt {option} failed errno {}",
                zmq::zmq_errno()
            ));
        }
    }
    Ok(())
}

fn rust_set_i32(socket: *mut c_void, option: c_int, value: c_int) -> Result<(), String> {
    if zmq::zmq_setsockopt(
        socket,
        option,
        (&value as *const c_int).cast(),
        std::mem::size_of_val(&value),
    ) != 0
    {
        return Err(format!(
            "rust setsockopt {option} failed errno {}",
            zmq::zmq_errno()
        ));
    }
    Ok(())
}

fn rust_set_bytes(socket: *mut c_void, option: c_int, value: &[u8]) -> Result<(), String> {
    if zmq::zmq_setsockopt(socket, option, value.as_ptr().cast(), value.len()) != 0 {
        return Err(format!(
            "rust setsockopt {option} failed errno {}",
            zmq::zmq_errno()
        ));
    }
    Ok(())
}

fn configure_rust_curve_server(
    socket: *mut c_void,
    secret_key: &[u8],
    allowed_client_key: &[u8],
) -> Result<(), String> {
    rust_set_i32(socket, ZMQ_CURVE_SERVER, 1)?;
    rust_set_bytes(socket, ZMQ_CURVE_SECRETKEY, secret_key)?;
    rust_set_bytes(socket, ZMQ_CURVE_PUBLICKEY, allowed_client_key)
}

fn configure_rust_curve_client(
    socket: *mut c_void,
    public_key: &[u8],
    secret_key: &[u8],
    server_key: &[u8],
) -> Result<(), String> {
    rust_set_bytes(socket, ZMQ_CURVE_PUBLICKEY, public_key)?;
    rust_set_bytes(socket, ZMQ_CURVE_SECRETKEY, secret_key)?;
    rust_set_bytes(socket, ZMQ_CURVE_SERVERKEY, server_key)
}

unsafe fn configure_cpp_socket(cpp: &CppZmq, socket: *mut c_void) -> Result<(), String> {
    if socket.is_null() {
        return Err("cpp socket creation failed".to_string());
    }
    let linger = 0i32;
    let timeout = 2_000i32;
    for (option, value) in [
        (ZMQ_LINGER, linger),
        (ZMQ_RCVTIMEO, timeout),
        (ZMQ_SNDTIMEO, timeout),
    ] {
        if unsafe {
            (cpp.setsockopt)(
                socket,
                option,
                (&value as *const i32).cast(),
                std::mem::size_of_val(&value),
            )
        } != 0
        {
            return Err(format!("cpp setsockopt {option} failed errno {}", unsafe {
                (cpp.zmq_errno)()
            }));
        }
    }
    Ok(())
}

unsafe fn cpp_set_i32(
    cpp: &CppZmq,
    socket: *mut c_void,
    option: c_int,
    value: c_int,
) -> Result<(), String> {
    if unsafe {
        (cpp.setsockopt)(
            socket,
            option,
            (&value as *const c_int).cast(),
            std::mem::size_of_val(&value),
        )
    } != 0
    {
        return Err(format!("cpp setsockopt {option} failed errno {}", unsafe {
            (cpp.zmq_errno)()
        }));
    }
    Ok(())
}

unsafe fn cpp_set_bytes(
    cpp: &CppZmq,
    socket: *mut c_void,
    option: c_int,
    value: &[u8],
) -> Result<(), String> {
    if unsafe { (cpp.setsockopt)(socket, option, value.as_ptr().cast(), value.len()) } != 0 {
        return Err(format!("cpp setsockopt {option} failed errno {}", unsafe {
            (cpp.zmq_errno)()
        }));
    }
    Ok(())
}

unsafe fn configure_cpp_curve_server(
    cpp: &CppZmq,
    socket: *mut c_void,
    secret_key: &[u8],
    allowed_client_key: &[u8],
) -> Result<(), String> {
    unsafe {
        cpp_set_i32(cpp, socket, ZMQ_CURVE_SERVER, 1)?;
        cpp_set_bytes(cpp, socket, ZMQ_CURVE_SECRETKEY, secret_key)?;
        cpp_set_bytes(cpp, socket, ZMQ_CURVE_PUBLICKEY, allowed_client_key)
    }
}

unsafe fn configure_cpp_curve_client(
    cpp: &CppZmq,
    socket: *mut c_void,
    public_key: &[u8],
    secret_key: &[u8],
    server_key: &[u8],
) -> Result<(), String> {
    unsafe {
        cpp_set_bytes(cpp, socket, ZMQ_CURVE_PUBLICKEY, public_key)?;
        cpp_set_bytes(cpp, socket, ZMQ_CURVE_SECRETKEY, secret_key)?;
        cpp_set_bytes(cpp, socket, ZMQ_CURVE_SERVERKEY, server_key)
    }
}

unsafe fn configure_cpp_wss_server(cpp: &CppZmq, socket: *mut c_void) -> Result<(), String> {
    unsafe {
        cpp_set_bytes(cpp, socket, ZMQ_WSS_CERT_PEM, WSS_CERT_PEM)?;
        cpp_set_bytes(cpp, socket, ZMQ_WSS_KEY_PEM, WSS_KEY_PEM)
    }
}

unsafe fn configure_cpp_wss_client(cpp: &CppZmq, socket: *mut c_void) -> Result<(), String> {
    unsafe {
        cpp_set_bytes(cpp, socket, ZMQ_WSS_TRUST_PEM, WSS_CERT_PEM)?;
        cpp_set_bytes(cpp, socket, ZMQ_WSS_HOSTNAME, WSS_HOSTNAME)
    }
}

fn rust_send(socket: *mut c_void, payload: &[u8]) -> Result<(), String> {
    for _ in 0..3 {
        let rc = zmq::zmq_send(socket, payload.as_ptr().cast(), payload.len(), 0);
        if rc == payload.len() as c_int {
            return Ok(());
        }
        if !is_again(zmq::zmq_errno()) {
            return Err(format!(
                "rust send failed rc {rc} errno {}",
                zmq::zmq_errno()
            ));
        }
        std::thread::sleep(Duration::from_micros(50));
    }
    Err("rust send timed out".to_string())
}

fn rust_recv(socket: *mut c_void) -> Result<usize, String> {
    let mut buffer = [0u8; 256];
    rust_recv_into(socket, &mut buffer)
}

fn rust_recv_into(socket: *mut c_void, buffer: &mut [u8]) -> Result<usize, String> {
    for _ in 0..3 {
        let rc = zmq::zmq_recv(socket, buffer.as_mut_ptr().cast(), buffer.len(), 0);
        if rc >= 0 {
            return Ok(rc as usize);
        }
        if !is_again(zmq::zmq_errno()) {
            return Err(format!(
                "rust recv failed rc {rc} errno {}",
                zmq::zmq_errno()
            ));
        }
        std::thread::sleep(Duration::from_micros(50));
    }
    Err("rust recv timed out".to_string())
}

fn rust_recv_dontwait(socket: *mut c_void) -> Result<Option<usize>, String> {
    let mut buffer = [0u8; 256];
    let rc = zmq::zmq_recv(
        socket,
        buffer.as_mut_ptr().cast(),
        buffer.len(),
        ZMQ_DONTWAIT,
    );
    if rc >= 0 {
        return Ok(Some(rc as usize));
    }
    if is_again(zmq::zmq_errno()) {
        return Ok(None);
    }
    Err(format!(
        "rust recv failed rc {rc} errno {}",
        zmq::zmq_errno()
    ))
}

unsafe fn cpp_send(cpp: &CppZmq, socket: *mut c_void, payload: &[u8]) -> Result<(), String> {
    for _ in 0..3 {
        let rc = unsafe { (cpp.send)(socket, payload.as_ptr().cast(), payload.len(), 0) };
        if rc == payload.len() as c_int {
            return Ok(());
        }
        if !is_again(unsafe { (cpp.zmq_errno)() }) {
            return Err(format!("cpp send failed rc {rc} errno {}", unsafe {
                (cpp.zmq_errno)()
            }));
        }
        std::thread::sleep(Duration::from_micros(50));
    }
    Err("cpp send timed out".to_string())
}

unsafe fn cpp_recv(cpp: &CppZmq, socket: *mut c_void) -> Result<usize, String> {
    let mut buffer = [0u8; 256];
    unsafe { cpp_recv_into(cpp, socket, &mut buffer) }
}

unsafe fn cpp_recv_into(
    cpp: &CppZmq,
    socket: *mut c_void,
    buffer: &mut [u8],
) -> Result<usize, String> {
    for _ in 0..3 {
        let rc = unsafe { (cpp.recv)(socket, buffer.as_mut_ptr().cast(), buffer.len(), 0) };
        if rc >= 0 {
            return Ok(rc as usize);
        }
        if !is_again(unsafe { (cpp.zmq_errno)() }) {
            return Err(format!("cpp recv failed rc {rc} errno {}", unsafe {
                (cpp.zmq_errno)()
            }));
        }
        std::thread::sleep(Duration::from_micros(50));
    }
    Err("cpp recv timed out".to_string())
}

unsafe fn cpp_recv_dontwait(cpp: &CppZmq, socket: *mut c_void) -> Result<Option<usize>, String> {
    let mut buffer = [0u8; 256];
    let rc = unsafe {
        (cpp.recv)(
            socket,
            buffer.as_mut_ptr().cast(),
            buffer.len(),
            ZMQ_DONTWAIT,
        )
    };
    if rc >= 0 {
        return Ok(Some(rc as usize));
    }
    if is_again(unsafe { (cpp.zmq_errno)() }) {
        return Ok(None);
    }
    Err(format!("cpp recv failed rc {rc} errno {}", unsafe {
        (cpp.zmq_errno)()
    }))
}

fn close_rust(ctx: *mut c_void, sockets: &[*mut c_void]) {
    for socket in sockets {
        zmq::zmq_close(*socket);
    }
    zmq::zmq_ctx_term(ctx);
}

fn is_again(errno: c_int) -> bool {
    errno == EAGAIN || errno == OS_EAGAIN
}

unsafe fn close_cpp(cpp: &CppZmq, ctx: *mut c_void, sockets: &[*mut c_void]) {
    for socket in sockets {
        unsafe { (cpp.close)(*socket) };
    }
    unsafe { (cpp.ctx_term)(ctx) };
}

fn endpoint(transport: Transport, seed: &str) -> Result<CString, String> {
    let endpoint = match transport {
        Transport::Inproc => format!("inproc://perf-{seed}"),
        Transport::Tcp => format!("tcp://127.0.0.1:{}", unused_tcp_port()),
        #[cfg(unix)]
        Transport::Ipc => format!("ipc:///tmp/libzmq-perf-{seed}.sock"),
    };
    CString::new(endpoint).map_err(|_| "endpoint contains interior nul".to_string())
}

fn settle_transport(transport: Transport) {
    match transport {
        Transport::Inproc => {}
        Transport::Tcp => std::thread::sleep(Duration::from_millis(100)),
        #[cfg(unix)]
        Transport::Ipc => std::thread::sleep(Duration::from_millis(20)),
    }
}

fn unused_tcp_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral tcp port")
        .local_addr()
        .expect("read local addr")
        .port()
}

fn subscription_prefixes() -> Vec<String> {
    (0..128).map(|index| format!("topic-{index:03}")).collect()
}

fn rust_curve_keypair() -> Result<(Vec<u8>, Vec<u8>), String> {
    let mut public_key = [0 as c_char; 41];
    let mut secret_key = [0 as c_char; 41];
    let rc = zmq::zmq_curve_keypair(public_key.as_mut_ptr(), secret_key.as_mut_ptr());
    if rc != 0 {
        return Err(format!(
            "rust curve keypair failed errno {}",
            zmq::zmq_errno()
        ));
    }
    let public = unsafe { CStr::from_ptr(public_key.as_ptr()) }
        .to_bytes()
        .to_vec();
    let secret = unsafe { CStr::from_ptr(secret_key.as_ptr()) }
        .to_bytes()
        .to_vec();
    Ok((public, secret))
}

fn monotonic_suffix() -> u128 {
    Instant::now().elapsed().as_nanos()
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(|left, right| left.total_cmp(right));
    values[values.len() / 2]
}

fn render_report(args: &Args, results: &[CaseResult]) -> String {
    let mut report = String::new();
    report.push_str("# Performance Report\n\n");
    report.push_str("Generated by `cargo run --release -p libzmq-test-harness --bin performance-gate -- --write docs/performance-report.md`.\n\n");
    if cfg!(debug_assertions) {
        report.push_str("Warning: this report was generated by a debug build; use `--release` for gate-quality numbers.\n\n");
    }
    report.push_str(&format!(
        "Oracle: `{}`\n\nIterations: `{}`\n\nSamples: `{}`\n\n",
        args.oracle.display(),
        args.iterations,
        args.samples
    ));
    report.push_str("| Case | Metric | C++ median | Rust median | Gate | Result |\n");
    report.push_str("| --- | --- | ---: | ---: | --- | --- |\n");
    for result in results {
        let gate = match result.metric {
            Metric::Latency => "rust <= cpp * 1.05",
            Metric::Throughput => "rust >= cpp * 0.95",
        };
        let cpp = match (result.metric, result.cpp_median) {
            (Metric::Latency, Some(value)) => format!("{value:.0} ns/msg"),
            (Metric::Throughput, Some(value)) => format!("{value:.0} msg/s"),
            (_, None) => "n/a".to_string(),
        };
        let rust = match (result.metric, result.rust_median) {
            (Metric::Latency, Some(value)) => format!("{value:.0} ns/msg"),
            (Metric::Throughput, Some(value)) => format!("{value:.0} msg/s"),
            (_, None) => "n/a".to_string(),
        };
        let status = match &result.status {
            CaseStatus::Pass => "PASS".to_string(),
            CaseStatus::Fail => "FAIL".to_string(),
            CaseStatus::Skipped(reason) => format!("SKIP: {}", sanitize_report_cell(reason)),
        };
        report.push_str(&format!(
            "| {} | {:?} | {} | {} | {} | {} |\n",
            result.name, result.metric, cpp, rust, gate, status
        ));
    }
    report
}

fn sanitize_report_cell(value: &str) -> String {
    value.replace('|', "\\|").replace('\n', " ")
}

fn workspace_root() -> Result<PathBuf, String> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .ancestors()
        .nth(2)
        .map(Path::to_path_buf)
        .ok_or_else(|| {
            format!(
                "cannot derive workspace root from {}",
                manifest_dir.display()
            )
        })
}

unsafe fn load_symbol<T: Copy>(handle: *mut c_void, name: &str) -> Result<T, String> {
    let name = CString::new(name).map_err(|_| "symbol name contains interior nul".to_string())?;
    let raw = unsafe { dlsym(handle, name.as_ptr()) };
    if raw.is_null() {
        return Err(format!("missing symbol {}", name.to_string_lossy()));
    }
    let mut out = mem::MaybeUninit::<T>::uninit();
    let out_ptr = out.as_mut_ptr().cast::<*mut c_void>();
    unsafe {
        ptr::write(out_ptr, raw);
        Ok(out.assume_init())
    }
}
