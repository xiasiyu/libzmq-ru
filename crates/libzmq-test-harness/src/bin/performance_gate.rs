#![allow(clippy::undocumented_unsafe_blocks)]

// This benchmark runner dynamically loads the original C++ libzmq C ABI, then
// compares it with the Rust implementation through the same C ABI surface.

use std::env;
use std::ffi::{c_char, c_int, c_void, CString};
use std::fs;
use std::mem;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::ptr;
use std::time::{Duration, Instant};

const RTLD_NOW: c_int = 2;
const ZMQ_PAIR: c_int = 0;
const ZMQ_PULL: c_int = 7;
const ZMQ_PUSH: c_int = 8;
const ZMQ_LINGER: c_int = 17;
const ZMQ_RCVTIMEO: c_int = 27;
const ZMQ_SNDTIMEO: c_int = 28;
const EAGAIN: c_int = 11;

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

#[derive(Debug)]
struct Args {
    oracle: PathBuf,
    write_path: Option<PathBuf>,
    iterations: usize,
    samples: usize,
}

#[derive(Debug)]
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
    cpp_median: f64,
    rust_median: f64,
    pass: bool,
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
    for transport in transports() {
        results.push(run_case(&cpp, transport, Metric::Latency, args)?);
        results.push(run_case(&cpp, transport, Metric::Throughput, args)?);
    }

    let report = render_report(args, &results);
    if let Some(path) = &args.write_path {
        let path = workspace_root()?.join(path);
        fs::write(&path, report).map_err(|error| format!("write {}: {error}", path.display()))?;
    } else {
        print!("{report}");
    }

    let failures = results.iter().filter(|result| !result.pass).count();
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

fn transports() -> Vec<Transport> {
    let mut transports = vec![Transport::Inproc, Transport::Tcp];
    #[cfg(unix)]
    transports.push(Transport::Ipc);
    transports
}

fn run_case(
    cpp: &CppZmq,
    transport: Transport,
    metric: Metric,
    args: &Args,
) -> Result<CaseResult, String> {
    let mut cpp_samples = Vec::with_capacity(args.samples);
    let mut rust_samples = Vec::with_capacity(args.samples);
    for sample in 0..args.samples {
        let seed = format!("{}-{sample}-{}", std::process::id(), monotonic_suffix());
        cpp_samples
            .push(unsafe { run_cpp_sample(cpp, transport, metric, args.iterations, &seed) }?);
        rust_samples.push(run_rust_sample(transport, metric, args.iterations, &seed)?);
    }
    let cpp_median = median(&mut cpp_samples);
    let rust_median = median(&mut rust_samples);
    let pass = match metric {
        Metric::Latency => rust_median <= cpp_median * LATENCY_LIMIT,
        Metric::Throughput => rust_median >= cpp_median * THROUGHPUT_LIMIT,
    };
    Ok(CaseResult {
        name: format!("{:?} {:?}", transport, metric),
        metric,
        cpp_median,
        rust_median,
        pass,
    })
}

fn run_rust_sample(
    transport: Transport,
    metric: Metric,
    iterations: usize,
    seed: &str,
) -> Result<f64, String> {
    match metric {
        Metric::Latency => run_rust_pair_latency(transport, iterations, seed),
        Metric::Throughput => run_rust_push_pull_throughput(transport, iterations, seed),
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

unsafe fn run_cpp_sample(
    cpp: &CppZmq,
    transport: Transport,
    metric: Metric,
    iterations: usize,
    seed: &str,
) -> Result<f64, String> {
    match metric {
        Metric::Latency => unsafe { run_cpp_pair_latency(cpp, transport, iterations, seed) },
        Metric::Throughput => unsafe {
            run_cpp_push_pull_throughput(cpp, transport, iterations, seed)
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

fn rust_send(socket: *mut c_void, payload: &[u8]) -> Result<(), String> {
    let rc = zmq::zmq_send(socket, payload.as_ptr().cast(), payload.len(), 0);
    if rc == payload.len() as c_int {
        Ok(())
    } else {
        Err(format!(
            "rust send failed rc {rc} errno {}",
            zmq::zmq_errno()
        ))
    }
}

fn rust_recv(socket: *mut c_void) -> Result<usize, String> {
    let mut buffer = [0u8; 256];
    for _ in 0..2_000 {
        let rc = zmq::zmq_recv(socket, buffer.as_mut_ptr().cast(), buffer.len(), 0);
        if rc >= 0 {
            return Ok(rc as usize);
        }
        if zmq::zmq_errno() != EAGAIN {
            return Err(format!(
                "rust recv failed rc {rc} errno {}",
                zmq::zmq_errno()
            ));
        }
        std::thread::sleep(Duration::from_micros(50));
    }
    Err("rust recv timed out".to_string())
}

unsafe fn cpp_send(cpp: &CppZmq, socket: *mut c_void, payload: &[u8]) -> Result<(), String> {
    let rc = unsafe { (cpp.send)(socket, payload.as_ptr().cast(), payload.len(), 0) };
    if rc == payload.len() as c_int {
        Ok(())
    } else {
        Err(format!("cpp send failed rc {rc} errno {}", unsafe {
            (cpp.zmq_errno)()
        }))
    }
}

unsafe fn cpp_recv(cpp: &CppZmq, socket: *mut c_void) -> Result<usize, String> {
    let mut buffer = [0u8; 256];
    for _ in 0..2_000 {
        let rc = unsafe { (cpp.recv)(socket, buffer.as_mut_ptr().cast(), buffer.len(), 0) };
        if rc >= 0 {
            return Ok(rc as usize);
        }
        if unsafe { (cpp.zmq_errno)() } != EAGAIN {
            return Err(format!("cpp recv failed rc {rc} errno {}", unsafe {
                (cpp.zmq_errno)()
            }));
        }
        std::thread::sleep(Duration::from_micros(50));
    }
    Err("cpp recv timed out".to_string())
}

fn close_rust(ctx: *mut c_void, sockets: &[*mut c_void]) {
    for socket in sockets {
        zmq::zmq_close(*socket);
    }
    zmq::zmq_ctx_term(ctx);
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
        "Oracle: `{}`  \nIterations: `{}`  \nSamples: `{}`\n\n",
        args.oracle.display(),
        args.iterations,
        args.samples
    ));
    report.push_str("| Case | Metric | C++ median | Rust median | Gate | Result |\n");
    report.push_str("| --- | --- | ---: | ---: | --- | --- |\n");
    for result in results {
        let (cpp, rust, gate) = match result.metric {
            Metric::Latency => (
                format!("{:.0} ns/msg", result.cpp_median),
                format!("{:.0} ns/msg", result.rust_median),
                "rust <= cpp * 1.05",
            ),
            Metric::Throughput => (
                format!("{:.0} msg/s", result.cpp_median),
                format!("{:.0} msg/s", result.rust_median),
                "rust >= cpp * 0.95",
            ),
        };
        report.push_str(&format!(
            "| {} | {:?} | {} | {} | {} | {} |\n",
            result.name,
            result.metric,
            cpp,
            rust,
            gate,
            if result.pass { "PASS" } else { "FAIL" }
        ));
    }
    report
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
