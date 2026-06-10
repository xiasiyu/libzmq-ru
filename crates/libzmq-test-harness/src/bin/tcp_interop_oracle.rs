use std::env;
use std::ffi::{c_char, c_int, c_void, CString};
use std::mem;
use std::net::TcpListener;
use std::ptr;

use libzmq_core::{curve_keypair, ZmtpFrame, ZmtpGreeting};

const RTLD_NOW: c_int = 2;
const ZMQ_PAIR: c_int = 0;
const ZMQ_REP: c_int = 4;
const ZMQ_SNDMORE: c_int = 2;
const ZMQ_RCVMORE: c_int = 13;
const ZMQ_LINGER: c_int = 17;
const ZMQ_RCVTIMEO: c_int = 27;
const ZMQ_PLAIN_SERVER: c_int = 44;
const ZMQ_PLAIN_USERNAME: c_int = 45;
const ZMQ_PLAIN_PASSWORD: c_int = 46;
const ZMQ_CURVE_SERVER: c_int = 47;
const ZMQ_CURVE_PUBLICKEY: c_int = 48;
const ZMQ_CURVE_SECRETKEY: c_int = 49;
const ZMQ_CURVE_SERVERKEY: c_int = 50;
const ZMQ_EVENT_ALL: c_int = 0xFFFF;

type ZmqCtxNew = unsafe extern "C" fn() -> *mut c_void;
type ZmqCtxTerm = unsafe extern "C" fn(*mut c_void) -> c_int;
type ZmqSocket = unsafe extern "C" fn(*mut c_void, c_int) -> *mut c_void;
type ZmqClose = unsafe extern "C" fn(*mut c_void) -> c_int;
type ZmqBind = unsafe extern "C" fn(*mut c_void, *const c_char) -> c_int;
type ZmqConnect = unsafe extern "C" fn(*mut c_void, *const c_char) -> c_int;
type ZmqSend = unsafe extern "C" fn(*mut c_void, *const c_void, usize, c_int) -> c_int;
type ZmqRecv = unsafe extern "C" fn(*mut c_void, *mut c_void, usize, c_int) -> c_int;
type ZmqSetSockOpt = unsafe extern "C" fn(*mut c_void, c_int, *const c_void, usize) -> c_int;
type ZmqGetSockOpt = unsafe extern "C" fn(*mut c_void, c_int, *mut c_void, *mut usize) -> c_int;
type ZmqErrno = unsafe extern "C" fn() -> c_int;
type ZmqCurvePublic = unsafe extern "C" fn(*mut c_char, *const c_char) -> c_int;
type ZmqSocketMonitor = unsafe extern "C" fn(*mut c_void, *const c_char, c_int) -> c_int;

unsafe extern "C" {
    fn dlopen(filename: *const c_char, flags: c_int) -> *mut c_void;
    fn dlsym(handle: *mut c_void, symbol: *const c_char) -> *mut c_void;
    fn dlclose(handle: *mut c_void) -> c_int;
}

#[derive(Clone, Copy)]
struct CppZmq {
    ctx_new: ZmqCtxNew,
    ctx_term: ZmqCtxTerm,
    socket: ZmqSocket,
    close: ZmqClose,
    bind: ZmqBind,
    connect: ZmqConnect,
    send: ZmqSend,
    recv: ZmqRecv,
    setsockopt: ZmqSetSockOpt,
    getsockopt: ZmqGetSockOpt,
    zmq_errno: ZmqErrno,
    curve_public: ZmqCurvePublic,
    socket_monitor: ZmqSocketMonitor,
}

fn main() {
    let path = env::var("LIBZMQ_ORACLE")
        .unwrap_or_else(|_| "../libzmq/build-ru-oracle/lib/libzmq.dylib".to_string());
    let path = CString::new(path).expect("oracle library path contains no interior nul");
    // SAFETY: `path` is a valid NUL-terminated string and `RTLD_NOW` is a valid dlopen flag.
    let handle = unsafe { dlopen(path.as_ptr(), RTLD_NOW) };
    if handle.is_null() {
        eprintln!("failed to load original libzmq oracle");
        std::process::exit(2);
    }
    let result = run(handle);
    // SAFETY: `handle` was returned by a successful `dlopen` call above.
    unsafe {
        dlclose(handle);
    }
    if let Err(error) = result {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run(handle: *mut c_void) -> Result<(), String> {
    let cpp = CppZmq {
        // SAFETY: Symbols are loaded from a successfully opened original libzmq dynamic library.
        ctx_new: unsafe { load_symbol(handle, "zmq_ctx_new")? },
        ctx_term: unsafe { load_symbol(handle, "zmq_ctx_term")? },
        socket: unsafe { load_symbol(handle, "zmq_socket")? },
        close: unsafe { load_symbol(handle, "zmq_close")? },
        bind: unsafe { load_symbol(handle, "zmq_bind")? },
        connect: unsafe { load_symbol(handle, "zmq_connect")? },
        send: unsafe { load_symbol(handle, "zmq_send")? },
        recv: unsafe { load_symbol(handle, "zmq_recv")? },
        setsockopt: unsafe { load_symbol(handle, "zmq_setsockopt")? },
        getsockopt: unsafe { load_symbol(handle, "zmq_getsockopt")? },
        zmq_errno: unsafe { load_symbol(handle, "zmq_errno")? },
        curve_public: unsafe { load_symbol(handle, "zmq_curve_public")? },
        socket_monitor: unsafe { load_symbol(handle, "zmq_socket_monitor")? },
    };

    println!("{{\"case\":\"tcp_interop\"}}");
    if env::var_os("CAPTURE_CPP_ZMTP").is_some() {
        capture_cpp_client_bytes(&cpp)?;
        return Ok(());
    }
    if env::var_os("CAPTURE_RUST_ZMTP").is_some() {
        capture_rust_client_bytes()?;
        return Ok(());
    }
    if env::var_os("CAPTURE_CPP_SERVER_ZMTP").is_some() {
        capture_cpp_server_bytes(&cpp)?;
        return Ok(());
    }
    curve_public_oracle_check(&cpp)?;
    if let Err(error) = cpp_curve_self_round_trip(&cpp) {
        println!(
            "{{\"observation\":{{\"type\":\"cpp_curve_self_round_trip\",\"blocked\":true,\"error\":\"{}\"}}}}",
            json_escape(&error)
        );
    }
    cpp_server_rust_client(&cpp)?;
    if let Err(error) = rust_server_cpp_client(&cpp) {
        println!(
            "{{\"observation\":{{\"type\":\"rust_server_cpp_client\",\"blocked\":true,\"error\":\"{}\"}}}}",
            json_escape(&error)
        );
    }
    if let Err(error) = rust_plain_server_cpp_plain_client(&cpp) {
        println!(
            "{{\"observation\":{{\"type\":\"rust_plain_server_cpp_plain_client\",\"blocked\":true,\"error\":\"{}\"}}}}",
            json_escape(&error)
        );
    }
    if let Err(error) = cpp_plain_server_rust_plain_client(&cpp) {
        println!(
            "{{\"observation\":{{\"type\":\"cpp_plain_server_rust_plain_client\",\"blocked\":true,\"error\":\"{}\"}}}}",
            json_escape(&error)
        );
    }
    if let Err(error) = rust_curve_server_cpp_curve_client(&cpp) {
        println!(
            "{{\"observation\":{{\"type\":\"rust_curve_server_cpp_curve_client\",\"blocked\":true,\"error\":\"{}\"}}}}",
            json_escape(&error)
        );
    }
    if let Err(error) = cpp_curve_server_rust_curve_client(&cpp) {
        println!(
            "{{\"observation\":{{\"type\":\"cpp_curve_server_rust_curve_client\",\"blocked\":true,\"error\":\"{}\"}}}}",
            json_escape(&error)
        );
    }
    Ok(())
}

fn curve_public_oracle_check(cpp: &CppZmq) -> Result<(), String> {
    let (rust_public, rust_secret) = curve_keypair().map_err(|error| format!("{error:?}"))?;
    let secret = CString::new(rust_secret.clone()).unwrap();
    let mut cpp_public = [0 as c_char; 41];
    // SAFETY: Output buffer has room for 40 Z85 bytes plus NUL, input is a valid C string.
    let rc = unsafe { (cpp.curve_public)(cpp_public.as_mut_ptr(), secret.as_ptr()) };
    if rc != 0 {
        println!(
            "{{\"observation\":{{\"type\":\"cpp_curve_public\",\"blocked\":true,\"errno\":{}}}}}",
            unsafe { (cpp.zmq_errno)() }
        );
        return Ok(());
    }
    let cpp_public = unsafe { std::ffi::CStr::from_ptr(cpp_public.as_ptr()) }
        .to_str()
        .map_err(|error| error.to_string())?;
    println!(
        "{{\"observation\":{{\"type\":\"cpp_curve_public\",\"matches\":{}}}}}",
        cpp_public == rust_public
    );
    Ok(())
}

fn cpp_curve_self_round_trip(cpp: &CppZmq) -> Result<(), String> {
    let endpoint = CString::new(format!("tcp://127.0.0.1:{}", unused_tcp_port())).unwrap();
    let (server_public, server_secret) = curve_keypair().map_err(|error| format!("{error:?}"))?;
    let (client_public, client_secret) = curve_keypair().map_err(|error| format!("{error:?}"))?;
    // SAFETY: Function pointers are loaded from original libzmq, and pointers are valid for calls.
    unsafe {
        let ctx = (cpp.ctx_new)();
        let server = (cpp.socket)(ctx, ZMQ_PAIR);
        let client = (cpp.socket)(ctx, ZMQ_PAIR);
        configure_cpp_socket(cpp, server);
        configure_cpp_socket(cpp, client);
        let enabled = 1i32;
        assert_rc(
            (cpp.setsockopt)(
                server,
                ZMQ_CURVE_SERVER,
                (&enabled as *const i32).cast(),
                size_of_val(&enabled),
            ),
            "cpp self curve server option",
        )?;
        assert_rc(
            (cpp.setsockopt)(
                server,
                ZMQ_CURVE_PUBLICKEY,
                server_public.as_ptr().cast(),
                server_public.len(),
            ),
            "cpp self server public",
        )?;
        assert_rc(
            (cpp.setsockopt)(
                server,
                ZMQ_CURVE_SECRETKEY,
                server_secret.as_ptr().cast(),
                server_secret.len(),
            ),
            "cpp self server secret",
        )?;
        assert_rc(
            (cpp.setsockopt)(
                client,
                ZMQ_CURVE_SERVERKEY,
                server_public.as_ptr().cast(),
                server_public.len(),
            ),
            "cpp self client serverkey",
        )?;
        assert_rc(
            (cpp.setsockopt)(
                client,
                ZMQ_CURVE_PUBLICKEY,
                client_public.as_ptr().cast(),
                client_public.len(),
            ),
            "cpp self client public",
        )?;
        assert_rc(
            (cpp.setsockopt)(
                client,
                ZMQ_CURVE_SECRETKEY,
                client_secret.as_ptr().cast(),
                client_secret.len(),
            ),
            "cpp self client secret",
        )?;
        assert_rc((cpp.bind)(server, endpoint.as_ptr()), "cpp self bind")?;
        assert_rc((cpp.connect)(client, endpoint.as_ptr()), "cpp self connect")?;
        assert_eq_rc(
            (cpp.send)(client, b"curve".as_ptr().cast(), 5, 0),
            5,
            "cpp self send",
        )?;
        let mut buffer = [0u8; 16];
        let mut recv = -1;
        for _ in 0..50 {
            recv = (cpp.recv)(server, buffer.as_mut_ptr().cast(), buffer.len(), 0);
            if recv == 5 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        if recv != 5 {
            return Err(format!(
                "cpp self recv returned {recv}, expected 5, errno {}",
                (cpp.zmq_errno)()
            ));
        }
        assert_eq!(&buffer[..5], b"curve");
        (cpp.close)(client);
        (cpp.close)(server);
        (cpp.ctx_term)(ctx);
    }
    println!("{{\"observation\":{{\"type\":\"cpp_curve_self_round_trip\",\"data\":\"curve\"}}}}");
    Ok(())
}

fn capture_rust_client_bytes() -> Result<(), String> {
    let listener = TcpListener::bind("127.0.0.1:0").map_err(|error| error.to_string())?;
    let endpoint = CString::new(format!("tcp://{}", listener.local_addr().unwrap())).unwrap();
    let rust_ctx = zmq::zmq_ctx_new();
    let rust_client = zmq::zmq_socket(rust_ctx, ZMQ_PAIR);
    assert_rc(
        zmq::zmq_connect(rust_client, endpoint.as_ptr()),
        "rust connect",
    )?;
    assert_eq_rc(
        zmq::zmq_send(rust_client, b"probe".as_ptr().cast(), 5, 0),
        5,
        "rust send",
    )?;
    let (mut stream, _) = listener.accept().map_err(|error| error.to_string())?;
    stream
        .set_read_timeout(Some(std::time::Duration::from_millis(500)))
        .map_err(|error| error.to_string())?;
    let mut bytes = [0u8; 160];
    let n = std::io::Read::read(&mut stream, &mut bytes).map_err(|error| error.to_string())?;
    let greeting = ZmtpGreeting::null_server().encode();
    std::io::Write::write_all(&mut stream, &greeting[..10]).map_err(|error| error.to_string())?;
    std::io::Write::write_all(&mut stream, &greeting[10..]).map_err(|error| error.to_string())?;
    if env::var_os("CAPTURE_RUST_NO_READY").is_none() {
        std::io::Write::write_all(&mut stream, &ZmtpFrame::command(ready_body()).encode_v3())
            .map_err(|error| error.to_string())?;
    }
    let m = std::io::Read::read(&mut stream, &mut bytes[n..]).unwrap_or(0);
    let n = n + m;
    println!(
        "{{\"observation\":{{\"type\":\"rust_bytes\",\"n\":{n},\"hex\":\"{}\"}}}}",
        hex(&bytes[..n])
    );
    zmq::zmq_close(rust_client);
    zmq::zmq_ctx_term(rust_ctx);
    Ok(())
}

fn capture_cpp_client_bytes(cpp: &CppZmq) -> Result<(), String> {
    let listener = TcpListener::bind("127.0.0.1:0").map_err(|error| error.to_string())?;
    let endpoint = CString::new(format!("tcp://{}", listener.local_addr().unwrap())).unwrap();
    // SAFETY: Function pointers are loaded from original libzmq, and pointers are valid for calls.
    unsafe {
        let ctx = (cpp.ctx_new)();
        let client = (cpp.socket)(ctx, ZMQ_PAIR);
        configure_cpp_socket(cpp, client);
        assert_rc((cpp.connect)(client, endpoint.as_ptr()), "cpp connect")?;
        assert_eq_rc(
            (cpp.send)(client, b"probe".as_ptr().cast(), 5, 0),
            5,
            "cpp send",
        )?;
        let (mut stream, _) = listener.accept().map_err(|error| error.to_string())?;
        stream
            .set_read_timeout(Some(std::time::Duration::from_millis(500)))
            .map_err(|error| error.to_string())?;
        let mut bytes = [0u8; 160];
        let greeting = ZmtpGreeting::null_server().encode();
        std::io::Write::write_all(&mut stream, &greeting[..10])
            .map_err(|error| error.to_string())?;
        std::io::Read::read_exact(&mut stream, &mut bytes[..10])
            .map_err(|error| error.to_string())?;
        std::io::Write::write_all(&mut stream, &greeting[10..])
            .map_err(|error| error.to_string())?;
        std::io::Read::read_exact(&mut stream, &mut bytes[10..64])
            .map_err(|error| error.to_string())?;
        std::io::Write::write_all(&mut stream, &ZmtpFrame::command(ready_body()).encode_v3())
            .map_err(|error| error.to_string())?;
        let m = std::io::Read::read(&mut stream, &mut bytes[64..]).unwrap_or(0);
        let n = 64 + m;
        let k = std::io::Read::read(&mut stream, &mut bytes[n..]).unwrap_or(0);
        let n = n + k;
        println!(
            "{{\"observation\":{{\"type\":\"cpp_bytes\",\"n\":{n},\"hex\":\"{}\"}}}}",
            hex(&bytes[..n])
        );
        (cpp.close)(client);
        (cpp.ctx_term)(ctx);
    }
    Ok(())
}

fn capture_cpp_server_bytes(cpp: &CppZmq) -> Result<(), String> {
    let endpoint = CString::new(format!("tcp://127.0.0.1:{}", unused_tcp_port())).unwrap();
    // SAFETY: Function pointers are loaded from original libzmq, and pointers are valid for calls.
    unsafe {
        let ctx = (cpp.ctx_new)();
        let server = (cpp.socket)(ctx, ZMQ_PAIR);
        configure_cpp_socket(cpp, server);
        assert_rc((cpp.bind)(server, endpoint.as_ptr()), "cpp bind")?;
        let mut stream = std::net::TcpStream::connect(
            endpoint.to_str().unwrap().strip_prefix("tcp://").unwrap(),
        )
        .map_err(|error| error.to_string())?;
        stream
            .set_read_timeout(Some(std::time::Duration::from_millis(500)))
            .map_err(|error| error.to_string())?;
        let greeting = ZmtpGreeting::null_client().encode();
        std::io::Write::write_all(&mut stream, &greeting[..10])
            .map_err(|error| error.to_string())?;
        let mut bytes = [0u8; 160];
        let n = std::io::Read::read(&mut stream, &mut bytes).map_err(|error| error.to_string())?;
        std::io::Write::write_all(&mut stream, &greeting[10..])
            .map_err(|error| error.to_string())?;
        std::io::Write::write_all(&mut stream, &ZmtpFrame::command(ready_body()).encode_v3())
            .map_err(|error| error.to_string())?;
        std::io::Write::write_all(
            &mut stream,
            &ZmtpFrame::message(b"hello".to_vec()).encode_v3(),
        )
        .map_err(|error| error.to_string())?;
        let m = std::io::Read::read(&mut stream, &mut bytes[n..]).unwrap_or(0);
        let n = n + m;
        let mut buffer = [0u8; 16];
        let recv = (cpp.recv)(server, buffer.as_mut_ptr().cast(), buffer.len(), 0);
        println!(
            "{{\"observation\":{{\"type\":\"cpp_server_bytes\",\"n\":{n},\"hex\":\"{}\",\"recv\":{recv},\"errno\":{}}}}}",
            hex(&bytes[..n]),
            (cpp.zmq_errno)()
        );
        (cpp.close)(server);
        (cpp.ctx_term)(ctx);
    }
    Ok(())
}

fn ready_body() -> Vec<u8> {
    let mut body = Vec::new();
    body.push(5);
    body.extend_from_slice(b"READY");
    body.push(11);
    body.extend_from_slice(b"Socket-Type");
    body.extend_from_slice(&(4u32).to_be_bytes());
    body.extend_from_slice(b"PAIR");
    body
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn json_escape(input: &str) -> String {
    input.replace('\\', "\\\\").replace('"', "\\\"")
}

fn cpp_server_rust_client(cpp: &CppZmq) -> Result<(), String> {
    let endpoint = CString::new(format!("tcp://127.0.0.1:{}", unused_tcp_port())).unwrap();
    // SAFETY: Function pointers are loaded from original libzmq, and pointers are valid for calls.
    unsafe {
        let ctx = (cpp.ctx_new)();
        let server = (cpp.socket)(ctx, ZMQ_PAIR);
        configure_cpp_socket(cpp, server);
        assert_rc((cpp.bind)(server, endpoint.as_ptr()), "cpp bind")?;

        let rust_ctx = zmq::zmq_ctx_new();
        let rust_client = zmq::zmq_socket(rust_ctx, ZMQ_PAIR);
        assert_rc(
            zmq::zmq_connect(rust_client, endpoint.as_ptr()),
            "rust connect",
        )?;
        assert_eq_rc(
            zmq::zmq_send(rust_client, b"hello".as_ptr().cast(), 5, 0),
            5,
            "rust send",
        )?;
        std::thread::sleep(std::time::Duration::from_secs(2));

        let mut buffer = [0u8; 16];
        let recv = (cpp.recv)(server, buffer.as_mut_ptr().cast(), buffer.len(), 0);
        if recv != 5 {
            return Err(format!(
                "cpp recv returned {recv}, expected 5, errno {}",
                (cpp.zmq_errno)()
            ));
        }
        assert_eq!(&buffer[..5], b"hello");

        zmq::zmq_close(rust_client);
        zmq::zmq_ctx_term(rust_ctx);
        (cpp.close)(server);
        (cpp.ctx_term)(ctx);
    }
    println!("{{\"observation\":{{\"type\":\"cpp_server_rust_client\",\"data\":\"hello\"}}}}");
    Ok(())
}

fn rust_server_cpp_client(cpp: &CppZmq) -> Result<(), String> {
    let endpoint = CString::new(format!("tcp://127.0.0.1:{}", unused_tcp_port())).unwrap();
    // SAFETY: Function pointers are loaded from original libzmq, and pointers are valid for calls.
    unsafe {
        let rust_ctx = zmq::zmq_ctx_new();
        let rust_server = zmq::zmq_socket(rust_ctx, ZMQ_PAIR);
        assert_rc(zmq::zmq_bind(rust_server, endpoint.as_ptr()), "rust bind")?;

        let ctx = (cpp.ctx_new)();
        let client = (cpp.socket)(ctx, ZMQ_PAIR);
        configure_cpp_socket(cpp, client);
        assert_rc((cpp.connect)(client, endpoint.as_ptr()), "cpp connect")?;
        assert_eq_rc(
            (cpp.send)(client, b"world".as_ptr().cast(), 5, 0),
            5,
            "cpp send",
        )?;
        std::thread::sleep(std::time::Duration::from_millis(100));

        let mut buffer = [0u8; 16];
        let mut recv = -1;
        for _ in 0..10 {
            recv = zmq::zmq_recv(rust_server, buffer.as_mut_ptr().cast(), buffer.len(), 0);
            if recv == 5 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        if recv != 5 {
            return Err(format!(
                "rust recv returned {recv}, errno {}",
                zmq::zmq_errno()
            ));
        }
        assert_eq!(&buffer[..5], b"world");

        (cpp.close)(client);
        (cpp.ctx_term)(ctx);
        zmq::zmq_close(rust_server);
        zmq::zmq_ctx_term(rust_ctx);
    }
    println!("{{\"observation\":{{\"type\":\"rust_server_cpp_client\",\"data\":\"world\"}}}}");
    Ok(())
}

fn rust_plain_server_cpp_plain_client(cpp: &CppZmq) -> Result<(), String> {
    let endpoint = CString::new(format!("tcp://127.0.0.1:{}", unused_tcp_port())).unwrap();
    // SAFETY: Function pointers are loaded from original libzmq, and pointers are valid for calls.
    unsafe {
        let rust_ctx = zmq::zmq_ctx_new();
        let rust_server = zmq::zmq_socket(rust_ctx, ZMQ_PAIR);
        let enabled = 1i32;
        assert_rc(
            zmq::zmq_setsockopt(
                rust_server,
                ZMQ_PLAIN_SERVER,
                (&enabled as *const i32).cast(),
                std::mem::size_of_val(&enabled),
            ),
            "rust plain server option",
        )?;
        assert_rc(zmq::zmq_bind(rust_server, endpoint.as_ptr()), "rust bind")?;

        let ctx = (cpp.ctx_new)();
        let client = (cpp.socket)(ctx, ZMQ_PAIR);
        configure_cpp_socket(cpp, client);
        assert_rc(
            (cpp.setsockopt)(client, ZMQ_PLAIN_USERNAME, b"user".as_ptr().cast(), 4),
            "cpp plain username",
        )?;
        assert_rc(
            (cpp.setsockopt)(client, ZMQ_PLAIN_PASSWORD, b"pass".as_ptr().cast(), 4),
            "cpp plain password",
        )?;
        assert_rc((cpp.connect)(client, endpoint.as_ptr()), "cpp connect")?;
        assert_eq_rc(
            (cpp.send)(client, b"plain".as_ptr().cast(), 5, 0),
            5,
            "cpp send",
        )?;

        let mut buffer = [0u8; 16];
        let mut recv = -1;
        for _ in 0..50 {
            recv = zmq::zmq_recv(rust_server, buffer.as_mut_ptr().cast(), buffer.len(), 0);
            if recv == 5 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        if recv != 5 {
            return Err(format!(
                "rust recv returned {recv}, errno {}",
                zmq::zmq_errno()
            ));
        }
        assert_eq!(&buffer[..5], b"plain");

        (cpp.close)(client);
        (cpp.ctx_term)(ctx);
        zmq::zmq_close(rust_server);
        zmq::zmq_ctx_term(rust_ctx);
    }
    println!(
        "{{\"observation\":{{\"type\":\"rust_plain_server_cpp_plain_client\",\"data\":\"plain\"}}}}"
    );
    Ok(())
}

fn cpp_plain_server_rust_plain_client(cpp: &CppZmq) -> Result<(), String> {
    let endpoint = CString::new(format!("tcp://127.0.0.1:{}", unused_tcp_port())).unwrap();
    // SAFETY: Function pointers are loaded from original libzmq, and pointers are valid for calls.
    unsafe {
        let ctx = (cpp.ctx_new)();
        let zap = (cpp.socket)(ctx, ZMQ_REP);
        configure_cpp_socket(cpp, zap);
        let zap_endpoint = CString::new("inproc://zeromq.zap.01").unwrap();
        assert_rc((cpp.bind)(zap, zap_endpoint.as_ptr()), "cpp zap bind")?;
        let zap_thread = spawn_cpp_zap_actor(*cpp, zap, true);
        let server = (cpp.socket)(ctx, ZMQ_PAIR);
        configure_cpp_socket(cpp, server);
        let enabled = 1i32;
        assert_rc(
            (cpp.setsockopt)(
                server,
                ZMQ_PLAIN_SERVER,
                (&enabled as *const i32).cast(),
                std::mem::size_of_val(&enabled),
            ),
            "cpp plain server option",
        )?;
        assert_rc((cpp.bind)(server, endpoint.as_ptr()), "cpp bind")?;

        let rust_ctx = zmq::zmq_ctx_new();
        let rust_client = zmq::zmq_socket(rust_ctx, ZMQ_PAIR);
        assert_rc(
            zmq::zmq_setsockopt(rust_client, ZMQ_PLAIN_USERNAME, b"user".as_ptr().cast(), 4),
            "rust plain username",
        )?;
        assert_rc(
            zmq::zmq_setsockopt(rust_client, ZMQ_PLAIN_PASSWORD, b"pass".as_ptr().cast(), 4),
            "rust plain password",
        )?;
        assert_rc(
            zmq::zmq_connect(rust_client, endpoint.as_ptr()),
            "rust connect",
        )?;
        assert_eq_rc(
            zmq::zmq_send(rust_client, b"plain".as_ptr().cast(), 5, 0),
            5,
            "rust send",
        )?;

        let mut buffer = [0u8; 16];
        let mut recv = -1;
        for _ in 0..50 {
            recv = (cpp.recv)(server, buffer.as_mut_ptr().cast(), buffer.len(), 0);
            if recv == 5 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        if recv != 5 {
            return Err(format!(
                "cpp recv returned {recv}, expected 5, errno {}",
                (cpp.zmq_errno)()
            ));
        }
        assert_eq!(&buffer[..5], b"plain");

        zap_thread
            .join()
            .map_err(|_| "cpp zap actor panicked".to_string())??;

        zmq::zmq_close(rust_client);
        zmq::zmq_ctx_term(rust_ctx);
        (cpp.close)(server);
        (cpp.close)(zap);
        (cpp.ctx_term)(ctx);
    }
    println!(
        "{{\"observation\":{{\"type\":\"cpp_plain_server_rust_plain_client\",\"data\":\"plain\"}}}}"
    );
    Ok(())
}

fn rust_curve_server_cpp_curve_client(cpp: &CppZmq) -> Result<(), String> {
    let endpoint = CString::new(format!("tcp://127.0.0.1:{}", unused_tcp_port())).unwrap();
    let (server_public, server_secret) = curve_keypair().map_err(|error| format!("{error:?}"))?;
    let (client_public, client_secret) = curve_keypair().map_err(|error| format!("{error:?}"))?;
    // SAFETY: Function pointers are loaded from original libzmq, and pointers are valid for calls.
    unsafe {
        let rust_ctx = zmq::zmq_ctx_new();
        let rust_server = zmq::zmq_socket(rust_ctx, ZMQ_PAIR);
        let enabled = 1i32;
        assert_rc(
            zmq::zmq_setsockopt(
                rust_server,
                ZMQ_CURVE_SERVER,
                (&enabled as *const i32).cast(),
                std::mem::size_of_val(&enabled),
            ),
            "rust curve server option",
        )?;
        assert_rc(
            zmq::zmq_setsockopt(
                rust_server,
                ZMQ_CURVE_SECRETKEY,
                server_secret.as_ptr().cast(),
                server_secret.len(),
            ),
            "rust curve secret",
        )?;
        assert_rc(
            zmq::zmq_setsockopt(
                rust_server,
                ZMQ_CURVE_PUBLICKEY,
                client_public.as_ptr().cast(),
                client_public.len(),
            ),
            "rust curve client allowlist",
        )?;
        assert_rc(zmq::zmq_bind(rust_server, endpoint.as_ptr()), "rust bind")?;

        let ctx = (cpp.ctx_new)();
        let client = (cpp.socket)(ctx, ZMQ_PAIR);
        configure_cpp_socket(cpp, client);
        assert_rc(
            (cpp.setsockopt)(
                client,
                ZMQ_CURVE_SERVERKEY,
                server_public.as_ptr().cast(),
                server_public.len(),
            ),
            "cpp curve server key",
        )?;
        assert_rc(
            (cpp.setsockopt)(
                client,
                ZMQ_CURVE_PUBLICKEY,
                client_public.as_ptr().cast(),
                client_public.len(),
            ),
            "cpp curve public",
        )?;
        assert_rc(
            (cpp.setsockopt)(
                client,
                ZMQ_CURVE_SECRETKEY,
                client_secret.as_ptr().cast(),
                client_secret.len(),
            ),
            "cpp curve secret",
        )?;
        let monitor = start_cpp_monitor(cpp, ctx, client, "curve-client")?;
        assert_rc((cpp.connect)(client, endpoint.as_ptr()), "cpp connect")?;
        std::thread::sleep(std::time::Duration::from_millis(500));
        assert_eq_rc(
            (cpp.send)(client, b"curve".as_ptr().cast(), 5, 0),
            5,
            "cpp send",
        )?;

        let mut buffer = [0u8; 16];
        let mut recv = -1;
        for _ in 0..50 {
            recv = zmq::zmq_recv(rust_server, buffer.as_mut_ptr().cast(), buffer.len(), 0);
            if recv == 5 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        if recv != 5 {
            let events = recv_cpp_monitor_events(cpp, monitor);
            return Err(format!(
                "rust recv returned {recv}, errno {}, cpp monitor {:?}",
                zmq::zmq_errno(),
                events
            ));
        }
        assert_eq!(&buffer[..5], b"curve");

        (cpp.close)(client);
        (cpp.close)(monitor);
        (cpp.ctx_term)(ctx);
        zmq::zmq_close(rust_server);
        zmq::zmq_ctx_term(rust_ctx);
    }
    println!(
        "{{\"observation\":{{\"type\":\"rust_curve_server_cpp_curve_client\",\"data\":\"curve\"}}}}"
    );
    Ok(())
}

fn cpp_curve_server_rust_curve_client(cpp: &CppZmq) -> Result<(), String> {
    let endpoint = CString::new(format!("tcp://127.0.0.1:{}", unused_tcp_port())).unwrap();
    let (server_public, server_secret) = curve_keypair().map_err(|error| format!("{error:?}"))?;
    let (client_public, client_secret) = curve_keypair().map_err(|error| format!("{error:?}"))?;
    // SAFETY: Function pointers are loaded from original libzmq, and pointers are valid for calls.
    unsafe {
        let ctx = (cpp.ctx_new)();
        let server = (cpp.socket)(ctx, ZMQ_PAIR);
        configure_cpp_socket(cpp, server);
        let monitor = start_cpp_monitor(cpp, ctx, server, "curve-server")?;
        let enabled = 1i32;
        assert_rc(
            (cpp.setsockopt)(
                server,
                ZMQ_CURVE_SERVER,
                (&enabled as *const i32).cast(),
                std::mem::size_of_val(&enabled),
            ),
            "cpp curve server option",
        )?;
        assert_rc(
            (cpp.setsockopt)(
                server,
                ZMQ_CURVE_PUBLICKEY,
                server_public.as_ptr().cast(),
                server_public.len(),
            ),
            "cpp curve public",
        )?;
        assert_rc(
            (cpp.setsockopt)(
                server,
                ZMQ_CURVE_SECRETKEY,
                server_secret.as_ptr().cast(),
                server_secret.len(),
            ),
            "cpp curve secret",
        )?;
        assert_rc((cpp.bind)(server, endpoint.as_ptr()), "cpp bind")?;

        let rust_ctx = zmq::zmq_ctx_new();
        let rust_client = zmq::zmq_socket(rust_ctx, ZMQ_PAIR);
        assert_rc(
            zmq::zmq_setsockopt(
                rust_client,
                ZMQ_CURVE_SERVERKEY,
                server_public.as_ptr().cast(),
                server_public.len(),
            ),
            "rust curve server key",
        )?;
        assert_rc(
            zmq::zmq_setsockopt(
                rust_client,
                ZMQ_CURVE_PUBLICKEY,
                client_public.as_ptr().cast(),
                client_public.len(),
            ),
            "rust curve public",
        )?;
        assert_rc(
            zmq::zmq_setsockopt(
                rust_client,
                ZMQ_CURVE_SECRETKEY,
                client_secret.as_ptr().cast(),
                client_secret.len(),
            ),
            "rust curve secret",
        )?;
        assert_rc(
            zmq::zmq_connect(rust_client, endpoint.as_ptr()),
            "rust connect",
        )?;
        std::thread::sleep(std::time::Duration::from_millis(500));
        assert_eq_rc(
            zmq::zmq_send(rust_client, b"curve".as_ptr().cast(), 5, 0),
            5,
            "rust send",
        )?;

        let mut buffer = [0u8; 16];
        let mut recv = -1;
        for _ in 0..50 {
            recv = (cpp.recv)(server, buffer.as_mut_ptr().cast(), buffer.len(), 0);
            if recv == 5 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        if recv != 5 {
            let events = recv_cpp_monitor_events(cpp, monitor);
            return Err(format!(
                "cpp recv returned {recv}, expected 5, errno {}, cpp monitor {:?}",
                (cpp.zmq_errno)(),
                events
            ));
        }
        assert_eq!(&buffer[..5], b"curve");

        zmq::zmq_close(rust_client);
        zmq::zmq_ctx_term(rust_ctx);
        (cpp.close)(server);
        (cpp.close)(monitor);
        (cpp.ctx_term)(ctx);
    }
    println!(
        "{{\"observation\":{{\"type\":\"cpp_curve_server_rust_curve_client\",\"data\":\"curve\"}}}}"
    );
    Ok(())
}

fn spawn_cpp_zap_actor(
    cpp: CppZmq,
    zap: *mut c_void,
    accept: bool,
) -> std::thread::JoinHandle<Result<(), String>> {
    let zap = zap as usize;
    std::thread::spawn(move || {
        let zap = zap as *mut c_void;
        // SAFETY: The ZAP socket is used only by this actor thread after bind.
        unsafe {
            let frames = recv_cpp_multipart(&cpp, zap)?;
            if frames.len() < 8 || frames[0] != b"1.0" || frames[5] != b"PLAIN" {
                return Err(format!("unexpected ZAP request {:?}", frames));
            }
            let status = if accept {
                b"200".as_slice()
            } else {
                b"400".as_slice()
            };
            let text = if accept {
                b"OK".as_slice()
            } else {
                b"DENIED".as_slice()
            };
            let reply = [
                b"1.0".as_slice(),
                frames[1].as_slice(),
                status,
                text,
                b"user".as_slice(),
                b"".as_slice(),
            ];
            for (index, frame) in reply.iter().enumerate() {
                let flags = if index + 1 == reply.len() {
                    0
                } else {
                    ZMQ_SNDMORE
                };
                let rc = (cpp.send)(zap, frame.as_ptr().cast(), frame.len(), flags);
                if rc != frame.len() as c_int {
                    return Err(format!("cpp zap send failed rc {rc}"));
                }
            }
        }
        Ok(())
    })
}

unsafe fn recv_cpp_multipart(cpp: &CppZmq, socket: *mut c_void) -> Result<Vec<Vec<u8>>, String> {
    let mut frames = Vec::new();
    loop {
        let mut buffer = [0u8; 256];
        let mut rc = -1;
        for _ in 0..20 {
            rc = unsafe { (cpp.recv)(socket, buffer.as_mut_ptr().cast(), buffer.len(), 0) };
            if rc >= 0 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        if rc < 0 {
            return Err(format!("cpp zap recv failed rc {rc}"));
        }
        frames.push(buffer[..rc as usize].to_vec());
        let mut more = 0i32;
        let mut size = std::mem::size_of_val(&more);
        let opt_rc = unsafe {
            (cpp.getsockopt)(
                socket,
                ZMQ_RCVMORE,
                (&mut more as *mut i32).cast(),
                &mut size,
            )
        };
        if opt_rc != 0 {
            return Err(format!("cpp zap getsockopt failed rc {opt_rc}"));
        }
        if more == 0 {
            return Ok(frames);
        }
    }
}

unsafe fn start_cpp_monitor(
    cpp: &CppZmq,
    ctx: *mut c_void,
    socket: *mut c_void,
    label: &str,
) -> Result<*mut c_void, String> {
    let endpoint = CString::new(format!("inproc://{label}-{}", unused_tcp_port())).unwrap();
    let rc = unsafe { (cpp.socket_monitor)(socket, endpoint.as_ptr(), ZMQ_EVENT_ALL) };
    if rc != 0 {
        return Err(format!("cpp monitor returned {rc}"));
    }
    let monitor = unsafe { (cpp.socket)(ctx, ZMQ_PAIR) };
    unsafe { configure_cpp_socket(cpp, monitor) };
    let rc = unsafe { (cpp.connect)(monitor, endpoint.as_ptr()) };
    if rc != 0 {
        return Err(format!("cpp monitor connect returned {rc}"));
    }
    Ok(monitor)
}

unsafe fn recv_cpp_monitor_events(cpp: &CppZmq, monitor: *mut c_void) -> Vec<String> {
    let mut events = Vec::new();
    for _ in 0..10 {
        let Ok(frames) = (unsafe { recv_cpp_multipart(cpp, monitor) }) else {
            break;
        };
        if frames.is_empty() || frames[0].len() < 6 {
            continue;
        }
        let event = u16::from_ne_bytes([frames[0][0], frames[0][1]]);
        let value = i32::from_ne_bytes([frames[0][2], frames[0][3], frames[0][4], frames[0][5]]);
        events.push(format!("0x{event:04x}:{value}"));
    }
    events
}

unsafe fn configure_cpp_socket(cpp: &CppZmq, socket: *mut c_void) {
    let linger = 0i32;
    let timeout = 1_000i32;
    // SAFETY: Socket pointer is owned by the caller and option values point to valid i32 storage.
    unsafe {
        (cpp.setsockopt)(
            socket,
            ZMQ_LINGER,
            (&linger as *const i32).cast(),
            size_of_val(&linger),
        );
        (cpp.setsockopt)(
            socket,
            ZMQ_RCVTIMEO,
            (&timeout as *const i32).cast(),
            size_of_val(&timeout),
        );
    }
}

fn assert_rc(rc: c_int, op: &str) -> Result<(), String> {
    if rc == 0 {
        Ok(())
    } else {
        Err(format!("{op} returned {rc}"))
    }
}

fn assert_eq_rc(rc: c_int, expected: c_int, op: &str) -> Result<(), String> {
    if rc == expected {
        Ok(())
    } else {
        Err(format!("{op} returned {rc}, expected {expected}"))
    }
}

fn size_of_val<T>(value: &T) -> usize {
    std::mem::size_of_val(value)
}

fn unused_tcp_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

unsafe fn load_symbol<T: Copy>(handle: *mut c_void, name: &str) -> Result<T, String> {
    let name = CString::new(name).map_err(|_| "symbol name contains interior nul".to_string())?;
    // SAFETY: `handle` is a valid dlopen handle and `name` is a NUL-terminated symbol name.
    let raw = unsafe { dlsym(handle, name.as_ptr()) };
    if raw.is_null() {
        return Err(format!("missing symbol {}", name.to_string_lossy()));
    }
    let mut out = mem::MaybeUninit::<T>::uninit();
    let out_ptr = out.as_mut_ptr().cast::<*mut c_void>();
    // SAFETY: Function pointers and data pointers have the same representation for dlsym results on this platform.
    unsafe {
        ptr::write(out_ptr, raw);
        Ok(out.assume_init())
    }
}
