use std::env;
use std::ffi::{c_char, c_int, c_void, CString};
use std::mem;
use std::net::TcpListener;
use std::ptr;

use libzmq_core::{ZmtpFrame, ZmtpGreeting};

const RTLD_NOW: c_int = 2;
const ZMQ_PAIR: c_int = 0;
const ZMQ_LINGER: c_int = 17;
const ZMQ_RCVTIMEO: c_int = 27;

type ZmqCtxNew = unsafe extern "C" fn() -> *mut c_void;
type ZmqCtxTerm = unsafe extern "C" fn(*mut c_void) -> c_int;
type ZmqSocket = unsafe extern "C" fn(*mut c_void, c_int) -> *mut c_void;
type ZmqClose = unsafe extern "C" fn(*mut c_void) -> c_int;
type ZmqBind = unsafe extern "C" fn(*mut c_void, *const c_char) -> c_int;
type ZmqConnect = unsafe extern "C" fn(*mut c_void, *const c_char) -> c_int;
type ZmqSend = unsafe extern "C" fn(*mut c_void, *const c_void, usize, c_int) -> c_int;
type ZmqRecv = unsafe extern "C" fn(*mut c_void, *mut c_void, usize, c_int) -> c_int;
type ZmqSetSockOpt = unsafe extern "C" fn(*mut c_void, c_int, *const c_void, usize) -> c_int;
type ZmqErrno = unsafe extern "C" fn() -> c_int;

unsafe extern "C" {
    fn dlopen(filename: *const c_char, flags: c_int) -> *mut c_void;
    fn dlsym(handle: *mut c_void, symbol: *const c_char) -> *mut c_void;
    fn dlclose(handle: *mut c_void) -> c_int;
}

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
    zmq_errno: ZmqErrno,
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
        zmq_errno: unsafe { load_symbol(handle, "zmq_errno")? },
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
    cpp_server_rust_client(&cpp)?;
    if let Err(error) = rust_server_cpp_client(&cpp) {
        println!(
            "{{\"observation\":{{\"type\":\"rust_server_cpp_client\",\"blocked\":true,\"error\":\"{}\"}}}}",
            json_escape(&error)
        );
    }
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
