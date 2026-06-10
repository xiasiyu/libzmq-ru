#![allow(clippy::undocumented_unsafe_blocks)]

// This oracle dynamically loads the original C++ libzmq C ABI for differential tests.

use std::env;
use std::ffi::{c_char, c_int, c_void, CString};
use std::mem;
use std::ptr;

const RTLD_NOW: c_int = 2;
const ZMQ_PAIR: c_int = 0;
const ZMQ_PULL: c_int = 7;
const ZMQ_PUSH: c_int = 8;
const ZMQ_DONTWAIT: c_int = 1;

type ZmqCtxNew = unsafe extern "C" fn() -> *mut c_void;
type ZmqCtxTerm = unsafe extern "C" fn(*mut c_void) -> c_int;
type ZmqSocket = unsafe extern "C" fn(*mut c_void, c_int) -> *mut c_void;
type ZmqClose = unsafe extern "C" fn(*mut c_void) -> c_int;
type ZmqBind = unsafe extern "C" fn(*mut c_void, *const c_char) -> c_int;
type ZmqConnect = unsafe extern "C" fn(*mut c_void, *const c_char) -> c_int;
type ZmqSend = unsafe extern "C" fn(*mut c_void, *const c_void, usize, c_int) -> c_int;
type ZmqRecv = unsafe extern "C" fn(*mut c_void, *mut c_void, usize, c_int) -> c_int;

unsafe extern "C" {
    fn dlopen(filename: *const c_char, flags: c_int) -> *mut c_void;
    fn dlsym(handle: *mut c_void, symbol: *const c_char) -> *mut c_void;
    fn dlclose(handle: *mut c_void) -> c_int;
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

    let result = run_oracle(handle);
    // SAFETY: `handle` was returned by a successful `dlopen` call above.
    unsafe {
        dlclose(handle);
    }
    if let Err(error) = result {
        eprintln!("{error}");
        std::process::exit(2);
    }
}

fn run_oracle(handle: *mut c_void) -> Result<(), String> {
    // SAFETY: Symbols are loaded from a successfully opened original libzmq dynamic library.
    let zmq_ctx_new: ZmqCtxNew = unsafe { load_symbol(handle, "zmq_ctx_new")? };
    let zmq_ctx_term: ZmqCtxTerm = unsafe { load_symbol(handle, "zmq_ctx_term")? };
    let zmq_socket: ZmqSocket = unsafe { load_symbol(handle, "zmq_socket")? };
    let zmq_close: ZmqClose = unsafe { load_symbol(handle, "zmq_close")? };
    let zmq_bind: ZmqBind = unsafe { load_symbol(handle, "zmq_bind")? };
    let zmq_connect: ZmqConnect = unsafe { load_symbol(handle, "zmq_connect")? };
    let zmq_send: ZmqSend = unsafe { load_symbol(handle, "zmq_send")? };
    let zmq_recv: ZmqRecv = unsafe { load_symbol(handle, "zmq_recv")? };

    println!("{{\"case\":\"stable_pattern_oracle\"}}");
    run_pair(
        zmq_ctx_new,
        zmq_ctx_term,
        zmq_socket,
        zmq_close,
        zmq_bind,
        zmq_connect,
        zmq_send,
        zmq_recv,
    )?;
    run_push_pull(
        zmq_ctx_new,
        zmq_ctx_term,
        zmq_socket,
        zmq_close,
        zmq_bind,
        zmq_connect,
        zmq_send,
        zmq_recv,
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_pair(
    zmq_ctx_new: ZmqCtxNew,
    zmq_ctx_term: ZmqCtxTerm,
    zmq_socket: ZmqSocket,
    zmq_close: ZmqClose,
    zmq_bind: ZmqBind,
    zmq_connect: ZmqConnect,
    zmq_send: ZmqSend,
    zmq_recv: ZmqRecv,
) -> Result<(), String> {
    unsafe {
        let ctx = zmq_ctx_new();
        let server = zmq_socket(ctx, ZMQ_PAIR);
        let client = zmq_socket(ctx, ZMQ_PAIR);
        let endpoint = CString::new("inproc://oracle_pair").unwrap();
        let bind_rc = zmq_bind(server, endpoint.as_ptr());
        let connect_rc = zmq_connect(client, endpoint.as_ptr());
        let send_rc = zmq_send(client, b"hello".as_ptr().cast(), 5, 0);
        let mut buffer = [0u8; 16];
        let recv_rc = zmq_recv(
            server,
            buffer.as_mut_ptr().cast(),
            buffer.len(),
            ZMQ_DONTWAIT,
        );
        println!("{{\"observation\":{{\"type\":\"pair\",\"bind\":{bind_rc},\"connect\":{connect_rc},\"send\":{send_rc},\"recv\":{recv_rc},\"data\":\"{}\"}}}}", String::from_utf8_lossy(&buffer[..recv_rc.max(0) as usize]));
        zmq_close(client);
        zmq_close(server);
        zmq_ctx_term(ctx);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_push_pull(
    zmq_ctx_new: ZmqCtxNew,
    zmq_ctx_term: ZmqCtxTerm,
    zmq_socket: ZmqSocket,
    zmq_close: ZmqClose,
    zmq_bind: ZmqBind,
    zmq_connect: ZmqConnect,
    zmq_send: ZmqSend,
    zmq_recv: ZmqRecv,
) -> Result<(), String> {
    unsafe {
        let ctx = zmq_ctx_new();
        let pull = zmq_socket(ctx, ZMQ_PULL);
        let push = zmq_socket(ctx, ZMQ_PUSH);
        let endpoint = CString::new("inproc://oracle_push_pull").unwrap();
        let bind_rc = zmq_bind(pull, endpoint.as_ptr());
        let connect_rc = zmq_connect(push, endpoint.as_ptr());
        let send_rc = zmq_send(push, b"job".as_ptr().cast(), 3, 0);
        let mut buffer = [0u8; 16];
        let recv_rc = zmq_recv(pull, buffer.as_mut_ptr().cast(), buffer.len(), ZMQ_DONTWAIT);
        println!("{{\"observation\":{{\"type\":\"push_pull\",\"bind\":{bind_rc},\"connect\":{connect_rc},\"send\":{send_rc},\"recv\":{recv_rc},\"data\":\"{}\"}}}}", String::from_utf8_lossy(&buffer[..recv_rc.max(0) as usize]));
        zmq_close(push);
        zmq_close(pull);
        zmq_ctx_term(ctx);
    }
    Ok(())
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
