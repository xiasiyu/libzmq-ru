use std::env;
use std::ffi::{c_char, c_int, c_void, CString};
use std::mem;
use std::ptr;

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

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 4 {
        eprintln!("usage: cpp_tcp_pair_peer <server|client> <endpoint> <payload>");
        std::process::exit(2);
    }
    let path = env::var("LIBZMQ_ORACLE")
        .unwrap_or_else(|_| "../libzmq/build-ru-oracle/lib/libzmq.dylib".to_string());
    let path = CString::new(path).unwrap();
    // SAFETY: `path` is a valid NUL-terminated string and `RTLD_NOW` is a valid dlopen flag.
    let handle = unsafe { dlopen(path.as_ptr(), RTLD_NOW) };
    if handle.is_null() {
        eprintln!("failed to load original libzmq oracle");
        std::process::exit(2);
    }
    let result = unsafe { run(handle, &args[1], &args[2], args[3].as_bytes()) };
    // SAFETY: `handle` was returned by a successful `dlopen` call above.
    unsafe {
        dlclose(handle);
    }
    if let Err(error) = result {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

unsafe fn run(
    handle: *mut c_void,
    mode: &str,
    endpoint: &str,
    payload: &[u8],
) -> Result<(), String> {
    // SAFETY: All function pointers are loaded from a valid libzmq handle and called with valid pointers.
    unsafe {
        let ctx_new: ZmqCtxNew = load_symbol(handle, "zmq_ctx_new")?;
        let ctx_term: ZmqCtxTerm = load_symbol(handle, "zmq_ctx_term")?;
        let socket_fn: ZmqSocket = load_symbol(handle, "zmq_socket")?;
        let close: ZmqClose = load_symbol(handle, "zmq_close")?;
        let bind: ZmqBind = load_symbol(handle, "zmq_bind")?;
        let connect: ZmqConnect = load_symbol(handle, "zmq_connect")?;
        let send: ZmqSend = load_symbol(handle, "zmq_send")?;
        let recv: ZmqRecv = load_symbol(handle, "zmq_recv")?;
        let setsockopt: ZmqSetSockOpt = load_symbol(handle, "zmq_setsockopt")?;
        let zmq_errno: ZmqErrno = load_symbol(handle, "zmq_errno")?;
        let endpoint = CString::new(endpoint).unwrap();
        let ctx = ctx_new();
        let socket = socket_fn(ctx, ZMQ_PAIR);
        let linger = 0i32;
        let timeout = 3_000i32;
        setsockopt(
            socket,
            ZMQ_LINGER,
            (&linger as *const i32).cast(),
            mem::size_of_val(&linger),
        );
        setsockopt(
            socket,
            ZMQ_RCVTIMEO,
            (&timeout as *const i32).cast(),
            mem::size_of_val(&timeout),
        );
        if mode == "server" {
            if bind(socket, endpoint.as_ptr()) != 0 {
                return Err("cpp bind failed".to_string());
            }
            let mut buffer = [0u8; 64];
            let rc = recv(socket, buffer.as_mut_ptr().cast(), buffer.len(), 0);
            if rc < 0 {
                return Err(format!("cpp recv failed rc {rc} errno {}", zmq_errno()));
            }
            println!("{}", String::from_utf8_lossy(&buffer[..rc as usize]));
        } else {
            if connect(socket, endpoint.as_ptr()) != 0 {
                return Err("cpp connect failed".to_string());
            }
            let rc = send(socket, payload.as_ptr().cast(), payload.len(), 0);
            if rc != payload.len() as i32 {
                return Err(format!("cpp send failed rc {rc} errno {}", zmq_errno()));
            }
            std::thread::sleep(std::time::Duration::from_secs(2));
        }
        close(socket);
        ctx_term(ctx);
    }
    Ok(())
}

unsafe fn load_symbol<T: Copy>(handle: *mut c_void, name: &str) -> Result<T, String> {
    let name = CString::new(name).unwrap();
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
