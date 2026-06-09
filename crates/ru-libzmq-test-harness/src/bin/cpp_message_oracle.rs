use std::env;
use std::ffi::{c_char, c_int, c_void, CString};
use std::mem;
use std::ptr;

const RTLD_NOW: c_int = 2;

#[cfg_attr(target_pointer_width = "64", repr(C, align(8)))]
#[cfg_attr(target_pointer_width = "32", repr(C, align(4)))]
struct ZmqMsg {
    bytes: [u8; 64],
}

type ZmqVersion = unsafe extern "C" fn(*mut c_int, *mut c_int, *mut c_int);
type ZmqMsgInitSize = unsafe extern "C" fn(*mut ZmqMsg, usize) -> c_int;
type ZmqMsgSize = unsafe extern "C" fn(*const ZmqMsg) -> usize;
type ZmqMsgClose = unsafe extern "C" fn(*mut ZmqMsg) -> c_int;

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
    let zmq_version: ZmqVersion = unsafe { load_symbol(handle, "zmq_version")? };
    // SAFETY: Symbols are loaded from a successfully opened original libzmq dynamic library.
    let zmq_msg_init_size: ZmqMsgInitSize = unsafe { load_symbol(handle, "zmq_msg_init_size")? };
    // SAFETY: Symbols are loaded from a successfully opened original libzmq dynamic library.
    let zmq_msg_size: ZmqMsgSize = unsafe { load_symbol(handle, "zmq_msg_size")? };
    // SAFETY: Symbols are loaded from a successfully opened original libzmq dynamic library.
    let zmq_msg_close: ZmqMsgClose = unsafe { load_symbol(handle, "zmq_msg_close")? };

    let mut major = 0;
    let mut minor = 0;
    let mut patch = 0;
    // SAFETY: Function pointer is resolved from original libzmq and output pointers are valid.
    unsafe {
        zmq_version(&mut major, &mut minor, &mut patch);
    }

    let mut msg = ZmqMsg { bytes: [0; 64] };
    // SAFETY: Function pointer is resolved from original libzmq and `msg` points to writable storage.
    let init_rc = unsafe { zmq_msg_init_size(&mut msg, 16) };
    // SAFETY: Function pointer is resolved from original libzmq and `msg` is initialized above.
    let size = unsafe { zmq_msg_size(&msg) };
    // SAFETY: Function pointer is resolved from original libzmq and `msg` is initialized above.
    let close_rc = unsafe { zmq_msg_close(&mut msg) };

    println!("{{\"case\":\"message_oracle\"}}");
    println!(
        "{{\"observation\":{{\"type\":\"version\",\"major\":{major},\"minor\":{minor},\"patch\":{patch}}}}}"
    );
    println!("{{\"observation\":{{\"type\":\"return_code\",\"op\":\"zmq_msg_init_size\",\"rc\":{init_rc}}}}}");
    println!("{{\"observation\":{{\"type\":\"message_size\",\"size\":{size}}}}}");
    println!("{{\"observation\":{{\"type\":\"return_code\",\"op\":\"zmq_msg_close\",\"rc\":{close_rc}}}}}");

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
