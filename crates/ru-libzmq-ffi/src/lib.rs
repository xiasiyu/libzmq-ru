use ru_libzmq_core::constants::*;
use ru_libzmq_core::{Context, Error, Message, Socket, SocketType};
use std::cell::Cell;
use std::convert::TryFrom;
use std::ffi::{c_char, c_int, c_void, CStr};
use std::ptr;

thread_local! {
    static LAST_ERRNO: Cell<c_int> = Cell::new(0);
}

const STR_INVALID_ARGUMENT: &[u8] = b"Invalid argument\0";
const STR_BAD_ADDRESS: &[u8] = b"Bad address\0";
const STR_AGAIN: &[u8] = b"Resource temporarily unavailable\0";
const STR_NOT_SUPPORTED: &[u8] = b"Operation not supported\0";
const STR_NOT_SOCKET: &[u8] = b"Socket operation on non-socket\0";
const STR_FSM: &[u8] = b"Operation cannot be accomplished in current state\0";
const STR_INCOMPAT_PROTO: &[u8] = b"The protocol is not compatible with the socket type\0";
const STR_TERMINATED: &[u8] = b"Context was terminated\0";
const STR_NO_THREAD: &[u8] = b"No thread available\0";
const STR_UNKNOWN: &[u8] = b"Unknown error\0";

#[cfg_attr(target_pointer_width = "64", repr(C, align(8)))]
#[cfg_attr(target_pointer_width = "32", repr(C, align(4)))]
#[allow(non_camel_case_types)]
pub struct zmq_msg_t {
    bytes: [u8; 64],
}

type ZmqFreeFn = Option<extern "C" fn(data: *mut c_void, hint: *mut c_void)>;

struct OpaqueContext {
    inner: Context,
}

struct OpaqueSocket {
    inner: Socket,
}

enum MessageStorage {
    Owned(Message),
    External {
        data: *mut c_void,
        size: usize,
        free_fn: ZmqFreeFn,
        hint: *mut c_void,
    },
}

struct FfiMessageInner {
    storage: MessageStorage,
}

impl FfiMessageInner {
    fn empty() -> Self {
        Self {
            storage: MessageStorage::Owned(Message::new()),
        }
    }

    fn with_size(size: usize) -> Self {
        Self {
            storage: MessageStorage::Owned(Message::from_vec(vec![0; size])),
        }
    }

    fn with_external(
        data: *mut c_void,
        size: usize,
        free_fn: ZmqFreeFn,
        hint: *mut c_void,
    ) -> Self {
        Self {
            storage: MessageStorage::External {
                data,
                size,
                free_fn,
                hint,
            },
        }
    }

    fn data(&mut self) -> *mut c_void {
        match &mut self.storage {
            MessageStorage::Owned(message) => message.data_mut().as_mut_ptr().cast(),
            MessageStorage::External { data, .. } => *data,
        }
    }

    fn size(&self) -> usize {
        match &self.storage {
            MessageStorage::Owned(message) => message.len(),
            MessageStorage::External { size, .. } => *size,
        }
    }
}

impl Drop for FfiMessageInner {
    fn drop(&mut self) {
        if let MessageStorage::External {
            data,
            free_fn: Some(free_fn),
            hint,
            ..
        } = &mut self.storage
        {
            free_fn(*data, *hint);
        }
    }
}

fn set_errno(errno: c_int) {
    LAST_ERRNO.with(|cell| cell.set(errno));
}

fn set_error(error: Error) -> c_int {
    set_errno(error.errno());
    -1
}

fn clear_errno() {
    set_errno(0);
}

fn write_msg_inner(msg: *mut zmq_msg_t, inner: *mut FfiMessageInner) {
    let bytes = (inner as usize).to_ne_bytes();
    // SAFETY: The caller already checked that `msg` points to writable zmq_msg_t storage.
    unsafe {
        let msg_bytes = &mut (*msg).bytes;
        msg_bytes[..bytes.len()].copy_from_slice(&bytes);
        msg_bytes[bytes.len()..].fill(0);
    }
}

fn read_msg_inner(msg: *const zmq_msg_t) -> *mut FfiMessageInner {
    let mut bytes = [0u8; std::mem::size_of::<usize>()];
    let len = bytes.len();
    // SAFETY: The caller already checked that `msg` points to initialized zmq_msg_t storage.
    unsafe {
        let msg_bytes = &(*msg).bytes;
        bytes.copy_from_slice(&msg_bytes[..len]);
    }
    usize::from_ne_bytes(bytes) as *mut FfiMessageInner
}

fn take_msg_inner(msg: *mut zmq_msg_t) -> *mut FfiMessageInner {
    let inner = read_msg_inner(msg.cast_const());
    // SAFETY: The caller already checked that `msg` points to writable zmq_msg_t storage.
    unsafe {
        (*msg).bytes.fill(0);
    }
    inner
}

fn context_from_raw(ctx: *mut c_void) -> Result<&'static mut OpaqueContext, Error> {
    if ctx.is_null() {
        return Err(Error::InvalidContext);
    }
    // SAFETY: C ABI callers receive context pointers only from `zmq_ctx_new`/`zmq_init`.
    Ok(unsafe { &mut *(ctx.cast::<OpaqueContext>()) })
}

fn socket_from_raw(socket: *mut c_void) -> Result<&'static mut OpaqueSocket, Error> {
    if socket.is_null() {
        return Err(Error::InvalidSocket);
    }
    // SAFETY: C ABI callers receive socket pointers only from `zmq_socket`.
    Ok(unsafe { &mut *(socket.cast::<OpaqueSocket>()) })
}

fn endpoint_from_raw(endpoint: *const c_char) -> Result<&'static str, Error> {
    if endpoint.is_null() {
        return Err(Error::InvalidArgument);
    }
    // SAFETY: libzmq C ABI requires a valid NUL-terminated endpoint string.
    let cstr = unsafe { CStr::from_ptr(endpoint) };
    cstr.to_str().map_err(|_| Error::InvalidArgument)
}

#[no_mangle]
pub extern "C" fn zmq_version(major: *mut c_int, minor: *mut c_int, patch: *mut c_int) {
    let (version_major, version_minor, version_patch) = ru_libzmq_core::version();
    // SAFETY: Each output pointer is optional in practice; non-null pointers are writable ints.
    unsafe {
        if !major.is_null() {
            *major = version_major;
        }
        if !minor.is_null() {
            *minor = version_minor;
        }
        if !patch.is_null() {
            *patch = version_patch;
        }
    }
}

#[no_mangle]
pub extern "C" fn zmq_errno() -> c_int {
    LAST_ERRNO.with(Cell::get)
}

#[no_mangle]
pub extern "C" fn zmq_strerror(errnum: c_int) -> *const c_char {
    match errnum {
        EINVAL => STR_INVALID_ARGUMENT.as_ptr().cast(),
        EFAULT => STR_BAD_ADDRESS.as_ptr().cast(),
        EAGAIN => STR_AGAIN.as_ptr().cast(),
        ENOTSUP => STR_NOT_SUPPORTED.as_ptr().cast(),
        ENOTSOCK => STR_NOT_SOCKET.as_ptr().cast(),
        EFSM => STR_FSM.as_ptr().cast(),
        ENOCOMPATPROTO => STR_INCOMPAT_PROTO.as_ptr().cast(),
        ETERM => STR_TERMINATED.as_ptr().cast(),
        EMTHREAD => STR_NO_THREAD.as_ptr().cast(),
        _ => STR_UNKNOWN.as_ptr().cast(),
    }
}

#[no_mangle]
pub extern "C" fn zmq_ctx_new() -> *mut c_void {
    match Context::new() {
        Ok(inner) => {
            clear_errno();
            Box::into_raw(Box::new(OpaqueContext { inner })).cast()
        }
        Err(error) => {
            set_errno(error.errno());
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn zmq_ctx_term(ctx: *mut c_void) -> c_int {
    let raw_ctx = ctx.cast::<OpaqueContext>();
    let ctx = match context_from_raw(ctx) {
        Ok(ctx) => ctx,
        Err(error) => return set_error(error),
    };
    if let Err(error) = ctx.inner.terminate() {
        return set_error(error);
    }
    // SAFETY: The pointer was allocated by `Box::into_raw` in `zmq_ctx_new` and is consumed once here.
    unsafe {
        drop(Box::from_raw(raw_ctx));
    }
    clear_errno();
    0
}

#[no_mangle]
pub extern "C" fn zmq_ctx_shutdown(ctx: *mut c_void) -> c_int {
    match context_from_raw(ctx).and_then(|ctx| ctx.inner.shutdown()) {
        Ok(()) => {
            clear_errno();
            0
        }
        Err(error) => set_error(error),
    }
}

#[no_mangle]
pub extern "C" fn zmq_ctx_set(ctx: *mut c_void, _option: c_int, _optval: c_int) -> c_int {
    if let Err(error) = context_from_raw(ctx) {
        return set_error(error);
    }
    set_error(Error::NotImplemented("zmq_ctx_set"))
}

#[no_mangle]
pub extern "C" fn zmq_ctx_get(ctx: *mut c_void, _option: c_int) -> c_int {
    if let Err(error) = context_from_raw(ctx) {
        return set_error(error);
    }
    set_error(Error::NotImplemented("zmq_ctx_get"))
}

#[no_mangle]
pub extern "C" fn zmq_init(io_threads: c_int) -> *mut c_void {
    if io_threads < 0 {
        set_errno(EINVAL);
        return ptr::null_mut();
    }
    zmq_ctx_new()
}

#[no_mangle]
pub extern "C" fn zmq_term(ctx: *mut c_void) -> c_int {
    zmq_ctx_term(ctx)
}

#[no_mangle]
pub extern "C" fn zmq_ctx_destroy(ctx: *mut c_void) -> c_int {
    zmq_ctx_term(ctx)
}

#[no_mangle]
pub extern "C" fn zmq_socket(ctx: *mut c_void, socket_type: c_int) -> *mut c_void {
    let ctx = match context_from_raw(ctx) {
        Ok(ctx) => ctx,
        Err(error) => {
            set_errno(error.errno());
            return ptr::null_mut();
        }
    };
    let socket_type = match SocketType::try_from(socket_type) {
        Ok(socket_type) => socket_type,
        Err(error) => {
            set_errno(error.errno());
            return ptr::null_mut();
        }
    };
    match ctx.inner.socket(socket_type) {
        Ok(inner) => {
            clear_errno();
            Box::into_raw(Box::new(OpaqueSocket { inner })).cast()
        }
        Err(error) => {
            set_errno(error.errno());
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn zmq_close(socket: *mut c_void) -> c_int {
    if socket.is_null() {
        return set_error(Error::InvalidSocket);
    }
    // SAFETY: The pointer was allocated by `Box::into_raw` in `zmq_socket` and is consumed once here.
    unsafe {
        drop(Box::from_raw(socket.cast::<OpaqueSocket>()));
    }
    clear_errno();
    0
}

#[no_mangle]
pub extern "C" fn zmq_bind(socket: *mut c_void, endpoint: *const c_char) -> c_int {
    let socket = match socket_from_raw(socket) {
        Ok(socket) => socket,
        Err(error) => return set_error(error),
    };
    let endpoint = match endpoint_from_raw(endpoint) {
        Ok(endpoint) => endpoint,
        Err(error) => return set_error(error),
    };
    match socket.inner.bind(endpoint) {
        Ok(()) => {
            clear_errno();
            0
        }
        Err(error) => set_error(error),
    }
}

#[no_mangle]
pub extern "C" fn zmq_connect(socket: *mut c_void, endpoint: *const c_char) -> c_int {
    let socket = match socket_from_raw(socket) {
        Ok(socket) => socket,
        Err(error) => return set_error(error),
    };
    let endpoint = match endpoint_from_raw(endpoint) {
        Ok(endpoint) => endpoint,
        Err(error) => return set_error(error),
    };
    match socket.inner.connect(endpoint) {
        Ok(()) => {
            clear_errno();
            0
        }
        Err(error) => set_error(error),
    }
}

#[no_mangle]
pub extern "C" fn zmq_msg_init(msg: *mut zmq_msg_t) -> c_int {
    if msg.is_null() {
        return set_error(Error::InvalidArgument);
    }
    write_msg_inner(msg, Box::into_raw(Box::new(FfiMessageInner::empty())));
    clear_errno();
    0
}

#[no_mangle]
pub extern "C" fn zmq_msg_init_size(msg: *mut zmq_msg_t, size: usize) -> c_int {
    if msg.is_null() {
        return set_error(Error::InvalidArgument);
    }
    write_msg_inner(
        msg,
        Box::into_raw(Box::new(FfiMessageInner::with_size(size))),
    );
    clear_errno();
    0
}

#[no_mangle]
pub extern "C" fn zmq_msg_init_data(
    msg: *mut zmq_msg_t,
    data: *mut c_void,
    size: usize,
    free_fn: ZmqFreeFn,
    hint: *mut c_void,
) -> c_int {
    if msg.is_null() {
        return set_error(Error::InvalidArgument);
    }
    write_msg_inner(
        msg,
        Box::into_raw(Box::new(FfiMessageInner::with_external(
            data, size, free_fn, hint,
        ))),
    );
    clear_errno();
    0
}

#[no_mangle]
pub extern "C" fn zmq_msg_close(msg: *mut zmq_msg_t) -> c_int {
    if msg.is_null() {
        return set_error(Error::InvalidArgument);
    }
    let inner = take_msg_inner(msg);
    if !inner.is_null() {
        // SAFETY: Message inner pointers are allocated by `Box::into_raw` in `zmq_msg_init*`.
        unsafe {
            drop(Box::from_raw(inner));
        }
    }
    clear_errno();
    0
}

#[no_mangle]
pub extern "C" fn zmq_msg_data(msg: *mut zmq_msg_t) -> *mut c_void {
    if msg.is_null() {
        set_errno(EINVAL);
        return ptr::null_mut();
    }
    let inner = read_msg_inner(msg.cast_const());
    if inner.is_null() {
        set_errno(EINVAL);
        return ptr::null_mut();
    }
    clear_errno();
    // SAFETY: Non-null message inner pointer is owned by the zmq_msg_t until close/move.
    unsafe { (*inner).data() }
}

#[no_mangle]
pub extern "C" fn zmq_msg_size(msg: *const zmq_msg_t) -> usize {
    if msg.is_null() {
        set_errno(EINVAL);
        return 0;
    }
    let inner = read_msg_inner(msg);
    if inner.is_null() {
        set_errno(EINVAL);
        return 0;
    }
    clear_errno();
    // SAFETY: Non-null message inner pointer is owned by the zmq_msg_t until close/move.
    unsafe { (*inner).size() }
}

#[no_mangle]
pub extern "C" fn zmq_send(
    socket: *mut c_void,
    _buf: *const c_void,
    _len: usize,
    _flags: c_int,
) -> c_int {
    if let Err(error) = socket_from_raw(socket) {
        return set_error(error);
    }
    set_error(Error::NotImplemented("zmq_send"))
}

#[no_mangle]
pub extern "C" fn zmq_recv(
    socket: *mut c_void,
    _buf: *mut c_void,
    _len: usize,
    _flags: c_int,
) -> c_int {
    if let Err(error) = socket_from_raw(socket) {
        return set_error(error);
    }
    set_error(Error::NotImplemented("zmq_recv"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ffi_version_matches_baseline() {
        let mut major = 0;
        let mut minor = 0;
        let mut patch = 0;
        zmq_version(&mut major, &mut minor, &mut patch);
        assert_eq!((major, minor, patch), (4, 3, 6));
    }

    #[test]
    fn ffi_message_size_round_trip() {
        let mut msg = zmq_msg_t { bytes: [0; 64] };
        assert_eq!(zmq_msg_init_size(&mut msg, 8), 0);
        assert_eq!(zmq_msg_size(&msg), 8);
        assert_eq!(zmq_msg_close(&mut msg), 0);
    }
}
