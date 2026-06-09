use ru_libzmq_core::constants::*;
use ru_libzmq_core::{Context, Error, Message, Socket, SocketType};
use std::cell::Cell;
use std::convert::TryFrom;
use std::ffi::{c_char, c_int, c_void, CStr, CString};
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

impl Default for zmq_msg_t {
    fn default() -> Self {
        Self { bytes: [0; 64] }
    }
}

type ZmqFreeFn = Option<extern "C" fn(data: *mut c_void, hint: *mut c_void)>;
type ZmqTimerFn = Option<extern "C" fn(timer_id: c_int, arg: *mut c_void)>;
type ZmqThreadFn = Option<extern "C" fn(arg: *mut c_void)>;

#[repr(C)]
pub struct ZmqPollItem {
    socket: *mut c_void,
    fd: ZmqFd,
    events: i16,
    revents: i16,
}

#[repr(C)]
pub struct ZmqPollerEvent {
    socket: *mut c_void,
    fd: ZmqFd,
    user_data: *mut c_void,
    events: i16,
}

#[repr(C)]
pub struct Iovec {
    iov_base: *mut c_void,
    iov_len: usize,
}

#[cfg(windows)]
type ZmqFd = usize;
#[cfg(not(windows))]
type ZmqFd = c_int;

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
    more: bool,
    routing_id: u32,
    group: Option<CString>,
    metadata: Vec<(CString, CString)>,
}

impl FfiMessageInner {
    fn empty() -> Self {
        Self {
            storage: MessageStorage::Owned(Message::new()),
            more: false,
            routing_id: 0,
            group: None,
            metadata: Vec::new(),
        }
    }

    fn with_size(size: usize) -> Self {
        Self {
            storage: MessageStorage::Owned(Message::from_vec(vec![0; size])),
            more: false,
            routing_id: 0,
            group: None,
            metadata: Vec::new(),
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
            more: false,
            routing_id: 0,
            group: None,
            metadata: Vec::new(),
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

    fn copy_owned(&self) -> Self {
        let bytes = match &self.storage {
            MessageStorage::Owned(message) => message.data().to_vec(),
            MessageStorage::External { data, size, .. } => {
                if *size == 0 {
                    Vec::new()
                } else {
                    // SAFETY: External message storage is valid for `size` bytes until the message is closed.
                    unsafe { std::slice::from_raw_parts((*data).cast::<u8>(), *size).to_vec() }
                }
            }
        };

        Self {
            storage: MessageStorage::Owned(Message::from_vec(bytes)),
            more: self.more,
            routing_id: self.routing_id,
            group: self.group.clone(),
            metadata: self.metadata.clone(),
        }
    }

    fn set_metadata(&mut self, key: &str, value: &str) -> Result<(), Error> {
        if key.is_empty() || key.as_bytes().contains(&0) || value.as_bytes().contains(&0) {
            return Err(Error::InvalidArgument);
        }

        let key = CString::new(key).map_err(|_| Error::InvalidArgument)?;
        let value = CString::new(value).map_err(|_| Error::InvalidArgument)?;

        if let Some((_, stored_value)) = self
            .metadata
            .iter_mut()
            .find(|(stored_key, _)| stored_key.as_bytes() == key.as_bytes())
        {
            *stored_value = value;
        } else {
            self.metadata.push((key, value));
        }

        Ok(())
    }

    fn metadata(&self, key: &[u8]) -> Option<&CString> {
        self.metadata
            .iter()
            .find(|(stored_key, _)| stored_key.as_bytes() == key)
            .map(|(_, value)| value)
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

fn unsupported_int(name: &'static str) -> c_int {
    set_error(Error::NotImplemented(name))
}

fn unsupported_ptr<T>(name: &'static str) -> *mut T {
    set_errno(Error::NotImplemented(name).errno());
    ptr::null_mut()
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
pub extern "C" fn zmq_msg_send(msg: *mut zmq_msg_t, socket: *mut c_void, flags: c_int) -> c_int {
    if msg.is_null() {
        return set_error(Error::InvalidArgument);
    }
    zmq_sendmsg(socket, msg, flags)
}

#[no_mangle]
pub extern "C" fn zmq_msg_recv(msg: *mut zmq_msg_t, socket: *mut c_void, flags: c_int) -> c_int {
    if msg.is_null() {
        return set_error(Error::InvalidArgument);
    }
    zmq_recvmsg(socket, msg, flags)
}

#[no_mangle]
pub extern "C" fn zmq_msg_move(dest: *mut zmq_msg_t, src: *mut zmq_msg_t) -> c_int {
    if dest.is_null() || src.is_null() {
        return set_error(Error::InvalidArgument);
    }
    let inner = take_msg_inner(src);
    write_msg_inner(dest, inner);
    write_msg_inner(src, Box::into_raw(Box::new(FfiMessageInner::empty())));
    clear_errno();
    0
}

#[no_mangle]
pub extern "C" fn zmq_msg_copy(dest: *mut zmq_msg_t, src: *mut zmq_msg_t) -> c_int {
    if dest.is_null() || src.is_null() {
        return set_error(Error::InvalidArgument);
    }
    let src_inner = read_msg_inner(src.cast_const());
    if src_inner.is_null() {
        return set_error(Error::InvalidArgument);
    }
    // SAFETY: Non-null source message inner pointer is owned by `src` until close/move.
    let copied = unsafe { (*src_inner).copy_owned() };
    write_msg_inner(dest, Box::into_raw(Box::new(copied)));
    clear_errno();
    0
}

#[no_mangle]
pub extern "C" fn zmq_msg_more(msg: *const zmq_msg_t) -> c_int {
    if msg.is_null() {
        return set_error(Error::InvalidArgument);
    }
    let inner = read_msg_inner(msg);
    if inner.is_null() {
        return set_error(Error::InvalidArgument);
    }
    clear_errno();
    // SAFETY: Non-null message inner pointer is owned by the zmq_msg_t until close/move.
    unsafe { i32::from((*inner).more) }
}

#[no_mangle]
pub extern "C" fn zmq_msg_get(msg: *const zmq_msg_t, _property: c_int) -> c_int {
    if msg.is_null() {
        return set_error(Error::InvalidArgument);
    }
    let inner = read_msg_inner(msg);
    if inner.is_null() {
        return set_error(Error::InvalidArgument);
    }
    // SAFETY: Non-null message inner pointer is owned by the zmq_msg_t until close/move.
    unsafe {
        match _property {
            ZMQ_MORE => {
                clear_errno();
                i32::from((*inner).more)
            }
            ZMQ_SHARED => {
                clear_errno();
                0
            }
            _ => unsupported_int("zmq_msg_get"),
        }
    }
}

#[no_mangle]
pub extern "C" fn zmq_msg_set(msg: *mut zmq_msg_t, _property: c_int, _optval: c_int) -> c_int {
    if msg.is_null() {
        return set_error(Error::InvalidArgument);
    }
    let inner = read_msg_inner(msg.cast_const());
    if inner.is_null() {
        return set_error(Error::InvalidArgument);
    }
    // SAFETY: Non-null message inner pointer is owned by the zmq_msg_t until close/move.
    unsafe {
        match _property {
            ZMQ_MORE => {
                (*inner).more = _optval != 0;
                clear_errno();
                0
            }
            _ => unsupported_int("zmq_msg_set"),
        }
    }
}

#[no_mangle]
pub extern "C" fn zmq_msg_gets(msg: *const zmq_msg_t, _property: *const c_char) -> *const c_char {
    if msg.is_null() {
        set_errno(EINVAL);
        return ptr::null();
    }
    if _property.is_null() {
        set_errno(EINVAL);
        return ptr::null();
    }
    let inner = read_msg_inner(msg);
    if inner.is_null() {
        set_errno(EINVAL);
        return ptr::null();
    }
    // SAFETY: libzmq C ABI requires a valid NUL-terminated property string.
    let property = unsafe { CStr::from_ptr(_property) };
    let property = property.to_bytes();
    if property == b"Group" {
        // SAFETY: Non-null message inner pointer is owned by the zmq_msg_t until close/move.
        if let Some(group) = unsafe { &(*inner).group } {
            clear_errno();
            return group.as_ptr();
        }
    }
    // SAFETY: Non-null message inner pointer is owned by the zmq_msg_t until close/move.
    if let Some(value) = unsafe { (*inner).metadata(property) } {
        clear_errno();
        return value.as_ptr();
    }
    set_errno(ENOTSUP);
    ptr::null()
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

#[no_mangle]
pub extern "C" fn zmq_setsockopt(
    socket: *mut c_void,
    _option: c_int,
    _optval: *const c_void,
    _optvallen: usize,
) -> c_int {
    if let Err(error) = socket_from_raw(socket) {
        return set_error(error);
    }
    unsupported_int("zmq_setsockopt")
}

#[no_mangle]
pub extern "C" fn zmq_getsockopt(
    socket: *mut c_void,
    _option: c_int,
    _optval: *mut c_void,
    _optvallen: *mut usize,
) -> c_int {
    if let Err(error) = socket_from_raw(socket) {
        return set_error(error);
    }
    unsupported_int("zmq_getsockopt")
}

#[no_mangle]
pub extern "C" fn zmq_unbind(socket: *mut c_void, _endpoint: *const c_char) -> c_int {
    if let Err(error) = socket_from_raw(socket) {
        return set_error(error);
    }
    unsupported_int("zmq_unbind")
}

#[no_mangle]
pub extern "C" fn zmq_disconnect(socket: *mut c_void, _endpoint: *const c_char) -> c_int {
    if let Err(error) = socket_from_raw(socket) {
        return set_error(error);
    }
    unsupported_int("zmq_disconnect")
}

#[no_mangle]
pub extern "C" fn zmq_send_const(
    socket: *mut c_void,
    buf: *const c_void,
    len: usize,
    flags: c_int,
) -> c_int {
    zmq_send(socket, buf, len, flags)
}

#[no_mangle]
pub extern "C" fn zmq_socket_monitor(
    socket: *mut c_void,
    _endpoint: *const c_char,
    _events: c_int,
) -> c_int {
    if let Err(error) = socket_from_raw(socket) {
        return set_error(error);
    }
    unsupported_int("zmq_socket_monitor")
}

#[no_mangle]
pub extern "C" fn zmq_poll(_items: *mut ZmqPollItem, _nitems: c_int, _timeout: isize) -> c_int {
    unsupported_int("zmq_poll")
}

#[no_mangle]
pub extern "C" fn zmq_proxy(
    _frontend: *mut c_void,
    _backend: *mut c_void,
    _capture: *mut c_void,
) -> c_int {
    unsupported_int("zmq_proxy")
}

#[no_mangle]
pub extern "C" fn zmq_proxy_steerable(
    _frontend: *mut c_void,
    _backend: *mut c_void,
    _capture: *mut c_void,
    _control: *mut c_void,
) -> c_int {
    unsupported_int("zmq_proxy_steerable")
}

#[no_mangle]
pub extern "C" fn zmq_has(_capability: *const c_char) -> c_int {
    0
}

#[no_mangle]
pub extern "C" fn zmq_device(_type: c_int, _frontend: *mut c_void, _backend: *mut c_void) -> c_int {
    unsupported_int("zmq_device")
}

#[no_mangle]
pub extern "C" fn zmq_sendmsg(socket: *mut c_void, _msg: *mut zmq_msg_t, _flags: c_int) -> c_int {
    if let Err(error) = socket_from_raw(socket) {
        return set_error(error);
    }
    unsupported_int("zmq_sendmsg")
}

#[no_mangle]
pub extern "C" fn zmq_recvmsg(socket: *mut c_void, _msg: *mut zmq_msg_t, _flags: c_int) -> c_int {
    if let Err(error) = socket_from_raw(socket) {
        return set_error(error);
    }
    unsupported_int("zmq_recvmsg")
}

#[no_mangle]
pub extern "C" fn zmq_sendiov(
    socket: *mut c_void,
    _iov: *mut Iovec,
    _count: usize,
    _flags: c_int,
) -> c_int {
    if let Err(error) = socket_from_raw(socket) {
        return set_error(error);
    }
    unsupported_int("zmq_sendiov")
}

#[no_mangle]
pub extern "C" fn zmq_recviov(
    socket: *mut c_void,
    _iov: *mut Iovec,
    _count: *mut usize,
    _flags: c_int,
) -> c_int {
    if let Err(error) = socket_from_raw(socket) {
        return set_error(error);
    }
    unsupported_int("zmq_recviov")
}

#[no_mangle]
pub extern "C" fn zmq_z85_encode(
    _dest: *mut c_char,
    _data: *const u8,
    _size: usize,
) -> *mut c_char {
    unsupported_ptr("zmq_z85_encode")
}

#[no_mangle]
pub extern "C" fn zmq_z85_decode(_dest: *mut u8, _string: *const c_char) -> *mut u8 {
    unsupported_ptr("zmq_z85_decode")
}

#[no_mangle]
pub extern "C" fn zmq_curve_keypair(_public_key: *mut c_char, _secret_key: *mut c_char) -> c_int {
    unsupported_int("zmq_curve_keypair")
}

#[no_mangle]
pub extern "C" fn zmq_curve_public(_public_key: *mut c_char, _secret_key: *const c_char) -> c_int {
    unsupported_int("zmq_curve_public")
}

#[no_mangle]
pub extern "C" fn zmq_atomic_counter_new() -> *mut c_void {
    unsupported_ptr("zmq_atomic_counter_new")
}

#[no_mangle]
pub extern "C" fn zmq_atomic_counter_set(_counter: *mut c_void, _value: c_int) {
    set_errno(ENOTSUP);
}

#[no_mangle]
pub extern "C" fn zmq_atomic_counter_inc(_counter: *mut c_void) -> c_int {
    unsupported_int("zmq_atomic_counter_inc")
}

#[no_mangle]
pub extern "C" fn zmq_atomic_counter_dec(_counter: *mut c_void) -> c_int {
    unsupported_int("zmq_atomic_counter_dec")
}

#[no_mangle]
pub extern "C" fn zmq_atomic_counter_value(_counter: *mut c_void) -> c_int {
    unsupported_int("zmq_atomic_counter_value")
}

#[no_mangle]
pub extern "C" fn zmq_atomic_counter_destroy(_counter: *mut *mut c_void) {
    set_errno(ENOTSUP);
}

#[no_mangle]
pub extern "C" fn zmq_timers_new() -> *mut c_void {
    unsupported_ptr("zmq_timers_new")
}

#[no_mangle]
pub extern "C" fn zmq_timers_destroy(_timers: *mut *mut c_void) -> c_int {
    unsupported_int("zmq_timers_destroy")
}

#[no_mangle]
pub extern "C" fn zmq_timers_add(
    _timers: *mut c_void,
    _interval: usize,
    _handler: ZmqTimerFn,
    _arg: *mut c_void,
) -> c_int {
    unsupported_int("zmq_timers_add")
}

#[no_mangle]
pub extern "C" fn zmq_timers_cancel(_timers: *mut c_void, _timer_id: c_int) -> c_int {
    unsupported_int("zmq_timers_cancel")
}

#[no_mangle]
pub extern "C" fn zmq_timers_set_interval(
    _timers: *mut c_void,
    _timer_id: c_int,
    _interval: usize,
) -> c_int {
    unsupported_int("zmq_timers_set_interval")
}

#[no_mangle]
pub extern "C" fn zmq_timers_reset(_timers: *mut c_void, _timer_id: c_int) -> c_int {
    unsupported_int("zmq_timers_reset")
}

#[no_mangle]
pub extern "C" fn zmq_timers_timeout(_timers: *mut c_void) -> isize {
    set_errno(ENOTSUP);
    -1
}

#[no_mangle]
pub extern "C" fn zmq_timers_execute(_timers: *mut c_void) -> c_int {
    unsupported_int("zmq_timers_execute")
}

#[no_mangle]
pub extern "C" fn zmq_stopwatch_start() -> *mut c_void {
    unsupported_ptr("zmq_stopwatch_start")
}

#[no_mangle]
pub extern "C" fn zmq_stopwatch_intermediate(_watch: *mut c_void) -> u64 {
    set_errno(ENOTSUP);
    0
}

#[no_mangle]
pub extern "C" fn zmq_stopwatch_stop(_watch: *mut c_void) -> u64 {
    set_errno(ENOTSUP);
    0
}

#[no_mangle]
pub extern "C" fn zmq_sleep(seconds: c_int) {
    if seconds > 0 {
        std::thread::sleep(std::time::Duration::from_secs(seconds as u64));
    }
}

#[no_mangle]
pub extern "C" fn zmq_threadstart(_func: ZmqThreadFn, _arg: *mut c_void) -> *mut c_void {
    unsupported_ptr("zmq_threadstart")
}

#[no_mangle]
pub extern "C" fn zmq_threadclose(_thread: *mut c_void) {
    set_errno(ENOTSUP);
}

#[no_mangle]
pub extern "C" fn zmq_ctx_set_ext(
    ctx: *mut c_void,
    _option: c_int,
    _optval: *const c_void,
    _optvallen: usize,
) -> c_int {
    if let Err(error) = context_from_raw(ctx) {
        return set_error(error);
    }
    unsupported_int("zmq_ctx_set_ext")
}

#[no_mangle]
pub extern "C" fn zmq_ctx_get_ext(
    ctx: *mut c_void,
    _option: c_int,
    _optval: *mut c_void,
    _optvallen: *mut usize,
) -> c_int {
    if let Err(error) = context_from_raw(ctx) {
        return set_error(error);
    }
    unsupported_int("zmq_ctx_get_ext")
}

#[no_mangle]
pub extern "C" fn zmq_join(socket: *mut c_void, _group: *const c_char) -> c_int {
    if let Err(error) = socket_from_raw(socket) {
        return set_error(error);
    }
    unsupported_int("zmq_join")
}

#[no_mangle]
pub extern "C" fn zmq_leave(socket: *mut c_void, _group: *const c_char) -> c_int {
    if let Err(error) = socket_from_raw(socket) {
        return set_error(error);
    }
    unsupported_int("zmq_leave")
}

#[no_mangle]
pub extern "C" fn zmq_connect_peer(socket: *mut c_void, _endpoint: *const c_char) -> u32 {
    if let Err(error) = socket_from_raw(socket) {
        set_errno(error.errno());
        return 0;
    }
    set_errno(ENOTSUP);
    0
}

#[no_mangle]
pub extern "C" fn zmq_disconnect_peer(socket: *mut c_void, _routing_id: u32) -> c_int {
    if let Err(error) = socket_from_raw(socket) {
        return set_error(error);
    }
    unsupported_int("zmq_disconnect_peer")
}

#[no_mangle]
pub extern "C" fn zmq_msg_set_routing_id(msg: *mut zmq_msg_t, _routing_id: u32) -> c_int {
    if msg.is_null() {
        return set_error(Error::InvalidArgument);
    }
    let inner = read_msg_inner(msg.cast_const());
    if inner.is_null() {
        return set_error(Error::InvalidArgument);
    }
    // SAFETY: Non-null message inner pointer is owned by the zmq_msg_t until close/move.
    unsafe {
        (*inner).routing_id = _routing_id;
        if let Err(error) = (*inner).set_metadata("Routing-Id", &_routing_id.to_string()) {
            return set_error(error);
        }
    }
    clear_errno();
    0
}

#[no_mangle]
pub extern "C" fn zmq_msg_routing_id(msg: *mut zmq_msg_t) -> u32 {
    if msg.is_null() {
        set_errno(EINVAL);
        return 0;
    }
    let inner = read_msg_inner(msg.cast_const());
    if inner.is_null() {
        set_errno(EINVAL);
        return 0;
    }
    clear_errno();
    // SAFETY: Non-null message inner pointer is owned by the zmq_msg_t until close/move.
    unsafe { (*inner).routing_id }
}

#[no_mangle]
pub extern "C" fn zmq_msg_set_group(msg: *mut zmq_msg_t, _group: *const c_char) -> c_int {
    if msg.is_null() || _group.is_null() {
        return set_error(Error::InvalidArgument);
    }
    let inner = read_msg_inner(msg.cast_const());
    if inner.is_null() {
        return set_error(Error::InvalidArgument);
    }
    // SAFETY: libzmq C ABI requires a valid NUL-terminated group string.
    let group = unsafe { CStr::from_ptr(_group) };
    if group.to_bytes().len() > 255 {
        return set_error(Error::InvalidArgument);
    }
    let group = match CString::new(group.to_bytes()) {
        Ok(group) => group,
        Err(_) => return set_error(Error::InvalidArgument),
    };
    // SAFETY: Non-null message inner pointer is owned by the zmq_msg_t until close/move.
    unsafe {
        (*inner).group = Some(group);
    }
    clear_errno();
    0
}

#[no_mangle]
pub extern "C" fn zmq_msg_group(msg: *mut zmq_msg_t) -> *const c_char {
    if msg.is_null() {
        set_errno(EINVAL);
        return ptr::null();
    }
    let inner = read_msg_inner(msg.cast_const());
    if inner.is_null() {
        set_errno(EINVAL);
        return ptr::null();
    }
    // SAFETY: Non-null message inner pointer is owned by the zmq_msg_t until close/move.
    unsafe {
        if let Some(group) = &(*inner).group {
            clear_errno();
            group.as_ptr()
        } else {
            clear_errno();
            ptr::null()
        }
    }
}

#[no_mangle]
pub extern "C" fn zmq_msg_init_buffer(
    msg: *mut zmq_msg_t,
    buf: *const c_void,
    size: usize,
) -> c_int {
    if msg.is_null() || (buf.is_null() && size != 0) {
        return set_error(Error::InvalidArgument);
    }
    let bytes = if size == 0 {
        Vec::new()
    } else {
        // SAFETY: `buf` was checked non-null for non-zero `size` and is read-only for `size` bytes.
        unsafe { std::slice::from_raw_parts(buf.cast::<u8>(), size).to_vec() }
    };
    let inner = FfiMessageInner {
        storage: MessageStorage::Owned(Message::from_vec(bytes)),
        more: false,
        routing_id: 0,
        group: None,
        metadata: Vec::new(),
    };
    write_msg_inner(msg, Box::into_raw(Box::new(inner)));
    clear_errno();
    0
}

#[no_mangle]
pub extern "C" fn zmq_poller_new() -> *mut c_void {
    unsupported_ptr("zmq_poller_new")
}

#[no_mangle]
pub extern "C" fn zmq_poller_destroy(_poller: *mut *mut c_void) -> c_int {
    unsupported_int("zmq_poller_destroy")
}

#[no_mangle]
pub extern "C" fn zmq_poller_size(_poller: *mut c_void) -> c_int {
    unsupported_int("zmq_poller_size")
}

#[no_mangle]
pub extern "C" fn zmq_poller_add(
    _poller: *mut c_void,
    _socket: *mut c_void,
    _user_data: *mut c_void,
    _events: i16,
) -> c_int {
    unsupported_int("zmq_poller_add")
}

#[no_mangle]
pub extern "C" fn zmq_poller_modify(
    _poller: *mut c_void,
    _socket: *mut c_void,
    _events: i16,
) -> c_int {
    unsupported_int("zmq_poller_modify")
}

#[no_mangle]
pub extern "C" fn zmq_poller_remove(_poller: *mut c_void, _socket: *mut c_void) -> c_int {
    unsupported_int("zmq_poller_remove")
}

#[no_mangle]
pub extern "C" fn zmq_poller_wait(
    _poller: *mut c_void,
    _event: *mut ZmqPollerEvent,
    _timeout: isize,
) -> c_int {
    unsupported_int("zmq_poller_wait")
}

#[no_mangle]
pub extern "C" fn zmq_poller_wait_all(
    _poller: *mut c_void,
    _events: *mut ZmqPollerEvent,
    _n_events: c_int,
    _timeout: isize,
) -> c_int {
    unsupported_int("zmq_poller_wait_all")
}

#[no_mangle]
pub extern "C" fn zmq_poller_fd(_poller: *mut c_void, _fd: *mut ZmqFd) -> c_int {
    unsupported_int("zmq_poller_fd")
}

#[no_mangle]
pub extern "C" fn zmq_poller_add_fd(
    _poller: *mut c_void,
    _fd: ZmqFd,
    _user_data: *mut c_void,
    _events: i16,
) -> c_int {
    unsupported_int("zmq_poller_add_fd")
}

#[no_mangle]
pub extern "C" fn zmq_poller_modify_fd(_poller: *mut c_void, _fd: ZmqFd, _events: i16) -> c_int {
    unsupported_int("zmq_poller_modify_fd")
}

#[no_mangle]
pub extern "C" fn zmq_poller_remove_fd(_poller: *mut c_void, _fd: ZmqFd) -> c_int {
    unsupported_int("zmq_poller_remove_fd")
}

#[no_mangle]
pub extern "C" fn zmq_socket_get_peer_state(
    socket: *mut c_void,
    _routing_id: *const c_void,
    _routing_id_size: usize,
) -> c_int {
    if let Err(error) = socket_from_raw(socket) {
        return set_error(error);
    }
    unsupported_int("zmq_socket_get_peer_state")
}

#[no_mangle]
pub extern "C" fn zmq_socket_monitor_versioned(
    socket: *mut c_void,
    _endpoint: *const c_char,
    _events: u64,
    _event_version: c_int,
    _type: c_int,
) -> c_int {
    if let Err(error) = socket_from_raw(socket) {
        return set_error(error);
    }
    unsupported_int("zmq_socket_monitor_versioned")
}

#[no_mangle]
pub extern "C" fn zmq_socket_monitor_pipes_stats(socket: *mut c_void) -> c_int {
    if let Err(error) = socket_from_raw(socket) {
        return set_error(error);
    }
    unsupported_int("zmq_socket_monitor_pipes_stats")
}

#[no_mangle]
pub extern "C" fn zmq_ppoll(
    _items: *mut ZmqPollItem,
    _nitems: c_int,
    _timeout: isize,
    _sigmask: *const c_void,
) -> c_int {
    unsupported_int("zmq_ppoll")
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
