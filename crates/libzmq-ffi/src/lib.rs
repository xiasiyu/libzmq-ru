// Exported C ABI functions must keep safe `extern "C"` signatures even when they
// validate and dereference raw C pointers internally.
#![allow(clippy::not_unsafe_ptr_arg_deref)]
#![allow(clippy::arc_with_non_send_sync)]

use libzmq_core::constants::*;
use libzmq_core::{Context, Error, Message, Socket, SocketType};
use std::cell::Cell;
use std::convert::TryFrom;
use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::ptr;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

thread_local! {
    static LAST_ERRNO: Cell<c_int> = const { Cell::new(0) };
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
#[derive(Clone, Copy)]
pub struct ZmqPollItem {
    pub socket: *mut c_void,
    pub fd: ZmqFd,
    pub events: i16,
    pub revents: i16,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ZmqPollerEvent {
    pub socket: *mut c_void,
    pub fd: ZmqFd,
    pub user_data: *mut c_void,
    pub events: i16,
}

#[repr(C)]
pub struct Iovec {
    pub iov_base: *mut c_void,
    pub iov_len: usize,
}

#[cfg(windows)]
type ZmqFd = usize;
#[cfg(not(windows))]
type ZmqFd = c_int;

fn invalid_zmq_fd() -> ZmqFd {
    #[cfg(windows)]
    {
        usize::MAX
    }
    #[cfg(not(windows))]
    {
        -1
    }
}

struct OpaqueContext {
    inner: Context,
}

struct OpaqueSocket {
    inner: Socket,
}

struct OpaqueAtomicCounter {
    value: AtomicI32,
}

struct OpaqueStopwatch {
    start: Instant,
}

struct OpaqueThread {
    handle: Option<JoinHandle<()>>,
}

struct OpaqueTimers {
    next_id: c_int,
    timers: Vec<TimerEntry>,
}

struct TimerEntry {
    id: c_int,
    interval: Duration,
    deadline: Instant,
    handler: ZmqTimerFn,
    arg: *mut c_void,
    active: bool,
}

struct OpaquePoller {
    entries: Mutex<Vec<PollerEntry>>,
}

struct PollerEntry {
    socket: *mut c_void,
    fd: ZmqFd,
    user_data: *mut c_void,
    events: i16,
    is_fd: bool,
}

enum MessageStorage {
    Owned(Message),
    External(Arc<ExternalMessage>),
}

struct ExternalMessage {
    data: *mut c_void,
    size: usize,
    free_fn: ZmqFreeFn,
    hint: *mut c_void,
}

impl Drop for ExternalMessage {
    fn drop(&mut self) {
        if let Some(free_fn) = self.free_fn {
            free_fn(self.data, self.hint);
        }
    }
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
            storage: MessageStorage::External(Arc::new(ExternalMessage {
                data,
                size,
                free_fn,
                hint,
            })),
            more: false,
            routing_id: 0,
            group: None,
            metadata: Vec::new(),
        }
    }

    fn data(&mut self) -> *mut c_void {
        match &mut self.storage {
            MessageStorage::Owned(message) => message.data_mut().as_mut_ptr().cast(),
            MessageStorage::External(external) => external.data,
        }
    }

    fn size(&self) -> usize {
        match &self.storage {
            MessageStorage::Owned(message) => message.len(),
            MessageStorage::External(external) => external.size,
        }
    }

    fn copy_message(&self) -> Self {
        let storage = match &self.storage {
            MessageStorage::Owned(message) => {
                MessageStorage::Owned(Message::from_vec(message.data().to_vec()))
            }
            MessageStorage::External(external) => MessageStorage::External(Arc::clone(external)),
        };

        Self {
            storage,
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

    fn to_core_message(&self) -> Result<Message, Error> {
        let data = match &self.storage {
            MessageStorage::Owned(message) => message.data().to_vec(),
            MessageStorage::External(external) => {
                if external.data.is_null() && external.size != 0 {
                    return Err(Error::InvalidArgument);
                }
                if external.size == 0 {
                    Vec::new()
                } else {
                    // SAFETY: External message storage is caller-provided as valid for `size` bytes until close.
                    unsafe { std::slice::from_raw_parts(external.data.cast::<u8>(), external.size) }
                        .to_vec()
                }
            }
        };
        let mut message = Message::from_vec(data);
        message.set_more(self.more);
        message.set_routing_id(self.routing_id);
        if let Some(group) = &self.group {
            message.set_group(
                group
                    .as_c_str()
                    .to_str()
                    .map_err(|_| Error::InvalidArgument)?,
            )?;
        }
        Ok(message)
    }

    fn from_core_message(message: Message) -> Self {
        let more = message.more();
        let routing_id = message.routing_id();
        let group = message.group().and_then(|group| CString::new(group).ok());
        Self {
            storage: MessageStorage::Owned(message),
            more,
            routing_id,
            group,
            metadata: Vec::new(),
        }
    }

    fn metadata(&self, key: &[u8]) -> Option<&CString> {
        self.metadata
            .iter()
            .find(|(stored_key, _)| stored_key.as_bytes() == key)
            .map(|(_, value)| value)
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

fn is_settable_bytes_sockopt(option: c_int) -> bool {
    matches!(
        option,
        ZMQ_PLAIN_USERNAME
            | ZMQ_ROUTING_ID
            | ZMQ_CONNECT_ROUTING_ID
            | ZMQ_PLAIN_PASSWORD
            | ZMQ_CURVE_PUBLICKEY
            | ZMQ_CURVE_SECRETKEY
            | ZMQ_CURVE_SERVERKEY
            | ZMQ_ZAP_DOMAIN
            | ZMQ_GSSAPI_PRINCIPAL
            | ZMQ_GSSAPI_SERVICE_PRINCIPAL
            | ZMQ_SOCKS_PROXY
            | ZMQ_SOCKS_USERNAME
            | ZMQ_SOCKS_PASSWORD
            | ZMQ_BINDTODEVICE
            | ZMQ_HELLO_MSG
            | ZMQ_DISCONNECT_MSG
            | ZMQ_HICCUP_MSG
    )
}

fn is_gettable_bytes_sockopt(option: c_int) -> bool {
    is_settable_bytes_sockopt(option)
        && !matches!(
            option,
            ZMQ_CONNECT_ROUTING_ID | ZMQ_HELLO_MSG | ZMQ_DISCONNECT_MSG | ZMQ_HICCUP_MSG
        )
        || option == ZMQ_LAST_ENDPOINT
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

fn poller_from_raw(poller: *mut c_void) -> Result<&'static mut OpaquePoller, Error> {
    if poller.is_null() {
        return Err(Error::InvalidArgument);
    }
    // SAFETY: C ABI callers receive poller pointers only from `zmq_poller_new`.
    Ok(unsafe { &mut *(poller.cast::<OpaquePoller>()) })
}

fn timers_from_raw(timers: *mut c_void) -> Result<&'static mut OpaqueTimers, Error> {
    if timers.is_null() {
        return Err(Error::InvalidArgument);
    }
    // SAFETY: C ABI callers receive timer pointers only from `zmq_timers_new`.
    Ok(unsafe { &mut *(timers.cast::<OpaqueTimers>()) })
}

fn socket_revents(socket: *mut c_void, requested: i16) -> Result<i16, Error> {
    let socket = socket_from_raw(socket)?;
    Ok(socket.inner.events()? & requested)
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
    let (version_major, version_minor, version_patch) = libzmq_core::version();
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
    match context_from_raw(ctx).and_then(|ctx| ctx.inner.set_option(_option, _optval)) {
        Ok(()) => {
            clear_errno();
            0
        }
        Err(error) => set_error(error),
    }
}

#[no_mangle]
pub extern "C" fn zmq_ctx_get(ctx: *mut c_void, _option: c_int) -> c_int {
    match context_from_raw(ctx).and_then(|ctx| ctx.inner.get_option(_option)) {
        Ok(value) => {
            clear_errno();
            value
        }
        Err(error) => set_error(error),
    }
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
    let copied = unsafe { (*src_inner).copy_message() };
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
            _ => set_error(Error::InvalidArgument),
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
    set_error(Error::InvalidArgument)
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
    buf: *const c_void,
    len: usize,
    flags: c_int,
) -> c_int {
    let socket = match socket_from_raw(socket) {
        Ok(socket) => socket,
        Err(error) => return set_error(error),
    };
    if buf.is_null() && len != 0 {
        return set_error(Error::InvalidArgument);
    }
    let bytes = if len == 0 {
        &[]
    } else {
        // SAFETY: `buf` was checked non-null for non-zero `len` and is read-only for `len` bytes.
        unsafe { std::slice::from_raw_parts(buf.cast::<u8>(), len) }
    };
    let message = Message::from_slice(bytes);
    match socket.inner.send(message, flags) {
        Ok(size) => {
            clear_errno();
            size as c_int
        }
        Err(error) => set_error(error),
    }
}

#[no_mangle]
pub extern "C" fn zmq_recv(
    socket: *mut c_void,
    buf: *mut c_void,
    len: usize,
    flags: c_int,
) -> c_int {
    let socket = match socket_from_raw(socket) {
        Ok(socket) => socket,
        Err(error) => return set_error(error),
    };
    if buf.is_null() && len != 0 {
        return set_error(Error::InvalidArgument);
    }
    match socket.inner.recv(flags) {
        Ok(message) => {
            let copy_len = len.min(message.len());
            if copy_len != 0 {
                // SAFETY: `buf` was checked non-null for non-zero `len`, and `copy_len <= len`.
                unsafe {
                    ptr::copy_nonoverlapping(message.data().as_ptr(), buf.cast::<u8>(), copy_len);
                }
            }
            clear_errno();
            message.len() as c_int
        }
        Err(error) => set_error(error),
    }
}

#[no_mangle]
pub extern "C" fn zmq_setsockopt(
    socket: *mut c_void,
    option: c_int,
    optval: *const c_void,
    optvallen: usize,
) -> c_int {
    let socket = match socket_from_raw(socket) {
        Ok(socket) => socket,
        Err(error) => return set_error(error),
    };
    if matches!(option, ZMQ_SUBSCRIBE | ZMQ_UNSUBSCRIBE) {
        if optval.is_null() && optvallen != 0 {
            return set_error(Error::InvalidArgument);
        }
        let prefix = if optvallen == 0 {
            &[][..]
        } else {
            // SAFETY: `optval` was checked non-null for non-zero `optvallen` and is read-only.
            unsafe { std::slice::from_raw_parts(optval.cast::<u8>(), optvallen) }
        };
        let result = if option == ZMQ_SUBSCRIBE {
            socket.inner.subscribe(prefix)
        } else {
            socket.inner.unsubscribe(prefix)
        };
        return match result {
            Ok(()) => {
                clear_errno();
                0
            }
            Err(error) => set_error(error),
        };
    }
    if is_settable_bytes_sockopt(option) || option == ZMQ_XPUB_WELCOME_MSG {
        if optval.is_null() && optvallen != 0 {
            return set_error(Error::InvalidArgument);
        }
        let value = if optvallen == 0 {
            &[][..]
        } else {
            // SAFETY: `optval` was checked non-null for non-zero `optvallen` and is read-only.
            unsafe { std::slice::from_raw_parts(optval.cast::<u8>(), optvallen) }
        };
        return match socket.inner.set_option_bytes(option, value) {
            Ok(()) => {
                clear_errno();
                0
            }
            Err(error) => set_error(error),
        };
    }
    if option == ZMQ_AFFINITY {
        if optval.is_null() || optvallen != std::mem::size_of::<u64>() {
            return set_error(Error::InvalidArgument);
        }
        // SAFETY: `optval` is non-null and `optvallen` matches `u64` size.
        let value = unsafe { *(optval.cast::<u64>()) };
        return match socket.inner.set_option_u64(option, value) {
            Ok(()) => {
                clear_errno();
                0
            }
            Err(error) => set_error(error),
        };
    }
    if option == ZMQ_MAXMSGSIZE {
        if optval.is_null() || optvallen != std::mem::size_of::<i64>() {
            return set_error(Error::InvalidArgument);
        }
        // SAFETY: `optval` is non-null and `optvallen` matches `i64` size.
        let value = unsafe { *(optval.cast::<i64>()) };
        return match socket.inner.set_option_i64(option, value) {
            Ok(()) => {
                clear_errno();
                0
            }
            Err(error) => set_error(error),
        };
    }
    if optval.is_null() || optvallen != std::mem::size_of::<c_int>() {
        return set_error(Error::InvalidArgument);
    }
    // SAFETY: `optval` was checked non-null and `optvallen` matches `c_int` size.
    let value = unsafe { *(optval.cast::<c_int>()) };
    match socket.inner.set_option_i32(option, value) {
        Ok(()) => {
            clear_errno();
            0
        }
        Err(error) => set_error(error),
    }
}

#[no_mangle]
pub extern "C" fn zmq_getsockopt(
    socket: *mut c_void,
    option: c_int,
    optval: *mut c_void,
    optvallen: *mut usize,
) -> c_int {
    let socket = match socket_from_raw(socket) {
        Ok(socket) => socket,
        Err(error) => return set_error(error),
    };
    if optval.is_null() || optvallen.is_null() {
        return set_error(Error::InvalidArgument);
    }
    // SAFETY: `optvallen` is non-null and points to caller-provided storage.
    let available = unsafe { *optvallen };
    if is_gettable_bytes_sockopt(option) {
        let value = match socket.inner.get_option_bytes(option) {
            Ok(value) => value,
            Err(error) => return set_error(error),
        };
        if available < value.len() {
            return set_error(Error::InvalidArgument);
        }
        if !value.is_empty() {
            // SAFETY: `optval` and `optvallen` are non-null; caller supplied enough storage.
            unsafe {
                ptr::copy_nonoverlapping(value.as_ptr(), optval.cast::<u8>(), value.len());
            }
        }
        // SAFETY: `optvallen` is non-null and points to caller-provided storage.
        unsafe {
            *optvallen = value.len();
        }
        clear_errno();
        return 0;
    }
    if option == ZMQ_AFFINITY {
        if available != std::mem::size_of::<u64>() {
            return set_error(Error::InvalidArgument);
        }
        let value = match socket.inner.get_option_u64(option) {
            Ok(value) => value,
            Err(error) => return set_error(error),
        };
        // SAFETY: `optval` and `optvallen` are non-null; caller supplied exactly `u64` space.
        unsafe {
            *(optval.cast::<u64>()) = value;
            *optvallen = std::mem::size_of::<u64>();
        }
        clear_errno();
        return 0;
    }
    if option == ZMQ_MAXMSGSIZE {
        if available != std::mem::size_of::<i64>() {
            return set_error(Error::InvalidArgument);
        }
        let value = match socket.inner.get_option_i64(option) {
            Ok(value) => value,
            Err(error) => return set_error(error),
        };
        // SAFETY: `optval` and `optvallen` are non-null; caller supplied exactly `i64` space.
        unsafe {
            *(optval.cast::<i64>()) = value;
            *optvallen = std::mem::size_of::<i64>();
        }
        clear_errno();
        return 0;
    }
    if available < std::mem::size_of::<c_int>() {
        return set_error(Error::InvalidArgument);
    }
    match socket.inner.get_option_i32(option) {
        Ok(value) => {
            // SAFETY: `optval` and `optvallen` are non-null; caller supplied enough space for `c_int`.
            unsafe {
                *(optval.cast::<c_int>()) = value;
                *optvallen = std::mem::size_of::<c_int>();
            }
            clear_errno();
            0
        }
        Err(error) => set_error(error),
    }
}

#[no_mangle]
pub extern "C" fn zmq_unbind(socket: *mut c_void, endpoint: *const c_char) -> c_int {
    let socket = match socket_from_raw(socket) {
        Ok(socket) => socket,
        Err(error) => return set_error(error),
    };
    let endpoint = match endpoint_from_raw(endpoint) {
        Ok(endpoint) => endpoint,
        Err(error) => return set_error(error),
    };
    match socket.inner.unbind(endpoint) {
        Ok(()) => {
            clear_errno();
            0
        }
        Err(error) => set_error(error),
    }
}

#[no_mangle]
pub extern "C" fn zmq_disconnect(socket: *mut c_void, endpoint: *const c_char) -> c_int {
    let socket = match socket_from_raw(socket) {
        Ok(socket) => socket,
        Err(error) => return set_error(error),
    };
    let endpoint = match endpoint_from_raw(endpoint) {
        Ok(endpoint) => endpoint,
        Err(error) => return set_error(error),
    };
    match socket.inner.disconnect(endpoint) {
        Ok(()) => {
            clear_errno();
            0
        }
        Err(error) => set_error(error),
    }
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
    endpoint: *const c_char,
    events: c_int,
) -> c_int {
    if endpoint.is_null() {
        return set_error(Error::InvalidArgument);
    }
    let socket = match socket_from_raw(socket) {
        Ok(socket) => socket,
        Err(error) => return set_error(error),
    };
    let endpoint = match endpoint_from_raw(endpoint) {
        Ok(endpoint) => endpoint,
        Err(error) => return set_error(error),
    };
    match socket.inner.monitor(endpoint, events as u64) {
        Ok(()) => {
            clear_errno();
            0
        }
        Err(error) => set_error(error),
    }
}

#[no_mangle]
pub extern "C" fn zmq_poll(items: *mut ZmqPollItem, nitems: c_int, timeout: isize) -> c_int {
    if nitems < 0 || (items.is_null() && nitems != 0) {
        return set_error(Error::InvalidArgument);
    }
    if nitems == 0 {
        if timeout > 0 {
            std::thread::sleep(Duration::from_millis(timeout as u64));
        }
        clear_errno();
        return 0;
    }

    let mut ready = 0;
    // SAFETY: `items` is non-null and points to `nitems` poll item records per C ABI contract.
    let items = unsafe { std::slice::from_raw_parts_mut(items, nitems as usize) };
    for item in items.iter_mut() {
        item.revents = 0;
        if !item.socket.is_null() {
            match socket_revents(item.socket, item.events) {
                Ok(revents) => {
                    item.revents = revents;
                    if revents != 0 {
                        ready += 1;
                    }
                }
                Err(error) => return set_error(error),
            }
        }
    }
    if ready == 0 && timeout > 0 {
        std::thread::sleep(Duration::from_millis(timeout as u64));
    }
    clear_errno();
    ready
}

#[no_mangle]
pub extern "C" fn zmq_proxy(
    _frontend: *mut c_void,
    _backend: *mut c_void,
    _capture: *mut c_void,
) -> c_int {
    let frontend = match socket_from_raw(_frontend) {
        Ok(socket) => socket,
        Err(error) => return set_error(error),
    };
    let backend = match socket_from_raw(_backend) {
        Ok(socket) => socket,
        Err(error) => return set_error(error),
    };
    if let Ok(message) = frontend.inner.recv(ZMQ_DONTWAIT) {
        if let Err(error) = backend.inner.send(message.clone(), 0) {
            return set_error(error);
        }
        if !_capture.is_null() {
            if let Ok(capture) = socket_from_raw(_capture) {
                let _ = capture.inner.send(message, 0);
            }
        }
    }
    clear_errno();
    0
}

#[no_mangle]
pub extern "C" fn zmq_proxy_steerable(
    _frontend: *mut c_void,
    _backend: *mut c_void,
    _capture: *mut c_void,
    _control: *mut c_void,
) -> c_int {
    zmq_proxy(_frontend, _backend, _capture)
}

#[no_mangle]
pub extern "C" fn zmq_has(capability: *const c_char) -> c_int {
    if capability.is_null() {
        return 0;
    }
    // SAFETY: `zmq_has` follows the C ABI contract: callers pass a valid
    // NUL-terminated capability string.
    let capability = unsafe { CStr::from_ptr(capability) }.to_bytes();
    let available = match capability {
        b"ipc" => cfg!(feature = "ipc"),
        b"pgm" => false,
        b"tipc" => false,
        b"norm" => cfg!(feature = "norm"),
        b"curve" => true,
        b"gssapi" => cfg!(feature = "gssapi"),
        b"vmci" => false,
        b"draft" => true,
        b"WS" => true,
        b"WSS" => cfg!(feature = "wss"),
        b"vsock" => false,
        _ => false,
    };
    i32::from(available)
}

#[no_mangle]
pub extern "C" fn zmq_device(_type: c_int, frontend: *mut c_void, backend: *mut c_void) -> c_int {
    zmq_proxy(frontend, backend, ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn zmq_sendmsg(socket: *mut c_void, msg: *mut zmq_msg_t, flags: c_int) -> c_int {
    let socket = match socket_from_raw(socket) {
        Ok(socket) => socket,
        Err(error) => return set_error(error),
    };
    if msg.is_null() {
        return set_error(Error::InvalidArgument);
    }
    let inner = read_msg_inner(msg.cast_const());
    if inner.is_null() {
        return set_error(Error::InvalidArgument);
    }
    // SAFETY: Non-null message inner pointer is owned by `msg` until close/move.
    let message = match unsafe { (*inner).to_core_message() } {
        Ok(message) => message,
        Err(error) => return set_error(error),
    };
    let send_flags = flags | if message.more() { ZMQ_SNDMORE } else { 0 };
    match socket.inner.send(message, send_flags) {
        Ok(size) => {
            clear_errno();
            size as c_int
        }
        Err(error) => set_error(error),
    }
}

#[no_mangle]
pub extern "C" fn zmq_recvmsg(socket: *mut c_void, msg: *mut zmq_msg_t, flags: c_int) -> c_int {
    let socket = match socket_from_raw(socket) {
        Ok(socket) => socket,
        Err(error) => return set_error(error),
    };
    if msg.is_null() {
        return set_error(Error::InvalidArgument);
    }
    match socket.inner.recv(flags) {
        Ok(message) => {
            let size = message.len();
            let previous = take_msg_inner(msg);
            if !previous.is_null() {
                // SAFETY: Previous message inner pointer is owned by `msg` and is being replaced.
                unsafe {
                    drop(Box::from_raw(previous));
                }
            }
            write_msg_inner(
                msg,
                Box::into_raw(Box::new(FfiMessageInner::from_core_message(message))),
            );
            clear_errno();
            size as c_int
        }
        Err(error) => set_error(error),
    }
}

#[no_mangle]
pub extern "C" fn zmq_sendiov(
    socket: *mut c_void,
    iov: *mut Iovec,
    count: usize,
    flags: c_int,
) -> c_int {
    let socket = match socket_from_raw(socket) {
        Ok(socket) => socket,
        Err(error) => return set_error(error),
    };
    if iov.is_null() || count == 0 {
        return set_error(Error::InvalidArgument);
    }
    // SAFETY: Caller provides `count` valid iovec entries by C ABI contract.
    let entries = unsafe { std::slice::from_raw_parts(iov, count) };
    let mut rc = 0;
    for (index, entry) in entries.iter().enumerate() {
        if entry.iov_base.is_null() && entry.iov_len != 0 {
            return set_error(Error::InvalidArgument);
        }
        let data = if entry.iov_len == 0 {
            Vec::new()
        } else {
            // SAFETY: Non-null `iov_base` points to `iov_len` bytes by C ABI contract.
            unsafe { std::slice::from_raw_parts(entry.iov_base.cast::<u8>(), entry.iov_len) }
                .to_vec()
        };
        let send_flags = if index == count - 1 {
            flags & !ZMQ_SNDMORE
        } else {
            flags
        };
        match socket.inner.send(Message::from_vec(data), send_flags) {
            Ok(size) => rc = size as c_int,
            Err(error) => return set_error(error),
        }
    }
    clear_errno();
    rc
}

#[no_mangle]
pub extern "C" fn zmq_recviov(
    socket: *mut c_void,
    iov: *mut Iovec,
    count: *mut usize,
    flags: c_int,
) -> c_int {
    let socket = match socket_from_raw(socket) {
        Ok(socket) => socket,
        Err(error) => return set_error(error),
    };
    if count.is_null() || iov.is_null() {
        return set_error(Error::InvalidArgument);
    }
    // SAFETY: `count` is non-null and writable by C ABI contract.
    let capacity = unsafe { *count };
    if capacity == 0 {
        return set_error(Error::InvalidArgument);
    }
    // SAFETY: Caller provides `capacity` writable iovec entries by C ABI contract.
    let entries = unsafe { std::slice::from_raw_parts_mut(iov, capacity) };
    // SAFETY: `count` is non-null and writable by C ABI contract.
    unsafe {
        *count = 0;
    }
    let mut read = 0;
    for entry in entries.iter_mut() {
        match socket.inner.recv(flags) {
            Ok(message) => {
                let allocation_len = message.len().max(1);
                // SAFETY: `malloc` returns memory that C callers may release with `free`.
                let allocation = unsafe { libc::malloc(allocation_len) };
                if allocation.is_null() {
                    return set_error(Error::OutOfMemory);
                }
                if !message.is_empty() {
                    // SAFETY: Allocation is writable for at least message length bytes.
                    unsafe {
                        ptr::copy_nonoverlapping(
                            message.data().as_ptr(),
                            allocation.cast::<u8>(),
                            message.len(),
                        );
                    }
                }
                entry.iov_base = allocation;
                entry.iov_len = message.len();
                read += 1;
                // SAFETY: `count` is non-null and writable by C ABI contract.
                unsafe {
                    *count = read;
                }
                if !message.more() {
                    clear_errno();
                    return read as c_int;
                }
            }
            Err(error) => return set_error(error),
        }
    }
    clear_errno();
    read as c_int
}

#[no_mangle]
pub extern "C" fn zmq_z85_encode(dest: *mut c_char, data: *const u8, size: usize) -> *mut c_char {
    if dest.is_null() || (data.is_null() && size != 0) {
        set_errno(EINVAL);
        return ptr::null_mut();
    }
    let bytes = if size == 0 {
        &[][..]
    } else {
        // SAFETY: `data` was checked non-null for non-zero `size` and points to `size` bytes.
        unsafe { std::slice::from_raw_parts(data, size) }
    };
    match libzmq_core::z85_encode(bytes) {
        Ok(encoded) => {
            // SAFETY: C ABI requires caller to provide `size * 5 / 4 + 1` writable bytes.
            unsafe {
                ptr::copy_nonoverlapping(encoded.as_ptr().cast::<c_char>(), dest, encoded.len());
                *dest.add(encoded.len()) = 0;
            }
            clear_errno();
            dest
        }
        Err(error) => {
            set_errno(error.errno());
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn zmq_z85_decode(dest: *mut u8, string: *const c_char) -> *mut u8 {
    if dest.is_null() || string.is_null() {
        set_errno(EINVAL);
        return ptr::null_mut();
    }
    // SAFETY: C ABI requires `string` to point to a valid NUL-terminated Z85 string.
    let string = unsafe { CStr::from_ptr(string) };
    let string = match string.to_str() {
        Ok(string) => string,
        Err(_) => {
            set_errno(EINVAL);
            return ptr::null_mut();
        }
    };
    match libzmq_core::z85_decode(string) {
        Ok(decoded) => {
            // SAFETY: C ABI requires caller to provide `strlen(string) * 4 / 5` writable bytes.
            unsafe {
                ptr::copy_nonoverlapping(decoded.as_ptr(), dest, decoded.len());
            }
            clear_errno();
            dest
        }
        Err(error) => {
            set_errno(error.errno());
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn zmq_curve_keypair(public_key: *mut c_char, secret_key: *mut c_char) -> c_int {
    if public_key.is_null() || secret_key.is_null() {
        return set_error(Error::InvalidArgument);
    }
    match libzmq_core::curve_keypair() {
        Ok((public, secret)) => {
            // SAFETY: C ABI requires caller to provide 41 writable bytes for each key.
            unsafe {
                ptr::copy_nonoverlapping(
                    public.as_ptr().cast::<c_char>(),
                    public_key,
                    public.len(),
                );
                *public_key.add(public.len()) = 0;
                ptr::copy_nonoverlapping(
                    secret.as_ptr().cast::<c_char>(),
                    secret_key,
                    secret.len(),
                );
                *secret_key.add(secret.len()) = 0;
            }
            clear_errno();
            0
        }
        Err(error) => set_error(error),
    }
}

#[no_mangle]
pub extern "C" fn zmq_curve_public(public_key: *mut c_char, secret_key: *const c_char) -> c_int {
    if public_key.is_null() || secret_key.is_null() {
        return set_error(Error::InvalidArgument);
    }
    // SAFETY: C ABI requires `secret_key` to point to a valid NUL-terminated 40-byte Z85 string.
    let secret_key = unsafe { CStr::from_ptr(secret_key) };
    let secret_key = match secret_key.to_str() {
        Ok(secret_key) => secret_key,
        Err(_) => return set_error(Error::InvalidArgument),
    };
    match libzmq_core::curve_public(secret_key) {
        Ok(public) => {
            // SAFETY: C ABI requires caller to provide 41 writable bytes for the public key.
            unsafe {
                ptr::copy_nonoverlapping(
                    public.as_ptr().cast::<c_char>(),
                    public_key,
                    public.len(),
                );
                *public_key.add(public.len()) = 0;
            }
            clear_errno();
            0
        }
        Err(error) => set_error(error),
    }
}

#[no_mangle]
pub extern "C" fn zmq_atomic_counter_new() -> *mut c_void {
    clear_errno();
    Box::into_raw(Box::new(OpaqueAtomicCounter {
        value: AtomicI32::new(0),
    }))
    .cast()
}

#[no_mangle]
pub extern "C" fn zmq_atomic_counter_set(counter: *mut c_void, value: c_int) {
    if counter.is_null() {
        set_errno(EINVAL);
        return;
    }
    // SAFETY: Pointer is allocated by `zmq_atomic_counter_new` and remains owned by caller.
    unsafe {
        (*(counter.cast::<OpaqueAtomicCounter>()))
            .value
            .store(value, Ordering::SeqCst)
    };
    clear_errno();
}

#[no_mangle]
pub extern "C" fn zmq_atomic_counter_inc(counter: *mut c_void) -> c_int {
    if counter.is_null() {
        return set_error(Error::InvalidArgument);
    }
    // SAFETY: Pointer is allocated by `zmq_atomic_counter_new` and remains owned by caller.
    let previous = unsafe {
        (*(counter.cast::<OpaqueAtomicCounter>()))
            .value
            .fetch_add(1, Ordering::SeqCst)
    };
    clear_errno();
    previous
}

#[no_mangle]
pub extern "C" fn zmq_atomic_counter_dec(counter: *mut c_void) -> c_int {
    if counter.is_null() {
        return set_error(Error::InvalidArgument);
    }
    // SAFETY: Pointer is allocated by `zmq_atomic_counter_new` and remains owned by caller.
    let previous = unsafe {
        (*(counter.cast::<OpaqueAtomicCounter>()))
            .value
            .fetch_sub(1, Ordering::SeqCst)
    };
    clear_errno();
    previous
}

#[no_mangle]
pub extern "C" fn zmq_atomic_counter_value(counter: *mut c_void) -> c_int {
    if counter.is_null() {
        return set_error(Error::InvalidArgument);
    }
    // SAFETY: Pointer is allocated by `zmq_atomic_counter_new` and remains owned by caller.
    let value = unsafe {
        (*(counter.cast::<OpaqueAtomicCounter>()))
            .value
            .load(Ordering::SeqCst)
    };
    clear_errno();
    value
}

#[no_mangle]
pub extern "C" fn zmq_atomic_counter_destroy(counter: *mut *mut c_void) {
    if counter.is_null() {
        set_errno(EINVAL);
        return;
    }
    // SAFETY: `counter` points to caller storage; inner pointer was allocated by `zmq_atomic_counter_new`.
    unsafe {
        if !(*counter).is_null() {
            drop(Box::from_raw((*counter).cast::<OpaqueAtomicCounter>()));
            *counter = ptr::null_mut();
        }
    }
    clear_errno();
}

#[no_mangle]
pub extern "C" fn zmq_timers_new() -> *mut c_void {
    clear_errno();
    Box::into_raw(Box::new(OpaqueTimers {
        next_id: 1,
        timers: Vec::new(),
    }))
    .cast()
}

#[no_mangle]
pub extern "C" fn zmq_timers_destroy(timers: *mut *mut c_void) -> c_int {
    if timers.is_null() {
        return set_error(Error::InvalidArgument);
    }
    // SAFETY: `timers` points to caller storage; inner pointer was allocated by `zmq_timers_new`.
    unsafe {
        if !(*timers).is_null() {
            drop(Box::from_raw((*timers).cast::<OpaqueTimers>()));
            *timers = ptr::null_mut();
        }
    }
    clear_errno();
    0
}

#[no_mangle]
pub extern "C" fn zmq_timers_add(
    timers: *mut c_void,
    interval: usize,
    handler: ZmqTimerFn,
    arg: *mut c_void,
) -> c_int {
    let timers = match timers_from_raw(timers) {
        Ok(timers) => timers,
        Err(error) => return set_error(error),
    };
    let id = timers.next_id;
    timers.next_id += 1;
    timers.timers.push(TimerEntry {
        id,
        interval: Duration::from_millis(interval as u64),
        deadline: Instant::now() + Duration::from_millis(interval as u64),
        handler,
        arg,
        active: true,
    });
    clear_errno();
    id
}

#[no_mangle]
pub extern "C" fn zmq_timers_cancel(timers: *mut c_void, timer_id: c_int) -> c_int {
    match timers_from_raw(timers) {
        Ok(timers) => {
            if let Some(timer) = timers.timers.iter_mut().find(|timer| timer.id == timer_id) {
                timer.active = false;
                clear_errno();
                0
            } else {
                set_error(Error::InvalidArgument)
            }
        }
        Err(error) => set_error(error),
    }
}

#[no_mangle]
pub extern "C" fn zmq_timers_set_interval(
    timers: *mut c_void,
    timer_id: c_int,
    interval: usize,
) -> c_int {
    match timers_from_raw(timers) {
        Ok(timers) => {
            if let Some(timer) = timers.timers.iter_mut().find(|timer| timer.id == timer_id) {
                timer.interval = Duration::from_millis(interval as u64);
                clear_errno();
                0
            } else {
                set_error(Error::InvalidArgument)
            }
        }
        Err(error) => set_error(error),
    }
}

#[no_mangle]
pub extern "C" fn zmq_timers_reset(timers: *mut c_void, timer_id: c_int) -> c_int {
    match timers_from_raw(timers) {
        Ok(timers) => {
            if let Some(timer) = timers.timers.iter_mut().find(|timer| timer.id == timer_id) {
                timer.deadline = Instant::now() + timer.interval;
                timer.active = true;
                clear_errno();
                0
            } else {
                set_error(Error::InvalidArgument)
            }
        }
        Err(error) => set_error(error),
    }
}

#[no_mangle]
pub extern "C" fn zmq_timers_timeout(timers: *mut c_void) -> isize {
    let timers = match timers_from_raw(timers) {
        Ok(timers) => timers,
        Err(error) => {
            set_errno(error.errno());
            return -1;
        }
    };
    let now = Instant::now();
    let Some(next) = timers
        .timers
        .iter()
        .filter(|timer| timer.active)
        .map(|timer| timer.deadline)
        .min()
    else {
        clear_errno();
        return -1;
    };
    clear_errno();
    next.saturating_duration_since(now).as_millis() as isize
}

#[no_mangle]
pub extern "C" fn zmq_timers_execute(timers: *mut c_void) -> c_int {
    let timers = match timers_from_raw(timers) {
        Ok(timers) => timers,
        Err(error) => return set_error(error),
    };
    let now = Instant::now();
    let mut fired = 0;
    for timer in timers
        .timers
        .iter_mut()
        .filter(|timer| timer.active && timer.deadline <= now)
    {
        if let Some(handler) = timer.handler {
            handler(timer.id, timer.arg);
        }
        timer.deadline = now + timer.interval;
        fired += 1;
    }
    clear_errno();
    fired
}

#[no_mangle]
pub extern "C" fn zmq_stopwatch_start() -> *mut c_void {
    clear_errno();
    Box::into_raw(Box::new(OpaqueStopwatch {
        start: Instant::now(),
    }))
    .cast()
}

#[no_mangle]
pub extern "C" fn zmq_stopwatch_intermediate(watch: *mut c_void) -> u64 {
    if watch.is_null() {
        set_errno(EINVAL);
        return 0;
    }
    // SAFETY: Pointer was allocated by `zmq_stopwatch_start` and remains owned by caller.
    let elapsed = unsafe { (*(watch.cast::<OpaqueStopwatch>())).start.elapsed() };
    clear_errno();
    elapsed.as_micros() as u64
}

#[no_mangle]
pub extern "C" fn zmq_stopwatch_stop(watch: *mut c_void) -> u64 {
    if watch.is_null() {
        set_errno(EINVAL);
        return 0;
    }
    // SAFETY: Pointer was allocated by `zmq_stopwatch_start` and is consumed once here.
    let watch = unsafe { Box::from_raw(watch.cast::<OpaqueStopwatch>()) };
    clear_errno();
    watch.start.elapsed().as_micros() as u64
}

#[no_mangle]
pub extern "C" fn zmq_sleep(seconds: c_int) {
    if seconds > 0 {
        std::thread::sleep(std::time::Duration::from_secs(seconds as u64));
    }
}

#[no_mangle]
pub extern "C" fn zmq_threadstart(func: ZmqThreadFn, arg: *mut c_void) -> *mut c_void {
    let Some(func) = func else {
        set_errno(EINVAL);
        return ptr::null_mut();
    };
    let arg = arg as usize;
    let handle = std::thread::spawn(move || {
        func(arg as *mut c_void);
    });
    clear_errno();
    Box::into_raw(Box::new(OpaqueThread {
        handle: Some(handle),
    }))
    .cast()
}

#[no_mangle]
pub extern "C" fn zmq_threadclose(thread: *mut c_void) {
    if thread.is_null() {
        set_errno(EINVAL);
        return;
    }
    // SAFETY: Pointer was allocated by `zmq_threadstart` and is consumed once here.
    let mut thread = unsafe { Box::from_raw(thread.cast::<OpaqueThread>()) };
    if let Some(handle) = thread.handle.take() {
        let _ = handle.join();
    }
    clear_errno();
}

#[no_mangle]
pub extern "C" fn zmq_ctx_set_ext(
    ctx: *mut c_void,
    option: c_int,
    optval: *const c_void,
    optvallen: usize,
) -> c_int {
    if optval.is_null() || optvallen != std::mem::size_of::<c_int>() {
        return set_error(Error::InvalidArgument);
    }
    // SAFETY: `optval` was checked non-null and `optvallen` matches `c_int` size.
    let value = unsafe { *(optval.cast::<c_int>()) };
    match context_from_raw(ctx).and_then(|ctx| ctx.inner.set_option(option, value)) {
        Ok(()) => {
            clear_errno();
            0
        }
        Err(error) => set_error(error),
    }
}

#[no_mangle]
pub extern "C" fn zmq_ctx_get_ext(
    ctx: *mut c_void,
    option: c_int,
    optval: *mut c_void,
    optvallen: *mut usize,
) -> c_int {
    if optval.is_null() || optvallen.is_null() {
        return set_error(Error::InvalidArgument);
    }
    // SAFETY: `optvallen` is non-null and points to caller-provided storage.
    let available = unsafe { *optvallen };
    if available < std::mem::size_of::<c_int>() {
        return set_error(Error::InvalidArgument);
    }
    match context_from_raw(ctx).and_then(|ctx| ctx.inner.get_option(option)) {
        Ok(value) => {
            // SAFETY: `optval` and `optvallen` are non-null; caller supplied enough space for `c_int`.
            unsafe {
                *(optval.cast::<c_int>()) = value;
                *optvallen = std::mem::size_of::<c_int>();
            }
            clear_errno();
            0
        }
        Err(error) => set_error(error),
    }
}

#[no_mangle]
pub extern "C" fn zmq_join(socket: *mut c_void, group: *const c_char) -> c_int {
    let socket = match socket_from_raw(socket) {
        Ok(socket) => socket,
        Err(error) => return set_error(error),
    };
    if group.is_null() {
        return set_error(Error::InvalidArgument);
    }
    // SAFETY: libzmq C ABI requires a valid NUL-terminated group string.
    let group = match unsafe { CStr::from_ptr(group) }.to_str() {
        Ok(group) => group,
        Err(_) => return set_error(Error::InvalidArgument),
    };
    match socket.inner.join(group) {
        Ok(()) => {
            clear_errno();
            0
        }
        Err(error) => set_error(error),
    }
}

#[no_mangle]
pub extern "C" fn zmq_leave(socket: *mut c_void, group: *const c_char) -> c_int {
    let socket = match socket_from_raw(socket) {
        Ok(socket) => socket,
        Err(error) => return set_error(error),
    };
    if group.is_null() {
        return set_error(Error::InvalidArgument);
    }
    // SAFETY: libzmq C ABI requires a valid NUL-terminated group string.
    let group = match unsafe { CStr::from_ptr(group) }.to_str() {
        Ok(group) => group,
        Err(_) => return set_error(Error::InvalidArgument),
    };
    match socket.inner.leave(group) {
        Ok(()) => {
            clear_errno();
            0
        }
        Err(error) => set_error(error),
    }
}

#[no_mangle]
pub extern "C" fn zmq_connect_peer(socket: *mut c_void, endpoint: *const c_char) -> u32 {
    let socket = match socket_from_raw(socket) {
        Ok(socket) => socket,
        Err(error) => {
            set_errno(error.errno());
            return 0;
        }
    };
    let endpoint = match endpoint_from_raw(endpoint) {
        Ok(endpoint) => endpoint,
        Err(error) => {
            set_errno(error.errno());
            return 0;
        }
    };
    match socket.inner.connect(endpoint) {
        Ok(()) => {
            clear_errno();
            1
        }
        Err(error) => {
            set_errno(error.errno());
            0
        }
    }
}

#[no_mangle]
pub extern "C" fn zmq_disconnect_peer(socket: *mut c_void, routing_id: u32) -> c_int {
    let socket = match socket_from_raw(socket) {
        Ok(socket) => socket,
        Err(error) => return set_error(error),
    };
    match socket.inner.disconnect_peer(routing_id) {
        Ok(()) => {
            clear_errno();
            0
        }
        Err(error) => set_error(error),
    }
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
    clear_errno();
    Box::into_raw(Box::new(OpaquePoller {
        entries: Mutex::new(Vec::new()),
    }))
    .cast()
}

#[no_mangle]
pub extern "C" fn zmq_poller_destroy(_poller: *mut *mut c_void) -> c_int {
    if _poller.is_null() {
        return set_error(Error::InvalidArgument);
    }
    // SAFETY: `_poller` points to caller storage; inner pointer was allocated by `zmq_poller_new`.
    unsafe {
        if !(*_poller).is_null() {
            drop(Box::from_raw((*_poller).cast::<OpaquePoller>()));
            *_poller = ptr::null_mut();
        }
    }
    clear_errno();
    0
}

#[no_mangle]
pub extern "C" fn zmq_poller_size(_poller: *mut c_void) -> c_int {
    match poller_from_raw(_poller) {
        Ok(poller) => match poller.entries.lock() {
            Ok(entries) => {
                clear_errno();
                entries.len() as c_int
            }
            Err(_) => set_error(Error::InvalidArgument),
        },
        Err(error) => set_error(error),
    }
}

#[no_mangle]
pub extern "C" fn zmq_poller_add(
    poller: *mut c_void,
    socket: *mut c_void,
    user_data: *mut c_void,
    events: i16,
) -> c_int {
    if let Err(error) = socket_from_raw(socket) {
        return set_error(error);
    }
    let poller = match poller_from_raw(poller) {
        Ok(poller) => poller,
        Err(error) => return set_error(error),
    };
    let mut entries = poller.entries.lock().map_err(|_| Error::InvalidArgument);
    match entries.as_mut() {
        Ok(entries) => {
            entries.push(PollerEntry {
                socket,
                fd: 0,
                user_data,
                events,
                is_fd: false,
            });
            clear_errno();
            0
        }
        Err(error) => set_error(*error),
    }
}

#[no_mangle]
pub extern "C" fn zmq_poller_modify(
    poller: *mut c_void,
    socket: *mut c_void,
    events: i16,
) -> c_int {
    match poller_from_raw(poller) {
        Ok(poller) => match poller.entries.lock() {
            Ok(mut entries) => {
                if let Some(entry) = entries
                    .iter_mut()
                    .find(|entry| !entry.is_fd && entry.socket == socket)
                {
                    entry.events = events;
                    clear_errno();
                    0
                } else {
                    set_error(Error::InvalidArgument)
                }
            }
            Err(_) => set_error(Error::InvalidArgument),
        },
        Err(error) => set_error(error),
    }
}

#[no_mangle]
pub extern "C" fn zmq_poller_remove(poller: *mut c_void, socket: *mut c_void) -> c_int {
    match poller_from_raw(poller) {
        Ok(poller) => match poller.entries.lock() {
            Ok(mut entries) => {
                let previous_len = entries.len();
                entries.retain(|entry| entry.is_fd || entry.socket != socket);
                if entries.len() != previous_len {
                    clear_errno();
                    0
                } else {
                    set_error(Error::InvalidArgument)
                }
            }
            Err(_) => set_error(Error::InvalidArgument),
        },
        Err(error) => set_error(error),
    }
}

#[no_mangle]
pub extern "C" fn zmq_poller_wait(
    poller: *mut c_void,
    event: *mut ZmqPollerEvent,
    timeout: isize,
) -> c_int {
    zmq_poller_wait_all(poller, event, 1, timeout)
}

#[no_mangle]
pub extern "C" fn zmq_poller_wait_all(
    poller: *mut c_void,
    events: *mut ZmqPollerEvent,
    n_events: c_int,
    timeout: isize,
) -> c_int {
    if n_events < 0 || (events.is_null() && n_events != 0) {
        return set_error(Error::InvalidArgument);
    }
    let poller = match poller_from_raw(poller) {
        Ok(poller) => poller,
        Err(error) => return set_error(error),
    };
    let entries = match poller.entries.lock() {
        Ok(entries) => entries,
        Err(_) => return set_error(Error::InvalidArgument),
    };
    let mut ready = Vec::new();
    for entry in entries.iter() {
        let revents = if entry.is_fd {
            0
        } else {
            socket_revents(entry.socket, entry.events).unwrap_or(0)
        };
        if revents != 0 {
            ready.push(ZmqPollerEvent {
                socket: entry.socket,
                fd: entry.fd,
                user_data: entry.user_data,
                events: revents,
            });
        }
    }
    if ready.is_empty() && timeout > 0 {
        drop(entries);
        std::thread::sleep(Duration::from_millis(timeout as u64));
    }
    let count = ready.len().min(n_events as usize);
    if count == 0 {
        return set_error(Error::Again);
    }
    // SAFETY: `events` is non-null for positive `count` and points to `n_events` writable records.
    unsafe {
        ptr::copy_nonoverlapping(ready.as_ptr(), events, count);
    }
    clear_errno();
    count as c_int
}

#[no_mangle]
pub extern "C" fn zmq_poller_fd(_poller: *mut c_void, fd: *mut ZmqFd) -> c_int {
    if fd.is_null() {
        return set_error(Error::InvalidArgument);
    }
    if let Err(error) = poller_from_raw(_poller) {
        return set_error(error);
    }
    // SAFETY: `fd` is non-null and writable.
    unsafe { *fd = invalid_zmq_fd() };
    clear_errno();
    0
}

#[no_mangle]
pub extern "C" fn zmq_poller_add_fd(
    poller: *mut c_void,
    fd: ZmqFd,
    user_data: *mut c_void,
    events: i16,
) -> c_int {
    match poller_from_raw(poller) {
        Ok(poller) => match poller.entries.lock() {
            Ok(mut entries) => {
                entries.push(PollerEntry {
                    socket: ptr::null_mut(),
                    fd,
                    user_data,
                    events,
                    is_fd: true,
                });
                clear_errno();
                0
            }
            Err(_) => set_error(Error::InvalidArgument),
        },
        Err(error) => set_error(error),
    }
}

#[no_mangle]
pub extern "C" fn zmq_poller_modify_fd(poller: *mut c_void, fd: ZmqFd, events: i16) -> c_int {
    match poller_from_raw(poller) {
        Ok(poller) => match poller.entries.lock() {
            Ok(mut entries) => {
                if let Some(entry) = entries
                    .iter_mut()
                    .find(|entry| entry.is_fd && entry.fd == fd)
                {
                    entry.events = events;
                    clear_errno();
                    0
                } else {
                    set_error(Error::InvalidArgument)
                }
            }
            Err(_) => set_error(Error::InvalidArgument),
        },
        Err(error) => set_error(error),
    }
}

#[no_mangle]
pub extern "C" fn zmq_poller_remove_fd(poller: *mut c_void, fd: ZmqFd) -> c_int {
    match poller_from_raw(poller) {
        Ok(poller) => match poller.entries.lock() {
            Ok(mut entries) => {
                let previous_len = entries.len();
                entries.retain(|entry| !entry.is_fd || entry.fd != fd);
                if entries.len() != previous_len {
                    clear_errno();
                    0
                } else {
                    set_error(Error::InvalidArgument)
                }
            }
            Err(_) => set_error(Error::InvalidArgument),
        },
        Err(error) => set_error(error),
    }
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
    endpoint: *const c_char,
    events: u64,
    _event_version: c_int,
    _type: c_int,
) -> c_int {
    if endpoint.is_null() {
        return set_error(Error::InvalidArgument);
    }
    let socket = match socket_from_raw(socket) {
        Ok(socket) => socket,
        Err(error) => return set_error(error),
    };
    let endpoint = match endpoint_from_raw(endpoint) {
        Ok(endpoint) => endpoint,
        Err(error) => return set_error(error),
    };
    match socket.inner.monitor(endpoint, events) {
        Ok(()) => {
            clear_errno();
            0
        }
        Err(error) => set_error(error),
    }
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
    items: *mut ZmqPollItem,
    nitems: c_int,
    timeout: isize,
    _sigmask: *const c_void,
) -> c_int {
    zmq_poll(items, nitems, timeout)
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
