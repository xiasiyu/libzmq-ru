use std::ffi::CStr;
use std::ffi::{c_char, c_int, c_void};
use std::mem::{align_of, size_of, MaybeUninit};
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};

use zmq::*;

const ZMQ_HAUSNUMERO: c_int = 156_384_712;
const ENOTSUP: c_int = ZMQ_HAUSNUMERO + 1;
const ENOTSOCK: c_int = ZMQ_HAUSNUMERO + 9;
const EFAULT: c_int = 14;
const EINVAL: c_int = 22;
const ZMQ_PAIR: c_int = 0;
const ZMQ_MORE: c_int = 1;
const ZMQ_IO_THREADS: c_int = 1;
const ZMQ_MAX_SOCKETS: c_int = 2;
const ZMQ_TYPE: c_int = 16;
const ZMQ_LINGER: c_int = 17;
const ZMQ_SNDHWM: c_int = 23;
const ZMQ_RCVHWM: c_int = 24;

static FREE_CALLBACK_COUNT: AtomicUsize = AtomicUsize::new(0);

extern "C" fn count_free_callback(_data: *mut c_void, _hint: *mut c_void) {
    FREE_CALLBACK_COUNT.fetch_add(1, Ordering::SeqCst);
}

#[test]
fn message_abi_size_and_alignment_match_libzmq() {
    assert_eq!(size_of::<zmq_msg_t>(), 64);
    assert_eq!(align_of::<zmq_msg_t>(), size_of::<*const c_void>());
}

#[test]
fn version_api_matches_libzmq_baseline() {
    let mut major = 0;
    let mut minor = 0;
    let mut patch = 0;

    zmq_version(&mut major, &mut minor, &mut patch);

    assert_eq!((major, minor, patch), (4, 3, 6));
}

#[test]
fn errno_api_reports_last_ffi_error() {
    assert_eq!(zmq_ctx_shutdown(ptr::null_mut()), -1);
    assert_eq!(zmq_errno(), EFAULT);
}

#[test]
fn invalid_socket_pointer_reports_enotsock() {
    assert_eq!(zmq_close(ptr::null_mut()), -1);
    assert_eq!(zmq_errno(), ENOTSOCK);
}

#[test]
fn invalid_socket_type_reports_einval() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());

    let socket = zmq_socket(ctx, -1);

    assert!(socket.is_null());
    assert_eq!(zmq_errno(), EINVAL);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn message_init_size_data_and_close_work_at_abi_boundary() {
    let mut msg = MaybeUninit::<zmq_msg_t>::uninit();

    assert_eq!(zmq_msg_init_size(msg.as_mut_ptr(), 16), 0);
    let mut msg = unsafe { msg.assume_init() };

    assert_eq!(zmq_msg_size(&msg), 16);
    assert!(!zmq_msg_data(&mut msg).is_null());
    assert_eq!(zmq_msg_close(&mut msg), 0);
}

#[test]
fn message_copy_clones_payload_without_taking_external_callback_ownership() {
    FREE_CALLBACK_COUNT.store(0, Ordering::SeqCst);
    let mut data = [1u8, 2, 3, 4];
    let mut original = MaybeUninit::<zmq_msg_t>::uninit();
    let mut copy = MaybeUninit::<zmq_msg_t>::uninit();

    assert_eq!(
        zmq_msg_init_data(
            original.as_mut_ptr(),
            data.as_mut_ptr().cast(),
            data.len(),
            Some(count_free_callback),
            ptr::null_mut(),
        ),
        0
    );
    assert_eq!(zmq_msg_init(copy.as_mut_ptr()), 0);
    let mut original = unsafe { original.assume_init() };
    let mut copy = unsafe { copy.assume_init() };

    assert_eq!(zmq_msg_copy(&mut copy, &mut original), 0);
    assert_eq!(zmq_msg_size(&copy), data.len());
    assert_eq!(zmq_msg_data(&mut copy), data.as_mut_ptr().cast());
    assert_eq!(zmq_msg_data(&mut original), data.as_mut_ptr().cast());
    assert_eq!(FREE_CALLBACK_COUNT.load(Ordering::SeqCst), 0);

    assert_eq!(zmq_msg_close(&mut copy), 0);
    assert_eq!(FREE_CALLBACK_COUNT.load(Ordering::SeqCst), 0);
    assert_eq!(zmq_msg_close(&mut original), 0);
    assert_eq!(FREE_CALLBACK_COUNT.load(Ordering::SeqCst), 1);
}

#[test]
fn message_move_transfers_payload_and_resets_source_to_empty() {
    let mut source = MaybeUninit::<zmq_msg_t>::uninit();
    let mut dest = MaybeUninit::<zmq_msg_t>::uninit();

    assert_eq!(zmq_msg_init_size(source.as_mut_ptr(), 7), 0);
    assert_eq!(zmq_msg_init(dest.as_mut_ptr()), 0);
    let mut source = unsafe { source.assume_init() };
    let mut dest = unsafe { dest.assume_init() };

    assert_eq!(zmq_msg_move(&mut dest, &mut source), 0);
    assert_eq!(zmq_msg_size(&dest), 7);
    assert_eq!(zmq_msg_size(&source), 0);

    assert_eq!(zmq_msg_close(&mut dest), 0);
    assert_eq!(zmq_msg_close(&mut source), 0);
}

#[test]
fn message_lifecycle_matrix_preserves_size_and_close_behavior() {
    for size in [0usize, 1, 8, 31, 32, 33, 64, 1024] {
        let mut source = MaybeUninit::<zmq_msg_t>::uninit();
        let mut copy = MaybeUninit::<zmq_msg_t>::uninit();
        let mut moved = MaybeUninit::<zmq_msg_t>::uninit();

        assert_eq!(zmq_msg_init_size(source.as_mut_ptr(), size), 0);
        assert_eq!(zmq_msg_init(copy.as_mut_ptr()), 0);
        assert_eq!(zmq_msg_init(moved.as_mut_ptr()), 0);

        let mut source = unsafe { source.assume_init() };
        let mut copy = unsafe { copy.assume_init() };
        let mut moved = unsafe { moved.assume_init() };

        assert_eq!(zmq_msg_size(&source), size);
        assert_eq!(zmq_msg_copy(&mut copy, &mut source), 0);
        assert_eq!(zmq_msg_size(&copy), size);
        assert_eq!(zmq_msg_move(&mut moved, &mut source), 0);
        assert_eq!(zmq_msg_size(&moved), size);
        assert_eq!(zmq_msg_size(&source), 0);

        assert_eq!(zmq_msg_close(&mut copy), 0);
        assert_eq!(zmq_msg_close(&mut moved), 0);
        assert_eq!(zmq_msg_close(&mut source), 0);
    }
}

#[test]
fn message_more_get_and_set_are_consistent() {
    let mut msg = MaybeUninit::<zmq_msg_t>::uninit();
    assert_eq!(zmq_msg_init(msg.as_mut_ptr()), 0);
    let mut msg = unsafe { msg.assume_init() };

    assert_eq!(zmq_msg_more(&msg), 0);
    assert_eq!(zmq_msg_get(&msg, ZMQ_MORE), 0);
    assert_eq!(zmq_msg_set(&mut msg, ZMQ_MORE, 1), 0);
    assert_eq!(zmq_msg_more(&msg), 1);
    assert_eq!(zmq_msg_get(&msg, ZMQ_MORE), 1);

    assert_eq!(zmq_msg_close(&mut msg), 0);
}

#[test]
fn draft_message_routing_group_and_init_buffer_are_available() {
    let payload = [9u8, 8, 7];
    let group = c"updates";
    let property = c"Group";
    let routing_property = c"Routing-Id";
    let mut msg = MaybeUninit::<zmq_msg_t>::uninit();

    assert_eq!(
        zmq_msg_init_buffer(msg.as_mut_ptr(), payload.as_ptr().cast(), payload.len()),
        0
    );
    let mut msg = unsafe { msg.assume_init() };

    assert_eq!(zmq_msg_size(&msg), payload.len());
    assert_eq!(zmq_msg_set_routing_id(&mut msg, 42), 0);
    assert_eq!(zmq_msg_routing_id(&mut msg), 42);
    assert_eq!(zmq_msg_set_group(&mut msg, group.as_ptr()), 0);

    let group_ptr = zmq_msg_group(&mut msg);
    assert!(!group_ptr.is_null());
    let group_value = unsafe { CStr::from_ptr(group_ptr) };
    assert_eq!(group_value, group);

    let gets_ptr = zmq_msg_gets(&msg, property.as_ptr());
    assert!(!gets_ptr.is_null());
    let gets_value = unsafe { CStr::from_ptr(gets_ptr) };
    assert_eq!(gets_value, group);

    let routing_gets_ptr = zmq_msg_gets(&msg, routing_property.as_ptr());
    assert!(!routing_gets_ptr.is_null());
    let routing_value = unsafe { CStr::from_ptr(routing_gets_ptr) };
    assert_eq!(routing_value.to_str().unwrap(), "42");

    assert_eq!(zmq_msg_close(&mut msg), 0);
}

#[test]
fn unimplemented_socket_operations_return_explicit_error() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let socket = zmq_socket(ctx, ZMQ_PAIR);
    assert!(!socket.is_null());

    assert_eq!(zmq_send(socket, ptr::null(), 0, 0), -1);
    assert_eq!(zmq_errno(), ENOTSUP);

    assert_eq!(zmq_close(socket), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn context_options_round_trip_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());

    assert_eq!(zmq_ctx_get(ctx, ZMQ_IO_THREADS), 1);
    assert_eq!(zmq_ctx_set(ctx, ZMQ_IO_THREADS, 2), 0);
    assert_eq!(zmq_ctx_get(ctx, ZMQ_IO_THREADS), 2);
    assert_eq!(zmq_ctx_set(ctx, ZMQ_MAX_SOCKETS, 2048), 0);
    assert_eq!(zmq_ctx_get(ctx, ZMQ_MAX_SOCKETS), 2048);
    assert_eq!(zmq_ctx_set(ctx, ZMQ_IO_THREADS, -1), -1);
    assert_eq!(zmq_errno(), EINVAL);

    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn socket_options_round_trip_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let socket = zmq_socket(ctx, ZMQ_PAIR);
    assert!(!socket.is_null());

    let mut value = 0;
    let mut size = size_of::<c_int>();
    assert_eq!(
        zmq_getsockopt(
            socket,
            ZMQ_TYPE,
            (&mut value as *mut c_int).cast(),
            &mut size
        ),
        0
    );
    assert_eq!(value, ZMQ_PAIR);
    assert_eq!(size, size_of::<c_int>());

    value = 0;
    size = size_of::<c_int>();
    assert_eq!(
        zmq_getsockopt(
            socket,
            ZMQ_LINGER,
            (&mut value as *mut c_int).cast(),
            &mut size
        ),
        0
    );
    assert_eq!(value, -1);

    value = 10;
    assert_eq!(
        zmq_setsockopt(
            socket,
            ZMQ_SNDHWM,
            (&value as *const c_int).cast(),
            size_of::<c_int>()
        ),
        0
    );
    value = 11;
    assert_eq!(
        zmq_setsockopt(
            socket,
            ZMQ_RCVHWM,
            (&value as *const c_int).cast(),
            size_of::<c_int>()
        ),
        0
    );

    value = 0;
    size = size_of::<c_int>();
    assert_eq!(
        zmq_getsockopt(
            socket,
            ZMQ_SNDHWM,
            (&mut value as *mut c_int).cast(),
            &mut size
        ),
        0
    );
    assert_eq!(value, 10);
    assert_eq!(
        zmq_getsockopt(
            socket,
            ZMQ_RCVHWM,
            (&mut value as *mut c_int).cast(),
            &mut size
        ),
        0
    );
    assert_eq!(value, 11);

    value = -1;
    assert_eq!(
        zmq_setsockopt(
            socket,
            ZMQ_SNDHWM,
            (&value as *const c_int).cast(),
            size_of::<c_int>()
        ),
        -1
    );
    assert_eq!(zmq_errno(), EINVAL);

    assert_eq!(zmq_close(socket), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn stable_and_draft_symbols_are_exported_to_rust_tests() {
    let _ = zmq_errno as extern "C" fn() -> c_int;
    let _ = zmq_strerror as extern "C" fn(c_int) -> *const c_char;
    let _ = zmq_version as extern "C" fn(*mut c_int, *mut c_int, *mut c_int);
    let _ = zmq_ctx_new as extern "C" fn() -> *mut c_void;
    let _ = zmq_ctx_term as extern "C" fn(*mut c_void) -> c_int;
    let _ = zmq_ctx_shutdown as extern "C" fn(*mut c_void) -> c_int;
    let _ = zmq_ctx_set as extern "C" fn(*mut c_void, c_int, c_int) -> c_int;
    let _ = zmq_ctx_get as extern "C" fn(*mut c_void, c_int) -> c_int;
    let _ = zmq_init as extern "C" fn(c_int) -> *mut c_void;
    let _ = zmq_term as extern "C" fn(*mut c_void) -> c_int;
    let _ = zmq_ctx_destroy as extern "C" fn(*mut c_void) -> c_int;
    let _ = zmq_socket as extern "C" fn(*mut c_void, c_int) -> *mut c_void;
    let _ = zmq_close as extern "C" fn(*mut c_void) -> c_int;
    let _ = zmq_setsockopt as extern "C" fn(*mut c_void, c_int, *const c_void, usize) -> c_int;
    let _ = zmq_getsockopt as extern "C" fn(*mut c_void, c_int, *mut c_void, *mut usize) -> c_int;
    let _ = zmq_bind as extern "C" fn(*mut c_void, *const c_char) -> c_int;
    let _ = zmq_connect as extern "C" fn(*mut c_void, *const c_char) -> c_int;
    let _ = zmq_unbind as extern "C" fn(*mut c_void, *const c_char) -> c_int;
    let _ = zmq_disconnect as extern "C" fn(*mut c_void, *const c_char) -> c_int;
    let _ = zmq_send as extern "C" fn(*mut c_void, *const c_void, usize, c_int) -> c_int;
    let _ = zmq_send_const as extern "C" fn(*mut c_void, *const c_void, usize, c_int) -> c_int;
    let _ = zmq_recv as extern "C" fn(*mut c_void, *mut c_void, usize, c_int) -> c_int;
    let _ = zmq_socket_monitor as extern "C" fn(*mut c_void, *const c_char, c_int) -> c_int;

    let _ = zmq_msg_init as extern "C" fn(*mut zmq_msg_t) -> c_int;
    let _ = zmq_msg_init_size as extern "C" fn(*mut zmq_msg_t, usize) -> c_int;
    let _ = zmq_msg_init_data
        as extern "C" fn(
            *mut zmq_msg_t,
            *mut c_void,
            usize,
            Option<extern "C" fn(*mut c_void, *mut c_void)>,
            *mut c_void,
        ) -> c_int;
    let _ = zmq_msg_send as extern "C" fn(*mut zmq_msg_t, *mut c_void, c_int) -> c_int;
    let _ = zmq_msg_recv as extern "C" fn(*mut zmq_msg_t, *mut c_void, c_int) -> c_int;
    let _ = zmq_msg_close as extern "C" fn(*mut zmq_msg_t) -> c_int;
    let _ = zmq_msg_move as extern "C" fn(*mut zmq_msg_t, *mut zmq_msg_t) -> c_int;
    let _ = zmq_msg_copy as extern "C" fn(*mut zmq_msg_t, *mut zmq_msg_t) -> c_int;
    let _ = zmq_msg_data as extern "C" fn(*mut zmq_msg_t) -> *mut c_void;
    let _ = zmq_msg_size as extern "C" fn(*const zmq_msg_t) -> usize;
    let _ = zmq_msg_more as extern "C" fn(*const zmq_msg_t) -> c_int;
    let _ = zmq_msg_get as extern "C" fn(*const zmq_msg_t, c_int) -> c_int;
    let _ = zmq_msg_set as extern "C" fn(*mut zmq_msg_t, c_int, c_int) -> c_int;
    let _ = zmq_msg_gets as extern "C" fn(*const zmq_msg_t, *const c_char) -> *const c_char;

    let _ = zmq_poll as extern "C" fn(*mut ZmqPollItem, c_int, isize) -> c_int;
    let _ = zmq_proxy as extern "C" fn(*mut c_void, *mut c_void, *mut c_void) -> c_int;
    let _ = zmq_proxy_steerable
        as extern "C" fn(*mut c_void, *mut c_void, *mut c_void, *mut c_void) -> c_int;
    let _ = zmq_has as extern "C" fn(*const c_char) -> c_int;
    let _ = zmq_device as extern "C" fn(c_int, *mut c_void, *mut c_void) -> c_int;
    let _ = zmq_sendmsg as extern "C" fn(*mut c_void, *mut zmq_msg_t, c_int) -> c_int;
    let _ = zmq_recvmsg as extern "C" fn(*mut c_void, *mut zmq_msg_t, c_int) -> c_int;
    let _ = zmq_sendiov as extern "C" fn(*mut c_void, *mut Iovec, usize, c_int) -> c_int;
    let _ = zmq_recviov as extern "C" fn(*mut c_void, *mut Iovec, *mut usize, c_int) -> c_int;

    let _ = zmq_z85_encode as extern "C" fn(*mut c_char, *const u8, usize) -> *mut c_char;
    let _ = zmq_z85_decode as extern "C" fn(*mut u8, *const c_char) -> *mut u8;
    let _ = zmq_curve_keypair as extern "C" fn(*mut c_char, *mut c_char) -> c_int;
    let _ = zmq_curve_public as extern "C" fn(*mut c_char, *const c_char) -> c_int;

    let _ = zmq_atomic_counter_new as extern "C" fn() -> *mut c_void;
    let _ = zmq_atomic_counter_set as extern "C" fn(*mut c_void, c_int);
    let _ = zmq_atomic_counter_inc as extern "C" fn(*mut c_void) -> c_int;
    let _ = zmq_atomic_counter_dec as extern "C" fn(*mut c_void) -> c_int;
    let _ = zmq_atomic_counter_value as extern "C" fn(*mut c_void) -> c_int;
    let _ = zmq_atomic_counter_destroy as extern "C" fn(*mut *mut c_void);

    let _ = zmq_timers_new as extern "C" fn() -> *mut c_void;
    let _ = zmq_timers_destroy as extern "C" fn(*mut *mut c_void) -> c_int;
    let _ = zmq_timers_add
        as extern "C" fn(
            *mut c_void,
            usize,
            Option<extern "C" fn(c_int, *mut c_void)>,
            *mut c_void,
        ) -> c_int;
    let _ = zmq_timers_cancel as extern "C" fn(*mut c_void, c_int) -> c_int;
    let _ = zmq_timers_set_interval as extern "C" fn(*mut c_void, c_int, usize) -> c_int;
    let _ = zmq_timers_reset as extern "C" fn(*mut c_void, c_int) -> c_int;
    let _ = zmq_timers_timeout as extern "C" fn(*mut c_void) -> isize;
    let _ = zmq_timers_execute as extern "C" fn(*mut c_void) -> c_int;

    let _ = zmq_stopwatch_start as extern "C" fn() -> *mut c_void;
    let _ = zmq_stopwatch_intermediate as extern "C" fn(*mut c_void) -> u64;
    let _ = zmq_stopwatch_stop as extern "C" fn(*mut c_void) -> u64;
    let _ = zmq_sleep as extern "C" fn(c_int);
    let _ = zmq_threadstart
        as extern "C" fn(Option<extern "C" fn(*mut c_void)>, *mut c_void) -> *mut c_void;
    let _ = zmq_threadclose as extern "C" fn(*mut c_void);

    let _ = zmq_ctx_set_ext as extern "C" fn(*mut c_void, c_int, *const c_void, usize) -> c_int;
    let _ = zmq_ctx_get_ext as extern "C" fn(*mut c_void, c_int, *mut c_void, *mut usize) -> c_int;
    let _ = zmq_join as extern "C" fn(*mut c_void, *const c_char) -> c_int;
    let _ = zmq_leave as extern "C" fn(*mut c_void, *const c_char) -> c_int;
    let _ = zmq_connect_peer as extern "C" fn(*mut c_void, *const c_char) -> u32;
    let _ = zmq_disconnect_peer as extern "C" fn(*mut c_void, u32) -> c_int;
    let _ = zmq_msg_set_routing_id as extern "C" fn(*mut zmq_msg_t, u32) -> c_int;
    let _ = zmq_msg_routing_id as extern "C" fn(*mut zmq_msg_t) -> u32;
    let _ = zmq_msg_set_group as extern "C" fn(*mut zmq_msg_t, *const c_char) -> c_int;
    let _ = zmq_msg_group as extern "C" fn(*mut zmq_msg_t) -> *const c_char;
    let _ = zmq_msg_init_buffer as extern "C" fn(*mut zmq_msg_t, *const c_void, usize) -> c_int;

    let _ = zmq_poller_new as extern "C" fn() -> *mut c_void;
    let _ = zmq_poller_destroy as extern "C" fn(*mut *mut c_void) -> c_int;
    let _ = zmq_poller_size as extern "C" fn(*mut c_void) -> c_int;
    let _ = zmq_poller_add as extern "C" fn(*mut c_void, *mut c_void, *mut c_void, i16) -> c_int;
    let _ = zmq_poller_modify as extern "C" fn(*mut c_void, *mut c_void, i16) -> c_int;
    let _ = zmq_poller_remove as extern "C" fn(*mut c_void, *mut c_void) -> c_int;
    let _ = zmq_poller_wait as extern "C" fn(*mut c_void, *mut ZmqPollerEvent, isize) -> c_int;
    let _ = zmq_poller_wait_all
        as extern "C" fn(*mut c_void, *mut ZmqPollerEvent, c_int, isize) -> c_int;
    let _ = zmq_poller_fd as extern "C" fn(*mut c_void, *mut c_int) -> c_int;
    let _ = zmq_poller_add_fd as extern "C" fn(*mut c_void, c_int, *mut c_void, i16) -> c_int;
    let _ = zmq_poller_modify_fd as extern "C" fn(*mut c_void, c_int, i16) -> c_int;
    let _ = zmq_poller_remove_fd as extern "C" fn(*mut c_void, c_int) -> c_int;
    let _ = zmq_socket_get_peer_state as extern "C" fn(*mut c_void, *const c_void, usize) -> c_int;
    let _ = zmq_socket_monitor_versioned
        as extern "C" fn(*mut c_void, *const c_char, u64, c_int, c_int) -> c_int;
    let _ = zmq_socket_monitor_pipes_stats as extern "C" fn(*mut c_void) -> c_int;
    let _ = zmq_ppoll as extern "C" fn(*mut ZmqPollItem, c_int, isize, *const c_void) -> c_int;
}
