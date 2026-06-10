use std::ffi::CStr;
use std::ffi::{c_char, c_int, c_void};
use std::mem::{align_of, size_of, MaybeUninit};
use std::net::TcpListener;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};

use zmq::*;

const ZMQ_HAUSNUMERO: c_int = 156_384_712;
const ENOTSOCK: c_int = ZMQ_HAUSNUMERO + 9;
const EHOSTUNREACH: c_int = ZMQ_HAUSNUMERO + 17;
const EFSM: c_int = ZMQ_HAUSNUMERO + 51;
const EAGAIN: c_int = 11;
const EFAULT: c_int = 14;
const EINVAL: c_int = 22;
const ZMQ_PAIR: c_int = 0;
const ZMQ_PUB: c_int = 1;
const ZMQ_SUB: c_int = 2;
const ZMQ_REQ: c_int = 3;
const ZMQ_REP: c_int = 4;
const ZMQ_DEALER: c_int = 5;
const ZMQ_ROUTER: c_int = 6;
const ZMQ_PULL: c_int = 7;
const ZMQ_PUSH: c_int = 8;
const ZMQ_XPUB: c_int = 9;
const ZMQ_XSUB: c_int = 10;
const ZMQ_SNDMORE: c_int = 2;
const ZMQ_MORE: c_int = 1;
const ZMQ_RCVMORE: c_int = 13;
const ZMQ_FD: c_int = 14;
const ZMQ_EVENTS: c_int = 15;
const ZMQ_IO_THREADS: c_int = 1;
const ZMQ_MAX_SOCKETS: c_int = 2;
const ZMQ_TYPE: c_int = 16;
const ZMQ_LINGER: c_int = 17;
const ZMQ_SNDHWM: c_int = 23;
const ZMQ_RCVHWM: c_int = 24;
const ZMQ_ROUTER_MANDATORY: c_int = 33;
const ZMQ_REQ_RELAXED: c_int = 53;
const ZMQ_CONFLATE: c_int = 54;
const ZMQ_SUBSCRIBE: c_int = 6;
const ZMQ_XPUB_WELCOME_MSG: c_int = 72;
const ZMQ_POLLIN: i16 = 1;
const ZMQ_POLLOUT: i16 = 2;
const ZMQ_EVENT_LISTENING: c_int = 0x0008;

static FREE_CALLBACK_COUNT: AtomicUsize = AtomicUsize::new(0);
static TIMER_CALLBACK_COUNT: AtomicUsize = AtomicUsize::new(0);
static THREAD_CALLBACK_COUNT: AtomicUsize = AtomicUsize::new(0);

extern "C" fn count_free_callback(_data: *mut c_void, _hint: *mut c_void) {
    FREE_CALLBACK_COUNT.fetch_add(1, Ordering::SeqCst);
}

extern "C" fn count_timer_callback(_timer_id: c_int, _arg: *mut c_void) {
    TIMER_CALLBACK_COUNT.fetch_add(1, Ordering::SeqCst);
}

extern "C" fn count_thread_callback(_arg: *mut c_void) {
    THREAD_CALLBACK_COUNT.fetch_add(1, Ordering::SeqCst);
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
    assert_eq!(zmq_errno(), EAGAIN);

    assert_eq!(zmq_close(socket), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn pair_inproc_round_trip_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let server = zmq_socket(ctx, ZMQ_PAIR);
    let client = zmq_socket(ctx, ZMQ_PAIR);
    assert!(!server.is_null());
    assert!(!client.is_null());

    let endpoint = c"inproc://c_pair";
    assert_eq!(zmq_bind(server, endpoint.as_ptr()), 0);
    assert_eq!(zmq_connect(client, endpoint.as_ptr()), 0);

    let payload = b"hello";
    assert_eq!(
        zmq_send(client, payload.as_ptr().cast(), payload.len(), 0),
        payload.len() as c_int
    );

    let mut buffer = [0u8; 16];
    assert_eq!(
        zmq_recv(server, buffer.as_mut_ptr().cast(), buffer.len(), 0),
        payload.len() as c_int
    );
    assert_eq!(&buffer[..payload.len()], payload);

    let response = b"world";
    assert_eq!(
        zmq_send(server, response.as_ptr().cast(), response.len(), 0),
        response.len() as c_int
    );

    let mut buffer = [0u8; 16];
    assert_eq!(
        zmq_recv(client, buffer.as_mut_ptr().cast(), buffer.len(), 0),
        response.len() as c_int
    );
    assert_eq!(&buffer[..response.len()], response);

    assert_eq!(zmq_disconnect(client, endpoint.as_ptr()), 0);
    assert_eq!(
        zmq_send(client, payload.as_ptr().cast(), payload.len(), 0),
        -1
    );
    assert_eq!(zmq_errno(), EAGAIN);

    assert_eq!(zmq_close(client), 0);
    assert_eq!(zmq_close(server), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn pair_tcp_round_trip_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let server = zmq_socket(ctx, ZMQ_PAIR);
    let client = zmq_socket(ctx, ZMQ_PAIR);
    assert!(!server.is_null());
    assert!(!client.is_null());

    let endpoint =
        std::ffi::CString::new(format!("tcp://127.0.0.1:{}", unused_tcp_port())).unwrap();
    assert_eq!(zmq_bind(server, endpoint.as_ptr()), 0);
    assert_eq!(zmq_connect(client, endpoint.as_ptr()), 0);

    assert_eq!(zmq_send(client, b"hello".as_ptr().cast(), 5, 0), 5);
    let mut buffer = [0u8; 16];
    assert_eq!(recv_retry(server, &mut buffer), 5);
    assert_eq!(&buffer[..5], b"hello");

    assert_eq!(zmq_send(server, b"world".as_ptr().cast(), 5, 0), 5);
    assert_eq!(recv_retry(client, &mut buffer), 5);
    assert_eq!(&buffer[..5], b"world");

    assert_eq!(zmq_close(client), 0);
    assert_eq!(zmq_close(server), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn push_pull_tcp_round_trip_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let pull = zmq_socket(ctx, ZMQ_PULL);
    let push = zmq_socket(ctx, ZMQ_PUSH);
    assert!(!pull.is_null());
    assert!(!push.is_null());

    let endpoint =
        std::ffi::CString::new(format!("tcp://127.0.0.1:{}", unused_tcp_port())).unwrap();
    assert_eq!(zmq_bind(pull, endpoint.as_ptr()), 0);
    assert_eq!(zmq_connect(push, endpoint.as_ptr()), 0);

    assert_eq!(zmq_send(push, b"job".as_ptr().cast(), 3, 0), 3);
    let mut buffer = [0u8; 16];
    assert_eq!(recv_retry(pull, &mut buffer), 3);
    assert_eq!(&buffer[..3], b"job");

    assert_eq!(zmq_close(push), 0);
    assert_eq!(zmq_close(pull), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn req_rep_tcp_round_trip_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let rep = zmq_socket(ctx, ZMQ_REP);
    let req = zmq_socket(ctx, ZMQ_REQ);
    assert!(!rep.is_null());
    assert!(!req.is_null());

    let endpoint =
        std::ffi::CString::new(format!("tcp://127.0.0.1:{}", unused_tcp_port())).unwrap();
    assert_eq!(zmq_bind(rep, endpoint.as_ptr()), 0);
    assert_eq!(zmq_connect(req, endpoint.as_ptr()), 0);

    assert_eq!(zmq_send(req, b"question".as_ptr().cast(), 8, 0), 8);
    let mut buffer = [0u8; 16];
    assert_eq!(recv_retry(rep, &mut buffer), 8);
    assert_eq!(&buffer[..8], b"question");
    assert_eq!(zmq_send(rep, b"answer".as_ptr().cast(), 6, 0), 6);
    assert_eq!(recv_retry(req, &mut buffer), 6);
    assert_eq!(&buffer[..6], b"answer");

    assert_eq!(zmq_close(req), 0);
    assert_eq!(zmq_close(rep), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[cfg(unix)]
#[test]
fn pair_ipc_round_trip_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let server = zmq_socket(ctx, ZMQ_PAIR);
    let client = zmq_socket(ctx, ZMQ_PAIR);
    assert!(!server.is_null());
    assert!(!client.is_null());

    let path = std::env::temp_dir().join(format!(
        "libzmq-c-ipc-{}-round-trip.sock",
        std::process::id()
    ));
    let endpoint = std::ffi::CString::new(format!("ipc://{}", path.display())).unwrap();
    assert_eq!(zmq_bind(server, endpoint.as_ptr()), 0);
    assert_eq!(zmq_connect(client, endpoint.as_ptr()), 0);

    assert_eq!(zmq_send(client, b"hello".as_ptr().cast(), 5, 0), 5);
    let mut buffer = [0u8; 16];
    assert_eq!(
        zmq_recv(server, buffer.as_mut_ptr().cast(), buffer.len(), 0),
        5
    );
    assert_eq!(&buffer[..5], b"hello");

    assert_eq!(zmq_send(server, b"world".as_ptr().cast(), 5, 0), 5);
    assert_eq!(
        zmq_recv(client, buffer.as_mut_ptr().cast(), buffer.len(), 0),
        5
    );
    assert_eq!(&buffer[..5], b"world");

    assert_eq!(zmq_close(client), 0);
    assert_eq!(zmq_close(server), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
    let _ = std::fs::remove_file(path);
}

fn unused_tcp_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn recv_retry(socket: *mut c_void, buffer: &mut [u8]) -> c_int {
    let mut rc = -1;
    for _ in 0..20 {
        rc = zmq_recv(socket, buffer.as_mut_ptr().cast(), buffer.len(), 0);
        if rc >= 0 {
            return rc;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    rc
}

#[test]
fn pair_inproc_multipart_more_state_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let server = zmq_socket(ctx, ZMQ_PAIR);
    let client = zmq_socket(ctx, ZMQ_PAIR);
    assert!(!server.is_null());
    assert!(!client.is_null());

    let endpoint = c"inproc://c_multipart_pair";
    assert_eq!(zmq_bind(server, endpoint.as_ptr()), 0);
    assert_eq!(zmq_connect(client, endpoint.as_ptr()), 0);

    let first = b"part1";
    let second = b"part2";
    assert_eq!(
        zmq_send(client, first.as_ptr().cast(), first.len(), ZMQ_SNDMORE),
        first.len() as c_int
    );
    assert_eq!(
        zmq_send(client, second.as_ptr().cast(), second.len(), 0),
        second.len() as c_int
    );

    let mut buffer = [0u8; 16];
    assert_eq!(
        zmq_recv(server, buffer.as_mut_ptr().cast(), buffer.len(), 0),
        first.len() as c_int
    );
    assert_eq!(&buffer[..first.len()], first);

    let mut more = 0;
    let mut size = size_of::<c_int>();
    assert_eq!(
        zmq_getsockopt(
            server,
            ZMQ_RCVMORE,
            (&mut more as *mut c_int).cast(),
            &mut size
        ),
        0
    );
    assert_eq!(more, 1);

    assert_eq!(
        zmq_recv(server, buffer.as_mut_ptr().cast(), buffer.len(), 0),
        second.len() as c_int
    );
    assert_eq!(&buffer[..second.len()], second);
    assert_eq!(
        zmq_getsockopt(
            server,
            ZMQ_RCVMORE,
            (&mut more as *mut c_int).cast(),
            &mut size
        ),
        0
    );
    assert_eq!(more, 0);

    assert_eq!(zmq_close(client), 0);
    assert_eq!(zmq_close(server), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn pair_inproc_msg_send_recv_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let server = zmq_socket(ctx, ZMQ_PAIR);
    let client = zmq_socket(ctx, ZMQ_PAIR);
    assert!(!server.is_null());
    assert!(!client.is_null());

    let endpoint = c"inproc://c_msg_pair";
    assert_eq!(zmq_bind(server, endpoint.as_ptr()), 0);
    assert_eq!(zmq_connect(client, endpoint.as_ptr()), 0);

    let mut outbound = MaybeUninit::<zmq_msg_t>::uninit();
    assert_eq!(zmq_msg_init_size(outbound.as_mut_ptr(), 4), 0);
    let mut outbound = unsafe { outbound.assume_init() };
    unsafe {
        ptr::copy_nonoverlapping(
            b"ping".as_ptr(),
            zmq_msg_data(&mut outbound).cast::<u8>(),
            4,
        );
    }

    assert_eq!(zmq_msg_send(&mut outbound, client, 0), 4);

    let mut inbound = MaybeUninit::<zmq_msg_t>::uninit();
    assert_eq!(zmq_msg_init(inbound.as_mut_ptr()), 0);
    let mut inbound = unsafe { inbound.assume_init() };
    assert_eq!(zmq_msg_recv(&mut inbound, server, 0), 4);
    assert_eq!(zmq_msg_size(&inbound), 4);
    let data = unsafe { std::slice::from_raw_parts(zmq_msg_data(&mut inbound).cast::<u8>(), 4) };
    assert_eq!(data, b"ping");

    assert_eq!(zmq_msg_close(&mut inbound), 0);
    assert_eq!(zmq_msg_close(&mut outbound), 0);
    assert_eq!(zmq_close(client), 0);
    assert_eq!(zmq_close(server), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn push_pull_inproc_round_trip_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let pull = zmq_socket(ctx, ZMQ_PULL);
    let push = zmq_socket(ctx, ZMQ_PUSH);
    assert!(!pull.is_null());
    assert!(!push.is_null());

    let endpoint = c"inproc://c_push_pull";
    assert_eq!(zmq_bind(pull, endpoint.as_ptr()), 0);
    assert_eq!(zmq_connect(push, endpoint.as_ptr()), 0);

    let payload = b"job";
    assert_eq!(
        zmq_send(push, payload.as_ptr().cast(), payload.len(), 0),
        payload.len() as c_int
    );
    let mut buffer = [0u8; 8];
    assert_eq!(
        zmq_recv(pull, buffer.as_mut_ptr().cast(), buffer.len(), 0),
        payload.len() as c_int
    );
    assert_eq!(&buffer[..payload.len()], payload);

    assert_eq!(
        zmq_send(pull, payload.as_ptr().cast(), payload.len(), 0),
        -1
    );
    assert_eq!(
        zmq_recv(push, buffer.as_mut_ptr().cast(), buffer.len(), 0),
        -1
    );

    assert_eq!(zmq_close(push), 0);
    assert_eq!(zmq_close(pull), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn dealer_router_inproc_sets_routing_id_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let router = zmq_socket(ctx, ZMQ_ROUTER);
    let dealer = zmq_socket(ctx, ZMQ_DEALER);
    assert!(!router.is_null());
    assert!(!dealer.is_null());

    let endpoint = c"inproc://c_dealer_router";
    assert_eq!(zmq_bind(router, endpoint.as_ptr()), 0);
    assert_eq!(zmq_connect(dealer, endpoint.as_ptr()), 0);

    let payload = b"request";
    assert_eq!(
        zmq_send(dealer, payload.as_ptr().cast(), payload.len(), 0),
        payload.len() as c_int
    );

    let mut inbound = MaybeUninit::<zmq_msg_t>::uninit();
    assert_eq!(zmq_msg_init(inbound.as_mut_ptr()), 0);
    let mut inbound = unsafe { inbound.assume_init() };
    assert_eq!(
        zmq_msg_recv(&mut inbound, router, 0),
        payload.len() as c_int
    );
    assert_ne!(zmq_msg_routing_id(&mut inbound), 0);

    assert_eq!(zmq_msg_close(&mut inbound), 0);
    assert_eq!(zmq_close(dealer), 0);
    assert_eq!(zmq_close(router), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn router_mandatory_reports_unroutable_peer_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let router = zmq_socket(ctx, ZMQ_ROUTER);
    assert!(!router.is_null());

    let value = 1;
    assert_eq!(
        zmq_setsockopt(
            router,
            ZMQ_ROUTER_MANDATORY,
            (&value as *const c_int).cast(),
            size_of::<c_int>()
        ),
        0
    );
    let endpoint = c"inproc://c_router_mandatory";
    assert_eq!(zmq_bind(router, endpoint.as_ptr()), 0);

    let mut message = MaybeUninit::<zmq_msg_t>::uninit();
    assert_eq!(zmq_msg_init_size(message.as_mut_ptr(), 4), 0);
    let mut message = unsafe { message.assume_init() };
    assert_eq!(zmq_msg_set_routing_id(&mut message, 999), 0);
    assert_eq!(zmq_msg_send(&mut message, router, 0), -1);
    assert_eq!(zmq_errno(), EHOSTUNREACH);

    assert_eq!(zmq_msg_close(&mut message), 0);
    assert_eq!(zmq_close(router), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn req_rep_inproc_enforces_fsm_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let rep = zmq_socket(ctx, ZMQ_REP);
    let req = zmq_socket(ctx, ZMQ_REQ);
    assert!(!rep.is_null());
    assert!(!req.is_null());

    let endpoint = c"inproc://c_req_rep";
    assert_eq!(zmq_bind(rep, endpoint.as_ptr()), 0);
    assert_eq!(zmq_connect(req, endpoint.as_ptr()), 0);

    let mut buffer = [0u8; 16];
    assert_eq!(
        zmq_recv(req, buffer.as_mut_ptr().cast(), buffer.len(), 0),
        -1
    );
    assert_eq!(zmq_errno(), EFSM);

    let request = b"request";
    assert_eq!(
        zmq_send(req, request.as_ptr().cast(), request.len(), 0),
        request.len() as c_int
    );
    assert_eq!(zmq_send(req, request.as_ptr().cast(), request.len(), 0), -1);
    assert_eq!(zmq_errno(), EFSM);

    assert_eq!(
        zmq_recv(rep, buffer.as_mut_ptr().cast(), buffer.len(), 0),
        request.len() as c_int
    );

    let response = b"reply";
    assert_eq!(
        zmq_send(rep, response.as_ptr().cast(), response.len(), 0),
        response.len() as c_int
    );
    assert_eq!(
        zmq_recv(req, buffer.as_mut_ptr().cast(), buffer.len(), 0),
        response.len() as c_int
    );

    assert_eq!(zmq_close(req), 0);
    assert_eq!(zmq_close(rep), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn req_relaxed_allows_replacing_pending_request_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let rep = zmq_socket(ctx, ZMQ_REP);
    let req = zmq_socket(ctx, ZMQ_REQ);
    assert!(!rep.is_null());
    assert!(!req.is_null());

    let value = 1;
    assert_eq!(
        zmq_setsockopt(
            req,
            ZMQ_REQ_RELAXED,
            (&value as *const c_int).cast(),
            size_of::<c_int>()
        ),
        0
    );
    let endpoint = c"inproc://c_req_relaxed";
    assert_eq!(zmq_bind(rep, endpoint.as_ptr()), 0);
    assert_eq!(zmq_connect(req, endpoint.as_ptr()), 0);

    assert_eq!(zmq_send(req, b"one".as_ptr().cast(), 3, 0), 3);
    assert_eq!(zmq_send(req, b"two".as_ptr().cast(), 3, 0), 3);

    assert_eq!(zmq_close(req), 0);
    assert_eq!(zmq_close(rep), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn pub_sub_inproc_filters_subscriptions_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let publisher = zmq_socket(ctx, ZMQ_PUB);
    let subscriber = zmq_socket(ctx, ZMQ_SUB);
    assert!(!publisher.is_null());
    assert!(!subscriber.is_null());

    let prefix = b"topic";
    assert_eq!(
        zmq_setsockopt(
            subscriber,
            ZMQ_SUBSCRIBE,
            prefix.as_ptr().cast(),
            prefix.len()
        ),
        0
    );

    let endpoint = c"inproc://c_pub_sub";
    assert_eq!(zmq_bind(publisher, endpoint.as_ptr()), 0);
    assert_eq!(zmq_connect(subscriber, endpoint.as_ptr()), 0);

    let dropped = b"other:drop";
    assert_eq!(
        zmq_send(publisher, dropped.as_ptr().cast(), dropped.len(), 0),
        dropped.len() as c_int
    );
    let mut buffer = [0u8; 16];
    assert_eq!(
        zmq_recv(subscriber, buffer.as_mut_ptr().cast(), buffer.len(), 0),
        -1
    );
    assert_eq!(zmq_errno(), EAGAIN);

    let kept = b"topic:keep";
    assert_eq!(
        zmq_send(publisher, kept.as_ptr().cast(), kept.len(), 0),
        kept.len() as c_int
    );
    assert_eq!(
        zmq_recv(subscriber, buffer.as_mut_ptr().cast(), buffer.len(), 0),
        kept.len() as c_int
    );
    assert_eq!(&buffer[..kept.len()], kept);

    assert_eq!(zmq_close(subscriber), 0);
    assert_eq!(zmq_close(publisher), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn xpub_welcome_and_xsub_replay_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let publisher = zmq_socket(ctx, ZMQ_XPUB);
    let subscriber = zmq_socket(ctx, ZMQ_XSUB);
    assert!(!publisher.is_null());
    assert!(!subscriber.is_null());

    let welcome = b"welcome";
    assert_eq!(
        zmq_setsockopt(
            publisher,
            ZMQ_XPUB_WELCOME_MSG,
            welcome.as_ptr().cast(),
            welcome.len()
        ),
        0
    );
    let prefix = b"topic";
    assert_eq!(
        zmq_setsockopt(
            subscriber,
            ZMQ_SUBSCRIBE,
            prefix.as_ptr().cast(),
            prefix.len()
        ),
        0
    );

    let endpoint = c"inproc://c_xpub_welcome";
    assert_eq!(zmq_bind(publisher, endpoint.as_ptr()), 0);
    assert_eq!(zmq_connect(subscriber, endpoint.as_ptr()), 0);

    let mut buffer = [0u8; 16];
    assert_eq!(
        zmq_recv(publisher, buffer.as_mut_ptr().cast(), buffer.len(), 0),
        1 + prefix.len() as c_int
    );
    assert_eq!(&buffer[..1 + prefix.len()], b"\x01topic");
    assert_eq!(
        zmq_recv(subscriber, buffer.as_mut_ptr().cast(), buffer.len(), 0),
        welcome.len() as c_int
    );
    assert_eq!(&buffer[..welcome.len()], welcome);

    assert_eq!(zmq_close(subscriber), 0);
    assert_eq!(zmq_close(publisher), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn poll_and_poller_report_socket_readiness_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let server = zmq_socket(ctx, ZMQ_PAIR);
    let client = zmq_socket(ctx, ZMQ_PAIR);
    assert!(!server.is_null());
    assert!(!client.is_null());

    let endpoint = c"inproc://c_poll_pair";
    assert_eq!(zmq_bind(server, endpoint.as_ptr()), 0);
    assert_eq!(zmq_connect(client, endpoint.as_ptr()), 0);
    assert_eq!(zmq_send(client, b"ping".as_ptr().cast(), 4, 0), 4);

    let mut value = 0;
    let mut size = size_of::<c_int>();
    assert_eq!(
        zmq_getsockopt(
            server,
            ZMQ_EVENTS,
            (&mut value as *mut c_int).cast(),
            &mut size
        ),
        0
    );
    assert_eq!(value & ZMQ_POLLIN as c_int, ZMQ_POLLIN as c_int);
    assert_eq!(
        zmq_getsockopt(
            client,
            ZMQ_EVENTS,
            (&mut value as *mut c_int).cast(),
            &mut size
        ),
        0
    );
    assert_eq!(value & ZMQ_POLLOUT as c_int, ZMQ_POLLOUT as c_int);
    assert_eq!(
        zmq_getsockopt(server, ZMQ_FD, (&mut value as *mut c_int).cast(), &mut size),
        0
    );
    assert_eq!(value, -1);

    let mut item = ZmqPollItem {
        socket: server,
        fd: 0,
        events: ZMQ_POLLIN,
        revents: 0,
    };
    assert_eq!(zmq_poll(&mut item, 1, 0), 1);
    assert_eq!(item.revents & ZMQ_POLLIN, ZMQ_POLLIN);
    assert_eq!(zmq_ppoll(&mut item, 1, 0, ptr::null()), 1);

    let poller = zmq_poller_new();
    assert!(!poller.is_null());
    assert_eq!(
        zmq_poller_add(poller, server, ptr::null_mut(), ZMQ_POLLIN),
        0
    );
    assert_eq!(zmq_poller_size(poller), 1);
    let mut event = ZmqPollerEvent {
        socket: ptr::null_mut(),
        fd: 0,
        user_data: ptr::null_mut(),
        events: 0,
    };
    assert_eq!(zmq_poller_wait(poller, &mut event, 0), 1);
    assert_eq!(event.socket, server);
    assert_eq!(event.events & ZMQ_POLLIN, ZMQ_POLLIN);
    let mut fd = 0;
    assert_eq!(zmq_poller_fd(poller, &mut fd), 0);
    assert_eq!(fd, -1);
    assert_eq!(zmq_poller_remove(poller, server), 0);
    let mut poller_ptr = poller;
    assert_eq!(zmq_poller_destroy(&mut poller_ptr), 0);
    assert!(poller_ptr.is_null());

    assert_eq!(zmq_close(client), 0);
    assert_eq!(zmq_close(server), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn monitor_and_proxy_baseline_work_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let frontend = zmq_socket(ctx, ZMQ_PULL);
    let backend = zmq_socket(ctx, ZMQ_PUSH);
    let producer = zmq_socket(ctx, ZMQ_PUSH);
    let consumer = zmq_socket(ctx, ZMQ_PULL);
    let monitor = zmq_socket(ctx, ZMQ_PAIR);
    assert!(!frontend.is_null());
    assert!(!backend.is_null());
    assert!(!producer.is_null());
    assert!(!consumer.is_null());
    assert!(!monitor.is_null());

    let monitor_endpoint = c"inproc://c_monitor";
    let monitor_versioned_endpoint = c"inproc://c_monitor_v2";
    assert_eq!(
        zmq_socket_monitor(frontend, monitor_endpoint.as_ptr(), ZMQ_EVENT_LISTENING),
        0
    );
    assert_eq!(zmq_connect(monitor, monitor_endpoint.as_ptr()), 0);
    assert_eq!(
        zmq_socket_monitor_versioned(backend, monitor_versioned_endpoint.as_ptr(), 0, 2, 0),
        0
    );

    let front_endpoint = c"inproc://c_proxy_front";
    let back_endpoint = c"inproc://c_proxy_back";
    assert_eq!(zmq_bind(frontend, front_endpoint.as_ptr()), 0);
    let mut event_frame = [0u8; 16];
    assert_eq!(
        zmq_recv(
            monitor,
            event_frame.as_mut_ptr().cast(),
            event_frame.len(),
            0
        ),
        6
    );
    let event = u16::from_ne_bytes([event_frame[0], event_frame[1]]) as c_int;
    assert_eq!(event, ZMQ_EVENT_LISTENING);
    let mut endpoint_frame = [0u8; 64];
    assert_eq!(
        zmq_recv(
            monitor,
            endpoint_frame.as_mut_ptr().cast(),
            endpoint_frame.len(),
            0
        ),
        front_endpoint.to_bytes().len() as c_int
    );
    assert_eq!(
        &endpoint_frame[..front_endpoint.to_bytes().len()],
        front_endpoint.to_bytes()
    );
    assert_eq!(zmq_connect(producer, front_endpoint.as_ptr()), 0);
    assert_eq!(zmq_bind(backend, back_endpoint.as_ptr()), 0);
    assert_eq!(zmq_connect(consumer, back_endpoint.as_ptr()), 0);

    assert_eq!(zmq_send(producer, b"job".as_ptr().cast(), 3, 0), 3);
    assert_eq!(zmq_proxy(frontend, backend, ptr::null_mut()), 0);
    let mut buffer = [0u8; 8];
    assert_eq!(
        zmq_recv(consumer, buffer.as_mut_ptr().cast(), buffer.len(), 0),
        3
    );
    assert_eq!(&buffer[..3], b"job");
    assert_eq!(
        zmq_proxy_steerable(frontend, backend, ptr::null_mut(), ptr::null_mut()),
        0
    );

    assert_eq!(zmq_close(monitor), 0);
    assert_eq!(zmq_close(consumer), 0);
    assert_eq!(zmq_close(producer), 0);
    assert_eq!(zmq_close(backend), 0);
    assert_eq!(zmq_close(frontend), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn timers_atomic_stopwatch_and_thread_helpers_work_over_c_abi() {
    let counter = zmq_atomic_counter_new();
    assert!(!counter.is_null());
    zmq_atomic_counter_set(counter, 5);
    assert_eq!(zmq_atomic_counter_inc(counter), 5);
    assert_eq!(zmq_atomic_counter_value(counter), 6);
    assert_eq!(zmq_atomic_counter_dec(counter), 6);
    assert_eq!(zmq_atomic_counter_value(counter), 5);
    let mut counter_ptr = counter;
    zmq_atomic_counter_destroy(&mut counter_ptr);
    assert!(counter_ptr.is_null());

    TIMER_CALLBACK_COUNT.store(0, Ordering::SeqCst);
    let timers = zmq_timers_new();
    assert!(!timers.is_null());
    let timer_id = zmq_timers_add(timers, 0, Some(count_timer_callback), ptr::null_mut());
    assert!(timer_id > 0);
    assert_eq!(zmq_timers_timeout(timers), 0);
    assert_eq!(zmq_timers_execute(timers), 1);
    assert_eq!(TIMER_CALLBACK_COUNT.load(Ordering::SeqCst), 1);
    assert_eq!(zmq_timers_set_interval(timers, timer_id, 1), 0);
    assert_eq!(zmq_timers_reset(timers, timer_id), 0);
    assert_eq!(zmq_timers_cancel(timers, timer_id), 0);
    let mut timers_ptr = timers;
    assert_eq!(zmq_timers_destroy(&mut timers_ptr), 0);
    assert!(timers_ptr.is_null());

    let watch = zmq_stopwatch_start();
    assert!(!watch.is_null());
    let _ = zmq_stopwatch_intermediate(watch);
    let _ = zmq_stopwatch_stop(watch);

    THREAD_CALLBACK_COUNT.store(0, Ordering::SeqCst);
    let thread = zmq_threadstart(Some(count_thread_callback), ptr::null_mut());
    assert!(!thread.is_null());
    zmq_threadclose(thread);
    assert_eq!(THREAD_CALLBACK_COUNT.load(Ordering::SeqCst), 1);
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
    value = 1;
    assert_eq!(
        zmq_setsockopt(
            socket,
            ZMQ_CONFLATE,
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
    assert_eq!(
        zmq_getsockopt(
            socket,
            ZMQ_CONFLATE,
            (&mut value as *mut c_int).cast(),
            &mut size
        ),
        0
    );
    assert_eq!(value, 1);

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
