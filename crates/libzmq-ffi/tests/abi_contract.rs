#![allow(clippy::undocumented_unsafe_blocks)]

// These C ABI tests intentionally exercise raw-pointer and MaybeUninit boundaries.
// Production unsafe auditing is enforced by `unsafe-report` against crate source.

use std::ffi::{c_char, c_int, c_void};
use std::ffi::{CStr, CString};
use std::mem::{align_of, size_of, MaybeUninit};
use std::net::{TcpListener, UdpSocket};
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};

use zmq::*;

fn skip_synthetic_gssapi_test() -> bool {
    cfg!(feature = "gssapi") && std::env::var_os("LIBZMQ_TEST_REAL_GSSAPI").is_none()
}

const ZMQ_HAUSNUMERO: c_int = 156_384_712;
const ENOTSUP: c_int = ZMQ_HAUSNUMERO + 1;
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
const ZMQ_SERVER: c_int = 12;
const ZMQ_CLIENT: c_int = 13;
const ZMQ_RADIO: c_int = 14;
const ZMQ_DISH: c_int = 15;
const ZMQ_GATHER: c_int = 16;
const ZMQ_SCATTER: c_int = 17;
const ZMQ_DGRAM: c_int = 18;
const ZMQ_PEER: c_int = 19;
const ZMQ_CHANNEL: c_int = 20;
const ZMQ_SNDMORE: c_int = 2;
const ZMQ_MORE: c_int = 1;
const ZMQ_RCVMORE: c_int = 13;
const ZMQ_FD: c_int = 14;
const ZMQ_EVENTS: c_int = 15;
const ZMQ_IO_THREADS: c_int = 1;
const ZMQ_MAX_SOCKETS: c_int = 2;
const ZMQ_AFFINITY: c_int = 4;
const ZMQ_ROUTING_ID: c_int = 5;
const ZMQ_TYPE: c_int = 16;
const ZMQ_LINGER: c_int = 17;
const ZMQ_RATE: c_int = 8;
const ZMQ_RECOVERY_IVL: c_int = 9;
const ZMQ_SNDBUF: c_int = 11;
const ZMQ_RCVBUF: c_int = 12;
const ZMQ_RECONNECT_IVL: c_int = 18;
const ZMQ_BACKLOG: c_int = 19;
const ZMQ_RECONNECT_IVL_MAX: c_int = 21;
const ZMQ_MAXMSGSIZE: c_int = 22;
const ZMQ_SNDHWM: c_int = 23;
const ZMQ_RCVHWM: c_int = 24;
const ZMQ_MULTICAST_HOPS: c_int = 25;
const ZMQ_LAST_ENDPOINT: c_int = 32;
const ZMQ_ROUTER_MANDATORY: c_int = 33;
const ZMQ_TCP_KEEPALIVE: c_int = 34;
const ZMQ_TCP_KEEPALIVE_CNT: c_int = 35;
const ZMQ_TCP_KEEPALIVE_IDLE: c_int = 36;
const ZMQ_TCP_KEEPALIVE_INTVL: c_int = 37;
const ZMQ_IMMEDIATE: c_int = 39;
const ZMQ_IPV6: c_int = 42;
const ZMQ_MECHANISM: c_int = 43;
const ZMQ_PLAIN_SERVER: c_int = 44;
const ZMQ_PLAIN_USERNAME: c_int = 45;
const ZMQ_PLAIN_PASSWORD: c_int = 46;
const ZMQ_CURVE_SERVER: c_int = 47;
const ZMQ_CURVE_PUBLICKEY: c_int = 48;
const ZMQ_CURVE_SECRETKEY: c_int = 49;
const ZMQ_CURVE_SERVERKEY: c_int = 50;
const ZMQ_PROBE_ROUTER: c_int = 51;
const ZMQ_REQ_RELAXED: c_int = 53;
const ZMQ_CONFLATE: c_int = 54;
const ZMQ_ZAP_DOMAIN: c_int = 55;
const ZMQ_TOS: c_int = 57;
const ZMQ_CONNECT_ROUTING_ID: c_int = 61;
const ZMQ_GSSAPI_SERVER: c_int = 62;
const ZMQ_GSSAPI_PRINCIPAL: c_int = 63;
const ZMQ_GSSAPI_SERVICE_PRINCIPAL: c_int = 64;
const ZMQ_SUBSCRIBE: c_int = 6;
const ZMQ_UNSUBSCRIBE: c_int = 7;
const ZMQ_HANDSHAKE_IVL: c_int = 66;
const ZMQ_SOCKS_PROXY: c_int = 68;
const ZMQ_LOOPBACK_FASTPATH: c_int = 94;
const ZMQ_XPUB_WELCOME_MSG: c_int = 72;
const ZMQ_INVERT_MATCHING: c_int = 74;
const ZMQ_HEARTBEAT_IVL: c_int = 75;
const ZMQ_HEARTBEAT_TTL: c_int = 76;
const ZMQ_HEARTBEAT_TIMEOUT: c_int = 77;
const ZMQ_CONNECT_TIMEOUT: c_int = 79;
const ZMQ_TCP_MAXRT: c_int = 80;
const ZMQ_MULTICAST_MAXTPDU: c_int = 84;
const ZMQ_USE_FD: c_int = 89;
const ZMQ_BINDTODEVICE: c_int = 92;
const ZMQ_MULTICAST_LOOP: c_int = 96;
const ZMQ_XPUB_MANUAL_LAST_VALUE: c_int = 98;
const ZMQ_IN_BATCH_SIZE: c_int = 101;
const ZMQ_OUT_BATCH_SIZE: c_int = 102;
const ZMQ_RECONNECT_STOP: c_int = 109;
const ZMQ_HELLO_MSG: c_int = 110;
const ZMQ_DISCONNECT_MSG: c_int = 111;
const ZMQ_PRIORITY: c_int = 112;
const ZMQ_BUSY_POLL: c_int = 113;
const ZMQ_HICCUP_MSG: c_int = 114;
const ZMQ_XSUB_VERBOSE_UNSUBSCRIBE: c_int = 115;
const ZMQ_TOPICS_COUNT: c_int = 116;
const ZMQ_NORM_MODE: c_int = 117;
const ZMQ_NORM_UNICAST_NACK: c_int = 118;
const ZMQ_NORM_BUFFER_SIZE: c_int = 119;
const ZMQ_NORM_SEGMENT_SIZE: c_int = 120;
const ZMQ_NORM_BLOCK_SIZE: c_int = 121;
const ZMQ_NORM_NUM_PARITY: c_int = 122;
const ZMQ_NORM_NUM_AUTOPARITY: c_int = 123;
const ZMQ_NORM_PUSH: c_int = 124;
const ZMQ_POLLIN: i16 = 1;
const ZMQ_POLLOUT: i16 = 2;
const ZMQ_EVENT_LISTENING: c_int = 0x0008;
const ZMQ_NULL: c_int = 0;
const ZMQ_PLAIN: c_int = 1;
const ZMQ_CURVE: c_int = 2;
const ZMQ_GSSAPI: c_int = 3;
const ZMQ_NORM_CC: c_int = 1;
const ZMQ_NORM_CCE: c_int = 3;
const ZMQ_QUEUE: c_int = 3;
const ZMQ_SOCKS_USERNAME: c_int = 99;
const ZMQ_SOCKS_PASSWORD: c_int = 100;

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
fn zmq_has_reports_available_capabilities() {
    assert_eq!(has_capability("ipc"), i32::from(cfg!(feature = "ipc")));
    assert_eq!(has_capability("curve"), 1);
    assert_eq!(has_capability("draft"), 1);
    assert_eq!(has_capability("WS"), 1);
    assert_eq!(has_capability("WSS"), i32::from(cfg!(feature = "wss")));
    assert_eq!(
        has_capability("gssapi"),
        i32::from(cfg!(feature = "gssapi"))
    );
    assert_eq!(has_capability("pgm"), 0);
    assert_eq!(has_capability("epgm"), 0);
    assert_eq!(has_capability("norm"), i32::from(cfg!(feature = "norm")));
    assert_eq!(has_capability("tipc"), 0);
    assert_eq!(has_capability("vmci"), 0);
    assert_eq!(has_capability("vsock"), 0);
    assert_eq!(has_capability("tcp"), 0);
    assert_eq!(has_capability("unknown"), 0);
    assert_eq!(zmq_has(ptr::null()), 0);
}

fn has_capability(name: &str) -> i32 {
    let name = CString::new(name).unwrap();
    zmq_has(name.as_ptr())
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
fn message_more_get_and_set_match_original_semantics() {
    let mut msg = MaybeUninit::<zmq_msg_t>::uninit();
    assert_eq!(zmq_msg_init(msg.as_mut_ptr()), 0);
    let mut msg = unsafe { msg.assume_init() };

    assert_eq!(zmq_msg_more(&msg), 0);
    assert_eq!(zmq_msg_get(&msg, ZMQ_MORE), 0);
    assert_eq!(zmq_msg_set(&mut msg, ZMQ_MORE, 1), -1);
    assert_eq!(zmq_errno(), EINVAL);
    assert_eq!(zmq_msg_more(&msg), 0);
    assert_eq!(zmq_msg_get(&msg, ZMQ_MORE), 0);
    assert_eq!(zmq_msg_get(&msg, 999), -1);
    assert_eq!(zmq_errno(), EINVAL);

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

    for endpoint in [
        "pgm://127.0.0.1:1",
        "epgm://127.0.0.1:1",
        "norm://127.0.0.1:1",
        "tipc://{5560,0,0}",
        "vmci://1:1",
        "vsock://2:1",
    ] {
        let endpoint = std::ffi::CString::new(endpoint).unwrap();
        assert_eq!(zmq_bind(socket, endpoint.as_ptr()), -1);
        assert_eq!(zmq_errno(), ENOTSUP);
        assert_eq!(zmq_connect(socket, endpoint.as_ptr()), -1);
        assert_eq!(zmq_errno(), ENOTSUP);
    }

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
fn pair_ws_round_trip_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let server = zmq_socket(ctx, ZMQ_PAIR);
    let client = zmq_socket(ctx, ZMQ_PAIR);
    assert!(!server.is_null());
    assert!(!client.is_null());

    let endpoint =
        std::ffi::CString::new(format!("ws://127.0.0.1:{}/zmq", unused_tcp_port())).unwrap();
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
#[cfg(feature = "wss")]
fn pair_wss_round_trip_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let server = zmq_socket(ctx, ZMQ_PAIR);
    let client = zmq_socket(ctx, ZMQ_PAIR);
    assert!(!server.is_null());
    assert!(!client.is_null());

    let endpoint =
        std::ffi::CString::new(format!("wss://127.0.0.1:{}/zmq", unused_tcp_port())).unwrap();
    assert_eq!(zmq_bind(server, endpoint.as_ptr()), 0);
    assert_eq!(zmq_connect(client, endpoint.as_ptr()), 0);

    let client_value = client as usize;
    let client_thread = std::thread::spawn(move || {
        let client = client_value as *mut c_void;
        let send_rc = zmq_send(client, b"hello".as_ptr().cast(), 5, 0);
        let mut buffer = [0u8; 16];
        let recv_rc = recv_retry(client, &mut buffer);
        (send_rc, recv_rc, buffer)
    });

    let mut buffer = [0u8; 16];
    assert_eq!(recv_retry(server, &mut buffer), 5);
    assert_eq!(&buffer[..5], b"hello");
    assert_eq!(zmq_send(server, b"world".as_ptr().cast(), 5, 0), 5);

    let (send_rc, recv_rc, buffer) = client_thread.join().unwrap();
    assert_eq!(send_rc, 5);
    assert_eq!(recv_rc, 5);
    assert_eq!(&buffer[..5], b"world");

    assert_eq!(zmq_close(client), 0);
    assert_eq!(zmq_close(server), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn dgram_udp_round_trip_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let server = zmq_socket(ctx, ZMQ_DGRAM);
    let client = zmq_socket(ctx, ZMQ_DGRAM);
    assert!(!server.is_null());
    assert!(!client.is_null());

    let endpoint =
        std::ffi::CString::new(format!("udp://127.0.0.1:{}", unused_udp_port())).unwrap();
    assert_eq!(zmq_bind(server, endpoint.as_ptr()), 0);
    assert_eq!(zmq_connect(client, endpoint.as_ptr()), 0);

    assert_eq!(zmq_send(client, b"ping".as_ptr().cast(), 4, 0), 4);
    let mut buffer = [0u8; 16];
    assert_eq!(recv_retry(server, &mut buffer), 4);
    assert_eq!(&buffer[..4], b"ping");

    assert_eq!(zmq_send(server, b"pong".as_ptr().cast(), 4, 0), 4);
    assert_eq!(recv_retry(client, &mut buffer), 4);
    assert_eq!(&buffer[..4], b"pong");

    assert_eq!(zmq_close(client), 0);
    assert_eq!(zmq_close(server), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn dgram_udp_multicast_receives_group_datagram_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let receiver = zmq_socket(ctx, ZMQ_DGRAM);
    let sender = zmq_socket(ctx, ZMQ_DGRAM);
    assert!(!receiver.is_null());
    assert!(!sender.is_null());

    let endpoint =
        std::ffi::CString::new(format!("udp://239.255.0.2:{}", unused_udp_port())).unwrap();
    assert_eq!(zmq_bind(receiver, endpoint.as_ptr()), 0);
    assert_eq!(zmq_connect(sender, endpoint.as_ptr()), 0);

    assert_eq!(zmq_send(sender, b"mcast".as_ptr().cast(), 5, 0), 5);
    let mut buffer = [0u8; 16];
    assert_eq!(recv_retry(receiver, &mut buffer), 5);
    assert_eq!(&buffer[..5], b"mcast");

    assert_eq!(zmq_close(sender), 0);
    assert_eq!(zmq_close(receiver), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn server_client_tcp_round_trip_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let server = zmq_socket(ctx, ZMQ_SERVER);
    let client = zmq_socket(ctx, ZMQ_CLIENT);
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
fn peer_tcp_round_trip_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let server = zmq_socket(ctx, ZMQ_PEER);
    let client = zmq_socket(ctx, ZMQ_PEER);
    assert!(!server.is_null());
    assert!(!client.is_null());

    let endpoint =
        std::ffi::CString::new(format!("tcp://127.0.0.1:{}", unused_tcp_port())).unwrap();
    assert_eq!(zmq_bind(server, endpoint.as_ptr()), 0);
    assert_ne!(zmq_connect_peer(client, endpoint.as_ptr()), 0);

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
fn channel_tcp_round_trip_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let server = zmq_socket(ctx, ZMQ_CHANNEL);
    let client = zmq_socket(ctx, ZMQ_CHANNEL);
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
fn scatter_gather_tcp_round_trip_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let gather = zmq_socket(ctx, ZMQ_GATHER);
    let scatter = zmq_socket(ctx, ZMQ_SCATTER);
    assert!(!gather.is_null());
    assert!(!scatter.is_null());

    let endpoint =
        std::ffi::CString::new(format!("tcp://127.0.0.1:{}", unused_tcp_port())).unwrap();
    assert_eq!(zmq_bind(gather, endpoint.as_ptr()), 0);
    assert_eq!(zmq_connect(scatter, endpoint.as_ptr()), 0);

    assert_eq!(zmq_send(scatter, b"job".as_ptr().cast(), 3, 0), 3);
    let mut buffer = [0u8; 16];
    assert_eq!(recv_retry(gather, &mut buffer), 3);
    assert_eq!(&buffer[..3], b"job");

    assert_eq!(zmq_close(scatter), 0);
    assert_eq!(zmq_close(gather), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn pair_tcp_plain_round_trip_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let server = zmq_socket(ctx, ZMQ_PAIR);
    let client = zmq_socket(ctx, ZMQ_PAIR);
    assert!(!server.is_null());
    assert!(!client.is_null());

    let enabled = 1;
    assert_eq!(
        zmq_setsockopt(
            server,
            ZMQ_PLAIN_SERVER,
            (&enabled as *const c_int).cast(),
            size_of::<c_int>()
        ),
        0
    );
    assert_eq!(
        zmq_setsockopt(client, ZMQ_PLAIN_USERNAME, b"user".as_ptr().cast(), 4),
        0
    );
    assert_eq!(
        zmq_setsockopt(client, ZMQ_PLAIN_PASSWORD, b"pass".as_ptr().cast(), 4),
        0
    );

    let endpoint =
        std::ffi::CString::new(format!("tcp://127.0.0.1:{}", unused_tcp_port())).unwrap();
    assert_eq!(zmq_bind(server, endpoint.as_ptr()), 0);
    assert_eq!(zmq_connect(client, endpoint.as_ptr()), 0);

    let client_value = client as usize;
    let sender = std::thread::spawn(move || {
        let client = client_value as *mut c_void;
        zmq_send(client, b"hello".as_ptr().cast(), 5, 0)
    });
    let mut buffer = [0u8; 16];
    assert_eq!(recv_retry(server, &mut buffer), 5);
    assert_eq!(&buffer[..5], b"hello");
    assert_eq!(sender.join().unwrap(), 5);

    assert_eq!(zmq_close(client), 0);
    assert_eq!(zmq_close(server), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn pair_tcp_plain_rejects_bad_credentials_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let server = zmq_socket(ctx, ZMQ_PAIR);
    let client = zmq_socket(ctx, ZMQ_PAIR);
    assert!(!server.is_null());
    assert!(!client.is_null());

    let enabled = 1;
    assert_eq!(
        zmq_setsockopt(
            server,
            ZMQ_PLAIN_SERVER,
            (&enabled as *const c_int).cast(),
            size_of::<c_int>()
        ),
        0
    );
    assert_eq!(
        zmq_setsockopt(server, ZMQ_PLAIN_USERNAME, b"expected".as_ptr().cast(), 8),
        0
    );
    assert_eq!(
        zmq_setsockopt(server, ZMQ_PLAIN_PASSWORD, b"secret".as_ptr().cast(), 6),
        0
    );
    assert_eq!(
        zmq_setsockopt(client, ZMQ_PLAIN_USERNAME, b"wrong".as_ptr().cast(), 5),
        0
    );
    assert_eq!(
        zmq_setsockopt(client, ZMQ_PLAIN_PASSWORD, b"secret".as_ptr().cast(), 6),
        0
    );

    let endpoint =
        std::ffi::CString::new(format!("tcp://127.0.0.1:{}", unused_tcp_port())).unwrap();
    assert_eq!(zmq_bind(server, endpoint.as_ptr()), 0);
    assert_eq!(zmq_connect(client, endpoint.as_ptr()), 0);

    let client_value = client as usize;
    let sender = std::thread::spawn(move || {
        let client = client_value as *mut c_void;
        zmq_send(client, b"hello".as_ptr().cast(), 5, 0)
    });
    assert_eq!(recv_retry_errno(server), EINVAL);
    assert_eq!(sender.join().unwrap(), -1);

    assert_eq!(zmq_close(client), 0);
    assert_eq!(zmq_close(server), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn pair_tcp_plain_uses_zap_actor_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let zap = zmq_socket(ctx, ZMQ_REP);
    assert!(!zap.is_null());
    let zap_endpoint = std::ffi::CString::new("inproc://zeromq.zap.01").unwrap();
    assert_eq!(zmq_bind(zap, zap_endpoint.as_ptr()), 0);
    let zap_thread = spawn_plain_zap_actor_c(zap, true);

    let server = zmq_socket(ctx, ZMQ_PAIR);
    let client = zmq_socket(ctx, ZMQ_PAIR);
    assert!(!server.is_null());
    assert!(!client.is_null());
    configure_plain_pair(server, client);

    let endpoint =
        std::ffi::CString::new(format!("tcp://127.0.0.1:{}", unused_tcp_port())).unwrap();
    assert_eq!(zmq_bind(server, endpoint.as_ptr()), 0);
    assert_eq!(zmq_connect(client, endpoint.as_ptr()), 0);

    let client_value = client as usize;
    let sender = std::thread::spawn(move || {
        let client = client_value as *mut c_void;
        zmq_send(client, b"hello".as_ptr().cast(), 5, 0)
    });
    let mut buffer = [0u8; 16];
    assert_eq!(recv_retry(server, &mut buffer), 5);
    assert_eq!(&buffer[..5], b"hello");
    assert_eq!(sender.join().unwrap(), 5);
    zap_thread.join().unwrap();

    assert_eq!(zmq_close(client), 0);
    assert_eq!(zmq_close(server), 0);
    assert_eq!(zmq_close(zap), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn pair_tcp_plain_rejects_zap_denial_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let zap = zmq_socket(ctx, ZMQ_REP);
    assert!(!zap.is_null());
    let zap_endpoint = std::ffi::CString::new("inproc://zeromq.zap.01").unwrap();
    assert_eq!(zmq_bind(zap, zap_endpoint.as_ptr()), 0);
    let zap_thread = spawn_plain_zap_actor_c(zap, false);

    let server = zmq_socket(ctx, ZMQ_PAIR);
    let client = zmq_socket(ctx, ZMQ_PAIR);
    assert!(!server.is_null());
    assert!(!client.is_null());
    configure_plain_pair(server, client);

    let endpoint =
        std::ffi::CString::new(format!("tcp://127.0.0.1:{}", unused_tcp_port())).unwrap();
    assert_eq!(zmq_bind(server, endpoint.as_ptr()), 0);
    assert_eq!(zmq_connect(client, endpoint.as_ptr()), 0);

    let client_value = client as usize;
    let sender = std::thread::spawn(move || {
        let client = client_value as *mut c_void;
        zmq_send(client, b"hello".as_ptr().cast(), 5, 0)
    });
    let mut buffer = [0u8; 16];
    assert_eq!(
        zmq_recv(server, buffer.as_mut_ptr().cast(), buffer.len(), 0),
        -1
    );
    assert_eq!(zmq_errno(), EINVAL);
    assert_eq!(sender.join().unwrap(), -1);
    zap_thread.join().unwrap();

    assert_eq!(zmq_close(client), 0);
    assert_eq!(zmq_close(server), 0);
    assert_eq!(zmq_close(zap), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn pair_tcp_curve_round_trip_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let server = zmq_socket(ctx, ZMQ_PAIR);
    let client = zmq_socket(ctx, ZMQ_PAIR);
    assert!(!server.is_null());
    assert!(!client.is_null());
    configure_curve_pair(server, client, false);

    let mut mechanism = 0;
    let mut mechanism_size = size_of::<c_int>();
    assert_eq!(
        zmq_getsockopt(
            server,
            ZMQ_MECHANISM,
            (&mut mechanism as *mut c_int).cast(),
            &mut mechanism_size
        ),
        0
    );
    assert_eq!(mechanism, ZMQ_CURVE);

    let endpoint =
        std::ffi::CString::new(format!("tcp://127.0.0.1:{}", unused_tcp_port())).unwrap();
    assert_eq!(zmq_bind(server, endpoint.as_ptr()), 0);
    assert_eq!(zmq_connect(client, endpoint.as_ptr()), 0);

    let client_value = client as usize;
    let sender = std::thread::spawn(move || {
        let client = client_value as *mut c_void;
        zmq_send(client, b"curve".as_ptr().cast(), 5, 0)
    });
    let mut buffer = [0u8; 16];
    assert_eq!(recv_retry(server, &mut buffer), 5);
    assert_eq!(&buffer[..5], b"curve");
    assert_eq!(sender.join().unwrap(), 5);

    assert_eq!(zmq_close(client), 0);
    assert_eq!(zmq_close(server), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn pair_tcp_curve_uses_zap_actor_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let zap = zmq_socket(ctx, ZMQ_REP);
    assert!(!zap.is_null());
    let zap_endpoint = std::ffi::CString::new("inproc://zeromq.zap.01").unwrap();
    assert_eq!(zmq_bind(zap, zap_endpoint.as_ptr()), 0);
    let zap_thread = spawn_curve_zap_actor_c(zap, true);

    let server = zmq_socket(ctx, ZMQ_PAIR);
    let client = zmq_socket(ctx, ZMQ_PAIR);
    assert!(!server.is_null());
    assert!(!client.is_null());
    configure_curve_pair(server, client, true);

    let endpoint =
        std::ffi::CString::new(format!("tcp://127.0.0.1:{}", unused_tcp_port())).unwrap();
    assert_eq!(zmq_bind(server, endpoint.as_ptr()), 0);
    assert_eq!(zmq_connect(client, endpoint.as_ptr()), 0);

    let client_value = client as usize;
    let sender = std::thread::spawn(move || {
        let client = client_value as *mut c_void;
        zmq_send(client, b"curve".as_ptr().cast(), 5, 0)
    });
    let mut buffer = [0u8; 16];
    assert_eq!(recv_retry(server, &mut buffer), 5);
    assert_eq!(&buffer[..5], b"curve");
    assert_eq!(sender.join().unwrap(), 5);
    zap_thread.join().unwrap();

    assert_eq!(zmq_close(client), 0);
    assert_eq!(zmq_close(server), 0);
    assert_eq!(zmq_close(zap), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn pair_tcp_gssapi_round_trip_over_c_abi() {
    if skip_synthetic_gssapi_test() {
        return;
    }
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let server = zmq_socket(ctx, ZMQ_PAIR);
    let client = zmq_socket(ctx, ZMQ_PAIR);
    assert!(!server.is_null());
    assert!(!client.is_null());
    configure_gssapi_pair(server, client, b"client@EXAMPLE", b"client@EXAMPLE");

    let mut mechanism = 0;
    let mut mechanism_size = size_of::<c_int>();
    assert_eq!(
        zmq_getsockopt(
            server,
            ZMQ_MECHANISM,
            (&mut mechanism as *mut c_int).cast(),
            &mut mechanism_size
        ),
        0
    );
    assert_eq!(mechanism, ZMQ_GSSAPI);

    let endpoint =
        std::ffi::CString::new(format!("tcp://127.0.0.1:{}", unused_tcp_port())).unwrap();
    assert_eq!(zmq_bind(server, endpoint.as_ptr()), 0);
    assert_eq!(zmq_connect(client, endpoint.as_ptr()), 0);

    let client_value = client as usize;
    let sender = std::thread::spawn(move || {
        let client = client_value as *mut c_void;
        zmq_send(client, b"gss".as_ptr().cast(), 3, 0)
    });
    let mut buffer = [0u8; 16];
    assert_eq!(recv_retry(server, &mut buffer), 3);
    assert_eq!(&buffer[..3], b"gss");
    assert_eq!(sender.join().unwrap(), 3);

    assert_eq!(zmq_close(client), 0);
    assert_eq!(zmq_close(server), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn pair_tcp_gssapi_rejects_bad_principal_over_c_abi() {
    if skip_synthetic_gssapi_test() {
        return;
    }
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let server = zmq_socket(ctx, ZMQ_PAIR);
    let client = zmq_socket(ctx, ZMQ_PAIR);
    assert!(!server.is_null());
    assert!(!client.is_null());
    configure_gssapi_pair(server, client, b"expected@EXAMPLE", b"wrong@EXAMPLE");

    let endpoint =
        std::ffi::CString::new(format!("tcp://127.0.0.1:{}", unused_tcp_port())).unwrap();
    assert_eq!(zmq_bind(server, endpoint.as_ptr()), 0);
    assert_eq!(zmq_connect(client, endpoint.as_ptr()), 0);

    let client_value = client as usize;
    let sender = std::thread::spawn(move || {
        let client = client_value as *mut c_void;
        zmq_send(client, b"gss".as_ptr().cast(), 3, 0)
    });
    let mut buffer = [0u8; 16];
    assert_eq!(
        zmq_recv(server, buffer.as_mut_ptr().cast(), buffer.len(), 0),
        -1
    );
    assert_eq!(zmq_errno(), EINVAL);
    assert_eq!(sender.join().unwrap(), -1);

    assert_eq!(zmq_close(client), 0);
    assert_eq!(zmq_close(server), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn pair_tcp_gssapi_uses_zap_actor_over_c_abi() {
    if skip_synthetic_gssapi_test() {
        return;
    }
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let zap = zmq_socket(ctx, ZMQ_REP);
    assert!(!zap.is_null());
    let zap_endpoint = std::ffi::CString::new("inproc://zeromq.zap.01").unwrap();
    assert_eq!(zmq_bind(zap, zap_endpoint.as_ptr()), 0);
    let zap_thread = spawn_gssapi_zap_actor_c(zap, true);

    let server = zmq_socket(ctx, ZMQ_PAIR);
    let client = zmq_socket(ctx, ZMQ_PAIR);
    assert!(!server.is_null());
    assert!(!client.is_null());
    configure_gssapi_pair(server, client, b"", b"client@EXAMPLE");
    assert_eq!(
        zmq_setsockopt(server, ZMQ_ZAP_DOMAIN, b"domain".as_ptr().cast(), 6),
        0
    );

    let endpoint =
        std::ffi::CString::new(format!("tcp://127.0.0.1:{}", unused_tcp_port())).unwrap();
    assert_eq!(zmq_bind(server, endpoint.as_ptr()), 0);
    assert_eq!(zmq_connect(client, endpoint.as_ptr()), 0);

    let client_value = client as usize;
    let sender = std::thread::spawn(move || {
        let client = client_value as *mut c_void;
        zmq_send(client, b"gss".as_ptr().cast(), 3, 0)
    });
    let mut buffer = [0u8; 16];
    assert_eq!(recv_retry(server, &mut buffer), 3);
    assert_eq!(&buffer[..3], b"gss");
    assert_eq!(sender.join().unwrap(), 3);
    zap_thread.join().unwrap();

    assert_eq!(zmq_close(client), 0);
    assert_eq!(zmq_close(server), 0);
    assert_eq!(zmq_close(zap), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

fn configure_gssapi_pair(server: *mut c_void, client: *mut c_void, expected: &[u8], actual: &[u8]) {
    let enabled = 1;
    assert_eq!(
        zmq_setsockopt(
            server,
            ZMQ_GSSAPI_SERVER,
            (&enabled as *const c_int).cast(),
            size_of::<c_int>()
        ),
        0
    );
    assert_eq!(
        zmq_setsockopt(
            server,
            ZMQ_GSSAPI_PRINCIPAL,
            expected.as_ptr().cast(),
            expected.len()
        ),
        0
    );
    assert_eq!(
        zmq_setsockopt(
            client,
            ZMQ_GSSAPI_PRINCIPAL,
            actual.as_ptr().cast(),
            actual.len()
        ),
        0
    );
    assert_eq!(
        zmq_setsockopt(
            client,
            ZMQ_GSSAPI_SERVICE_PRINCIPAL,
            b"server@EXAMPLE".as_ptr().cast(),
            14
        ),
        0
    );
}

fn configure_curve_pair(server: *mut c_void, client: *mut c_void, zap_domain: bool) {
    let mut server_public = [0 as c_char; 41];
    let mut server_secret = [0 as c_char; 41];
    let mut client_public = [0 as c_char; 41];
    let mut client_secret = [0 as c_char; 41];
    assert_eq!(
        zmq_curve_keypair(server_public.as_mut_ptr(), server_secret.as_mut_ptr()),
        0
    );
    assert_eq!(
        zmq_curve_keypair(client_public.as_mut_ptr(), client_secret.as_mut_ptr()),
        0
    );
    let enabled = 1;
    assert_eq!(
        zmq_setsockopt(
            server,
            ZMQ_CURVE_SERVER,
            (&enabled as *const c_int).cast(),
            size_of::<c_int>()
        ),
        0
    );
    assert_eq!(
        zmq_setsockopt(
            server,
            ZMQ_CURVE_SECRETKEY,
            server_secret.as_ptr().cast(),
            40
        ),
        0
    );
    if zap_domain {
        assert_eq!(
            zmq_setsockopt(server, ZMQ_ZAP_DOMAIN, b"domain".as_ptr().cast(), 6),
            0
        );
    } else {
        assert_eq!(
            zmq_setsockopt(
                server,
                ZMQ_CURVE_PUBLICKEY,
                client_public.as_ptr().cast(),
                40
            ),
            0
        );
    }
    assert_eq!(
        zmq_setsockopt(
            client,
            ZMQ_CURVE_SERVERKEY,
            server_public.as_ptr().cast(),
            40
        ),
        0
    );
    assert_eq!(
        zmq_setsockopt(
            client,
            ZMQ_CURVE_PUBLICKEY,
            client_public.as_ptr().cast(),
            40
        ),
        0
    );
    assert_eq!(
        zmq_setsockopt(
            client,
            ZMQ_CURVE_SECRETKEY,
            client_secret.as_ptr().cast(),
            40
        ),
        0
    );
}

fn spawn_curve_zap_actor_c(zap: *mut c_void, accept: bool) -> std::thread::JoinHandle<()> {
    let zap_value = zap as usize;
    std::thread::spawn(move || {
        let zap = zap_value as *mut c_void;
        let frames = recv_multipart_c(zap);
        assert_eq!(frames[0], b"1.0");
        assert_eq!(frames[2], b"domain");
        assert_eq!(frames[5], b"CURVE");
        assert_eq!(frames[6].len(), 32);
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
            assert_eq!(
                zmq_send(zap, frame.as_ptr().cast(), frame.len(), flags),
                frame.len() as c_int
            );
        }
    })
}

fn spawn_gssapi_zap_actor_c(zap: *mut c_void, accept: bool) -> std::thread::JoinHandle<()> {
    let zap_value = zap as usize;
    std::thread::spawn(move || {
        let zap = zap_value as *mut c_void;
        let frames = recv_multipart_c(zap);
        assert_eq!(frames[0], b"1.0");
        assert_eq!(frames[2], b"domain");
        assert_eq!(frames[5], b"GSSAPI");
        assert_eq!(frames[6], b"client@EXAMPLE");
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
            assert_eq!(
                zmq_send(zap, frame.as_ptr().cast(), frame.len(), flags),
                frame.len() as c_int
            );
        }
    })
}

fn configure_plain_pair(server: *mut c_void, client: *mut c_void) {
    let enabled = 1;
    assert_eq!(
        zmq_setsockopt(
            server,
            ZMQ_PLAIN_SERVER,
            (&enabled as *const c_int).cast(),
            size_of::<c_int>()
        ),
        0
    );
    assert_eq!(
        zmq_setsockopt(server, ZMQ_ZAP_DOMAIN, b"domain".as_ptr().cast(), 6),
        0
    );
    assert_eq!(
        zmq_setsockopt(client, ZMQ_PLAIN_USERNAME, b"user".as_ptr().cast(), 4),
        0
    );
    assert_eq!(
        zmq_setsockopt(client, ZMQ_PLAIN_PASSWORD, b"pass".as_ptr().cast(), 4),
        0
    );
}

fn spawn_plain_zap_actor_c(zap: *mut c_void, accept: bool) -> std::thread::JoinHandle<()> {
    let zap_value = zap as usize;
    std::thread::spawn(move || {
        let zap = zap_value as *mut c_void;
        let frames = recv_multipart_c(zap);
        assert_eq!(frames[0], b"1.0");
        assert_eq!(frames[2], b"domain");
        assert_eq!(frames[5], b"PLAIN");
        assert_eq!(frames[6], b"user");
        assert_eq!(frames[7], b"pass");
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
            assert_eq!(
                zmq_send(zap, frame.as_ptr().cast(), frame.len(), flags),
                frame.len() as c_int
            );
        }
    })
}

fn recv_multipart_c(socket: *mut c_void) -> Vec<Vec<u8>> {
    let mut frames = Vec::new();
    loop {
        let mut buffer = [0u8; 256];
        let size = recv_retry(socket, &mut buffer);
        assert!(size >= 0);
        frames.push(buffer[..size as usize].to_vec());
        let mut more = 0;
        let mut more_size = size_of::<c_int>();
        assert_eq!(
            zmq_getsockopt(
                socket,
                ZMQ_RCVMORE,
                (&mut more as *mut c_int).cast(),
                &mut more_size
            ),
            0
        );
        if more == 0 {
            return frames;
        }
    }
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

fn unused_udp_port() -> u16 {
    UdpSocket::bind("127.0.0.1:0")
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

fn recv_retry_errno(socket: *mut c_void) -> c_int {
    let mut buffer = [0u8; 16];
    for _ in 0..20 {
        if zmq_recv(socket, buffer.as_mut_ptr().cast(), buffer.len(), 0) >= 0 {
            return 0;
        }
        let errno = zmq_errno();
        if errno != EAGAIN {
            return errno;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    zmq_errno()
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
fn iovec_send_and_recv_preserve_multipart_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let sender = zmq_socket(ctx, ZMQ_PAIR);
    let receiver = zmq_socket(ctx, ZMQ_PAIR);
    assert!(!sender.is_null());
    assert!(!receiver.is_null());

    let endpoint = c"inproc://c_iovec";
    assert_eq!(zmq_bind(receiver, endpoint.as_ptr()), 0);
    assert_eq!(zmq_connect(sender, endpoint.as_ptr()), 0);

    let first = b"hello";
    let second = b"world";
    let mut send_iov = [
        Iovec {
            iov_base: first.as_ptr() as *mut c_void,
            iov_len: first.len(),
        },
        Iovec {
            iov_base: second.as_ptr() as *mut c_void,
            iov_len: second.len(),
        },
    ];
    assert_eq!(
        zmq_sendiov(sender, send_iov.as_mut_ptr(), send_iov.len(), ZMQ_SNDMORE),
        5
    );

    let mut recv_iov = [
        Iovec {
            iov_base: ptr::null_mut(),
            iov_len: 0,
        },
        Iovec {
            iov_base: ptr::null_mut(),
            iov_len: 0,
        },
    ];
    let mut count = recv_iov.len();
    assert_eq!(
        zmq_recviov(receiver, recv_iov.as_mut_ptr(), &mut count, 0),
        2
    );
    assert_eq!(count, 2);
    assert_eq!(recv_iov[0].iov_len, first.len());
    assert_eq!(recv_iov[1].iov_len, second.len());
    unsafe {
        assert_eq!(
            std::slice::from_raw_parts(recv_iov[0].iov_base.cast::<u8>(), first.len()),
            first
        );
        assert_eq!(
            std::slice::from_raw_parts(recv_iov[1].iov_base.cast::<u8>(), second.len()),
            second
        );
        libc::free(recv_iov[0].iov_base);
        libc::free(recv_iov[1].iov_base);
    }

    assert_eq!(zmq_sendiov(sender, ptr::null_mut(), 1, 0), -1);
    assert_eq!(zmq_errno(), EINVAL);
    assert_eq!(zmq_recviov(receiver, ptr::null_mut(), &mut count, 0), -1);
    assert_eq!(zmq_errno(), EINVAL);

    assert_eq!(zmq_close(sender), 0);
    assert_eq!(zmq_close(receiver), 0);
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
fn channel_inproc_round_trip_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let server = zmq_socket(ctx, ZMQ_CHANNEL);
    let client = zmq_socket(ctx, ZMQ_CHANNEL);
    assert!(!server.is_null());
    assert!(!client.is_null());

    let endpoint = c"inproc://c_channel";
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
}

#[test]
fn scatter_gather_inproc_round_trip_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let gather = zmq_socket(ctx, ZMQ_GATHER);
    let scatter = zmq_socket(ctx, ZMQ_SCATTER);
    assert!(!gather.is_null());
    assert!(!scatter.is_null());

    let endpoint = c"inproc://c_scatter_gather";
    assert_eq!(zmq_bind(gather, endpoint.as_ptr()), 0);
    assert_eq!(zmq_connect(scatter, endpoint.as_ptr()), 0);

    assert_eq!(zmq_send(scatter, b"job".as_ptr().cast(), 3, 0), 3);
    let mut buffer = [0u8; 8];
    assert_eq!(
        zmq_recv(gather, buffer.as_mut_ptr().cast(), buffer.len(), 0),
        3
    );
    assert_eq!(&buffer[..3], b"job");

    assert_eq!(zmq_send(gather, b"bad".as_ptr().cast(), 3, 0), -1);
    assert_eq!(zmq_errno(), ENOTSUP);
    assert_eq!(
        zmq_recv(scatter, buffer.as_mut_ptr().cast(), buffer.len(), 0),
        -1
    );
    assert_eq!(zmq_errno(), ENOTSUP);

    assert_eq!(zmq_close(scatter), 0);
    assert_eq!(zmq_close(gather), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn scatter_gather_inproc_load_balances_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let scatter = zmq_socket(ctx, ZMQ_SCATTER);
    let gather_a = zmq_socket(ctx, ZMQ_GATHER);
    let gather_b = zmq_socket(ctx, ZMQ_GATHER);
    assert!(!scatter.is_null());
    assert!(!gather_a.is_null());
    assert!(!gather_b.is_null());

    let endpoint = c"inproc://c_scatter_lb";
    assert_eq!(zmq_bind(scatter, endpoint.as_ptr()), 0);
    assert_eq!(zmq_connect(gather_a, endpoint.as_ptr()), 0);
    assert_eq!(zmq_connect(gather_b, endpoint.as_ptr()), 0);

    assert_eq!(zmq_send(scatter, b"one".as_ptr().cast(), 3, 0), 3);
    assert_eq!(zmq_send(scatter, b"two".as_ptr().cast(), 3, 0), 3);

    let mut buffer = [0u8; 8];
    assert_eq!(
        zmq_recv(gather_a, buffer.as_mut_ptr().cast(), buffer.len(), 0),
        3
    );
    assert_eq!(&buffer[..3], b"one");
    assert_eq!(
        zmq_recv(gather_b, buffer.as_mut_ptr().cast(), buffer.len(), 0),
        3
    );
    assert_eq!(&buffer[..3], b"two");

    assert_eq!(zmq_close(gather_b), 0);
    assert_eq!(zmq_close(gather_a), 0);
    assert_eq!(zmq_close(scatter), 0);
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
fn probe_router_inproc_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let router = zmq_socket(ctx, ZMQ_ROUTER);
    let dealer = zmq_socket(ctx, ZMQ_DEALER);
    let pair = zmq_socket(ctx, ZMQ_PAIR);
    assert!(!router.is_null());
    assert!(!dealer.is_null());
    assert!(!pair.is_null());

    let mut probe = 1;
    assert_eq!(
        zmq_setsockopt(
            dealer,
            ZMQ_PROBE_ROUTER,
            (&probe as *const c_int).cast(),
            size_of::<c_int>()
        ),
        0
    );
    assert_eq!(
        zmq_setsockopt(
            pair,
            ZMQ_PROBE_ROUTER,
            (&probe as *const c_int).cast(),
            size_of::<c_int>()
        ),
        -1
    );
    assert_eq!(zmq_errno(), EINVAL);
    probe = -1;
    assert_eq!(
        zmq_setsockopt(
            dealer,
            ZMQ_PROBE_ROUTER,
            (&probe as *const c_int).cast(),
            size_of::<c_int>()
        ),
        -1
    );
    assert_eq!(zmq_errno(), EINVAL);

    let endpoint = c"inproc://c_probe_router";
    assert_eq!(zmq_bind(router, endpoint.as_ptr()), 0);
    assert_eq!(zmq_connect(dealer, endpoint.as_ptr()), 0);

    let mut inbound = MaybeUninit::<zmq_msg_t>::uninit();
    assert_eq!(zmq_msg_init(inbound.as_mut_ptr()), 0);
    let mut inbound = unsafe { inbound.assume_init() };
    assert_eq!(zmq_msg_recv(&mut inbound, router, 0), 0);
    assert_eq!(zmq_msg_size(&inbound), 0);
    assert_ne!(zmq_msg_routing_id(&mut inbound), 0);
    assert_eq!(zmq_msg_close(&mut inbound), 0);

    assert_eq!(zmq_close(pair), 0);
    assert_eq!(zmq_close(dealer), 0);
    assert_eq!(zmq_close(router), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn server_client_inproc_round_trip_sets_routing_id_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let server = zmq_socket(ctx, ZMQ_SERVER);
    let client = zmq_socket(ctx, ZMQ_CLIENT);
    assert!(!server.is_null());
    assert!(!client.is_null());

    let endpoint = c"inproc://c_server_client";
    assert_eq!(zmq_bind(server, endpoint.as_ptr()), 0);
    assert_eq!(zmq_connect(client, endpoint.as_ptr()), 0);

    assert_eq!(zmq_send(client, b"request".as_ptr().cast(), 7, 0), 7);

    let mut request = MaybeUninit::<zmq_msg_t>::uninit();
    assert_eq!(zmq_msg_init(request.as_mut_ptr()), 0);
    let mut request = unsafe { request.assume_init() };
    assert_eq!(zmq_msg_recv(&mut request, server, 0), 7);
    let routing_id = zmq_msg_routing_id(&mut request);
    assert_ne!(routing_id, 0);

    assert_eq!(
        zmq_send(server, b"missing route".as_ptr().cast(), 13, 0),
        -1
    );
    assert_eq!(zmq_errno(), EAGAIN);

    let mut reply = MaybeUninit::<zmq_msg_t>::uninit();
    assert_eq!(zmq_msg_init_size(reply.as_mut_ptr(), 5), 0);
    let mut reply = unsafe { reply.assume_init() };
    unsafe {
        ptr::copy_nonoverlapping(b"reply".as_ptr(), zmq_msg_data(&mut reply).cast::<u8>(), 5);
    }
    assert_eq!(zmq_msg_set_routing_id(&mut reply, routing_id), 0);
    assert_eq!(zmq_msg_send(&mut reply, server, 0), 5);

    let mut buffer = [0u8; 16];
    assert_eq!(
        zmq_recv(client, buffer.as_mut_ptr().cast(), buffer.len(), 0),
        5
    );
    assert_eq!(&buffer[..5], b"reply");
    assert_eq!(zmq_disconnect_peer(server, routing_id), 0);
    let mut disconnected_reply = MaybeUninit::<zmq_msg_t>::uninit();
    assert_eq!(zmq_msg_init_size(disconnected_reply.as_mut_ptr(), 4), 0);
    let mut disconnected_reply = unsafe { disconnected_reply.assume_init() };
    assert_eq!(
        zmq_msg_set_routing_id(&mut disconnected_reply, routing_id),
        0
    );
    assert_eq!(zmq_msg_send(&mut disconnected_reply, server, 0), -1);
    assert_eq!(zmq_errno(), EHOSTUNREACH);
    assert_eq!(zmq_msg_close(&mut disconnected_reply), 0);
    assert_eq!(zmq_disconnect_peer(server, routing_id), -1);
    assert_eq!(zmq_errno(), EHOSTUNREACH);
    assert_eq!(zmq_disconnect_peer(client, routing_id), -1);
    assert_eq!(zmq_errno(), ENOTSUP);

    assert_eq!(zmq_msg_close(&mut reply), 0);
    assert_eq!(zmq_msg_close(&mut request), 0);
    assert_eq!(zmq_close(client), 0);
    assert_eq!(zmq_close(server), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn peer_inproc_round_trip_sets_routing_id_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let bound = zmq_socket(ctx, ZMQ_PEER);
    let connected = zmq_socket(ctx, ZMQ_PEER);
    assert!(!bound.is_null());
    assert!(!connected.is_null());

    let endpoint = c"inproc://c_peer";
    assert_eq!(zmq_bind(bound, endpoint.as_ptr()), 0);
    assert_ne!(zmq_connect_peer(connected, endpoint.as_ptr()), 0);

    assert_eq!(zmq_send(connected, b"request".as_ptr().cast(), 7, 0), 7);

    let mut request = MaybeUninit::<zmq_msg_t>::uninit();
    assert_eq!(zmq_msg_init(request.as_mut_ptr()), 0);
    let mut request = unsafe { request.assume_init() };
    assert_eq!(zmq_msg_recv(&mut request, bound, 0), 7);
    let routing_id = zmq_msg_routing_id(&mut request);
    assert_ne!(routing_id, 0);

    assert_eq!(zmq_send(bound, b"missing route".as_ptr().cast(), 13, 0), -1);
    assert_eq!(zmq_errno(), EAGAIN);

    let mut reply = MaybeUninit::<zmq_msg_t>::uninit();
    assert_eq!(zmq_msg_init_size(reply.as_mut_ptr(), 5), 0);
    let mut reply = unsafe { reply.assume_init() };
    unsafe {
        ptr::copy_nonoverlapping(b"reply".as_ptr(), zmq_msg_data(&mut reply).cast::<u8>(), 5);
    }
    assert_eq!(zmq_msg_set_routing_id(&mut reply, routing_id), 0);
    assert_eq!(zmq_msg_send(&mut reply, bound, 0), 5);

    let mut buffer = [0u8; 16];
    assert_eq!(
        zmq_recv(connected, buffer.as_mut_ptr().cast(), buffer.len(), 0),
        5
    );
    assert_eq!(&buffer[..5], b"reply");
    assert_eq!(zmq_disconnect_peer(bound, routing_id), 0);
    let mut disconnected_reply = MaybeUninit::<zmq_msg_t>::uninit();
    assert_eq!(zmq_msg_init_size(disconnected_reply.as_mut_ptr(), 4), 0);
    let mut disconnected_reply = unsafe { disconnected_reply.assume_init() };
    assert_eq!(
        zmq_msg_set_routing_id(&mut disconnected_reply, routing_id),
        0
    );
    assert_eq!(zmq_msg_send(&mut disconnected_reply, bound, 0), -1);
    assert_eq!(zmq_errno(), EHOSTUNREACH);
    assert_eq!(zmq_msg_close(&mut disconnected_reply), 0);
    assert_eq!(zmq_disconnect_peer(bound, routing_id), -1);
    assert_eq!(zmq_errno(), EHOSTUNREACH);

    assert_eq!(zmq_msg_close(&mut reply), 0);
    assert_eq!(zmq_msg_close(&mut request), 0);
    assert_eq!(zmq_close(connected), 0);
    assert_eq!(zmq_close(bound), 0);
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

#[cfg(feature = "norm")]
#[test]
fn pub_sub_norm_round_trip_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let publisher = zmq_socket(ctx, ZMQ_PUB);
    let subscriber = zmq_socket(ctx, ZMQ_SUB);
    assert!(!publisher.is_null());
    assert!(!subscriber.is_null());

    let endpoint = CString::new(format!("norm://127.0.0.1:{}", unused_udp_port())).unwrap();
    assert_eq!(zmq_bind(publisher, endpoint.as_ptr()), 0);
    assert_eq!(zmq_connect(subscriber, endpoint.as_ptr()), 0);
    assert_eq!(zmq_setsockopt(subscriber, ZMQ_SUBSCRIBE, ptr::null(), 0), 0);
    std::thread::sleep(std::time::Duration::from_millis(100));

    let payload = b"norm-c";
    assert_eq!(
        zmq_send(publisher, payload.as_ptr().cast(), payload.len(), 0),
        payload.len() as c_int
    );
    let mut buffer = [0u8; 16];
    assert_eq!(recv_retry(subscriber, &mut buffer), payload.len() as c_int);
    assert_eq!(&buffer[..payload.len()], payload);

    assert_eq!(zmq_close(subscriber), 0);
    assert_eq!(zmq_close(publisher), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn radio_dish_inproc_filters_groups_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let radio = zmq_socket(ctx, ZMQ_RADIO);
    let dish = zmq_socket(ctx, ZMQ_DISH);
    assert!(!radio.is_null());
    assert!(!dish.is_null());

    let endpoint = c"inproc://c_radio_dish";
    assert_eq!(zmq_bind(radio, endpoint.as_ptr()), 0);
    assert_eq!(zmq_connect(dish, endpoint.as_ptr()), 0);
    assert_eq!(zmq_join(dish, c"updates".as_ptr()), 0);

    let mut ignored = MaybeUninit::<zmq_msg_t>::uninit();
    assert_eq!(zmq_msg_init_size(ignored.as_mut_ptr(), 3), 0);
    let mut ignored = unsafe { ignored.assume_init() };
    unsafe {
        ptr::copy_nonoverlapping(b"old".as_ptr(), zmq_msg_data(&mut ignored).cast::<u8>(), 3);
    }
    assert_eq!(zmq_msg_set_group(&mut ignored, c"archive".as_ptr()), 0);
    assert_eq!(zmq_msg_send(&mut ignored, radio, 0), 3);

    let mut inbound = MaybeUninit::<zmq_msg_t>::uninit();
    assert_eq!(zmq_msg_init(inbound.as_mut_ptr()), 0);
    let mut inbound = unsafe { inbound.assume_init() };
    assert_eq!(zmq_msg_recv(&mut inbound, dish, 0), -1);
    assert_eq!(zmq_errno(), EAGAIN);

    let mut outbound = MaybeUninit::<zmq_msg_t>::uninit();
    assert_eq!(zmq_msg_init_size(outbound.as_mut_ptr(), 3), 0);
    let mut outbound = unsafe { outbound.assume_init() };
    unsafe {
        ptr::copy_nonoverlapping(b"new".as_ptr(), zmq_msg_data(&mut outbound).cast::<u8>(), 3);
    }
    assert_eq!(zmq_msg_set_group(&mut outbound, c"updates".as_ptr()), 0);
    assert_eq!(zmq_msg_send(&mut outbound, radio, 0), 3);
    assert_eq!(zmq_msg_recv(&mut inbound, dish, 0), 3);
    let data = unsafe { std::slice::from_raw_parts(zmq_msg_data(&mut inbound).cast::<u8>(), 3) };
    assert_eq!(data, b"new");
    let group = unsafe { CStr::from_ptr(zmq_msg_group(&mut inbound)) };
    assert_eq!(group.to_bytes(), b"updates");

    assert_eq!(zmq_leave(dish, c"updates".as_ptr()), 0);
    let mut later = MaybeUninit::<zmq_msg_t>::uninit();
    assert_eq!(zmq_msg_init_size(later.as_mut_ptr(), 5), 0);
    let mut later = unsafe { later.assume_init() };
    unsafe {
        ptr::copy_nonoverlapping(b"later".as_ptr(), zmq_msg_data(&mut later).cast::<u8>(), 5);
    }
    assert_eq!(zmq_msg_set_group(&mut later, c"updates".as_ptr()), 0);
    assert_eq!(zmq_msg_send(&mut later, radio, 0), 5);
    assert_eq!(zmq_msg_recv(&mut inbound, dish, 0), -1);
    assert_eq!(zmq_errno(), EAGAIN);

    assert_eq!(zmq_msg_close(&mut later), 0);
    assert_eq!(zmq_msg_close(&mut outbound), 0);
    assert_eq!(zmq_msg_close(&mut inbound), 0);
    assert_eq!(zmq_msg_close(&mut ignored), 0);
    assert_eq!(zmq_close(dish), 0);
    assert_eq!(zmq_close(radio), 0);
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
    assert_eq!(zmq_send(producer, b"dev".as_ptr().cast(), 3, 0), 3);
    assert_eq!(zmq_device(ZMQ_QUEUE, frontend, backend), 0);
    assert_eq!(
        zmq_recv(consumer, buffer.as_mut_ptr().cast(), buffer.len(), 0),
        3
    );
    assert_eq!(&buffer[..3], b"dev");

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

    let mut affinity = 0u64;
    size = size_of::<u64>();
    assert_eq!(
        zmq_getsockopt(
            socket,
            ZMQ_AFFINITY,
            (&mut affinity as *mut u64).cast(),
            &mut size
        ),
        0
    );
    assert_eq!(affinity, 0);
    assert_eq!(size, size_of::<u64>());
    affinity = 0x1020_3040_5060_7080;
    assert_eq!(
        zmq_setsockopt(
            socket,
            ZMQ_AFFINITY,
            (&affinity as *const u64).cast(),
            size_of::<u64>()
        ),
        0
    );
    affinity = 0;
    size = size_of::<u64>();
    assert_eq!(
        zmq_getsockopt(
            socket,
            ZMQ_AFFINITY,
            (&mut affinity as *mut u64).cast(),
            &mut size
        ),
        0
    );
    assert_eq!(affinity, 0x1020_3040_5060_7080);

    let mut maxmsgsize = 0i64;
    size = size_of::<i64>();
    assert_eq!(
        zmq_getsockopt(
            socket,
            ZMQ_MAXMSGSIZE,
            (&mut maxmsgsize as *mut i64).cast(),
            &mut size
        ),
        0
    );
    assert_eq!(maxmsgsize, -1);
    assert_eq!(size, size_of::<i64>());
    maxmsgsize = 1_048_576;
    assert_eq!(
        zmq_setsockopt(
            socket,
            ZMQ_MAXMSGSIZE,
            (&maxmsgsize as *const i64).cast(),
            size_of::<i64>()
        ),
        0
    );
    maxmsgsize = 0;
    size = size_of::<i64>();
    assert_eq!(
        zmq_getsockopt(
            socket,
            ZMQ_MAXMSGSIZE,
            (&mut maxmsgsize as *mut i64).cast(),
            &mut size
        ),
        0
    );
    assert_eq!(maxmsgsize, 1_048_576);

    let mut routing_buffer = [0u8; 32];
    size = routing_buffer.len();
    assert_eq!(
        zmq_getsockopt(
            socket,
            ZMQ_ROUTING_ID,
            routing_buffer.as_mut_ptr().cast(),
            &mut size
        ),
        0
    );
    assert_eq!(size, 0);

    let routing_id = b"raw-routing-id";
    assert_eq!(
        zmq_setsockopt(
            socket,
            ZMQ_ROUTING_ID,
            routing_id.as_ptr().cast(),
            routing_id.len()
        ),
        0
    );
    size = routing_buffer.len();
    assert_eq!(
        zmq_getsockopt(
            socket,
            ZMQ_ROUTING_ID,
            routing_buffer.as_mut_ptr().cast(),
            &mut size
        ),
        0
    );
    assert_eq!(size, routing_id.len());
    assert_eq!(&routing_buffer[..size], routing_id);
    size = routing_id.len() - 1;
    assert_eq!(
        zmq_getsockopt(
            socket,
            ZMQ_ROUTING_ID,
            routing_buffer.as_mut_ptr().cast(),
            &mut size
        ),
        -1
    );
    assert_eq!(zmq_errno(), EINVAL);
    assert_eq!(zmq_setsockopt(socket, ZMQ_ROUTING_ID, ptr::null(), 0), -1);
    assert_eq!(zmq_errno(), EINVAL);
    let oversized_routing_id = [b'x'; 256];
    assert_eq!(
        zmq_setsockopt(
            socket,
            ZMQ_ROUTING_ID,
            oversized_routing_id.as_ptr().cast(),
            oversized_routing_id.len()
        ),
        -1
    );
    assert_eq!(zmq_errno(), EINVAL);

    assert_eq!(
        zmq_setsockopt(
            socket,
            ZMQ_CONNECT_ROUTING_ID,
            routing_id.as_ptr().cast(),
            routing_id.len()
        ),
        -1
    );
    assert_eq!(zmq_errno(), EINVAL);
    let router = zmq_socket(ctx, ZMQ_ROUTER);
    assert!(!router.is_null());
    assert_eq!(
        zmq_setsockopt(
            router,
            ZMQ_CONNECT_ROUTING_ID,
            routing_id.as_ptr().cast(),
            routing_id.len()
        ),
        0
    );
    assert_eq!(
        zmq_setsockopt(router, ZMQ_CONNECT_ROUTING_ID, ptr::null(), 0),
        -1
    );
    assert_eq!(zmq_errno(), EINVAL);
    assert_eq!(zmq_close(router), 0);

    value = 1;
    assert_eq!(
        zmq_setsockopt(
            socket,
            ZMQ_AFFINITY,
            (&value as *const c_int).cast(),
            size_of::<c_int>()
        ),
        -1
    );
    assert_eq!(zmq_errno(), EINVAL);
    size = size_of::<c_int>();
    assert_eq!(
        zmq_getsockopt(
            socket,
            ZMQ_MAXMSGSIZE,
            (&mut value as *mut c_int).cast(),
            &mut size
        ),
        -1
    );
    assert_eq!(zmq_errno(), EINVAL);

    let endpoint = c"inproc://c_last_endpoint";
    assert_eq!(zmq_bind(socket, endpoint.as_ptr()), 0);
    let mut endpoint_buffer = [0u8; 64];
    let mut endpoint_size = endpoint_buffer.len();
    assert_eq!(
        zmq_getsockopt(
            socket,
            ZMQ_LAST_ENDPOINT,
            endpoint_buffer.as_mut_ptr().cast(),
            &mut endpoint_size
        ),
        0
    );
    assert_eq!(endpoint_size, endpoint.to_bytes_with_nul().len());
    assert_eq!(
        &endpoint_buffer[..endpoint_size],
        endpoint.to_bytes_with_nul()
    );
    endpoint_size = endpoint.to_bytes().len();
    assert_eq!(
        zmq_getsockopt(
            socket,
            ZMQ_LAST_ENDPOINT,
            endpoint_buffer.as_mut_ptr().cast(),
            &mut endpoint_size
        ),
        -1
    );
    assert_eq!(zmq_errno(), EINVAL);
    assert_eq!(
        zmq_setsockopt(
            socket,
            ZMQ_LAST_ENDPOINT,
            endpoint.as_ptr().cast(),
            endpoint.to_bytes().len()
        ),
        -1
    );
    assert_eq!(zmq_errno(), EINVAL);

    for (option, expected) in [
        (ZMQ_RATE, 100),
        (ZMQ_RECOVERY_IVL, 10000),
        (ZMQ_SNDBUF, -1),
        (ZMQ_RCVBUF, -1),
        (ZMQ_RECONNECT_IVL, 100),
        (ZMQ_RECONNECT_IVL_MAX, 0),
        (ZMQ_RECONNECT_STOP, 0),
        (ZMQ_BACKLOG, 100),
        (ZMQ_PRIORITY, 0),
        (ZMQ_IN_BATCH_SIZE, 8192),
        (ZMQ_OUT_BATCH_SIZE, 8192),
        (ZMQ_LOOPBACK_FASTPATH, 0),
        (ZMQ_MULTICAST_HOPS, 1),
        (ZMQ_MULTICAST_MAXTPDU, 1500),
        (ZMQ_MULTICAST_LOOP, 1),
        (ZMQ_TOS, 0),
        (ZMQ_CONNECT_TIMEOUT, 0),
        (ZMQ_TCP_MAXRT, 0),
        (ZMQ_TCP_KEEPALIVE, -1),
        (ZMQ_TCP_KEEPALIVE_CNT, -1),
        (ZMQ_TCP_KEEPALIVE_IDLE, -1),
        (ZMQ_TCP_KEEPALIVE_INTVL, -1),
        (ZMQ_HANDSHAKE_IVL, 30000),
        (ZMQ_HEARTBEAT_IVL, 0),
        (ZMQ_HEARTBEAT_TTL, 0),
        (ZMQ_HEARTBEAT_TIMEOUT, -1),
        (ZMQ_USE_FD, -1),
        (ZMQ_IPV6, 0),
        (ZMQ_IMMEDIATE, 0),
        (ZMQ_INVERT_MATCHING, 0),
    ] {
        value = 0;
        size = size_of::<c_int>();
        assert_eq!(
            zmq_getsockopt(socket, option, (&mut value as *mut c_int).cast(), &mut size),
            0
        );
        assert_eq!(value, expected, "default option {option}");
    }

    for (option, new_value, expected) in [
        (ZMQ_RATE, 200, 200),
        (ZMQ_RECOVERY_IVL, 12, 12),
        (ZMQ_SNDBUF, 1, 1),
        (ZMQ_RCVBUF, 2, 2),
        (ZMQ_RECONNECT_IVL, 33, 33),
        (ZMQ_RECONNECT_IVL_MAX, 44, 44),
        (ZMQ_RECONNECT_STOP, 7, 7),
        (ZMQ_BACKLOG, 55, 55),
        (ZMQ_PRIORITY, 3, 3),
        (ZMQ_IN_BATCH_SIZE, 4096, 4096),
        (ZMQ_OUT_BATCH_SIZE, 2048, 2048),
        (ZMQ_LOOPBACK_FASTPATH, 2, 1),
        (ZMQ_MULTICAST_HOPS, 2, 2),
        (ZMQ_MULTICAST_MAXTPDU, 1200, 1200),
        (ZMQ_MULTICAST_LOOP, 0, 0),
        (ZMQ_TOS, 16, 16),
        (ZMQ_CONNECT_TIMEOUT, 123, 123),
        (ZMQ_TCP_MAXRT, 456, 456),
        (ZMQ_TCP_KEEPALIVE, 1, 1),
        (ZMQ_TCP_KEEPALIVE_CNT, 3, 3),
        (ZMQ_TCP_KEEPALIVE_IDLE, 4, 4),
        (ZMQ_TCP_KEEPALIVE_INTVL, 5, 5),
        (ZMQ_HANDSHAKE_IVL, 1000, 1000),
        (ZMQ_HEARTBEAT_IVL, 10, 10),
        (ZMQ_HEARTBEAT_TTL, 1234, 1200),
        (ZMQ_HEARTBEAT_TIMEOUT, 20, 20),
        (ZMQ_USE_FD, 7, 7),
        (ZMQ_IPV6, 1, 1),
        (ZMQ_IMMEDIATE, 1, 1),
        (ZMQ_INVERT_MATCHING, 2, 1),
    ] {
        value = new_value;
        assert_eq!(
            zmq_setsockopt(
                socket,
                option,
                (&value as *const c_int).cast(),
                size_of::<c_int>()
            ),
            0,
            "set option {option}"
        );
        value = 0;
        size = size_of::<c_int>();
        assert_eq!(
            zmq_getsockopt(socket, option, (&mut value as *mut c_int).cast(), &mut size),
            0,
            "get option {option}"
        );
        assert_eq!(value, expected, "round-trip option {option}");
    }

    value = -5;
    assert_eq!(
        zmq_setsockopt(
            socket,
            ZMQ_BUSY_POLL,
            (&value as *const c_int).cast(),
            size_of::<c_int>()
        ),
        0
    );
    size = size_of::<c_int>();
    assert_eq!(
        zmq_getsockopt(
            socket,
            ZMQ_BUSY_POLL,
            (&mut value as *mut c_int).cast(),
            &mut size
        ),
        -1
    );
    assert_eq!(zmq_errno(), EINVAL);

    value = 0;
    size = size_of::<c_int>();
    assert_eq!(
        zmq_getsockopt(
            socket,
            ZMQ_NORM_MODE,
            (&mut value as *mut c_int).cast(),
            &mut size
        ),
        0
    );
    assert_eq!(value, ZMQ_NORM_CC);
    value = ZMQ_NORM_CCE;
    assert_eq!(
        zmq_setsockopt(
            socket,
            ZMQ_NORM_MODE,
            (&value as *const c_int).cast(),
            size_of::<c_int>()
        ),
        0
    );
    for (option, new_value, expected) in [
        (ZMQ_NORM_BUFFER_SIZE, 4096, 4096),
        (ZMQ_NORM_SEGMENT_SIZE, 1200, 1200),
        (ZMQ_NORM_BLOCK_SIZE, 64, 64),
        (ZMQ_NORM_NUM_PARITY, 8, 8),
        (ZMQ_NORM_NUM_AUTOPARITY, 2, 2),
        (ZMQ_NORM_UNICAST_NACK, 1, 1),
        (ZMQ_NORM_PUSH, 1, 1),
    ] {
        value = new_value;
        assert_eq!(
            zmq_setsockopt(
                socket,
                option,
                (&value as *const c_int).cast(),
                size_of::<c_int>()
            ),
            0
        );
        value = 0;
        size = size_of::<c_int>();
        assert_eq!(
            zmq_getsockopt(socket, option, (&mut value as *mut c_int).cast(), &mut size),
            0
        );
        assert_eq!(value, expected);
    }

    value = 5;
    assert_eq!(
        zmq_setsockopt(
            socket,
            ZMQ_NORM_MODE,
            (&value as *const c_int).cast(),
            size_of::<c_int>()
        ),
        -1
    );
    assert_eq!(zmq_errno(), EINVAL);

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

    for (option, invalid_value) in [
        (ZMQ_LINGER, -2),
        (ZMQ_RATE, 0),
        (ZMQ_PRIORITY, -1),
        (ZMQ_IN_BATCH_SIZE, 0),
        (ZMQ_OUT_BATCH_SIZE, 0),
        (ZMQ_MULTICAST_HOPS, 0),
        (ZMQ_TCP_KEEPALIVE, 2),
        (ZMQ_IPV6, 2),
        (ZMQ_IMMEDIATE, 2),
        (ZMQ_HANDSHAKE_IVL, -1),
        (ZMQ_HEARTBEAT_TTL, -1),
        (ZMQ_USE_FD, -2),
    ] {
        value = invalid_value;
        assert_eq!(
            zmq_setsockopt(
                socket,
                option,
                (&value as *const c_int).cast(),
                size_of::<c_int>()
            ),
            -1,
            "invalid option {option}"
        );
        assert_eq!(zmq_errno(), EINVAL);
    }

    assert_eq!(zmq_close(socket), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn string_socket_options_round_trip_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let socket = zmq_socket(ctx, ZMQ_PAIR);
    assert!(!socket.is_null());

    for option in [
        ZMQ_SOCKS_PROXY,
        ZMQ_SOCKS_USERNAME,
        ZMQ_SOCKS_PASSWORD,
        ZMQ_BINDTODEVICE,
    ] {
        let mut buffer = [0xFFu8; 32];
        let mut size = buffer.len();
        assert_eq!(
            zmq_getsockopt(socket, option, buffer.as_mut_ptr().cast(), &mut size),
            0,
            "default string option {option}"
        );
        assert_eq!(size, 1);
        assert_eq!(&buffer[..size], b"\0");
    }

    for (option, value) in [
        (ZMQ_SOCKS_PROXY, b"127.0.0.1:1080" as &[u8]),
        (ZMQ_SOCKS_USERNAME, b"alice"),
        (ZMQ_SOCKS_PASSWORD, b"secret"),
        (ZMQ_BINDTODEVICE, b"lo0"),
    ] {
        assert_eq!(
            zmq_setsockopt(socket, option, value.as_ptr().cast(), value.len()),
            0,
            "set string option {option}"
        );
        let mut buffer = [0u8; 32];
        let mut size = buffer.len();
        assert_eq!(
            zmq_getsockopt(socket, option, buffer.as_mut_ptr().cast(), &mut size),
            0,
            "get string option {option}"
        );
        assert_eq!(size, value.len() + 1);
        assert_eq!(&buffer[..value.len()], value);
        assert_eq!(buffer[value.len()], 0);

        size = value.len();
        assert_eq!(
            zmq_getsockopt(socket, option, buffer.as_mut_ptr().cast(), &mut size),
            -1,
            "small buffer string option {option}"
        );
        assert_eq!(zmq_errno(), EINVAL);

        assert_eq!(zmq_setsockopt(socket, option, ptr::null(), 0), 0);
        size = buffer.len();
        assert_eq!(
            zmq_getsockopt(socket, option, buffer.as_mut_ptr().cast(), &mut size),
            0
        );
        assert_eq!(size, 1);
        assert_eq!(&buffer[..size], b"\0");
    }

    assert_eq!(
        zmq_setsockopt(socket, ZMQ_SOCKS_USERNAME, ptr::null(), 1),
        -1
    );
    assert_eq!(zmq_errno(), EINVAL);

    assert_eq!(zmq_close(socket), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn draft_message_socket_options_set_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let socket = zmq_socket(ctx, ZMQ_DEALER);
    assert!(!socket.is_null());

    let value = b"draft-message";
    for option in [ZMQ_HELLO_MSG, ZMQ_DISCONNECT_MSG, ZMQ_HICCUP_MSG] {
        assert_eq!(
            zmq_setsockopt(socket, option, value.as_ptr().cast(), value.len()),
            0,
            "set draft message option {option}"
        );
        assert_eq!(
            zmq_setsockopt(socket, option, ptr::null(), 0),
            0,
            "clear draft message option {option}"
        );
        assert_eq!(
            zmq_setsockopt(socket, option, ptr::null(), 1),
            -1,
            "reject null non-empty draft message option {option}"
        );
        assert_eq!(zmq_errno(), EINVAL);

        let mut buffer = [0u8; 16];
        let mut size = buffer.len();
        assert_eq!(
            zmq_getsockopt(socket, option, buffer.as_mut_ptr().cast(), &mut size),
            -1,
            "draft message option {option} is not gettable"
        );
        assert_eq!(zmq_errno(), EINVAL);
    }

    assert_eq!(zmq_close(socket), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn xpub_xsub_draft_options_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let xpub = zmq_socket(ctx, ZMQ_XPUB);
    let xsub = zmq_socket(ctx, ZMQ_XSUB);
    let pair = zmq_socket(ctx, ZMQ_PAIR);
    assert!(!xpub.is_null());
    assert!(!xsub.is_null());
    assert!(!pair.is_null());

    let mut value = 1;
    assert_eq!(
        zmq_setsockopt(
            xpub,
            ZMQ_XPUB_MANUAL_LAST_VALUE,
            (&value as *const c_int).cast(),
            size_of::<c_int>()
        ),
        0
    );
    assert_eq!(
        zmq_setsockopt(
            xsub,
            ZMQ_XSUB_VERBOSE_UNSUBSCRIBE,
            (&value as *const c_int).cast(),
            size_of::<c_int>()
        ),
        0
    );
    value = -1;
    assert_eq!(
        zmq_setsockopt(
            xsub,
            ZMQ_XSUB_VERBOSE_UNSUBSCRIBE,
            (&value as *const c_int).cast(),
            size_of::<c_int>()
        ),
        0
    );
    assert_eq!(
        zmq_setsockopt(
            pair,
            ZMQ_XPUB_MANUAL_LAST_VALUE,
            (&value as *const c_int).cast(),
            size_of::<c_int>()
        ),
        -1
    );
    assert_eq!(zmq_errno(), EINVAL);

    let mut count = -1;
    let mut size = size_of::<c_int>();
    assert_eq!(
        zmq_getsockopt(
            xsub,
            ZMQ_TOPICS_COUNT,
            (&mut count as *mut c_int).cast(),
            &mut size
        ),
        0
    );
    assert_eq!(count, 0);
    assert_eq!(
        zmq_setsockopt(xsub, ZMQ_SUBSCRIBE, b"a".as_ptr().cast(), 1),
        0
    );
    assert_eq!(
        zmq_setsockopt(xsub, ZMQ_SUBSCRIBE, b"b".as_ptr().cast(), 1),
        0
    );
    assert_eq!(
        zmq_setsockopt(xsub, ZMQ_SUBSCRIBE, b"a".as_ptr().cast(), 1),
        0
    );
    count = -1;
    size = size_of::<c_int>();
    assert_eq!(
        zmq_getsockopt(
            xsub,
            ZMQ_TOPICS_COUNT,
            (&mut count as *mut c_int).cast(),
            &mut size
        ),
        0
    );
    assert_eq!(count, 2);
    assert_eq!(
        zmq_setsockopt(xsub, ZMQ_UNSUBSCRIBE, b"a".as_ptr().cast(), 1),
        0
    );
    count = -1;
    size = size_of::<c_int>();
    assert_eq!(
        zmq_getsockopt(
            xsub,
            ZMQ_TOPICS_COUNT,
            (&mut count as *mut c_int).cast(),
            &mut size
        ),
        0
    );
    assert_eq!(count, 1);

    assert_eq!(zmq_close(pair), 0);
    assert_eq!(zmq_close(xsub), 0);
    assert_eq!(zmq_close(xpub), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn security_options_round_trip_over_c_abi() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    let socket = zmq_socket(ctx, ZMQ_REQ);
    assert!(!socket.is_null());

    let mut value = 0;
    let mut size = size_of::<c_int>();
    assert_eq!(
        zmq_getsockopt(
            socket,
            ZMQ_MECHANISM,
            (&mut value as *mut c_int).cast(),
            &mut size
        ),
        0
    );
    assert_eq!(value, ZMQ_NULL);

    value = 1;
    assert_eq!(
        zmq_setsockopt(
            socket,
            ZMQ_PLAIN_SERVER,
            (&value as *const c_int).cast(),
            size_of::<c_int>()
        ),
        0
    );
    assert_eq!(
        zmq_setsockopt(socket, ZMQ_PLAIN_USERNAME, b"user".as_ptr().cast(), 4),
        0
    );
    assert_eq!(
        zmq_setsockopt(socket, ZMQ_PLAIN_PASSWORD, b"pass".as_ptr().cast(), 4),
        0
    );
    assert_eq!(
        zmq_setsockopt(socket, ZMQ_ZAP_DOMAIN, b"domain".as_ptr().cast(), 6),
        0
    );

    value = 0;
    size = size_of::<c_int>();
    assert_eq!(
        zmq_getsockopt(
            socket,
            ZMQ_MECHANISM,
            (&mut value as *mut c_int).cast(),
            &mut size
        ),
        0
    );
    assert_eq!(value, ZMQ_PLAIN);
    let mut bytes = [0u8; 16];
    let mut byte_size = bytes.len();
    assert_eq!(
        zmq_getsockopt(
            socket,
            ZMQ_PLAIN_USERNAME,
            bytes.as_mut_ptr().cast(),
            &mut byte_size
        ),
        0
    );
    assert_eq!(&bytes[..byte_size], b"user");

    assert_eq!(zmq_close(socket), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn z85_and_curve_helpers_work_over_c_abi() {
    let data = [0x86, 0x4F, 0xD2, 0x6F, 0xB5, 0x59, 0xF7, 0x5B];
    let mut encoded = [0 as c_char; 11];
    assert_eq!(
        zmq_z85_encode(encoded.as_mut_ptr(), data.as_ptr(), data.len()),
        encoded.as_mut_ptr()
    );
    let encoded_text = unsafe { CStr::from_ptr(encoded.as_ptr()) };
    assert_eq!(encoded_text.to_bytes(), b"HelloWorld");
    let mut decoded = [0u8; 8];
    assert_eq!(
        zmq_z85_decode(decoded.as_mut_ptr(), encoded.as_ptr()),
        decoded.as_mut_ptr()
    );
    assert_eq!(decoded, data);

    let mut public = [0 as c_char; 41];
    let mut secret = [0 as c_char; 41];
    assert_eq!(
        zmq_curve_keypair(public.as_mut_ptr(), secret.as_mut_ptr()),
        0
    );
    assert_eq!(
        unsafe { CStr::from_ptr(public.as_ptr()) }.to_bytes().len(),
        40
    );
    assert_eq!(
        unsafe { CStr::from_ptr(secret.as_ptr()) }.to_bytes().len(),
        40
    );
    let mut derived = [0 as c_char; 41];
    assert_eq!(zmq_curve_public(derived.as_mut_ptr(), secret.as_ptr()), 0);
    assert_eq!(
        unsafe { CStr::from_ptr(derived.as_ptr()) }.to_bytes(),
        unsafe { CStr::from_ptr(public.as_ptr()) }.to_bytes()
    );
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
