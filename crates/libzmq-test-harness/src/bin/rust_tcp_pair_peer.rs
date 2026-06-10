use std::env;
use std::ffi::CString;

const ZMQ_PAIR: i32 = 0;
const ZMQ_CURVE_SERVER: i32 = 47;
const ZMQ_CURVE_PUBLICKEY: i32 = 48;
const ZMQ_CURVE_SECRETKEY: i32 = 49;
const ZMQ_CURVE_SERVERKEY: i32 = 50;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 4 && args.len() != 9 {
        eprintln!("usage: rust_tcp_pair_peer <server|client> <endpoint> <payload> [curve <server_public> <server_secret> <client_public> <client_secret>]");
        std::process::exit(2);
    }
    let mode = &args[1];
    let endpoint = CString::new(args[2].as_str()).unwrap();
    let payload = args[3].as_bytes();
    let ctx = zmq::zmq_ctx_new();
    let socket = zmq::zmq_socket(ctx, ZMQ_PAIR);
    if args.len() == 9 {
        configure_curve(socket, mode, &args[4..9]);
    }
    if mode == "server" {
        assert_eq!(zmq::zmq_bind(socket, endpoint.as_ptr()), 0);
        let mut buffer = [0u8; 64];
        let mut rc = -1;
        for _ in 0..50 {
            rc = zmq::zmq_recv(socket, buffer.as_mut_ptr().cast(), buffer.len(), 0);
            if rc >= 0 {
                break;
            }
            if zmq::zmq_errno() != 11 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        if rc < 0 {
            eprintln!("rust recv failed errno {}", zmq::zmq_errno());
            std::process::exit(1);
        }
        println!("{}", String::from_utf8_lossy(&buffer[..rc as usize]));
    } else {
        assert_eq!(zmq::zmq_connect(socket, endpoint.as_ptr()), 0);
        let rc = zmq::zmq_send(socket, payload.as_ptr().cast(), payload.len(), 0);
        if rc != payload.len() as i32 {
            eprintln!("rust send failed rc {rc} errno {}", zmq::zmq_errno());
            std::process::exit(1);
        }
        std::thread::sleep(std::time::Duration::from_secs(2));
    }
    zmq::zmq_close(socket);
    zmq::zmq_ctx_term(ctx);
}

fn configure_curve(socket: *mut std::ffi::c_void, mode: &str, args: &[String]) {
    assert_eq!(args[0], "curve");
    let server_public = args[1].as_bytes();
    let server_secret = args[2].as_bytes();
    let client_public = args[3].as_bytes();
    let client_secret = args[4].as_bytes();
    if mode == "server" {
        let enabled = 1i32;
        assert_eq!(
            zmq::zmq_setsockopt(
                socket,
                ZMQ_CURVE_SERVER,
                (&enabled as *const i32).cast(),
                std::mem::size_of_val(&enabled),
            ),
            0
        );
        set_bytes(socket, ZMQ_CURVE_PUBLICKEY, client_public);
        set_bytes(socket, ZMQ_CURVE_SECRETKEY, server_secret);
    } else {
        set_bytes(socket, ZMQ_CURVE_SERVERKEY, server_public);
        set_bytes(socket, ZMQ_CURVE_PUBLICKEY, client_public);
        set_bytes(socket, ZMQ_CURVE_SECRETKEY, client_secret);
    }
}

fn set_bytes(socket: *mut std::ffi::c_void, option: i32, value: &[u8]) {
    assert_eq!(
        zmq::zmq_setsockopt(socket, option, value.as_ptr().cast(), value.len()),
        0
    );
}
