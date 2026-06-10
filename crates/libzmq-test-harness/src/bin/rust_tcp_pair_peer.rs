use std::env;
use std::ffi::CString;

const ZMQ_PAIR: i32 = 0;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 4 {
        eprintln!("usage: rust_tcp_pair_peer <server|client> <endpoint> <payload>");
        std::process::exit(2);
    }
    let mode = &args[1];
    let endpoint = CString::new(args[2].as_str()).unwrap();
    let payload = args[3].as_bytes();
    let ctx = zmq::zmq_ctx_new();
    let socket = zmq::zmq_socket(ctx, ZMQ_PAIR);
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
