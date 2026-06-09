use std::ffi::c_int;

use zmq::*;

const ZMQ_PAIR_T: c_int = 0;
const ZMQ_PULL_T: c_int = 7;
const ZMQ_PUSH_T: c_int = 8;
const ZMQ_DONTWAIT_T: c_int = 1;

fn main() {
    println!("{{\"case\":\"stable_pattern_oracle\"}}");
    run_pair();
    run_push_pull();
}

fn run_pair() {
    let ctx = zmq_ctx_new();
    let server = zmq_socket(ctx, ZMQ_PAIR_T);
    let client = zmq_socket(ctx, ZMQ_PAIR_T);
    let endpoint = c"inproc://oracle_pair";
    let bind_rc = zmq_bind(server, endpoint.as_ptr());
    let connect_rc = zmq_connect(client, endpoint.as_ptr());
    let send_rc = zmq_send(client, b"hello".as_ptr().cast(), 5, 0);
    let mut buffer = [0u8; 16];
    let recv_rc = zmq_recv(
        server,
        buffer.as_mut_ptr().cast(),
        buffer.len(),
        ZMQ_DONTWAIT_T,
    );
    println!("{{\"observation\":{{\"type\":\"pair\",\"bind\":{bind_rc},\"connect\":{connect_rc},\"send\":{send_rc},\"recv\":{recv_rc},\"data\":\"{}\"}}}}", String::from_utf8_lossy(&buffer[..recv_rc.max(0) as usize]));
    zmq_close(client);
    zmq_close(server);
    zmq_ctx_term(ctx);
}

fn run_push_pull() {
    let ctx = zmq_ctx_new();
    let pull = zmq_socket(ctx, ZMQ_PULL_T);
    let push = zmq_socket(ctx, ZMQ_PUSH_T);
    let endpoint = c"inproc://oracle_push_pull";
    let bind_rc = zmq_bind(pull, endpoint.as_ptr());
    let connect_rc = zmq_connect(push, endpoint.as_ptr());
    let send_rc = zmq_send(push, b"job".as_ptr().cast(), 3, 0);
    let mut buffer = [0u8; 16];
    let recv_rc = zmq_recv(
        pull,
        buffer.as_mut_ptr().cast(),
        buffer.len(),
        ZMQ_DONTWAIT_T,
    );
    println!("{{\"observation\":{{\"type\":\"push_pull\",\"bind\":{bind_rc},\"connect\":{connect_rc},\"send\":{send_rc},\"recv\":{recv_rc},\"data\":\"{}\"}}}}", String::from_utf8_lossy(&buffer[..recv_rc.max(0) as usize]));
    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}
