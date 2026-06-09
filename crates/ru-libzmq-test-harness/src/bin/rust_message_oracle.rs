use zmq::{zmq_msg_close, zmq_msg_init_size, zmq_msg_size, zmq_msg_t, zmq_version};

fn main() {
    let mut major = 0;
    let mut minor = 0;
    let mut patch = 0;
    zmq_version(&mut major, &mut minor, &mut patch);

    let mut msg = zmq_msg_t::default();
    let init_rc = zmq_msg_init_size(&mut msg, 16);
    let size = zmq_msg_size(&msg);
    let close_rc = zmq_msg_close(&mut msg);

    println!("{{\"case\":\"message_oracle\"}}");
    println!(
        "{{\"observation\":{{\"type\":\"version\",\"major\":{major},\"minor\":{minor},\"patch\":{patch}}}}}"
    );
    println!("{{\"observation\":{{\"type\":\"return_code\",\"op\":\"zmq_msg_init_size\",\"rc\":{init_rc}}}}}");
    println!("{{\"observation\":{{\"type\":\"message_size\",\"size\":{size}}}}}");
    println!("{{\"observation\":{{\"type\":\"return_code\",\"op\":\"zmq_msg_close\",\"rc\":{close_rc}}}}}");
}
