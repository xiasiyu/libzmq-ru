use ru_libzmq::{
    version, Context, Error, Message, SocketType, ZMQ_CONFLATE, ZMQ_IO_THREADS, ZMQ_LINGER,
    ZMQ_MAX_SOCKETS, ZMQ_RCVHWM, ZMQ_RCVMORE, ZMQ_SNDHWM, ZMQ_SNDMORE, ZMQ_TYPE,
};

#[test]
fn native_version_matches_c_abi_contract() {
    assert_eq!(version(), (4, 3, 6));
}

#[test]
fn native_context_creates_all_socket_types() {
    let ctx = Context::new().unwrap();
    let socket_types = [
        SocketType::Pair,
        SocketType::Pub,
        SocketType::Sub,
        SocketType::Req,
        SocketType::Rep,
        SocketType::Dealer,
        SocketType::Router,
        SocketType::Pull,
        SocketType::Push,
        SocketType::Xpub,
        SocketType::Xsub,
        SocketType::Stream,
        SocketType::Server,
        SocketType::Client,
        SocketType::Radio,
        SocketType::Dish,
        SocketType::Gather,
        SocketType::Scatter,
        SocketType::Dgram,
        SocketType::Peer,
        SocketType::Channel,
    ];

    for socket_type in socket_types {
        let socket = ctx.socket(socket_type).unwrap();
        assert_eq!(socket.socket_type(), socket_type);
    }
}

#[test]
fn native_message_constructors_preserve_payload() {
    let empty = Message::new();
    assert!(empty.is_empty());

    let from_str = Message::from("hello");
    assert_eq!(from_str.data(), b"hello");

    let from_vec = Message::from(vec![1, 2, 3]);
    assert_eq!(from_vec.data(), &[1, 2, 3]);
}

#[test]
fn native_message_routing_id_and_group_are_supported() {
    let mut message = Message::from("payload");

    message.set_routing_id(7);
    message.set_group("updates").unwrap();
    message.set_metadata("User-Id", "alice").unwrap();

    assert_eq!(message.routing_id(), 7);
    assert_eq!(message.group(), Some("updates"));
    assert_eq!(message.metadata("User-Id"), Some("alice"));
}

#[test]
fn native_unimplemented_socket_operations_are_explicit() {
    let ctx = Context::new().unwrap();
    let socket = ctx.socket(SocketType::Pair).unwrap();

    assert_eq!(socket.bind("tcp://127.0.0.1:1"), Err(Error::NotSupported));
    assert_eq!(
        socket.connect("tcp://127.0.0.1:1"),
        Err(Error::NotSupported)
    );
    assert_eq!(socket.send("hello"), Err(Error::Again));
    assert_eq!(socket.recv(), Err(Error::Again));
}

#[test]
fn native_pair_inproc_round_trip() {
    let ctx = Context::new().unwrap();
    let server = ctx.socket(SocketType::Pair).unwrap();
    let client = ctx.socket(SocketType::Pair).unwrap();

    server.bind("inproc://native_pair").unwrap();
    client.connect("inproc://native_pair").unwrap();

    assert_eq!(client.send("hello").unwrap(), 5);
    let received = server.recv().unwrap();
    assert_eq!(received.data(), b"hello");

    assert_eq!(server.send("world").unwrap(), 5);
    let received = client.recv().unwrap();
    assert_eq!(received.data(), b"world");
}

#[test]
fn native_pair_inproc_supports_pending_connect() {
    let ctx = Context::new().unwrap();
    let client = ctx.socket(SocketType::Pair).unwrap();
    let server = ctx.socket(SocketType::Pair).unwrap();

    client.connect("inproc://native_pending_pair").unwrap();
    assert_eq!(client.send("early"), Err(Error::Again));

    server.bind("inproc://native_pending_pair").unwrap();

    assert_eq!(client.send("ready").unwrap(), 5);
    let received = server.recv().unwrap();
    assert_eq!(received.data(), b"ready");
}

#[test]
fn native_pair_inproc_enforces_send_hwm() {
    let ctx = Context::new().unwrap();
    let server = ctx.socket(SocketType::Pair).unwrap();
    let client = ctx.socket(SocketType::Pair).unwrap();

    client.set_option_i32(ZMQ_SNDHWM, 1).unwrap();
    server.bind("inproc://native_hwm_pair").unwrap();
    client.connect("inproc://native_hwm_pair").unwrap();

    assert_eq!(client.send("one").unwrap(), 3);
    assert_eq!(client.send("two"), Err(Error::Again));
    let received = server.recv().unwrap();
    assert_eq!(received.data(), b"one");
}

#[test]
fn native_pair_inproc_conflate_keeps_latest_message() {
    let ctx = Context::new().unwrap();
    let server = ctx.socket(SocketType::Pair).unwrap();
    let client = ctx.socket(SocketType::Pair).unwrap();

    client.set_option_i32(ZMQ_SNDHWM, 1).unwrap();
    client.set_option_i32(ZMQ_CONFLATE, 1).unwrap();
    server.bind("inproc://native_conflate_pair").unwrap();
    client.connect("inproc://native_conflate_pair").unwrap();

    assert_eq!(client.send("one").unwrap(), 3);
    assert_eq!(client.send("two").unwrap(), 3);
    let received = server.recv().unwrap();
    assert_eq!(received.data(), b"two");
    assert_eq!(server.recv(), Err(Error::Again));
}

#[test]
fn native_pair_inproc_disconnect_removes_peer() {
    let ctx = Context::new().unwrap();
    let server = ctx.socket(SocketType::Pair).unwrap();
    let client = ctx.socket(SocketType::Pair).unwrap();

    server.bind("inproc://native_disconnect_pair").unwrap();
    client.connect("inproc://native_disconnect_pair").unwrap();
    client
        .disconnect("inproc://native_disconnect_pair")
        .unwrap();

    assert_eq!(client.send("late"), Err(Error::Again));
    assert_eq!(server.send("late"), Err(Error::Again));
}

#[test]
fn native_pair_inproc_preserves_multipart_more_state() {
    let ctx = Context::new().unwrap();
    let server = ctx.socket(SocketType::Pair).unwrap();
    let client = ctx.socket(SocketType::Pair).unwrap();

    server.bind("inproc://native_multipart_pair").unwrap();
    client.connect("inproc://native_multipart_pair").unwrap();

    assert_eq!(client.send_with_flags("part1", ZMQ_SNDMORE).unwrap(), 5);
    assert_eq!(client.send("part2").unwrap(), 5);

    let received = server.recv().unwrap();
    assert_eq!(received.data(), b"part1");
    assert!(received.more());
    assert_eq!(server.get_option_i32(ZMQ_RCVMORE).unwrap(), 1);

    let received = server.recv().unwrap();
    assert_eq!(received.data(), b"part2");
    assert!(!received.more());
    assert_eq!(server.get_option_i32(ZMQ_RCVMORE).unwrap(), 0);
}

#[test]
fn native_context_termination_blocks_later_socket_creation() {
    let ctx = Context::new().unwrap();
    ctx.terminate().unwrap();

    assert_eq!(
        ctx.socket(SocketType::Pair).map(|_| ()),
        Err(Error::Terminated)
    );
}

#[test]
fn native_context_options_round_trip() {
    let ctx = Context::new().unwrap();

    assert_eq!(ctx.get_option_i32(ZMQ_IO_THREADS).unwrap(), 1);
    assert_eq!(ctx.get_option_i32(ZMQ_MAX_SOCKETS).unwrap(), 1023);
    ctx.set_option_i32(ZMQ_IO_THREADS, 2).unwrap();
    ctx.set_option_i32(ZMQ_MAX_SOCKETS, 2048).unwrap();
    assert_eq!(ctx.get_option_i32(ZMQ_IO_THREADS).unwrap(), 2);
    assert_eq!(ctx.get_option_i32(ZMQ_MAX_SOCKETS).unwrap(), 2048);
}

#[test]
fn native_socket_options_round_trip() {
    let ctx = Context::new().unwrap();
    let socket = ctx.socket(SocketType::Req).unwrap();

    assert_eq!(
        socket.get_option_i32(ZMQ_TYPE).unwrap(),
        SocketType::Req as i32
    );
    assert_eq!(socket.get_option_i32(ZMQ_LINGER).unwrap(), -1);
    assert_eq!(socket.get_option_i32(ZMQ_SNDHWM).unwrap(), 1000);
    assert_eq!(socket.get_option_i32(ZMQ_RCVHWM).unwrap(), 1000);
    socket.set_option_i32(ZMQ_LINGER, 0).unwrap();
    socket.set_option_i32(ZMQ_SNDHWM, 10).unwrap();
    socket.set_option_i32(ZMQ_RCVHWM, 11).unwrap();
    assert_eq!(socket.get_option_i32(ZMQ_LINGER).unwrap(), 0);
    assert_eq!(socket.get_option_i32(ZMQ_SNDHWM).unwrap(), 10);
    assert_eq!(socket.get_option_i32(ZMQ_RCVHWM).unwrap(), 11);
}
