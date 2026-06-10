use libzmq::{
    version, Context, Error, Message, SocketType, ZMQ_CONFLATE, ZMQ_IO_THREADS, ZMQ_LINGER,
    ZMQ_MAX_SOCKETS, ZMQ_RCVHWM, ZMQ_RCVMORE, ZMQ_REQ_RELAXED, ZMQ_ROUTER_HANDOVER,
    ZMQ_ROUTER_MANDATORY, ZMQ_SNDHWM, ZMQ_SNDMORE, ZMQ_TYPE, ZMQ_XPUB_MANUAL, ZMQ_XPUB_NODROP,
    ZMQ_XPUB_VERBOSE, ZMQ_XPUB_WELCOME_MSG,
};
use std::io::{Read, Write};
use std::net::TcpListener;

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

    assert_eq!(socket.bind("udp://127.0.0.1:1"), Err(Error::NotSupported));
    assert_eq!(
        socket.connect("udp://127.0.0.1:1"),
        Err(Error::NotSupported)
    );
    assert_eq!(socket.send("hello"), Err(Error::Again));
    assert_eq!(socket.recv(), Err(Error::Again));
}

#[test]
fn native_pair_tcp_round_trip() {
    let port = unused_tcp_port();
    let endpoint = format!("tcp://127.0.0.1:{port}");
    let ctx = Context::new().unwrap();
    let server = ctx.socket(SocketType::Pair).unwrap();
    let client = ctx.socket(SocketType::Pair).unwrap();

    server.bind(&endpoint).unwrap();
    client.connect(&endpoint).unwrap();

    assert_eq!(client.send("hello").unwrap(), 5);
    let received = server.recv().unwrap();
    assert_eq!(received.data(), b"hello");

    assert_eq!(server.send("world").unwrap(), 5);
    let received = client.recv().unwrap();
    assert_eq!(received.data(), b"world");
}

#[test]
fn native_pair_tcp_connect_before_bind_reconnects() {
    let port = unused_tcp_port();
    let endpoint = format!("tcp://127.0.0.1:{port}");
    let ctx = Context::new().unwrap();
    let server = ctx.socket(SocketType::Pair).unwrap();
    let client = ctx.socket(SocketType::Pair).unwrap();

    client.connect(&endpoint).unwrap();
    server.bind(&endpoint).unwrap();

    let mut sent = false;
    for _ in 0..20 {
        match client.send("hello") {
            Ok(5) => {
                sent = true;
                break;
            }
            Err(Error::Again) => std::thread::sleep(std::time::Duration::from_millis(25)),
            other => panic!("unexpected send result: {other:?}"),
        }
    }
    assert!(sent, "client did not reconnect before retry deadline");
    let received = server.recv().unwrap();
    assert_eq!(received.data(), b"hello");
}

#[test]
fn native_push_pull_tcp_round_trip() {
    let port = unused_tcp_port();
    let endpoint = format!("tcp://127.0.0.1:{port}");
    let ctx = Context::new().unwrap();
    let pull = ctx.socket(SocketType::Pull).unwrap();
    let push = ctx.socket(SocketType::Push).unwrap();

    pull.bind(&endpoint).unwrap();
    push.connect(&endpoint).unwrap();

    assert_eq!(push.send("job").unwrap(), 3);
    let received = pull.recv().unwrap();
    assert_eq!(received.data(), b"job");
}

#[test]
fn native_req_rep_tcp_round_trip() {
    let port = unused_tcp_port();
    let endpoint = format!("tcp://127.0.0.1:{port}");
    let ctx = Context::new().unwrap();
    let rep = ctx.socket(SocketType::Rep).unwrap();
    let req = ctx.socket(SocketType::Req).unwrap();

    rep.bind(&endpoint).unwrap();
    req.connect(&endpoint).unwrap();

    assert_eq!(req.send("question").unwrap(), 8);
    let received = rep.recv().unwrap();
    assert_eq!(received.data(), b"question");
    assert_eq!(rep.send("answer").unwrap(), 6);
    let received = req.recv().unwrap();
    assert_eq!(received.data(), b"answer");
}

#[test]
fn native_stream_tcp_uses_raw_bytes() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("tcp://{}", listener.local_addr().unwrap());
    let peer = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut buffer = [0u8; 3];
        stream.read_exact(&mut buffer).unwrap();
        assert_eq!(&buffer, b"raw");
        stream.write_all(b"ack").unwrap();
    });

    let ctx = Context::new().unwrap();
    let stream = ctx.socket(SocketType::Stream).unwrap();
    stream.connect(&endpoint).unwrap();
    assert_eq!(stream.send("raw").unwrap(), 3);
    let received = stream.recv().unwrap();
    assert_eq!(received.data(), b"ack");
    peer.join().unwrap();
}

#[cfg(unix)]
#[test]
fn native_pair_ipc_round_trip() {
    let path = std::env::temp_dir().join(format!(
        "libzmq-native-ipc-{}-round-trip.sock",
        std::process::id()
    ));
    let endpoint = format!("ipc://{}", path.display());
    let ctx = Context::new().unwrap();
    let server = ctx.socket(SocketType::Pair).unwrap();
    let client = ctx.socket(SocketType::Pair).unwrap();

    server.bind(&endpoint).unwrap();
    client.connect(&endpoint).unwrap();

    assert_eq!(client.send("hello").unwrap(), 5);
    let received = server.recv().unwrap();
    assert_eq!(received.data(), b"hello");

    assert_eq!(server.send("world").unwrap(), 5);
    let received = client.recv().unwrap();
    assert_eq!(received.data(), b"world");
    let _ = std::fs::remove_file(path);
}

fn unused_tcp_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
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
fn native_push_pull_inproc_round_trip() {
    let ctx = Context::new().unwrap();
    let pull = ctx.socket(SocketType::Pull).unwrap();
    let push = ctx.socket(SocketType::Push).unwrap();

    pull.bind("inproc://native_push_pull").unwrap();
    push.connect("inproc://native_push_pull").unwrap();

    assert_eq!(push.send("job").unwrap(), 3);
    let received = pull.recv().unwrap();
    assert_eq!(received.data(), b"job");
}

#[test]
fn native_push_pull_inproc_allows_push_bind() {
    let ctx = Context::new().unwrap();
    let push = ctx.socket(SocketType::Push).unwrap();
    let pull = ctx.socket(SocketType::Pull).unwrap();

    push.bind("inproc://native_push_bound").unwrap();
    pull.connect("inproc://native_push_bound").unwrap();

    assert_eq!(push.send("job").unwrap(), 3);
    let received = pull.recv().unwrap();
    assert_eq!(received.data(), b"job");
}

#[test]
fn native_push_inproc_load_balances_between_pulls() {
    let ctx = Context::new().unwrap();
    let push = ctx.socket(SocketType::Push).unwrap();
    let pull_a = ctx.socket(SocketType::Pull).unwrap();
    let pull_b = ctx.socket(SocketType::Pull).unwrap();

    push.bind("inproc://native_push_lb").unwrap();
    pull_a.connect("inproc://native_push_lb").unwrap();
    pull_b.connect("inproc://native_push_lb").unwrap();

    assert_eq!(push.send("one").unwrap(), 3);
    assert_eq!(push.send("two").unwrap(), 3);

    let received = pull_a.recv().unwrap();
    assert_eq!(received.data(), b"one");
    let received = pull_b.recv().unwrap();
    assert_eq!(received.data(), b"two");
}

#[test]
fn native_push_pull_reject_wrong_direction_operations() {
    let ctx = Context::new().unwrap();
    let push = ctx.socket(SocketType::Push).unwrap();
    let pull = ctx.socket(SocketType::Pull).unwrap();

    assert_eq!(pull.send("bad"), Err(Error::NotSupported));
    assert_eq!(push.recv(), Err(Error::NotSupported));
}

#[test]
fn native_dealer_router_inproc_round_trip_sets_routing_id() {
    let ctx = Context::new().unwrap();
    let router = ctx.socket(SocketType::Router).unwrap();
    let dealer = ctx.socket(SocketType::Dealer).unwrap();

    router.bind("inproc://native_dealer_router").unwrap();
    dealer.connect("inproc://native_dealer_router").unwrap();

    assert_eq!(dealer.send("request").unwrap(), 7);
    let received = router.recv().unwrap();
    assert_eq!(received.data(), b"request");
    assert_ne!(received.routing_id(), 0);

    assert_eq!(router.send("reply").unwrap(), 5);
    let received = dealer.recv().unwrap();
    assert_eq!(received.data(), b"reply");
}

#[test]
fn native_router_inproc_routes_by_routing_id() {
    let ctx = Context::new().unwrap();
    let router = ctx.socket(SocketType::Router).unwrap();
    let dealer_a = ctx.socket(SocketType::Dealer).unwrap();
    let dealer_b = ctx.socket(SocketType::Dealer).unwrap();

    router.bind("inproc://native_router_target").unwrap();
    dealer_a.connect("inproc://native_router_target").unwrap();
    dealer_b.connect("inproc://native_router_target").unwrap();

    assert_eq!(dealer_a.send("hello").unwrap(), 5);
    let received = router.recv().unwrap();
    let routing_id = received.routing_id();
    assert_ne!(routing_id, 0);

    let mut reply = Message::from("targeted");
    reply.set_routing_id(routing_id);
    assert_eq!(router.send(reply).unwrap(), 8);

    let received = dealer_a.recv().unwrap();
    assert_eq!(received.data(), b"targeted");
    assert_eq!(dealer_b.recv(), Err(Error::Again));
}

#[test]
fn native_router_mandatory_reports_unroutable_peer() {
    let ctx = Context::new().unwrap();
    let router = ctx.socket(SocketType::Router).unwrap();
    router.set_option_i32(ZMQ_ROUTER_MANDATORY, 1).unwrap();
    router.bind("inproc://native_router_mandatory").unwrap();

    let mut message = Message::from("lost");
    message.set_routing_id(999);

    assert_eq!(router.send(message), Err(Error::HostUnreachable));
}

#[test]
fn native_req_rep_inproc_enforces_strict_fsm() {
    let ctx = Context::new().unwrap();
    let rep = ctx.socket(SocketType::Rep).unwrap();
    let req = ctx.socket(SocketType::Req).unwrap();

    rep.bind("inproc://native_req_rep").unwrap();
    req.connect("inproc://native_req_rep").unwrap();

    assert_eq!(req.recv(), Err(Error::InvalidState));
    assert_eq!(req.send("request").unwrap(), 7);
    assert_eq!(req.send("again"), Err(Error::InvalidState));

    let received = rep.recv().unwrap();
    assert_eq!(received.data(), b"request");
    assert_eq!(rep.recv(), Err(Error::InvalidState));
    assert_eq!(rep.send("reply").unwrap(), 5);

    let received = req.recv().unwrap();
    assert_eq!(received.data(), b"reply");
}

#[test]
fn native_rep_traceback_replies_to_request_origin() {
    let ctx = Context::new().unwrap();
    let rep = ctx.socket(SocketType::Rep).unwrap();
    let req_a = ctx.socket(SocketType::Req).unwrap();
    let req_b = ctx.socket(SocketType::Req).unwrap();

    rep.bind("inproc://native_rep_traceback").unwrap();
    req_a.connect("inproc://native_rep_traceback").unwrap();
    req_b.connect("inproc://native_rep_traceback").unwrap();

    assert_eq!(req_a.send("a").unwrap(), 1);
    assert_eq!(req_b.send("b").unwrap(), 1);

    let received = rep.recv().unwrap();
    assert_eq!(received.data(), b"a");
    assert_eq!(rep.send("reply-a").unwrap(), 7);
    let reply = req_a.recv().unwrap();
    assert_eq!(reply.data(), b"reply-a");
    assert_eq!(req_b.recv(), Err(Error::Again));

    let received = rep.recv().unwrap();
    assert_eq!(received.data(), b"b");
    assert_eq!(rep.send("reply-b").unwrap(), 7);
    let reply = req_b.recv().unwrap();
    assert_eq!(reply.data(), b"reply-b");
}

#[test]
fn native_req_relaxed_allows_replacing_pending_request() {
    let ctx = Context::new().unwrap();
    let rep = ctx.socket(SocketType::Rep).unwrap();
    let req = ctx.socket(SocketType::Req).unwrap();

    req.set_option_i32(ZMQ_REQ_RELAXED, 1).unwrap();
    rep.bind("inproc://native_req_relaxed").unwrap();
    req.connect("inproc://native_req_relaxed").unwrap();

    assert_eq!(req.send("one").unwrap(), 3);
    assert_eq!(req.send("two").unwrap(), 3);
}

#[test]
fn native_pub_sub_inproc_filters_by_subscription() {
    let ctx = Context::new().unwrap();
    let publisher = ctx.socket(SocketType::Pub).unwrap();
    let subscriber = ctx.socket(SocketType::Sub).unwrap();

    subscriber.subscribe(b"topic").unwrap();
    publisher.bind("inproc://native_pub_sub").unwrap();
    subscriber.connect("inproc://native_pub_sub").unwrap();

    assert_eq!(publisher.send("other:drop").unwrap(), 10);
    assert_eq!(subscriber.recv(), Err(Error::Again));
    assert_eq!(publisher.send("topic:keep").unwrap(), 10);
    let received = subscriber.recv().unwrap();
    assert_eq!(received.data(), b"topic:keep");
}

#[test]
fn native_pub_inproc_distributes_to_all_matching_subscribers() {
    let ctx = Context::new().unwrap();
    let publisher = ctx.socket(SocketType::Pub).unwrap();
    let subscriber_a = ctx.socket(SocketType::Sub).unwrap();
    let subscriber_b = ctx.socket(SocketType::Sub).unwrap();

    subscriber_a.subscribe(b"topic").unwrap();
    subscriber_b.subscribe(b"topic").unwrap();
    publisher.bind("inproc://native_pub_dist").unwrap();
    subscriber_a.connect("inproc://native_pub_dist").unwrap();
    subscriber_b.connect("inproc://native_pub_dist").unwrap();

    assert_eq!(publisher.send("topic:all").unwrap(), 9);
    let received = subscriber_a.recv().unwrap();
    assert_eq!(received.data(), b"topic:all");
    let received = subscriber_b.recv().unwrap();
    assert_eq!(received.data(), b"topic:all");
}

#[test]
fn native_xpub_xsub_inproc_filters_by_subscription() {
    let ctx = Context::new().unwrap();
    let publisher = ctx.socket(SocketType::Xpub).unwrap();
    let subscriber = ctx.socket(SocketType::Xsub).unwrap();

    subscriber.subscribe(b"x").unwrap();
    publisher.bind("inproc://native_xpub_xsub").unwrap();
    subscriber.connect("inproc://native_xpub_xsub").unwrap();

    assert_eq!(publisher.send("y-drop").unwrap(), 6);
    assert_eq!(subscriber.recv(), Err(Error::Again));
    assert_eq!(publisher.send("x-keep").unwrap(), 6);
    let received = subscriber.recv().unwrap();
    assert_eq!(received.data(), b"x-keep");
}

#[test]
fn native_xpub_replays_xsub_subscription_and_sends_welcome() {
    let ctx = Context::new().unwrap();
    let publisher = ctx.socket(SocketType::Xpub).unwrap();
    let subscriber = ctx.socket(SocketType::Xsub).unwrap();

    publisher
        .set_option_bytes(ZMQ_XPUB_WELCOME_MSG, b"welcome")
        .unwrap();
    subscriber.subscribe(b"topic").unwrap();
    publisher.bind("inproc://native_xpub_replay").unwrap();
    subscriber.connect("inproc://native_xpub_replay").unwrap();

    let subscription = publisher.recv().unwrap();
    assert_eq!(subscription.data(), b"\x01topic");
    let welcome = subscriber.recv().unwrap();
    assert_eq!(welcome.data(), b"welcome");
}

#[test]
fn native_stream_inproc_round_trip() {
    let ctx = Context::new().unwrap();
    let server = ctx.socket(SocketType::Stream).unwrap();
    let client = ctx.socket(SocketType::Stream).unwrap();

    server.bind("inproc://native_stream").unwrap();
    client.connect("inproc://native_stream").unwrap();

    assert_eq!(client.send("bytes").unwrap(), 5);
    let received = server.recv().unwrap();
    assert_eq!(received.data(), b"bytes");
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

    let router = ctx.socket(SocketType::Router).unwrap();
    router.set_option_i32(ZMQ_ROUTER_MANDATORY, 1).unwrap();
    router.set_option_i32(ZMQ_ROUTER_HANDOVER, 1).unwrap();
    assert_eq!(router.get_option_i32(ZMQ_ROUTER_MANDATORY).unwrap(), 1);
    assert_eq!(router.get_option_i32(ZMQ_ROUTER_HANDOVER).unwrap(), 1);

    let xpub = ctx.socket(SocketType::Xpub).unwrap();
    xpub.set_option_i32(ZMQ_XPUB_VERBOSE, 1).unwrap();
    xpub.set_option_i32(ZMQ_XPUB_MANUAL, 1).unwrap();
    xpub.set_option_i32(ZMQ_XPUB_NODROP, 1).unwrap();
    assert_eq!(xpub.get_option_i32(ZMQ_XPUB_VERBOSE).unwrap(), 1);
    assert_eq!(xpub.get_option_i32(ZMQ_XPUB_MANUAL).unwrap(), 1);
    assert_eq!(xpub.get_option_i32(ZMQ_XPUB_NODROP).unwrap(), 1);
}
