use libzmq::{
    curve_keypair, version, Context, Error, Message, SocketType, ZMQ_CONFLATE, ZMQ_CURVE,
    ZMQ_CURVE_PUBLICKEY, ZMQ_CURVE_SECRETKEY, ZMQ_CURVE_SERVER, ZMQ_CURVE_SERVERKEY, ZMQ_GSSAPI,
    ZMQ_GSSAPI_PRINCIPAL, ZMQ_GSSAPI_SERVER, ZMQ_GSSAPI_SERVICE_PRINCIPAL, ZMQ_IO_THREADS,
    ZMQ_LINGER, ZMQ_MAX_SOCKETS, ZMQ_MECHANISM, ZMQ_NORM_BLOCK_SIZE, ZMQ_NORM_BUFFER_SIZE,
    ZMQ_NORM_CC, ZMQ_NORM_CCE, ZMQ_NORM_MODE, ZMQ_NORM_NUM_AUTOPARITY, ZMQ_NORM_NUM_PARITY,
    ZMQ_NORM_PUSH, ZMQ_NORM_SEGMENT_SIZE, ZMQ_NORM_UNICAST_NACK, ZMQ_NULL, ZMQ_PLAIN,
    ZMQ_PLAIN_PASSWORD, ZMQ_PLAIN_SERVER, ZMQ_PLAIN_USERNAME, ZMQ_RCVHWM, ZMQ_RCVMORE,
    ZMQ_REQ_RELAXED, ZMQ_ROUTER_HANDOVER, ZMQ_ROUTER_MANDATORY, ZMQ_SNDHWM, ZMQ_SNDMORE, ZMQ_TYPE,
    ZMQ_XPUB_MANUAL, ZMQ_XPUB_NODROP, ZMQ_XPUB_VERBOSE, ZMQ_XPUB_WELCOME_MSG, ZMQ_ZAP_DOMAIN,
};
use std::io::{Read, Write};
use std::net::{TcpListener, UdpSocket};

fn skip_synthetic_gssapi_test() -> bool {
    cfg!(feature = "gssapi") && std::env::var_os("LIBZMQ_TEST_REAL_GSSAPI").is_none()
}

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
    for endpoint in [
        "pgm://127.0.0.1:1",
        "epgm://127.0.0.1:1",
        "norm://127.0.0.1:1",
        "tipc://{5560,0,0}",
        "vmci://1:1",
        "vsock://2:1",
    ] {
        assert_eq!(socket.bind(endpoint), Err(Error::NotSupported));
        assert_eq!(socket.connect(endpoint), Err(Error::NotSupported));
    }
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
    let received = recv_retry_native(&server).unwrap();
    assert_eq!(received.data(), b"hello");

    assert_eq!(server.send("world").unwrap(), 5);
    let received = client.recv().unwrap();
    assert_eq!(received.data(), b"world");
}

#[test]
fn native_pair_ws_round_trip() {
    let port = unused_tcp_port();
    let endpoint = format!("ws://127.0.0.1:{port}/zmq");
    let ctx = Context::new().unwrap();
    let server = ctx.socket(SocketType::Pair).unwrap();
    let client = ctx.socket(SocketType::Pair).unwrap();

    server.bind(&endpoint).unwrap();
    client.connect(&endpoint).unwrap();

    assert_eq!(client.send("hello").unwrap(), 5);
    let received = recv_retry_native(&server).unwrap();
    assert_eq!(received.data(), b"hello");

    assert_eq!(server.send("world").unwrap(), 5);
    let received = client.recv().unwrap();
    assert_eq!(received.data(), b"world");
}

#[test]
#[cfg(feature = "wss")]
fn native_pair_wss_round_trip() {
    let port = unused_tcp_port();
    let endpoint = format!("wss://127.0.0.1:{port}/zmq");
    let ctx = Context::new().unwrap();
    let server = ctx.socket(SocketType::Pair).unwrap();
    let client = ctx.socket(SocketType::Pair).unwrap();

    server.bind(&endpoint).unwrap();
    client.connect(&endpoint).unwrap();

    let client_thread = std::thread::spawn(move || {
        client.send("hello")?;
        client.recv().map(|message| message.data().to_vec())
    });
    let received = recv_retry_native(&server).unwrap();
    assert_eq!(received.data(), b"hello");

    assert_eq!(server.send("world").unwrap(), 5);
    assert_eq!(client_thread.join().unwrap().unwrap(), b"world");
}

#[test]
fn native_server_client_tcp_round_trip() {
    let port = unused_tcp_port();
    let endpoint = format!("tcp://127.0.0.1:{port}");
    let ctx = Context::new().unwrap();
    let server = ctx.socket(SocketType::Server).unwrap();
    let client = ctx.socket(SocketType::Client).unwrap();

    server.bind(&endpoint).unwrap();
    client.connect(&endpoint).unwrap();

    assert_eq!(client.send("hello").unwrap(), 5);
    let received = recv_retry_native(&server).unwrap();
    assert_eq!(received.data(), b"hello");

    assert_eq!(server.send("world").unwrap(), 5);
    let received = client.recv().unwrap();
    assert_eq!(received.data(), b"world");
}

#[test]
fn native_channel_tcp_round_trip() {
    let port = unused_tcp_port();
    let endpoint = format!("tcp://127.0.0.1:{port}");
    let ctx = Context::new().unwrap();
    let server = ctx.socket(SocketType::Channel).unwrap();
    let client = ctx.socket(SocketType::Channel).unwrap();

    server.bind(&endpoint).unwrap();
    client.connect(&endpoint).unwrap();

    assert_eq!(client.send("hello").unwrap(), 5);
    let received = recv_retry_native(&server).unwrap();
    assert_eq!(received.data(), b"hello");

    assert_eq!(server.send("world").unwrap(), 5);
    let received = client.recv().unwrap();
    assert_eq!(received.data(), b"world");
}

#[test]
fn native_scatter_gather_tcp_round_trip() {
    let port = unused_tcp_port();
    let endpoint = format!("tcp://127.0.0.1:{port}");
    let ctx = Context::new().unwrap();
    let gather = ctx.socket(SocketType::Gather).unwrap();
    let scatter = ctx.socket(SocketType::Scatter).unwrap();

    gather.bind(&endpoint).unwrap();
    scatter.connect(&endpoint).unwrap();

    assert_eq!(scatter.send("job").unwrap(), 3);
    let received = recv_retry_native(&gather).unwrap();
    assert_eq!(received.data(), b"job");
}

#[test]
fn native_pair_tcp_plain_round_trip() {
    let port = unused_tcp_port();
    let endpoint = format!("tcp://127.0.0.1:{port}");
    let ctx = Context::new().unwrap();
    let server = ctx.socket(SocketType::Pair).unwrap();
    let client = ctx.socket(SocketType::Pair).unwrap();

    server.set_option_i32(ZMQ_PLAIN_SERVER, 1).unwrap();
    client
        .set_option_bytes(ZMQ_PLAIN_USERNAME, b"user")
        .unwrap();
    client
        .set_option_bytes(ZMQ_PLAIN_PASSWORD, b"pass")
        .unwrap();
    server.bind(&endpoint).unwrap();
    client.connect(&endpoint).unwrap();

    let sender = std::thread::spawn(move || client.send("hello"));
    let received = recv_retry_native(&server).unwrap();
    assert_eq!(received.data(), b"hello");
    assert_eq!(sender.join().unwrap().unwrap(), 5);
}

#[test]
fn native_pair_tcp_plain_rejects_bad_credentials() {
    let port = unused_tcp_port();
    let endpoint = format!("tcp://127.0.0.1:{port}");
    let ctx = Context::new().unwrap();
    let server = ctx.socket(SocketType::Pair).unwrap();
    let client = ctx.socket(SocketType::Pair).unwrap();

    server.set_option_i32(ZMQ_PLAIN_SERVER, 1).unwrap();
    server
        .set_option_bytes(ZMQ_PLAIN_USERNAME, b"expected")
        .unwrap();
    server
        .set_option_bytes(ZMQ_PLAIN_PASSWORD, b"secret")
        .unwrap();
    client
        .set_option_bytes(ZMQ_PLAIN_USERNAME, b"wrong")
        .unwrap();
    client
        .set_option_bytes(ZMQ_PLAIN_PASSWORD, b"secret")
        .unwrap();
    server.bind(&endpoint).unwrap();
    client.connect(&endpoint).unwrap();

    let sender = std::thread::spawn(move || client.send("hello"));
    assert_eq!(recv_retry_error_native(&server), Error::InvalidArgument);
    assert!(sender.join().unwrap().is_err());
}

#[test]
fn native_pair_tcp_plain_uses_zap_actor() {
    let port = unused_tcp_port();
    let endpoint = format!("tcp://127.0.0.1:{port}");
    let ctx = Context::new().unwrap();
    let zap = ctx.socket(SocketType::Rep).unwrap();
    zap.bind("inproc://zeromq.zap.01").unwrap();
    let zap_thread = spawn_plain_zap_actor(zap, true);
    let server = ctx.socket(SocketType::Pair).unwrap();
    let client = ctx.socket(SocketType::Pair).unwrap();

    server.set_option_i32(ZMQ_PLAIN_SERVER, 1).unwrap();
    server.set_option_bytes(ZMQ_ZAP_DOMAIN, b"domain").unwrap();
    client
        .set_option_bytes(ZMQ_PLAIN_USERNAME, b"user")
        .unwrap();
    client
        .set_option_bytes(ZMQ_PLAIN_PASSWORD, b"pass")
        .unwrap();
    server.bind(&endpoint).unwrap();
    client.connect(&endpoint).unwrap();

    let sender = std::thread::spawn(move || client.send("hello"));
    let received = recv_retry_native(&server).unwrap();
    assert_eq!(received.data(), b"hello");
    assert_eq!(sender.join().unwrap().unwrap(), 5);
    zap_thread.join().unwrap();
}

#[test]
fn native_pair_tcp_plain_rejects_zap_denial() {
    let port = unused_tcp_port();
    let endpoint = format!("tcp://127.0.0.1:{port}");
    let ctx = Context::new().unwrap();
    let zap = ctx.socket(SocketType::Rep).unwrap();
    zap.bind("inproc://zeromq.zap.01").unwrap();
    let zap_thread = spawn_plain_zap_actor(zap, false);
    let server = ctx.socket(SocketType::Pair).unwrap();
    let client = ctx.socket(SocketType::Pair).unwrap();

    server.set_option_i32(ZMQ_PLAIN_SERVER, 1).unwrap();
    client
        .set_option_bytes(ZMQ_PLAIN_USERNAME, b"user")
        .unwrap();
    client
        .set_option_bytes(ZMQ_PLAIN_PASSWORD, b"pass")
        .unwrap();
    server.bind(&endpoint).unwrap();
    client.connect(&endpoint).unwrap();

    let sender = std::thread::spawn(move || client.send("hello"));
    assert_eq!(recv_retry_error_native(&server), Error::InvalidArgument);
    assert!(sender.join().unwrap().is_err());
    zap_thread.join().unwrap();
}

#[test]
fn native_pair_tcp_curve_round_trip() {
    let port = unused_tcp_port();
    let endpoint = format!("tcp://127.0.0.1:{port}");
    let ctx = Context::new().unwrap();
    let server = ctx.socket(SocketType::Pair).unwrap();
    let client = ctx.socket(SocketType::Pair).unwrap();
    let (server_public, server_secret) = curve_keypair().unwrap();
    let (client_public, client_secret) = curve_keypair().unwrap();

    server.set_option_i32(ZMQ_CURVE_SERVER, 1).unwrap();
    server
        .set_option_bytes(ZMQ_CURVE_SECRETKEY, server_secret.as_bytes())
        .unwrap();
    server
        .set_option_bytes(ZMQ_CURVE_PUBLICKEY, client_public.as_bytes())
        .unwrap();
    client
        .set_option_bytes(ZMQ_CURVE_SERVERKEY, server_public.as_bytes())
        .unwrap();
    client
        .set_option_bytes(ZMQ_CURVE_PUBLICKEY, client_public.as_bytes())
        .unwrap();
    client
        .set_option_bytes(ZMQ_CURVE_SECRETKEY, client_secret.as_bytes())
        .unwrap();
    assert_eq!(server.get_option_i32(ZMQ_MECHANISM).unwrap(), ZMQ_CURVE);
    server.bind(&endpoint).unwrap();
    client.connect(&endpoint).unwrap();

    let sender = std::thread::spawn(move || client.send("curve"));
    let received = recv_retry_native(&server).unwrap();
    assert_eq!(received.data(), b"curve");
    assert_eq!(sender.join().unwrap().unwrap(), 5);
}

#[test]
fn native_pair_tcp_curve_rejects_unknown_client_key() {
    let port = unused_tcp_port();
    let endpoint = format!("tcp://127.0.0.1:{port}");
    let ctx = Context::new().unwrap();
    let server = ctx.socket(SocketType::Pair).unwrap();
    let client = ctx.socket(SocketType::Pair).unwrap();
    let (server_public, server_secret) = curve_keypair().unwrap();
    let (allowed_public, _) = curve_keypair().unwrap();
    let (client_public, client_secret) = curve_keypair().unwrap();

    server.set_option_i32(ZMQ_CURVE_SERVER, 1).unwrap();
    server
        .set_option_bytes(ZMQ_CURVE_SECRETKEY, server_secret.as_bytes())
        .unwrap();
    server
        .set_option_bytes(ZMQ_CURVE_PUBLICKEY, allowed_public.as_bytes())
        .unwrap();
    client
        .set_option_bytes(ZMQ_CURVE_SERVERKEY, server_public.as_bytes())
        .unwrap();
    client
        .set_option_bytes(ZMQ_CURVE_PUBLICKEY, client_public.as_bytes())
        .unwrap();
    client
        .set_option_bytes(ZMQ_CURVE_SECRETKEY, client_secret.as_bytes())
        .unwrap();
    server.bind(&endpoint).unwrap();
    client.connect(&endpoint).unwrap();

    let sender = std::thread::spawn(move || client.send("curve"));
    assert_eq!(recv_retry_error_native(&server), Error::InvalidArgument);
    assert!(sender.join().unwrap().is_err());
}

#[test]
fn native_pair_tcp_curve_uses_zap_actor() {
    let port = unused_tcp_port();
    let endpoint = format!("tcp://127.0.0.1:{port}");
    let ctx = Context::new().unwrap();
    let zap = ctx.socket(SocketType::Rep).unwrap();
    zap.bind("inproc://zeromq.zap.01").unwrap();
    let zap_thread = spawn_curve_zap_actor(zap, true);
    let server = ctx.socket(SocketType::Pair).unwrap();
    let client = ctx.socket(SocketType::Pair).unwrap();
    let (server_public, server_secret) = curve_keypair().unwrap();
    let (client_public, client_secret) = curve_keypair().unwrap();

    server.set_option_i32(ZMQ_CURVE_SERVER, 1).unwrap();
    server
        .set_option_bytes(ZMQ_CURVE_SECRETKEY, server_secret.as_bytes())
        .unwrap();
    server.set_option_bytes(ZMQ_ZAP_DOMAIN, b"domain").unwrap();
    client
        .set_option_bytes(ZMQ_CURVE_SERVERKEY, server_public.as_bytes())
        .unwrap();
    client
        .set_option_bytes(ZMQ_CURVE_PUBLICKEY, client_public.as_bytes())
        .unwrap();
    client
        .set_option_bytes(ZMQ_CURVE_SECRETKEY, client_secret.as_bytes())
        .unwrap();
    server.bind(&endpoint).unwrap();
    client.connect(&endpoint).unwrap();

    let sender = std::thread::spawn(move || client.send("curve"));
    let received = recv_retry_native(&server).unwrap();
    assert_eq!(received.data(), b"curve");
    assert_eq!(sender.join().unwrap().unwrap(), 5);
    zap_thread.join().unwrap();
}

#[test]
fn native_pair_tcp_gssapi_round_trip() {
    if skip_synthetic_gssapi_test() {
        return;
    }
    let port = unused_tcp_port();
    let endpoint = format!("tcp://127.0.0.1:{port}");
    let ctx = Context::new().unwrap();
    let server = ctx.socket(SocketType::Pair).unwrap();
    let client = ctx.socket(SocketType::Pair).unwrap();

    server.set_option_i32(ZMQ_GSSAPI_SERVER, 1).unwrap();
    server
        .set_option_bytes(ZMQ_GSSAPI_PRINCIPAL, b"client@EXAMPLE")
        .unwrap();
    client
        .set_option_bytes(ZMQ_GSSAPI_PRINCIPAL, b"client@EXAMPLE")
        .unwrap();
    client
        .set_option_bytes(ZMQ_GSSAPI_SERVICE_PRINCIPAL, b"server@EXAMPLE")
        .unwrap();
    assert_eq!(server.get_option_i32(ZMQ_MECHANISM).unwrap(), ZMQ_GSSAPI);
    server.bind(&endpoint).unwrap();
    client.connect(&endpoint).unwrap();

    let sender = std::thread::spawn(move || client.send("gss"));
    let received = recv_retry_native(&server).unwrap();
    assert_eq!(received.data(), b"gss");
    assert_eq!(sender.join().unwrap().unwrap(), 3);
}

#[test]
fn native_pair_tcp_gssapi_rejects_bad_principal() {
    if skip_synthetic_gssapi_test() {
        return;
    }
    let port = unused_tcp_port();
    let endpoint = format!("tcp://127.0.0.1:{port}");
    let ctx = Context::new().unwrap();
    let server = ctx.socket(SocketType::Pair).unwrap();
    let client = ctx.socket(SocketType::Pair).unwrap();

    server.set_option_i32(ZMQ_GSSAPI_SERVER, 1).unwrap();
    server
        .set_option_bytes(ZMQ_GSSAPI_PRINCIPAL, b"expected@EXAMPLE")
        .unwrap();
    client
        .set_option_bytes(ZMQ_GSSAPI_PRINCIPAL, b"wrong@EXAMPLE")
        .unwrap();
    client
        .set_option_bytes(ZMQ_GSSAPI_SERVICE_PRINCIPAL, b"server@EXAMPLE")
        .unwrap();
    server.bind(&endpoint).unwrap();
    client.connect(&endpoint).unwrap();

    let sender = std::thread::spawn(move || client.send("gss"));
    assert!(matches!(server.recv(), Err(Error::InvalidArgument)));
    assert!(sender.join().unwrap().is_err());
}

#[test]
fn native_pair_tcp_gssapi_uses_zap_actor() {
    if skip_synthetic_gssapi_test() {
        return;
    }
    let port = unused_tcp_port();
    let endpoint = format!("tcp://127.0.0.1:{port}");
    let ctx = Context::new().unwrap();
    let zap = ctx.socket(SocketType::Rep).unwrap();
    zap.bind("inproc://zeromq.zap.01").unwrap();
    let zap_thread = spawn_gssapi_zap_actor(zap, true);
    let server = ctx.socket(SocketType::Pair).unwrap();
    let client = ctx.socket(SocketType::Pair).unwrap();

    server.set_option_i32(ZMQ_GSSAPI_SERVER, 1).unwrap();
    server.set_option_bytes(ZMQ_ZAP_DOMAIN, b"domain").unwrap();
    client
        .set_option_bytes(ZMQ_GSSAPI_PRINCIPAL, b"client@EXAMPLE")
        .unwrap();
    client
        .set_option_bytes(ZMQ_GSSAPI_SERVICE_PRINCIPAL, b"server@EXAMPLE")
        .unwrap();
    server.bind(&endpoint).unwrap();
    client.connect(&endpoint).unwrap();

    let sender = std::thread::spawn(move || client.send("gss"));
    let received = recv_retry_native(&server).unwrap();
    assert_eq!(received.data(), b"gss");
    assert_eq!(sender.join().unwrap().unwrap(), 3);
    zap_thread.join().unwrap();
}

fn spawn_plain_zap_actor(zap: libzmq::Socket, accept: bool) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let mut frames = Vec::new();
        loop {
            let message = recv_retry_native(&zap).unwrap();
            let more = message.more();
            frames.push(message.data().to_vec());
            if !more {
                break;
            }
        }
        assert_eq!(frames[0], b"1.0");
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
            zap.send_with_flags(*frame, flags).unwrap();
        }
    })
}

fn spawn_curve_zap_actor(zap: libzmq::Socket, accept: bool) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let mut frames = Vec::new();
        loop {
            let message = recv_retry_native(&zap).unwrap();
            let more = message.more();
            frames.push(message.data().to_vec());
            if !more {
                break;
            }
        }
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
            zap.send_with_flags(*frame, flags).unwrap();
        }
    })
}

fn spawn_gssapi_zap_actor(zap: libzmq::Socket, accept: bool) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let mut frames = Vec::new();
        loop {
            let message = recv_retry_native(&zap).unwrap();
            let more = message.more();
            frames.push(message.data().to_vec());
            if !more {
                break;
            }
        }
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
            zap.send_with_flags(*frame, flags).unwrap();
        }
    })
}

fn recv_retry_native(socket: &libzmq::Socket) -> libzmq::Result<Message> {
    for _ in 0..100 {
        match socket.recv() {
            Ok(message) => return Ok(message),
            Err(Error::Again) => std::thread::sleep(std::time::Duration::from_millis(10)),
            Err(error) => return Err(error),
        }
    }
    Err(Error::Again)
}

fn recv_retry_error_native(socket: &libzmq::Socket) -> Error {
    for _ in 0..100 {
        match socket.recv() {
            Ok(_) => return Error::InvalidState,
            Err(Error::Again) => std::thread::sleep(std::time::Duration::from_millis(10)),
            Err(error) => return error,
        }
    }
    Error::Again
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

#[test]
fn native_dgram_udp_round_trip() {
    let port = unused_udp_port();
    let endpoint = format!("udp://127.0.0.1:{port}");
    let ctx = Context::new().unwrap();
    let server = ctx.socket(SocketType::Dgram).unwrap();
    let client = ctx.socket(SocketType::Dgram).unwrap();

    server.bind(&endpoint).unwrap();
    client.connect(&endpoint).unwrap();

    assert_eq!(client.send("ping").unwrap(), 4);
    let received = recv_retry_native(&server).unwrap();
    assert_eq!(received.data(), b"ping");

    assert_eq!(server.send("pong").unwrap(), 4);
    let received = recv_retry_native(&client).unwrap();
    assert_eq!(received.data(), b"pong");
}

#[test]
fn native_dgram_udp_multicast_receives_group_datagram() {
    let port = unused_udp_port();
    let endpoint = format!("udp://239.255.0.1:{port}");
    let ctx = Context::new().unwrap();
    let receiver = ctx.socket(SocketType::Dgram).unwrap();
    let sender = ctx.socket(SocketType::Dgram).unwrap();

    receiver.bind(&endpoint).unwrap();
    sender.connect(&endpoint).unwrap();

    assert_eq!(sender.send("mcast").unwrap(), 5);
    let received = recv_retry_native(&receiver).unwrap();
    assert_eq!(received.data(), b"mcast");
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

fn unused_udp_port() -> u16 {
    UdpSocket::bind("127.0.0.1:0")
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
fn native_channel_inproc_round_trip() {
    let ctx = Context::new().unwrap();
    let server = ctx.socket(SocketType::Channel).unwrap();
    let client = ctx.socket(SocketType::Channel).unwrap();

    server.bind("inproc://native_channel").unwrap();
    client.connect("inproc://native_channel").unwrap();

    assert_eq!(client.send("hello").unwrap(), 5);
    let received = server.recv().unwrap();
    assert_eq!(received.data(), b"hello");

    assert_eq!(server.send("world").unwrap(), 5);
    let received = client.recv().unwrap();
    assert_eq!(received.data(), b"world");
}

#[test]
fn native_scatter_gather_inproc_round_trip() {
    let ctx = Context::new().unwrap();
    let gather = ctx.socket(SocketType::Gather).unwrap();
    let scatter = ctx.socket(SocketType::Scatter).unwrap();

    gather.bind("inproc://native_scatter_gather").unwrap();
    scatter.connect("inproc://native_scatter_gather").unwrap();

    assert_eq!(scatter.send("job").unwrap(), 3);
    let received = gather.recv().unwrap();
    assert_eq!(received.data(), b"job");
}

#[test]
fn native_scatter_gather_inproc_load_balances_between_gathers() {
    let ctx = Context::new().unwrap();
    let scatter = ctx.socket(SocketType::Scatter).unwrap();
    let gather_a = ctx.socket(SocketType::Gather).unwrap();
    let gather_b = ctx.socket(SocketType::Gather).unwrap();

    scatter.bind("inproc://native_scatter_lb").unwrap();
    gather_a.connect("inproc://native_scatter_lb").unwrap();
    gather_b.connect("inproc://native_scatter_lb").unwrap();

    assert_eq!(scatter.send("one").unwrap(), 3);
    assert_eq!(scatter.send("two").unwrap(), 3);

    let received = gather_a.recv().unwrap();
    assert_eq!(received.data(), b"one");
    let received = gather_b.recv().unwrap();
    assert_eq!(received.data(), b"two");
}

#[test]
fn native_scatter_gather_reject_wrong_direction_operations() {
    let ctx = Context::new().unwrap();
    let scatter = ctx.socket(SocketType::Scatter).unwrap();
    let gather = ctx.socket(SocketType::Gather).unwrap();

    assert_eq!(gather.send("bad"), Err(Error::NotSupported));
    assert_eq!(scatter.recv(), Err(Error::NotSupported));
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
fn native_server_client_inproc_round_trip_sets_routing_id() {
    let ctx = Context::new().unwrap();
    let server = ctx.socket(SocketType::Server).unwrap();
    let client = ctx.socket(SocketType::Client).unwrap();

    server.bind("inproc://native_server_client").unwrap();
    client.connect("inproc://native_server_client").unwrap();

    assert_eq!(client.send("request").unwrap(), 7);
    let received = server.recv().unwrap();
    assert_eq!(received.data(), b"request");
    assert_ne!(received.routing_id(), 0);

    assert_eq!(server.send("missing route"), Err(Error::Again));

    let mut reply = Message::from("reply");
    reply.set_routing_id(received.routing_id());
    assert_eq!(server.send(reply).unwrap(), 5);
    let received = client.recv().unwrap();
    assert_eq!(received.data(), b"reply");
}

#[test]
fn native_peer_inproc_round_trip_sets_routing_id() {
    let ctx = Context::new().unwrap();
    let bound = ctx.socket(SocketType::Peer).unwrap();
    let connected = ctx.socket(SocketType::Peer).unwrap();

    bound.bind("inproc://native_peer").unwrap();
    let peer_id = connected.connect_peer("inproc://native_peer").unwrap();
    assert_ne!(peer_id, 0);

    assert_eq!(connected.send("request").unwrap(), 7);
    let received = bound.recv().unwrap();
    assert_eq!(received.data(), b"request");
    assert_ne!(received.routing_id(), 0);

    assert_eq!(bound.send("missing route"), Err(Error::Again));

    let mut reply = Message::from("reply");
    reply.set_routing_id(received.routing_id());
    assert_eq!(bound.send(reply).unwrap(), 5);
    let received = connected.recv().unwrap();
    assert_eq!(received.data(), b"reply");
}

#[test]
fn native_peer_tcp_round_trip() {
    let port = unused_tcp_port();
    let endpoint = format!("tcp://127.0.0.1:{port}");
    let ctx = Context::new().unwrap();
    let server = ctx.socket(SocketType::Peer).unwrap();
    let client = ctx.socket(SocketType::Peer).unwrap();

    server.bind(&endpoint).unwrap();
    let peer_id = client.connect_peer(&endpoint).unwrap();
    assert_ne!(peer_id, 0);

    assert_eq!(client.send("hello").unwrap(), 5);
    let received = recv_retry_native(&server).unwrap();
    assert_eq!(received.data(), b"hello");

    assert_eq!(server.send("world").unwrap(), 5);
    let received = client.recv().unwrap();
    assert_eq!(received.data(), b"world");
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
fn native_radio_dish_inproc_filters_by_group() {
    let ctx = Context::new().unwrap();
    let radio = ctx.socket(SocketType::Radio).unwrap();
    let dish = ctx.socket(SocketType::Dish).unwrap();

    radio.bind("inproc://native_radio_dish").unwrap();
    dish.connect("inproc://native_radio_dish").unwrap();
    dish.join("updates").unwrap();

    let mut ignored = Message::from("old");
    ignored.set_group("archive").unwrap();
    assert_eq!(radio.send(ignored).unwrap(), 3);
    assert_eq!(dish.recv(), Err(Error::Again));

    let mut message = Message::from("new");
    message.set_group("updates").unwrap();
    assert_eq!(radio.send(message).unwrap(), 3);
    let received = dish.recv().unwrap();
    assert_eq!(received.data(), b"new");
    assert_eq!(received.group(), Some("updates"));

    dish.leave("updates").unwrap();
    let mut later = Message::from("later");
    later.set_group("updates").unwrap();
    assert_eq!(radio.send(later).unwrap(), 5);
    assert_eq!(dish.recv(), Err(Error::Again));
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

    assert_eq!(socket.get_option_i32(ZMQ_NORM_MODE).unwrap(), ZMQ_NORM_CC);
    assert_eq!(socket.get_option_i32(ZMQ_NORM_BUFFER_SIZE).unwrap(), 2048);
    assert_eq!(socket.get_option_i32(ZMQ_NORM_SEGMENT_SIZE).unwrap(), 1400);
    assert_eq!(socket.get_option_i32(ZMQ_NORM_BLOCK_SIZE).unwrap(), 16);
    assert_eq!(socket.get_option_i32(ZMQ_NORM_NUM_PARITY).unwrap(), 4);
    assert_eq!(socket.get_option_i32(ZMQ_NORM_NUM_AUTOPARITY).unwrap(), 0);
    assert_eq!(socket.get_option_i32(ZMQ_NORM_UNICAST_NACK).unwrap(), 0);
    assert_eq!(socket.get_option_i32(ZMQ_NORM_PUSH).unwrap(), 0);
    socket.set_option_i32(ZMQ_NORM_MODE, ZMQ_NORM_CCE).unwrap();
    socket.set_option_i32(ZMQ_NORM_BUFFER_SIZE, 4096).unwrap();
    socket.set_option_i32(ZMQ_NORM_SEGMENT_SIZE, 1200).unwrap();
    socket.set_option_i32(ZMQ_NORM_BLOCK_SIZE, 64).unwrap();
    socket.set_option_i32(ZMQ_NORM_NUM_PARITY, 8).unwrap();
    socket.set_option_i32(ZMQ_NORM_NUM_AUTOPARITY, 2).unwrap();
    socket.set_option_i32(ZMQ_NORM_UNICAST_NACK, 1).unwrap();
    socket.set_option_i32(ZMQ_NORM_PUSH, 1).unwrap();
    assert_eq!(socket.get_option_i32(ZMQ_NORM_MODE).unwrap(), ZMQ_NORM_CCE);
    assert_eq!(socket.get_option_i32(ZMQ_NORM_BUFFER_SIZE).unwrap(), 4096);
    assert_eq!(socket.get_option_i32(ZMQ_NORM_SEGMENT_SIZE).unwrap(), 1200);
    assert_eq!(socket.get_option_i32(ZMQ_NORM_BLOCK_SIZE).unwrap(), 64);
    assert_eq!(socket.get_option_i32(ZMQ_NORM_NUM_PARITY).unwrap(), 8);
    assert_eq!(socket.get_option_i32(ZMQ_NORM_NUM_AUTOPARITY).unwrap(), 2);
    assert_eq!(socket.get_option_i32(ZMQ_NORM_UNICAST_NACK).unwrap(), 1);
    assert_eq!(socket.get_option_i32(ZMQ_NORM_PUSH).unwrap(), 1);
    assert_eq!(
        socket.set_option_i32(ZMQ_NORM_MODE, 5),
        Err(Error::InvalidArgument)
    );
    assert_eq!(
        socket.set_option_i32(ZMQ_NORM_BLOCK_SIZE, 256),
        Err(Error::InvalidArgument)
    );
}

#[test]
fn native_security_options_round_trip() {
    let ctx = Context::new().unwrap();
    let socket = ctx.socket(SocketType::Req).unwrap();

    assert_eq!(socket.get_option_i32(ZMQ_MECHANISM).unwrap(), ZMQ_NULL);
    socket.set_option_i32(ZMQ_PLAIN_SERVER, 1).unwrap();
    socket
        .set_option_bytes(ZMQ_PLAIN_USERNAME, b"user")
        .unwrap();
    socket
        .set_option_bytes(ZMQ_PLAIN_PASSWORD, b"pass")
        .unwrap();
    socket.set_option_bytes(ZMQ_ZAP_DOMAIN, b"domain").unwrap();

    assert_eq!(socket.get_option_i32(ZMQ_MECHANISM).unwrap(), ZMQ_PLAIN);
    assert_eq!(socket.get_option_i32(ZMQ_PLAIN_SERVER).unwrap(), 1);
    assert_eq!(
        socket.get_option_bytes(ZMQ_PLAIN_USERNAME).unwrap(),
        b"user"
    );
    assert_eq!(
        socket.get_option_bytes(ZMQ_PLAIN_PASSWORD).unwrap(),
        b"pass"
    );
    assert_eq!(socket.get_option_bytes(ZMQ_ZAP_DOMAIN).unwrap(), b"domain");
}
