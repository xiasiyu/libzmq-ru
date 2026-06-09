use ru_libzmq_core::{Context, SocketType};
use ru_libzmq_test_harness::{Observation, Operation, TraceCase};

fn main() {
    let mut traces = Vec::new();
    traces.push(version_trace());
    traces.push(context_socket_trace());
    traces.push(pair_inproc_trace());
    traces.push(push_pull_inproc_trace());
    traces.push(pub_sub_inproc_trace());

    for trace in traces {
        println!("{}", trace.to_json_lines());
    }
}

fn pair_inproc_trace() -> TraceCase {
    let ctx = Context::new().unwrap();
    let server = ctx.socket(SocketType::Pair).unwrap();
    let client = ctx.socket(SocketType::Pair).unwrap();
    server.bind("inproc://trace_pair").unwrap();
    client.connect("inproc://trace_pair").unwrap();
    let send_rc = client
        .send("hello".into(), 0)
        .map(|size| size as i32)
        .unwrap_or(-1);
    let received = server.recv(0).unwrap();

    TraceCase {
        name: "pair_inproc",
        operations: vec![
            Operation::ContextNew,
            Operation::SocketNew { socket_type: 0 },
            Operation::SocketNew { socket_type: 0 },
            Operation::Bind {
                endpoint: "inproc://trace_pair",
            },
            Operation::Connect {
                endpoint: "inproc://trace_pair",
            },
            Operation::Send { size: 5 },
            Operation::Recv,
        ],
        observations: vec![
            Observation::ReturnCode { rc: send_rc },
            Observation::Message {
                data: String::from_utf8_lossy(received.data()).into_owned(),
                routing_id: received.routing_id(),
            },
        ],
    }
}

fn push_pull_inproc_trace() -> TraceCase {
    let ctx = Context::new().unwrap();
    let pull = ctx.socket(SocketType::Pull).unwrap();
    let push = ctx.socket(SocketType::Push).unwrap();
    pull.bind("inproc://trace_push_pull").unwrap();
    push.connect("inproc://trace_push_pull").unwrap();
    let send_rc = push
        .send("job".into(), 0)
        .map(|size| size as i32)
        .unwrap_or(-1);
    let received = pull.recv(0).unwrap();

    TraceCase {
        name: "push_pull_inproc",
        operations: vec![
            Operation::ContextNew,
            Operation::SocketNew { socket_type: 7 },
            Operation::SocketNew { socket_type: 8 },
            Operation::Bind {
                endpoint: "inproc://trace_push_pull",
            },
            Operation::Connect {
                endpoint: "inproc://trace_push_pull",
            },
            Operation::Send { size: 3 },
            Operation::Recv,
        ],
        observations: vec![
            Observation::ReturnCode { rc: send_rc },
            Observation::Message {
                data: String::from_utf8_lossy(received.data()).into_owned(),
                routing_id: received.routing_id(),
            },
        ],
    }
}

fn pub_sub_inproc_trace() -> TraceCase {
    let ctx = Context::new().unwrap();
    let publisher = ctx.socket(SocketType::Pub).unwrap();
    let subscriber = ctx.socket(SocketType::Sub).unwrap();
    subscriber.subscribe(b"topic").unwrap();
    publisher.bind("inproc://trace_pub_sub").unwrap();
    subscriber.connect("inproc://trace_pub_sub").unwrap();
    let send_rc = publisher
        .send("topic:keep".into(), 0)
        .map(|size| size as i32)
        .unwrap_or(-1);
    let received = subscriber.recv(0).unwrap();

    TraceCase {
        name: "pub_sub_inproc",
        operations: vec![
            Operation::ContextNew,
            Operation::SocketNew { socket_type: 1 },
            Operation::SocketNew { socket_type: 2 },
            Operation::Subscribe { prefix: "topic" },
            Operation::Bind {
                endpoint: "inproc://trace_pub_sub",
            },
            Operation::Connect {
                endpoint: "inproc://trace_pub_sub",
            },
            Operation::Send { size: 10 },
            Operation::Recv,
        ],
        observations: vec![
            Observation::ReturnCode { rc: send_rc },
            Observation::Message {
                data: String::from_utf8_lossy(received.data()).into_owned(),
                routing_id: received.routing_id(),
            },
        ],
    }
}

fn version_trace() -> TraceCase {
    let (major, minor, patch) = ru_libzmq_core::version();
    TraceCase {
        name: "version",
        operations: vec![Operation::Version],
        observations: vec![Observation::Version {
            major,
            minor,
            patch,
        }],
    }
}

fn context_socket_trace() -> TraceCase {
    let ctx = Context::new();
    let mut observations = Vec::new();
    observations.push(Observation::Pointer {
        is_null: ctx.is_err(),
    });

    if let Ok(ctx) = &ctx {
        observations.push(Observation::Pointer {
            is_null: ctx.socket(SocketType::Pair).is_err(),
        });
        observations.push(Observation::ReturnCode {
            rc: if ctx.terminate().is_ok() { 0 } else { -1 },
        });
    }

    TraceCase {
        name: "context_socket_pair",
        operations: vec![
            Operation::ContextNew,
            Operation::SocketNew { socket_type: 0 },
            Operation::ContextTerm,
        ],
        observations,
    }
}
