use ru_libzmq_core::{Context, SocketType};
use ru_libzmq_test_harness::{Observation, Operation, TraceCase};

fn main() {
    let mut traces = Vec::new();
    traces.push(version_trace());
    traces.push(context_socket_trace());

    for trace in traces {
        println!("{}", trace.to_json_lines());
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
