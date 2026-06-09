#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceCase {
    pub name: &'static str,
    pub operations: Vec<Operation>,
    pub observations: Vec<Observation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operation {
    Version,
    ContextNew,
    SocketNew { socket_type: i32 },
    ContextTerm,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Observation {
    Version { major: i32, minor: i32, patch: i32 },
    Pointer { is_null: bool },
    ReturnCode { rc: i32 },
    Errno { errno: i32 },
}

impl TraceCase {
    pub fn to_json_lines(&self) -> String {
        let mut lines = Vec::new();
        lines.push(format!("{{\"case\":\"{}\"}}", escape(self.name)));
        for operation in &self.operations {
            lines.push(format!("{{\"operation\":{}}}", operation.to_json()));
        }
        for observation in &self.observations {
            lines.push(format!("{{\"observation\":{}}}", observation.to_json()));
        }
        lines.join("\n")
    }
}

impl Operation {
    fn to_json(&self) -> String {
        match self {
            Self::Version => "{\"type\":\"version\"}".to_string(),
            Self::ContextNew => "{\"type\":\"context_new\"}".to_string(),
            Self::SocketNew { socket_type } => {
                format!("{{\"type\":\"socket_new\",\"socket_type\":{socket_type}}}")
            }
            Self::ContextTerm => "{\"type\":\"context_term\"}".to_string(),
        }
    }
}

impl Observation {
    fn to_json(&self) -> String {
        match self {
            Self::Version {
                major,
                minor,
                patch,
            } => {
                format!("{{\"type\":\"version\",\"major\":{major},\"minor\":{minor},\"patch\":{patch}}}")
            }
            Self::Pointer { is_null } => {
                format!("{{\"type\":\"pointer\",\"is_null\":{is_null}}}")
            }
            Self::ReturnCode { rc } => format!("{{\"type\":\"return_code\",\"rc\":{rc}}}"),
            Self::Errno { errno } => format!("{{\"type\":\"errno\",\"errno\":{errno}}}"),
        }
    }
}

fn escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::{Observation, Operation, TraceCase};

    #[test]
    fn trace_renders_json_lines() {
        let trace = TraceCase {
            name: "version",
            operations: vec![Operation::Version],
            observations: vec![Observation::Version {
                major: 4,
                minor: 3,
                patch: 6,
            }],
        };

        let rendered = trace.to_json_lines();

        assert!(rendered.contains("\"case\":\"version\""));
        assert!(rendered.contains("\"operation\""));
        assert!(rendered.contains("\"observation\""));
    }
}
