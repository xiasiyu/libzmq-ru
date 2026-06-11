use crate::constants::*;
use std::fmt;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    InvalidArgument,
    InvalidContext,
    InvalidSocket,
    NotSupported,
    ProtocolNotSupported,
    Terminated,
    Again,
    InvalidState,
    HostUnreachable,
    OutOfMemory,
    NotImplemented(&'static str),
}

impl Error {
    pub fn errno(self) -> i32 {
        match self {
            Self::InvalidArgument => EINVAL,
            Self::InvalidContext => EFAULT,
            Self::InvalidSocket => ENOTSOCK,
            Self::NotSupported | Self::NotImplemented(_) => ENOTSUP,
            Self::ProtocolNotSupported => EPROTONOSUPPORT,
            Self::Terminated => ETERM,
            Self::Again => EAGAIN,
            Self::InvalidState => EFSM,
            Self::HostUnreachable => EHOSTUNREACH,
            Self::OutOfMemory => ENOMEM,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidArgument => f.write_str("invalid argument"),
            Self::InvalidContext => f.write_str("invalid context"),
            Self::InvalidSocket => f.write_str("socket operation on non-socket"),
            Self::NotSupported => f.write_str("operation not supported"),
            Self::ProtocolNotSupported => f.write_str("protocol not supported"),
            Self::Terminated => f.write_str("context was terminated"),
            Self::Again => f.write_str("resource temporarily unavailable"),
            Self::InvalidState => f.write_str("operation cannot be accomplished in current state"),
            Self::HostUnreachable => f.write_str("host unreachable"),
            Self::OutOfMemory => f.write_str("out of memory"),
            Self::NotImplemented(name) => write!(f, "{name} is not implemented yet"),
        }
    }
}

impl std::error::Error for Error {}
