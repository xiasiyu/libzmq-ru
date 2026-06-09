pub mod constants;
pub mod context;
pub mod error;
pub mod message;
pub mod socket;

pub use constants::*;
pub use context::Context;
pub use error::{Error, Result};
pub use message::Message;
pub use socket::{Socket, SocketType};

pub fn version() -> (i32, i32, i32) {
    (ZMQ_VERSION_MAJOR, ZMQ_VERSION_MINOR, ZMQ_VERSION_PATCH)
}
