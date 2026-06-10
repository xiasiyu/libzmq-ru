pub mod constants;
pub mod context;
pub mod error;
pub mod message;
pub mod pipe;
pub mod security;
pub mod socket;
pub mod transport;

pub use constants::*;
pub use context::Context;
pub use error::{Error, Result};
pub use message::Message;
pub use security::{curve_keypair, curve_public, z85_decode, z85_encode, ZapReply, ZapRequest};
pub use socket::{Socket, SocketType};
pub use transport::{
    Endpoint, IpcEndpoint, NormEndpoint, TcpEndpoint, ZmtpFrame, ZmtpGreeting, ZmtpMetadata,
};

pub fn version() -> (i32, i32, i32) {
    (ZMQ_VERSION_MAJOR, ZMQ_VERSION_MINOR, ZMQ_VERSION_PATCH)
}
