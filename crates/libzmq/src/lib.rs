pub use libzmq_core::{constants::*, Error, Message, Result, SocketType};

pub struct Context {
    inner: libzmq_core::Context,
}

impl Context {
    pub fn new() -> Result<Self> {
        Ok(Self {
            inner: libzmq_core::Context::new()?,
        })
    }

    pub fn socket(&self, socket_type: SocketType) -> Result<Socket> {
        Ok(Socket {
            inner: self.inner.socket(socket_type)?,
        })
    }

    pub fn shutdown(&self) -> Result<()> {
        self.inner.shutdown()
    }

    pub fn terminate(&self) -> Result<()> {
        self.inner.terminate()
    }

    pub fn set_option_i32(&self, option: i32, value: i32) -> Result<()> {
        self.inner.set_option(option, value)
    }

    pub fn get_option_i32(&self, option: i32) -> Result<i32> {
        self.inner.get_option(option)
    }
}

pub struct Socket {
    inner: libzmq_core::Socket,
}

impl Socket {
    pub fn socket_type(&self) -> SocketType {
        self.inner.socket_type()
    }

    pub fn bind(&self, endpoint: &str) -> Result<()> {
        self.inner.bind(endpoint)
    }

    pub fn unbind(&self, endpoint: &str) -> Result<()> {
        self.inner.unbind(endpoint)
    }

    pub fn connect(&self, endpoint: &str) -> Result<()> {
        self.inner.connect(endpoint)
    }

    pub fn disconnect(&self, endpoint: &str) -> Result<()> {
        self.inner.disconnect(endpoint)
    }

    pub fn subscribe(&self, prefix: &[u8]) -> Result<()> {
        self.inner.subscribe(prefix)
    }

    pub fn unsubscribe(&self, prefix: &[u8]) -> Result<()> {
        self.inner.unsubscribe(prefix)
    }

    pub fn send<M>(&self, message: M) -> Result<usize>
    where
        M: Into<Message>,
    {
        self.inner.send(message.into(), 0)
    }

    pub fn send_with_flags<M>(&self, message: M, flags: i32) -> Result<usize>
    where
        M: Into<Message>,
    {
        self.inner.send(message.into(), flags)
    }

    pub fn recv(&self) -> Result<Message> {
        self.inner.recv(0)
    }

    pub fn set_option_i32(&self, option: i32, value: i32) -> Result<()> {
        self.inner.set_option_i32(option, value)
    }

    pub fn set_option_bytes(&self, option: i32, value: &[u8]) -> Result<()> {
        self.inner.set_option_bytes(option, value)
    }

    pub fn get_option_i32(&self, option: i32) -> Result<i32> {
        self.inner.get_option_i32(option)
    }
}

pub fn version() -> (i32, i32, i32) {
    libzmq_core::version()
}

#[cfg(test)]
mod tests {
    use super::{version, Context, SocketType};

    #[test]
    fn version_matches_libzmq_baseline() {
        assert_eq!(version(), (4, 3, 6));
    }

    #[test]
    fn context_creates_typed_socket() {
        let ctx = Context::new().unwrap();
        let socket = ctx.socket(SocketType::Req).unwrap();
        assert_eq!(socket.socket_type(), SocketType::Req);
    }
}
