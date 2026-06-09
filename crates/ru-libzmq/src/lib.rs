pub use ru_libzmq_core::{Error, Message, Result, SocketType};

pub struct Context {
    inner: ru_libzmq_core::Context,
}

impl Context {
    pub fn new() -> Result<Self> {
        Ok(Self {
            inner: ru_libzmq_core::Context::new()?,
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
}

pub struct Socket {
    inner: ru_libzmq_core::Socket,
}

impl Socket {
    pub fn socket_type(&self) -> SocketType {
        self.inner.socket_type()
    }

    pub fn bind(&self, endpoint: &str) -> Result<()> {
        self.inner.bind(endpoint)
    }

    pub fn connect(&self, endpoint: &str) -> Result<()> {
        self.inner.connect(endpoint)
    }

    pub fn send<M>(&self, message: M) -> Result<usize>
    where
        M: Into<Message>,
    {
        self.inner.send(message.into(), 0)
    }

    pub fn recv(&self) -> Result<Message> {
        self.inner.recv(0)
    }
}

pub fn version() -> (i32, i32, i32) {
    ru_libzmq_core::version()
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
