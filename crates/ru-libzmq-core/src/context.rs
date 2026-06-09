use crate::{Error, Result, Socket, SocketType};
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

const STATE_RUNNING: u8 = 0;
const STATE_SHUTTING_DOWN: u8 = 1;
const STATE_TERMINATED: u8 = 2;

#[derive(Debug)]
pub struct Context {
    state: AtomicU8,
    next_socket_id: AtomicUsize,
}

impl Context {
    pub fn new() -> Result<Self> {
        Ok(Self {
            state: AtomicU8::new(STATE_RUNNING),
            next_socket_id: AtomicUsize::new(1),
        })
    }

    pub fn shutdown(&self) -> Result<()> {
        self.state.store(STATE_SHUTTING_DOWN, Ordering::SeqCst);
        Ok(())
    }

    pub fn terminate(&self) -> Result<()> {
        self.state.store(STATE_TERMINATED, Ordering::SeqCst);
        Ok(())
    }

    pub fn socket(&self, socket_type: SocketType) -> Result<Socket> {
        self.ensure_running()?;
        let id = self.next_socket_id.fetch_add(1, Ordering::Relaxed);
        Ok(Socket::new(id, socket_type))
    }

    pub fn is_terminated(&self) -> bool {
        self.state.load(Ordering::SeqCst) == STATE_TERMINATED
    }

    fn ensure_running(&self) -> Result<()> {
        match self.state.load(Ordering::SeqCst) {
            STATE_RUNNING => Ok(()),
            STATE_SHUTTING_DOWN | STATE_TERMINATED => Err(Error::Terminated),
            _ => Err(Error::InvalidContext),
        }
    }
}
