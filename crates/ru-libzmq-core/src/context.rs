use crate::{Error, Result, Socket, SocketType};
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::Mutex;

const STATE_RUNNING: u8 = 0;
const STATE_SHUTTING_DOWN: u8 = 1;
const STATE_TERMINATED: u8 = 2;

#[derive(Debug)]
pub struct Context {
    state: AtomicU8,
    next_socket_id: AtomicUsize,
    options: Mutex<ContextOptions>,
}

#[derive(Debug, Clone)]
struct ContextOptions {
    io_threads: i32,
    max_sockets: i32,
    max_msgsz: i32,
    thread_priority: i32,
    thread_sched_policy: i32,
    zero_copy_recv: i32,
}

impl Default for ContextOptions {
    fn default() -> Self {
        Self {
            io_threads: crate::ZMQ_IO_THREADS_DFLT,
            max_sockets: crate::ZMQ_MAX_SOCKETS_DFLT,
            max_msgsz: i32::MAX,
            thread_priority: crate::ZMQ_THREAD_PRIORITY_DFLT,
            thread_sched_policy: crate::ZMQ_THREAD_SCHED_POLICY_DFLT,
            zero_copy_recv: 0,
        }
    }
}

impl Context {
    pub fn new() -> Result<Self> {
        Ok(Self {
            state: AtomicU8::new(STATE_RUNNING),
            next_socket_id: AtomicUsize::new(1),
            options: Mutex::new(ContextOptions::default()),
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

    pub fn set_option(&self, option: i32, value: i32) -> Result<()> {
        if matches!(option, crate::ZMQ_IO_THREADS | crate::ZMQ_MAX_SOCKETS) && value < 0 {
            return Err(Error::InvalidArgument);
        }

        let mut options = self.options.lock().map_err(|_| Error::InvalidContext)?;
        match option {
            crate::ZMQ_IO_THREADS => options.io_threads = value,
            crate::ZMQ_MAX_SOCKETS => options.max_sockets = value,
            crate::ZMQ_MAX_MSGSZ => options.max_msgsz = value,
            crate::ZMQ_THREAD_PRIORITY => options.thread_priority = value,
            crate::ZMQ_THREAD_SCHED_POLICY => options.thread_sched_policy = value,
            crate::ZMQ_ZERO_COPY_RECV => options.zero_copy_recv = i32::from(value != 0),
            crate::ZMQ_THREAD_AFFINITY_CPU_ADD | crate::ZMQ_THREAD_AFFINITY_CPU_REMOVE => {}
            crate::ZMQ_THREAD_NAME_PREFIX => {}
            _ => return Err(Error::InvalidArgument),
        }
        Ok(())
    }

    pub fn get_option(&self, option: i32) -> Result<i32> {
        let options = self.options.lock().map_err(|_| Error::InvalidContext)?;
        match option {
            crate::ZMQ_IO_THREADS => Ok(options.io_threads),
            crate::ZMQ_MAX_SOCKETS => Ok(options.max_sockets),
            crate::ZMQ_SOCKET_LIMIT => Ok(options.max_sockets),
            crate::ZMQ_MAX_MSGSZ => Ok(options.max_msgsz),
            crate::ZMQ_MSG_T_SIZE => Ok(64),
            crate::ZMQ_THREAD_SCHED_POLICY => Ok(options.thread_sched_policy),
            crate::ZMQ_ZERO_COPY_RECV => Ok(options.zero_copy_recv),
            _ => Err(Error::InvalidArgument),
        }
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
