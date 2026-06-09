use crate::{Error, Message, Result, Socket, SocketType};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const STATE_RUNNING: u8 = 0;
const STATE_SHUTTING_DOWN: u8 = 1;
const STATE_TERMINATED: u8 = 2;

#[derive(Debug)]
pub struct Context {
    shared: Arc<ContextShared>,
}

#[derive(Debug)]
pub(crate) struct ContextShared {
    state: AtomicU8,
    next_socket_id: AtomicUsize,
    options: Mutex<ContextOptions>,
    inproc_endpoints: Mutex<HashMap<String, Arc<InprocEndpoint>>>,
    pending_inproc: Mutex<HashMap<String, Vec<MessageQueue>>>,
}

pub(crate) type MessageQueue = Arc<Mutex<VecDeque<Message>>>;

#[derive(Debug)]
pub(crate) struct InprocEndpoint {
    binder_inbox: MessageQueue,
    peers: Mutex<Vec<MessageQueue>>,
}

impl InprocEndpoint {
    pub(crate) fn new(binder_inbox: MessageQueue) -> Self {
        Self {
            binder_inbox,
            peers: Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn binder_inbox(&self) -> MessageQueue {
        Arc::clone(&self.binder_inbox)
    }

    pub(crate) fn add_peer(&self, peer_inbox: MessageQueue) -> Result<()> {
        let mut peers = self.peers.lock().map_err(|_| Error::InvalidSocket)?;
        peers.push(peer_inbox);
        Ok(())
    }

    pub(crate) fn remove_peer(&self, peer_inbox: &MessageQueue) -> Result<bool> {
        let mut peers = self.peers.lock().map_err(|_| Error::InvalidSocket)?;
        let previous_len = peers.len();
        peers.retain(|peer| !Arc::ptr_eq(peer, peer_inbox));
        Ok(peers.len() != previous_len)
    }

    pub(crate) fn first_peer(&self) -> Result<Option<MessageQueue>> {
        let peers = self.peers.lock().map_err(|_| Error::InvalidSocket)?;
        Ok(peers.first().cloned())
    }
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
            shared: Arc::new(ContextShared {
                state: AtomicU8::new(STATE_RUNNING),
                next_socket_id: AtomicUsize::new(1),
                options: Mutex::new(ContextOptions::default()),
                inproc_endpoints: Mutex::new(HashMap::new()),
                pending_inproc: Mutex::new(HashMap::new()),
            }),
        })
    }

    pub fn shutdown(&self) -> Result<()> {
        self.shared
            .state
            .store(STATE_SHUTTING_DOWN, Ordering::SeqCst);
        Ok(())
    }

    pub fn terminate(&self) -> Result<()> {
        self.shared.state.store(STATE_TERMINATED, Ordering::SeqCst);
        Ok(())
    }

    pub fn socket(&self, socket_type: SocketType) -> Result<Socket> {
        self.ensure_running()?;
        let id = self.shared.next_socket_id.fetch_add(1, Ordering::Relaxed);
        Ok(Socket::new(id, socket_type, Arc::clone(&self.shared)))
    }

    pub fn set_option(&self, option: i32, value: i32) -> Result<()> {
        if matches!(option, crate::ZMQ_IO_THREADS | crate::ZMQ_MAX_SOCKETS) && value < 0 {
            return Err(Error::InvalidArgument);
        }

        let mut options = self
            .shared
            .options
            .lock()
            .map_err(|_| Error::InvalidContext)?;
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
        let options = self
            .shared
            .options
            .lock()
            .map_err(|_| Error::InvalidContext)?;
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
        self.shared.state.load(Ordering::SeqCst) == STATE_TERMINATED
    }

    fn ensure_running(&self) -> Result<()> {
        match self.shared.state.load(Ordering::SeqCst) {
            STATE_RUNNING => Ok(()),
            STATE_SHUTTING_DOWN | STATE_TERMINATED => Err(Error::Terminated),
            _ => Err(Error::InvalidContext),
        }
    }
}

impl ContextShared {
    pub(crate) fn bind_inproc(
        &self,
        endpoint: &str,
        inbox: MessageQueue,
    ) -> Result<Arc<InprocEndpoint>> {
        let mut endpoints = self
            .inproc_endpoints
            .lock()
            .map_err(|_| Error::InvalidContext)?;
        if endpoints.contains_key(endpoint) {
            return Err(Error::InvalidArgument);
        }
        let endpoint_state = Arc::new(InprocEndpoint::new(inbox));
        let pending_peers = self
            .pending_inproc
            .lock()
            .map_err(|_| Error::InvalidContext)?
            .remove(endpoint)
            .unwrap_or_default();
        for peer in pending_peers {
            endpoint_state.add_peer(peer)?;
        }
        endpoints.insert(endpoint.to_string(), Arc::clone(&endpoint_state));
        Ok(endpoint_state)
    }

    pub(crate) fn connect_inproc(
        &self,
        endpoint: &str,
        inbox: MessageQueue,
    ) -> Result<Option<Arc<InprocEndpoint>>> {
        let endpoints = self
            .inproc_endpoints
            .lock()
            .map_err(|_| Error::InvalidContext)?;
        if let Some(endpoint_state) = endpoints.get(endpoint).cloned() {
            endpoint_state.add_peer(inbox)?;
            return Ok(Some(endpoint_state));
        }
        drop(endpoints);

        let mut pending = self
            .pending_inproc
            .lock()
            .map_err(|_| Error::InvalidContext)?;
        pending.entry(endpoint.to_string()).or_default().push(inbox);
        Ok(None)
    }

    pub(crate) fn unbind_inproc(&self, endpoint: &str) -> Result<Arc<InprocEndpoint>> {
        self.inproc_endpoints
            .lock()
            .map_err(|_| Error::InvalidContext)?
            .remove(endpoint)
            .ok_or(Error::InvalidArgument)
    }

    pub(crate) fn disconnect_inproc(&self, endpoint: &str, inbox: &MessageQueue) -> Result<()> {
        let mut removed = false;
        if let Some(endpoint_state) = self
            .inproc_endpoints
            .lock()
            .map_err(|_| Error::InvalidContext)?
            .get(endpoint)
            .cloned()
        {
            removed = endpoint_state.remove_peer(inbox)?;
        }

        let mut pending = self
            .pending_inproc
            .lock()
            .map_err(|_| Error::InvalidContext)?;
        if let Some(peers) = pending.get_mut(endpoint) {
            let previous_len = peers.len();
            peers.retain(|peer| !Arc::ptr_eq(peer, inbox));
            removed |= peers.len() != previous_len;
            if peers.is_empty() {
                pending.remove(endpoint);
            }
        }

        if removed {
            Ok(())
        } else {
            Err(Error::InvalidArgument)
        }
    }

    pub(crate) fn inproc_endpoint(&self, endpoint: &str) -> Result<Option<Arc<InprocEndpoint>>> {
        Ok(self
            .inproc_endpoints
            .lock()
            .map_err(|_| Error::InvalidContext)?
            .get(endpoint)
            .cloned())
    }
}
