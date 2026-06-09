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
    pending_inproc: Mutex<HashMap<String, Vec<InprocPeer>>>,
}

pub(crate) type MessageQueue = Arc<Mutex<VecDeque<Message>>>;
pub(crate) type SubscriptionSet = Arc<Mutex<Vec<Vec<u8>>>>;
pub(crate) type WelcomeMessage = Arc<Mutex<Option<Vec<u8>>>>;

#[derive(Debug)]
pub(crate) struct InprocEndpoint {
    binder_inbox: MessageQueue,
    binder_type: SocketType,
    binder_subscriptions: SubscriptionSet,
    binder_welcome: WelcomeMessage,
    peers: Mutex<Vec<InprocPeer>>,
    next_peer: AtomicUsize,
}

#[derive(Debug, Clone)]
struct InprocPeer {
    id: usize,
    inbox: MessageQueue,
    socket_type: SocketType,
    subscriptions: SubscriptionSet,
}

impl InprocEndpoint {
    pub(crate) fn new(
        binder_inbox: MessageQueue,
        binder_type: SocketType,
        binder_subscriptions: SubscriptionSet,
        binder_welcome: WelcomeMessage,
    ) -> Self {
        Self {
            binder_inbox,
            binder_type,
            binder_subscriptions,
            binder_welcome,
            peers: Mutex::new(Vec::new()),
            next_peer: AtomicUsize::new(0),
        }
    }

    pub(crate) fn binder_inbox(&self) -> MessageQueue {
        Arc::clone(&self.binder_inbox)
    }

    pub(crate) fn binder_type(&self) -> SocketType {
        self.binder_type
    }

    pub(crate) fn binder_accepts(&self, message: &Message) -> Result<bool> {
        socket_type_accepts_message(self.binder_type, &self.binder_subscriptions, message)
    }

    pub(crate) fn add_peer(
        &self,
        peer_id: usize,
        peer_inbox: MessageQueue,
        peer_type: SocketType,
        peer_subscriptions: SubscriptionSet,
    ) -> Result<()> {
        if !inproc_types_compatible(self.binder_type, peer_type) {
            return Err(Error::NotSupported);
        }
        let mut peers = self.peers.lock().map_err(|_| Error::InvalidSocket)?;
        peers.push(InprocPeer {
            id: peer_id,
            inbox: peer_inbox,
            socket_type: peer_type,
            subscriptions: peer_subscriptions,
        });
        let peer = peers.last().cloned();
        drop(peers);
        if let Some(peer) = peer {
            self.replay_xsub_subscriptions(&peer)?;
            self.send_welcome_to_xsub(&peer)?;
        }
        Ok(())
    }

    pub(crate) fn remove_peer(&self, peer_id: usize) -> Result<bool> {
        let mut peers = self.peers.lock().map_err(|_| Error::InvalidSocket)?;
        let previous_len = peers.len();
        peers.retain(|peer| peer.id != peer_id);
        Ok(peers.len() != previous_len)
    }

    pub(crate) fn first_peer(&self) -> Result<Option<MessageQueue>> {
        let peers = self.peers.lock().map_err(|_| Error::InvalidSocket)?;
        Ok(peers.first().map(|peer| Arc::clone(&peer.inbox)))
    }

    pub(crate) fn next_peer(&self) -> Result<Option<MessageQueue>> {
        let peers = self.peers.lock().map_err(|_| Error::InvalidSocket)?;
        if peers.is_empty() {
            return Ok(None);
        }
        let index = self.next_peer.fetch_add(1, Ordering::Relaxed) % peers.len();
        Ok(Some(Arc::clone(&peers[index].inbox)))
    }

    pub(crate) fn peer_by_id(&self, id: usize) -> Result<Option<MessageQueue>> {
        let peers = self.peers.lock().map_err(|_| Error::InvalidSocket)?;
        Ok(peers
            .iter()
            .find(|peer| peer.id == id)
            .map(|peer| Arc::clone(&peer.inbox)))
    }

    pub(crate) fn matching_peers(&self, message: &Message) -> Result<Vec<MessageQueue>> {
        let peers = self.peers.lock().map_err(|_| Error::InvalidSocket)?;
        let mut outboxes = Vec::new();
        for peer in peers.iter() {
            if socket_type_accepts_message(peer.socket_type, &peer.subscriptions, message)? {
                outboxes.push(Arc::clone(&peer.inbox));
            }
        }
        Ok(outboxes)
    }

    pub(crate) fn replay_subscription(&self, prefix: &[u8]) -> Result<()> {
        if self.binder_type != SocketType::Xpub {
            return Ok(());
        }
        let mut frame = Vec::with_capacity(prefix.len() + 1);
        frame.push(1);
        frame.extend_from_slice(prefix);
        self.binder_inbox
            .lock()
            .map_err(|_| Error::InvalidSocket)?
            .push_back(Message::from_vec(frame));
        Ok(())
    }

    fn replay_xsub_subscriptions(&self, peer: &InprocPeer) -> Result<()> {
        if self.binder_type != SocketType::Xpub || peer.socket_type != SocketType::Xsub {
            return Ok(());
        }
        let subscriptions = peer
            .subscriptions
            .lock()
            .map_err(|_| Error::InvalidSocket)?;
        for prefix in subscriptions.iter() {
            self.replay_subscription(prefix)?;
        }
        Ok(())
    }

    fn send_welcome_to_xsub(&self, peer: &InprocPeer) -> Result<()> {
        if self.binder_type != SocketType::Xpub || peer.socket_type != SocketType::Xsub {
            return Ok(());
        }
        if let Some(message) = self
            .binder_welcome
            .lock()
            .map_err(|_| Error::InvalidSocket)?
            .clone()
        {
            peer.inbox
                .lock()
                .map_err(|_| Error::InvalidSocket)?
                .push_back(Message::from_vec(message));
        }
        Ok(())
    }
}

fn inproc_types_compatible(a: SocketType, b: SocketType) -> bool {
    matches!(
        (a, b),
        (SocketType::Pair, SocketType::Pair)
            | (SocketType::Push, SocketType::Pull)
            | (SocketType::Pull, SocketType::Push)
            | (SocketType::Dealer, SocketType::Router)
            | (SocketType::Router, SocketType::Dealer)
            | (SocketType::Req, SocketType::Rep)
            | (SocketType::Rep, SocketType::Req)
            | (SocketType::Pub, SocketType::Sub)
            | (SocketType::Sub, SocketType::Pub)
            | (SocketType::Xpub, SocketType::Xsub)
            | (SocketType::Xsub, SocketType::Xpub)
            | (SocketType::Stream, SocketType::Stream)
    )
}

fn socket_type_accepts_message(
    socket_type: SocketType,
    subscriptions: &SubscriptionSet,
    message: &Message,
) -> Result<bool> {
    if !matches!(socket_type, SocketType::Sub | SocketType::Xsub) {
        return Ok(true);
    }
    let subscriptions = subscriptions.lock().map_err(|_| Error::InvalidSocket)?;
    Ok(subscriptions
        .iter()
        .any(|prefix| message.data().starts_with(prefix)))
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
        _id: usize,
        inbox: MessageQueue,
        socket_type: SocketType,
        subscriptions: SubscriptionSet,
        welcome: WelcomeMessage,
    ) -> Result<Arc<InprocEndpoint>> {
        let mut endpoints = self
            .inproc_endpoints
            .lock()
            .map_err(|_| Error::InvalidContext)?;
        if endpoints.contains_key(endpoint) {
            return Err(Error::InvalidArgument);
        }
        let endpoint_state = Arc::new(InprocEndpoint::new(
            inbox,
            socket_type,
            subscriptions,
            welcome,
        ));
        let pending_peers = self
            .pending_inproc
            .lock()
            .map_err(|_| Error::InvalidContext)?
            .remove(endpoint)
            .unwrap_or_default();
        for peer in pending_peers {
            endpoint_state.add_peer(peer.id, peer.inbox, peer.socket_type, peer.subscriptions)?;
        }
        endpoints.insert(endpoint.to_string(), Arc::clone(&endpoint_state));
        Ok(endpoint_state)
    }

    pub(crate) fn connect_inproc(
        &self,
        endpoint: &str,
        id: usize,
        inbox: MessageQueue,
        socket_type: SocketType,
        subscriptions: SubscriptionSet,
    ) -> Result<Option<Arc<InprocEndpoint>>> {
        let endpoints = self
            .inproc_endpoints
            .lock()
            .map_err(|_| Error::InvalidContext)?;
        if let Some(endpoint_state) = endpoints.get(endpoint).cloned() {
            endpoint_state.add_peer(id, inbox, socket_type, subscriptions)?;
            return Ok(Some(endpoint_state));
        }
        drop(endpoints);

        let mut pending = self
            .pending_inproc
            .lock()
            .map_err(|_| Error::InvalidContext)?;
        pending
            .entry(endpoint.to_string())
            .or_default()
            .push(InprocPeer {
                id,
                inbox,
                socket_type,
                subscriptions,
            });
        Ok(None)
    }

    pub(crate) fn unbind_inproc(&self, endpoint: &str) -> Result<Arc<InprocEndpoint>> {
        self.inproc_endpoints
            .lock()
            .map_err(|_| Error::InvalidContext)?
            .remove(endpoint)
            .ok_or(Error::InvalidArgument)
    }

    pub(crate) fn disconnect_inproc(&self, endpoint: &str, id: usize) -> Result<()> {
        let mut removed = false;
        if let Some(endpoint_state) = self
            .inproc_endpoints
            .lock()
            .map_err(|_| Error::InvalidContext)?
            .get(endpoint)
            .cloned()
        {
            removed = endpoint_state.remove_peer(id)?;
        }

        let mut pending = self
            .pending_inproc
            .lock()
            .map_err(|_| Error::InvalidContext)?;
        if let Some(peers) = pending.get_mut(endpoint) {
            let previous_len = peers.len();
            peers.retain(|peer| peer.id != id);
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
