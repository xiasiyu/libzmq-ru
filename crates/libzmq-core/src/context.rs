use crate::{Error, Message, Result, Socket, SocketType};
use std::collections::{HashMap, HashSet, VecDeque};
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
pub(crate) type SubscriptionSet = Arc<Mutex<SubscriptionState>>;
pub(crate) type WelcomeMessage = Arc<Mutex<Option<Vec<u8>>>>;
pub(crate) type XpubSubscriptionPolicy = Arc<Mutex<XpubSubscriptionPolicyState>>;

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct XpubSubscriptionPolicyState {
    pub(crate) verbose_subscribe: bool,
    pub(crate) verbose_unsubscribe: bool,
    pub(crate) manual: bool,
}

#[derive(Debug)]
pub(crate) struct SubscriptionState {
    prefixes: Vec<Vec<u8>>,
    exact: HashMap<Vec<u8>, usize>,
    first_bytes: [bool; 256],
    lengths: Vec<usize>,
    has_empty: bool,
}

impl Default for SubscriptionState {
    fn default() -> Self {
        Self {
            prefixes: Vec::new(),
            exact: HashMap::new(),
            first_bytes: [false; 256],
            lengths: Vec::new(),
            has_empty: false,
        }
    }
}

impl SubscriptionState {
    pub(crate) fn insert(&mut self, prefix: &[u8]) -> bool {
        if let Some(count) = self.exact.get_mut(prefix) {
            *count += 1;
            return false;
        }
        self.exact.insert(prefix.to_vec(), 1);
        self.has_empty |= prefix.is_empty();
        if let Some(first) = prefix.first() {
            self.first_bytes[*first as usize] = true;
        }
        if !self.lengths.contains(&prefix.len()) {
            self.lengths.push(prefix.len());
        }
        self.prefixes.push(prefix.to_vec());
        true
    }

    pub(crate) fn remove(&mut self, prefix: &[u8]) -> bool {
        match self.exact.get_mut(prefix) {
            Some(count) if *count > 1 => {
                *count -= 1;
                return false;
            }
            Some(_) => {}
            None => return false,
        }
        self.exact.remove(prefix);
        self.prefixes.retain(|stored| stored.as_slice() != prefix);
        self.has_empty = self.exact.contains_key(&[][..]);
        self.first_bytes = [false; 256];
        self.lengths.clear();
        for stored in &self.prefixes {
            if let Some(first) = stored.first() {
                self.first_bytes[*first as usize] = true;
            }
            if !self.lengths.contains(&stored.len()) {
                self.lengths.push(stored.len());
            }
        }
        true
    }

    fn iter_prefixes(&self) -> impl Iterator<Item = &[u8]> {
        self.prefixes.iter().map(Vec::as_slice)
    }

    pub(crate) fn matches_prefix_of(&self, data: &[u8]) -> bool {
        if self.has_empty {
            return true;
        }
        if data
            .first()
            .is_none_or(|first| !self.first_bytes[*first as usize])
        {
            return false;
        }
        self.lengths
            .iter()
            .any(|len| *len <= data.len() && self.exact.contains_key(&data[..*len]))
    }

    pub(crate) fn count(&self) -> usize {
        self.exact.len()
    }

    fn contains_exact(&self, value: &[u8]) -> bool {
        self.exact.contains_key(value)
    }
}

#[derive(Debug)]
pub(crate) struct InprocEndpoint {
    binder_inbox: MessageQueue,
    binder_type: SocketType,
    binder_subscriptions: SubscriptionSet,
    binder_welcome: WelcomeMessage,
    binder_xpub_policy: XpubSubscriptionPolicy,
    xpub_peer_topics: Mutex<HashMap<Vec<u8>, HashSet<usize>>>,
    xpub_manual_topics: Mutex<HashMap<Vec<u8>, HashSet<usize>>>,
    xpub_last_peer: Mutex<Option<usize>>,
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
        binder_xpub_policy: XpubSubscriptionPolicy,
    ) -> Self {
        Self {
            binder_inbox,
            binder_type,
            binder_subscriptions,
            binder_welcome,
            binder_xpub_policy,
            xpub_peer_topics: Mutex::new(HashMap::new()),
            xpub_manual_topics: Mutex::new(HashMap::new()),
            xpub_last_peer: Mutex::new(None),
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
        let removed_peer = peers.iter().find(|peer| peer.id == peer_id).cloned();
        let previous_len = peers.len();
        peers.retain(|peer| peer.id != peer_id);
        let removed = peers.len() != previous_len;
        drop(peers);
        if let Some(peer) = removed_peer.filter(|peer| peer.socket_type == SocketType::Xsub) {
            self.remove_xsub_peer_subscriptions(&peer)?;
        }
        Ok(removed)
    }

    fn remove_xsub_peer_subscriptions(&self, peer: &InprocPeer) -> Result<()> {
        if self.binder_type != SocketType::Xpub {
            return Ok(());
        }
        let prefixes: Vec<Vec<u8>> = peer
            .subscriptions
            .lock()
            .map_err(|_| Error::InvalidSocket)?
            .iter_prefixes()
            .map(ToOwned::to_owned)
            .collect();
        for prefix in &prefixes {
            self.replay_unsubscription(peer.id, prefix, false)?;
        }
        let mut manual_topics = self
            .xpub_manual_topics
            .lock()
            .map_err(|_| Error::InvalidSocket)?;
        for prefix in prefixes {
            if let Some(peers) = manual_topics.get_mut(&prefix) {
                peers.remove(&peer.id);
                if peers.is_empty() {
                    manual_topics.remove(&prefix);
                }
            }
        }
        Ok(())
    }

    pub(crate) fn first_peer(&self) -> Result<Option<MessageQueue>> {
        let peers = self.peers.lock().map_err(|_| Error::InvalidSocket)?;
        Ok(peers.first().map(|peer| Arc::clone(&peer.inbox)))
    }

    pub(crate) fn peer_count(&self) -> Result<usize> {
        Ok(self.peers.lock().map_err(|_| Error::InvalidSocket)?.len())
    }

    pub(crate) fn peer_queue_depths(&self) -> Result<Vec<usize>> {
        let peers = self.peers.lock().map_err(|_| Error::InvalidSocket)?;
        peers
            .iter()
            .map(|peer| {
                peer.inbox
                    .lock()
                    .map_err(|_| Error::InvalidSocket)
                    .map(|queue| queue.len())
            })
            .collect()
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
        let manual_xpub = self.binder_type == SocketType::Xpub
            && self
                .binder_xpub_policy
                .lock()
                .map_err(|_| Error::InvalidSocket)?
                .manual;
        let manual_matches = if manual_xpub {
            Some(self.manual_matching_peer_ids(message)?)
        } else {
            None
        };
        let mut outboxes = Vec::new();
        for peer in peers.iter() {
            let accepts = if let Some(manual_matches) = &manual_matches {
                manual_matches.contains(&peer.id)
            } else {
                socket_type_accepts_message(peer.socket_type, &peer.subscriptions, message)?
            };
            if accepts {
                outboxes.push(Arc::clone(&peer.inbox));
            }
        }
        Ok(outboxes)
    }

    fn manual_matching_peer_ids(&self, message: &Message) -> Result<HashSet<usize>> {
        let topics = self
            .xpub_manual_topics
            .lock()
            .map_err(|_| Error::InvalidSocket)?;
        let mut ids = HashSet::new();
        for (prefix, peers) in topics.iter() {
            if prefix.len() <= message.len() && message.data().starts_with(prefix) {
                ids.extend(peers.iter().copied());
            }
        }
        Ok(ids)
    }

    pub(crate) fn manual_subscribe_last_peer(&self, prefix: &[u8]) -> Result<()> {
        if self.binder_type != SocketType::Xpub {
            return Err(Error::NotSupported);
        }
        let Some(peer_id) = *self
            .xpub_last_peer
            .lock()
            .map_err(|_| Error::InvalidSocket)?
        else {
            return Ok(());
        };
        self.xpub_manual_topics
            .lock()
            .map_err(|_| Error::InvalidSocket)?
            .entry(prefix.to_vec())
            .or_default()
            .insert(peer_id);
        Ok(())
    }

    pub(crate) fn manual_unsubscribe_last_peer(&self, prefix: &[u8]) -> Result<()> {
        if self.binder_type != SocketType::Xpub {
            return Err(Error::NotSupported);
        }
        let Some(peer_id) = *self
            .xpub_last_peer
            .lock()
            .map_err(|_| Error::InvalidSocket)?
        else {
            return Ok(());
        };
        let mut topics = self
            .xpub_manual_topics
            .lock()
            .map_err(|_| Error::InvalidSocket)?;
        if let Some(peers) = topics.get_mut(prefix) {
            peers.remove(&peer_id);
            if peers.is_empty() {
                topics.remove(prefix);
            }
        }
        Ok(())
    }

    pub(crate) fn send_owned_to_matching_peers(&self, message: Message) -> Result<usize> {
        let peers = self.peers.lock().map_err(|_| Error::InvalidSocket)?;
        if let [peer] = peers.as_slice() {
            if socket_type_accepts_message(peer.socket_type, &peer.subscriptions, &message)? {
                peer.inbox
                    .lock()
                    .map_err(|_| Error::InvalidSocket)?
                    .push_back(message);
                return Ok(1);
            }
            return Ok(0);
        }
        let mut sent = 0;
        for peer in peers.iter() {
            if socket_type_accepts_message(peer.socket_type, &peer.subscriptions, &message)? {
                peer.inbox
                    .lock()
                    .map_err(|_| Error::InvalidSocket)?
                    .push_back(message.clone());
                sent += 1;
            }
        }
        Ok(sent)
    }

    pub(crate) fn replay_subscription(&self, peer_id: usize, prefix: &[u8]) -> Result<()> {
        if !self.should_replay_subscription(peer_id, prefix, false)? {
            return Ok(());
        }
        self.replay_subscription_change(true, prefix)
    }

    pub(crate) fn replay_duplicate_subscription(
        &self,
        peer_id: usize,
        prefix: &[u8],
    ) -> Result<()> {
        if !self.should_replay_subscription(peer_id, prefix, true)? {
            return Ok(());
        }
        self.replay_subscription_change(true, prefix)
    }

    fn should_replay_subscription(
        &self,
        peer_id: usize,
        prefix: &[u8],
        duplicate_from_peer: bool,
    ) -> Result<bool> {
        if self.binder_type != SocketType::Xpub {
            return Ok(false);
        }
        let policy = *self
            .binder_xpub_policy
            .lock()
            .map_err(|_| Error::InvalidSocket)?;
        if duplicate_from_peer {
            let notify = policy.verbose_subscribe || policy.manual;
            if notify {
                *self
                    .xpub_last_peer
                    .lock()
                    .map_err(|_| Error::InvalidSocket)? = Some(peer_id);
            }
            return Ok(notify);
        }

        let mut topics = self
            .xpub_peer_topics
            .lock()
            .map_err(|_| Error::InvalidSocket)?;
        let peers = topics.entry(prefix.to_vec()).or_default();
        peers.insert(peer_id);
        let notify = peers.len() == 1 || policy.verbose_subscribe || policy.manual;
        if notify {
            *self
                .xpub_last_peer
                .lock()
                .map_err(|_| Error::InvalidSocket)? = Some(peer_id);
        }
        Ok(notify)
    }

    pub(crate) fn replay_unsubscription(
        &self,
        peer_id: usize,
        prefix: &[u8],
        unmatched_from_peer: bool,
    ) -> Result<()> {
        if !self.should_replay_unsubscription(peer_id, prefix, unmatched_from_peer)? {
            return Ok(());
        }
        self.replay_subscription_change(false, prefix)
    }

    fn should_replay_unsubscription(
        &self,
        peer_id: usize,
        prefix: &[u8],
        unmatched_from_peer: bool,
    ) -> Result<bool> {
        if self.binder_type != SocketType::Xpub {
            return Ok(false);
        }
        let policy = *self
            .binder_xpub_policy
            .lock()
            .map_err(|_| Error::InvalidSocket)?;
        if unmatched_from_peer {
            *self
                .xpub_last_peer
                .lock()
                .map_err(|_| Error::InvalidSocket)? = Some(peer_id);
            return Ok(true);
        }

        let mut topics = self
            .xpub_peer_topics
            .lock()
            .map_err(|_| Error::InvalidSocket)?;
        let Some(peers) = topics.get_mut(prefix) else {
            return Ok(true);
        };
        peers.remove(&peer_id);
        if peers.is_empty() {
            topics.remove(prefix);
            *self
                .xpub_last_peer
                .lock()
                .map_err(|_| Error::InvalidSocket)? = Some(peer_id);
            return Ok(true);
        }
        let notify = policy.verbose_unsubscribe || policy.manual;
        if notify {
            *self
                .xpub_last_peer
                .lock()
                .map_err(|_| Error::InvalidSocket)? = Some(peer_id);
        }
        Ok(notify)
    }

    fn replay_subscription_change(&self, subscribe: bool, prefix: &[u8]) -> Result<()> {
        if self.binder_type != SocketType::Xpub {
            return Ok(());
        }
        let mut frame = Vec::with_capacity(prefix.len() + 1);
        frame.push(u8::from(subscribe));
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
        for prefix in subscriptions.iter_prefixes() {
            self.replay_subscription(peer.id, prefix)?;
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
            | (SocketType::Server, SocketType::Client)
            | (SocketType::Client, SocketType::Server)
            | (SocketType::Pub, SocketType::Sub)
            | (SocketType::Sub, SocketType::Pub)
            | (SocketType::Xpub, SocketType::Xsub)
            | (SocketType::Xsub, SocketType::Xpub)
            | (SocketType::Stream, SocketType::Stream)
            | (SocketType::Channel, SocketType::Channel)
            | (SocketType::Peer, SocketType::Peer)
            | (SocketType::Radio, SocketType::Dish)
            | (SocketType::Dish, SocketType::Radio)
            | (SocketType::Scatter, SocketType::Gather)
            | (SocketType::Gather, SocketType::Scatter)
    )
}

fn socket_type_accepts_message(
    socket_type: SocketType,
    subscriptions: &SubscriptionSet,
    message: &Message,
) -> Result<bool> {
    let subscriptions = subscriptions.lock().map_err(|_| Error::InvalidSocket)?;
    match socket_type {
        SocketType::Sub | SocketType::Xsub => Ok(subscriptions.matches_prefix_of(message.data())),
        SocketType::Dish => Ok(message
            .group()
            .is_some_and(|group| subscriptions.contains_exact(group.as_bytes()))),
        _ => Ok(true),
    }
}

fn atoi_i32(bytes: &[u8]) -> i32 {
    let mut index = 0;
    while bytes
        .get(index)
        .is_some_and(|byte| matches!(byte, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c))
    {
        index += 1;
    }
    let sign = match bytes.get(index) {
        Some(b'-') => {
            index += 1;
            -1
        }
        Some(b'+') => {
            index += 1;
            1
        }
        _ => 1,
    };
    let mut value = 0i32;
    while let Some(digit) = bytes
        .get(index)
        .and_then(|byte| byte.is_ascii_digit().then_some((byte - b'0') as i32))
    {
        value = value.saturating_mul(10).saturating_add(digit);
        index += 1;
    }
    value.saturating_mul(sign)
}

#[derive(Debug, Clone)]
struct ContextOptions {
    io_threads: i32,
    max_sockets: i32,
    max_msgsz: i32,
    thread_priority: i32,
    thread_sched_policy: i32,
    zero_copy_recv: i32,
    thread_name_prefix: Vec<u8>,
    thread_affinity_cpus: HashSet<i32>,
    ipv6: bool,
    blocky: bool,
}

impl Default for ContextOptions {
    fn default() -> Self {
        Self {
            io_threads: crate::ZMQ_IO_THREADS_DFLT,
            max_sockets: crate::ZMQ_MAX_SOCKETS_DFLT,
            max_msgsz: i32::MAX,
            thread_priority: crate::ZMQ_THREAD_PRIORITY_DFLT,
            thread_sched_policy: crate::ZMQ_THREAD_SCHED_POLICY_DFLT,
            zero_copy_recv: 1,
            thread_name_prefix: Vec::new(),
            thread_affinity_cpus: HashSet::new(),
            ipv6: false,
            blocky: true,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ContextSocketDefaults {
    pub(crate) ipv6: bool,
    pub(crate) blocky: bool,
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
        Ok(Socket::new(
            id,
            socket_type,
            Arc::clone(&self.shared),
            self.socket_defaults()?,
        ))
    }

    pub fn set_option(&self, option: i32, value: i32) -> Result<()> {
        if matches!(
            option,
            crate::ZMQ_IO_THREADS
                | crate::ZMQ_MAX_MSGSZ
                | crate::ZMQ_THREAD_PRIORITY
                | crate::ZMQ_THREAD_SCHED_POLICY
                | crate::ZMQ_THREAD_AFFINITY_CPU_ADD
                | crate::ZMQ_THREAD_AFFINITY_CPU_REMOVE
                | crate::ZMQ_ZERO_COPY_RECV
                | crate::ZMQ_IPV6
                | crate::ZMQ_BLOCKY
        ) && value < 0
        {
            return Err(Error::InvalidArgument);
        }
        if option == crate::ZMQ_MAX_SOCKETS && value < 1 {
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
            crate::ZMQ_IPV6 => options.ipv6 = value != 0,
            crate::ZMQ_BLOCKY => options.blocky = value != 0,
            crate::ZMQ_THREAD_PRIORITY => options.thread_priority = value,
            crate::ZMQ_THREAD_SCHED_POLICY => options.thread_sched_policy = value,
            crate::ZMQ_ZERO_COPY_RECV => options.zero_copy_recv = i32::from(value != 0),
            crate::ZMQ_THREAD_AFFINITY_CPU_ADD => {
                options.thread_affinity_cpus.insert(value);
            }
            crate::ZMQ_THREAD_AFFINITY_CPU_REMOVE => {
                if !options.thread_affinity_cpus.remove(&value) {
                    return Err(Error::InvalidArgument);
                }
            }
            crate::ZMQ_THREAD_NAME_PREFIX => {
                options.thread_name_prefix = value.to_string().into_bytes()
            }
            _ => return Err(Error::InvalidArgument),
        }
        Ok(())
    }

    pub fn set_option_bytes(&self, option: i32, value: &[u8]) -> Result<()> {
        let mut options = self
            .shared
            .options
            .lock()
            .map_err(|_| Error::InvalidContext)?;
        match option {
            crate::ZMQ_THREAD_NAME_PREFIX if !value.is_empty() && value.len() <= 16 => {
                options.thread_name_prefix = value.to_vec();
            }
            crate::ZMQ_THREAD_NAME_PREFIX => return Err(Error::InvalidArgument),
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
            crate::ZMQ_SOCKET_LIMIT => Ok(65_535),
            crate::ZMQ_MAX_MSGSZ => Ok(options.max_msgsz),
            crate::ZMQ_MSG_T_SIZE => Ok(64),
            crate::ZMQ_IPV6 => Ok(i32::from(options.ipv6)),
            crate::ZMQ_BLOCKY => Ok(i32::from(options.blocky)),
            crate::ZMQ_THREAD_SCHED_POLICY => Ok(options.thread_sched_policy),
            crate::ZMQ_THREAD_NAME_PREFIX => Ok(atoi_i32(&options.thread_name_prefix)),
            crate::ZMQ_ZERO_COPY_RECV => Ok(options.zero_copy_recv),
            _ => Err(Error::InvalidArgument),
        }
    }

    pub fn get_option_bytes(&self, option: i32) -> Result<Vec<u8>> {
        let options = self
            .shared
            .options
            .lock()
            .map_err(|_| Error::InvalidContext)?;
        match option {
            crate::ZMQ_THREAD_NAME_PREFIX => Ok(options.thread_name_prefix.clone()),
            _ => Err(Error::InvalidArgument),
        }
    }

    pub(crate) fn socket_defaults(&self) -> Result<ContextSocketDefaults> {
        let options = self
            .shared
            .options
            .lock()
            .map_err(|_| Error::InvalidContext)?;
        Ok(ContextSocketDefaults {
            ipv6: options.ipv6,
            blocky: options.blocky,
        })
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
    pub(crate) fn next_transient_socket_id(&self) -> usize {
        self.next_socket_id.fetch_add(1, Ordering::SeqCst)
    }

    pub(crate) fn bind_inproc(
        &self,
        endpoint: &str,
        inbox: MessageQueue,
        socket_type: SocketType,
        subscriptions: SubscriptionSet,
        welcome: WelcomeMessage,
        xpub_policy: XpubSubscriptionPolicy,
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
            xpub_policy,
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
