use crate::constants::*;
use crate::context::{
    ContextShared, InprocEndpoint, MessageQueue, SubscriptionSet, WelcomeMessage,
};
use crate::{Error, Message, Result};
use std::collections::VecDeque;
use std::convert::TryFrom;
use std::sync::{Arc, Mutex};

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SocketType {
    Pair = ZMQ_PAIR,
    Pub = ZMQ_PUB,
    Sub = ZMQ_SUB,
    Req = ZMQ_REQ,
    Rep = ZMQ_REP,
    Dealer = ZMQ_DEALER,
    Router = ZMQ_ROUTER,
    Pull = ZMQ_PULL,
    Push = ZMQ_PUSH,
    Xpub = ZMQ_XPUB,
    Xsub = ZMQ_XSUB,
    Stream = ZMQ_STREAM,
    Server = ZMQ_SERVER,
    Client = ZMQ_CLIENT,
    Radio = ZMQ_RADIO,
    Dish = ZMQ_DISH,
    Gather = ZMQ_GATHER,
    Scatter = ZMQ_SCATTER,
    Dgram = ZMQ_DGRAM,
    Peer = ZMQ_PEER,
    Channel = ZMQ_CHANNEL,
}

impl TryFrom<i32> for SocketType {
    type Error = Error;

    fn try_from(value: i32) -> Result<Self> {
        match value {
            ZMQ_PAIR => Ok(Self::Pair),
            ZMQ_PUB => Ok(Self::Pub),
            ZMQ_SUB => Ok(Self::Sub),
            ZMQ_REQ => Ok(Self::Req),
            ZMQ_REP => Ok(Self::Rep),
            ZMQ_DEALER => Ok(Self::Dealer),
            ZMQ_ROUTER => Ok(Self::Router),
            ZMQ_PULL => Ok(Self::Pull),
            ZMQ_PUSH => Ok(Self::Push),
            ZMQ_XPUB => Ok(Self::Xpub),
            ZMQ_XSUB => Ok(Self::Xsub),
            ZMQ_STREAM => Ok(Self::Stream),
            ZMQ_SERVER => Ok(Self::Server),
            ZMQ_CLIENT => Ok(Self::Client),
            ZMQ_RADIO => Ok(Self::Radio),
            ZMQ_DISH => Ok(Self::Dish),
            ZMQ_GATHER => Ok(Self::Gather),
            ZMQ_SCATTER => Ok(Self::Scatter),
            ZMQ_DGRAM => Ok(Self::Dgram),
            ZMQ_PEER => Ok(Self::Peer),
            ZMQ_CHANNEL => Ok(Self::Channel),
            _ => Err(Error::InvalidArgument),
        }
    }
}

#[derive(Debug)]
pub struct Socket {
    id: usize,
    socket_type: SocketType,
    options: Mutex<SocketOptions>,
    context: Arc<ContextShared>,
    inbox: MessageQueue,
    subscriptions: SubscriptionSet,
    xpub_welcome: WelcomeMessage,
    inproc: Mutex<InprocState>,
    last_recv_more: Mutex<bool>,
    last_recv_routing_id: Mutex<Option<u32>>,
    pattern_state: Mutex<Option<PatternState>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PatternState {
    ReadyToSend,
    ReadyToRecv,
}

#[derive(Debug, Default)]
struct InprocState {
    direct_outbox: Option<MessageQueue>,
    connected_endpoint: Option<String>,
    bound_endpoint_name: Option<String>,
    bound_endpoint: Option<Arc<InprocEndpoint>>,
}

#[derive(Debug, Clone)]
struct SocketOptions {
    linger: i32,
    sndhwm: i32,
    rcvhwm: i32,
    sndtimeo: i32,
    rcvtimeo: i32,
    conflate: bool,
    router_mandatory: bool,
    router_handover: bool,
    req_correlate: bool,
    req_relaxed: bool,
    xpub_verbose: bool,
    xpub_verboser: bool,
    xpub_nodrop: bool,
    xpub_manual: bool,
}

impl Default for SocketOptions {
    fn default() -> Self {
        Self {
            linger: -1,
            sndhwm: 1000,
            rcvhwm: 1000,
            sndtimeo: -1,
            rcvtimeo: -1,
            conflate: false,
            router_mandatory: false,
            router_handover: false,
            req_correlate: false,
            req_relaxed: false,
            xpub_verbose: false,
            xpub_verboser: false,
            xpub_nodrop: false,
            xpub_manual: false,
        }
    }
}

impl Socket {
    pub(crate) fn new(id: usize, socket_type: SocketType, context: Arc<ContextShared>) -> Self {
        Self {
            id,
            socket_type,
            options: Mutex::new(SocketOptions::default()),
            context,
            inbox: Arc::new(Mutex::new(VecDeque::new())),
            subscriptions: Arc::new(Mutex::new(Vec::new())),
            xpub_welcome: Arc::new(Mutex::new(None)),
            inproc: Mutex::new(InprocState::default()),
            last_recv_more: Mutex::new(false),
            last_recv_routing_id: Mutex::new(None),
            pattern_state: Mutex::new(match socket_type {
                SocketType::Req => Some(PatternState::ReadyToSend),
                SocketType::Rep => Some(PatternState::ReadyToRecv),
                _ => None,
            }),
        }
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn socket_type(&self) -> SocketType {
        self.socket_type
    }

    pub fn bind(&self, endpoint: &str) -> Result<()> {
        let endpoint = endpoint
            .strip_prefix("inproc://")
            .ok_or(Error::NotSupported)?;
        let bound = self.context.bind_inproc(
            endpoint,
            self.id,
            Arc::clone(&self.inbox),
            self.socket_type,
            Arc::clone(&self.subscriptions),
            Arc::clone(&self.xpub_welcome),
        )?;
        let mut inproc = self.inproc.lock().map_err(|_| Error::InvalidSocket)?;
        inproc.bound_endpoint_name = Some(endpoint.to_string());
        inproc.bound_endpoint = Some(bound);
        Ok(())
    }

    pub fn unbind(&self, endpoint: &str) -> Result<()> {
        let endpoint = endpoint
            .strip_prefix("inproc://")
            .ok_or(Error::NotSupported)?;
        let mut inproc = self.inproc.lock().map_err(|_| Error::InvalidSocket)?;
        if inproc.bound_endpoint_name.as_deref() != Some(endpoint) {
            return Err(Error::InvalidArgument);
        }
        self.context.unbind_inproc(endpoint)?;
        inproc.bound_endpoint_name = None;
        inproc.bound_endpoint = None;
        Ok(())
    }

    pub fn connect(&self, endpoint: &str) -> Result<()> {
        let endpoint = endpoint
            .strip_prefix("inproc://")
            .ok_or(Error::NotSupported)?;
        let bound = self.context.connect_inproc(
            endpoint,
            self.id,
            Arc::clone(&self.inbox),
            self.socket_type,
            Arc::clone(&self.subscriptions),
        )?;
        let mut inproc = self.inproc.lock().map_err(|_| Error::InvalidSocket)?;
        inproc.connected_endpoint = Some(endpoint.to_string());
        inproc.direct_outbox = bound.map(|bound| bound.binder_inbox());
        Ok(())
    }

    pub fn disconnect(&self, endpoint: &str) -> Result<()> {
        let endpoint = endpoint
            .strip_prefix("inproc://")
            .ok_or(Error::NotSupported)?;
        let mut inproc = self.inproc.lock().map_err(|_| Error::InvalidSocket)?;
        if inproc.connected_endpoint.as_deref() != Some(endpoint) {
            return Err(Error::InvalidArgument);
        }
        self.context.disconnect_inproc(endpoint, self.id)?;
        inproc.connected_endpoint = None;
        inproc.direct_outbox = None;
        Ok(())
    }

    pub fn send(&self, mut message: Message, flags: i32) -> Result<usize> {
        if !self.can_send() {
            return Err(Error::NotSupported);
        }
        self.ensure_can_send_for_pattern()?;
        let size = message.len();
        message.set_more(flags & ZMQ_SNDMORE != 0);
        self.apply_reply_routing_id(&mut message)?;
        self.apply_outgoing_routing_id(&mut message)?;
        let outboxes = self.resolve_outboxes(&message)?;
        if outboxes.is_empty() {
            if matches!(self.socket_type, SocketType::Pub | SocketType::Xpub) {
                if self.socket_type == SocketType::Xpub
                    && self
                        .options
                        .lock()
                        .map_err(|_| Error::InvalidSocket)?
                        .xpub_nodrop
                {
                    return Err(Error::Again);
                }
                self.after_pattern_send()?;
                return Ok(size);
            }
            if self.socket_type == SocketType::Router
                && message.routing_id() != 0
                && self
                    .options
                    .lock()
                    .map_err(|_| Error::InvalidSocket)?
                    .router_mandatory
            {
                return Err(Error::HostUnreachable);
            }
            return Err(Error::Again);
        }
        let options = self
            .options
            .lock()
            .map_err(|_| Error::InvalidSocket)?
            .clone();
        for outbox in outboxes {
            let mut queue = outbox.lock().map_err(|_| Error::InvalidSocket)?;
            if options.conflate {
                queue.clear();
            } else if options.sndhwm > 0 && queue.len() >= options.sndhwm as usize {
                return Err(Error::Again);
            }
            queue.push_back(message.clone());
        }
        self.after_pattern_send()?;
        Ok(size)
    }

    pub fn recv(&self, _flags: i32) -> Result<Message> {
        if !self.can_recv() {
            return Err(Error::NotSupported);
        }
        self.ensure_can_recv_for_pattern()?;
        let mut inbox = self.inbox.lock().map_err(|_| Error::InvalidSocket)?;
        let message = inbox.pop_front().ok_or(Error::Again)?;
        *self
            .last_recv_more
            .lock()
            .map_err(|_| Error::InvalidSocket)? = message.more();
        if message.routing_id() != 0 {
            *self
                .last_recv_routing_id
                .lock()
                .map_err(|_| Error::InvalidSocket)? = Some(message.routing_id());
        }
        self.after_pattern_recv()?;
        Ok(message)
    }

    pub fn subscribe(&self, prefix: &[u8]) -> Result<()> {
        if !matches!(self.socket_type, SocketType::Sub | SocketType::Xsub) {
            return Err(Error::NotSupported);
        }
        let mut subscriptions = self
            .subscriptions
            .lock()
            .map_err(|_| Error::InvalidSocket)?;
        if !subscriptions
            .iter()
            .any(|stored| stored.as_slice() == prefix)
        {
            subscriptions.push(prefix.to_vec());
        }
        drop(subscriptions);
        let inproc = self.inproc.lock().map_err(|_| Error::InvalidSocket)?;
        if let Some(endpoint) = &inproc.connected_endpoint {
            if let Some(bound_endpoint) = self.context.inproc_endpoint(endpoint)? {
                bound_endpoint.replay_subscription(prefix)?;
            }
        }
        Ok(())
    }

    pub fn unsubscribe(&self, prefix: &[u8]) -> Result<()> {
        if !matches!(self.socket_type, SocketType::Sub | SocketType::Xsub) {
            return Err(Error::NotSupported);
        }
        let mut subscriptions = self
            .subscriptions
            .lock()
            .map_err(|_| Error::InvalidSocket)?;
        subscriptions.retain(|stored| stored.as_slice() != prefix);
        Ok(())
    }

    pub fn set_option_i32(&self, option: i32, value: i32) -> Result<()> {
        let mut options = self.options.lock().map_err(|_| Error::InvalidSocket)?;
        match option {
            ZMQ_LINGER => options.linger = value,
            ZMQ_SNDHWM if value >= 0 => options.sndhwm = value,
            ZMQ_RCVHWM if value >= 0 => options.rcvhwm = value,
            ZMQ_SNDTIMEO if value >= -1 => options.sndtimeo = value,
            ZMQ_RCVTIMEO if value >= -1 => options.rcvtimeo = value,
            ZMQ_CONFLATE => options.conflate = value != 0,
            ZMQ_ROUTER_MANDATORY => options.router_mandatory = value != 0,
            ZMQ_ROUTER_HANDOVER => options.router_handover = value != 0,
            ZMQ_REQ_CORRELATE => options.req_correlate = value != 0,
            ZMQ_REQ_RELAXED => options.req_relaxed = value != 0,
            ZMQ_XPUB_VERBOSE => options.xpub_verbose = value != 0,
            ZMQ_XPUB_VERBOSER => options.xpub_verboser = value != 0,
            ZMQ_XPUB_NODROP => options.xpub_nodrop = value != 0,
            ZMQ_XPUB_MANUAL => options.xpub_manual = value != 0,
            ZMQ_SNDHWM | ZMQ_RCVHWM | ZMQ_SNDTIMEO | ZMQ_RCVTIMEO => {
                return Err(Error::InvalidArgument)
            }
            _ => return Err(Error::InvalidArgument),
        }
        Ok(())
    }

    pub fn set_option_bytes(&self, option: i32, value: &[u8]) -> Result<()> {
        match option {
            ZMQ_XPUB_WELCOME_MSG if self.socket_type == SocketType::Xpub => {
                *self.xpub_welcome.lock().map_err(|_| Error::InvalidSocket)? = Some(value.to_vec());
                Ok(())
            }
            ZMQ_XPUB_WELCOME_MSG => Err(Error::NotSupported),
            _ => Err(Error::InvalidArgument),
        }
    }

    pub fn get_option_i32(&self, option: i32) -> Result<i32> {
        let options = self.options.lock().map_err(|_| Error::InvalidSocket)?;
        match option {
            ZMQ_TYPE => Ok(self.socket_type as i32),
            ZMQ_LINGER => Ok(options.linger),
            ZMQ_SNDHWM => Ok(options.sndhwm),
            ZMQ_RCVHWM => Ok(options.rcvhwm),
            ZMQ_SNDTIMEO => Ok(options.sndtimeo),
            ZMQ_RCVTIMEO => Ok(options.rcvtimeo),
            ZMQ_CONFLATE => Ok(i32::from(options.conflate)),
            ZMQ_ROUTER_MANDATORY => Ok(i32::from(options.router_mandatory)),
            ZMQ_ROUTER_HANDOVER => Ok(i32::from(options.router_handover)),
            ZMQ_REQ_CORRELATE => Ok(i32::from(options.req_correlate)),
            ZMQ_REQ_RELAXED => Ok(i32::from(options.req_relaxed)),
            ZMQ_XPUB_VERBOSE => Ok(i32::from(options.xpub_verbose)),
            ZMQ_XPUB_VERBOSER => Ok(i32::from(options.xpub_verboser)),
            ZMQ_XPUB_NODROP => Ok(i32::from(options.xpub_nodrop)),
            ZMQ_XPUB_MANUAL => Ok(i32::from(options.xpub_manual)),
            ZMQ_RCVMORE => Ok(i32::from(
                *self
                    .last_recv_more
                    .lock()
                    .map_err(|_| Error::InvalidSocket)?,
            )),
            ZMQ_THREAD_SAFE => Ok(0),
            _ => Err(Error::InvalidArgument),
        }
    }

    fn resolve_outboxes(&self, message: &Message) -> Result<Vec<MessageQueue>> {
        let inproc = self.inproc.lock().map_err(|_| Error::InvalidSocket)?;
        if let Some(endpoint) = &inproc.connected_endpoint {
            if let Some(bound_endpoint) = self.context.inproc_endpoint(endpoint)? {
                return if bound_endpoint.binder_accepts(message)? {
                    Ok(vec![bound_endpoint.binder_inbox()])
                } else {
                    Ok(Vec::new())
                };
            }
        }
        if let Some(outbox) = &inproc.direct_outbox {
            return Ok(vec![Arc::clone(outbox)]);
        }
        if let Some(bound_endpoint) = &inproc.bound_endpoint {
            return match self.socket_type {
                SocketType::Pub | SocketType::Xpub => bound_endpoint.matching_peers(message),
                SocketType::Router if message.routing_id() != 0 => Ok(bound_endpoint
                    .peer_by_id(message.routing_id() as usize)?
                    .into_iter()
                    .collect()),
                SocketType::Rep if message.routing_id() != 0 => Ok(bound_endpoint
                    .peer_by_id(message.routing_id() as usize)?
                    .into_iter()
                    .collect()),
                SocketType::Push => Ok(bound_endpoint.next_peer()?.into_iter().collect()),
                _ => Ok(bound_endpoint.first_peer()?.into_iter().collect()),
            };
        }
        Err(Error::Again)
    }

    fn apply_outgoing_routing_id(&self, message: &mut Message) -> Result<()> {
        let inproc = self.inproc.lock().map_err(|_| Error::InvalidSocket)?;
        if let Some(endpoint) = &inproc.connected_endpoint {
            if let Some(bound_endpoint) = self.context.inproc_endpoint(endpoint)? {
                if matches!(
                    bound_endpoint.binder_type(),
                    SocketType::Router | SocketType::Rep
                ) {
                    message.set_routing_id(self.id as u32);
                }
            }
        }
        Ok(())
    }

    fn apply_reply_routing_id(&self, message: &mut Message) -> Result<()> {
        if self.socket_type == SocketType::Rep && message.routing_id() == 0 {
            if let Some(routing_id) = *self
                .last_recv_routing_id
                .lock()
                .map_err(|_| Error::InvalidSocket)?
            {
                message.set_routing_id(routing_id);
            }
        }
        Ok(())
    }

    fn can_send(&self) -> bool {
        matches!(
            self.socket_type,
            SocketType::Pair
                | SocketType::Push
                | SocketType::Dealer
                | SocketType::Router
                | SocketType::Req
                | SocketType::Rep
                | SocketType::Pub
                | SocketType::Xpub
                | SocketType::Xsub
                | SocketType::Stream
        )
    }

    fn can_recv(&self) -> bool {
        matches!(
            self.socket_type,
            SocketType::Pair
                | SocketType::Pull
                | SocketType::Dealer
                | SocketType::Router
                | SocketType::Req
                | SocketType::Rep
                | SocketType::Sub
                | SocketType::Xpub
                | SocketType::Xsub
                | SocketType::Stream
        )
    }

    fn ensure_can_send_for_pattern(&self) -> Result<()> {
        let state = self
            .pattern_state
            .lock()
            .map_err(|_| Error::InvalidSocket)?;
        let options = self.options.lock().map_err(|_| Error::InvalidSocket)?;
        match *state {
            Some(PatternState::ReadyToRecv)
                if self.socket_type == SocketType::Req && options.req_relaxed =>
            {
                Ok(())
            }
            Some(PatternState::ReadyToRecv) => Err(Error::InvalidState),
            _ => Ok(()),
        }
    }

    fn after_pattern_send(&self) -> Result<()> {
        let mut state = self
            .pattern_state
            .lock()
            .map_err(|_| Error::InvalidSocket)?;
        match *state {
            Some(PatternState::ReadyToSend) => *state = Some(PatternState::ReadyToRecv),
            Some(PatternState::ReadyToRecv)
                if self.socket_type == SocketType::Req
                    && self
                        .options
                        .lock()
                        .map_err(|_| Error::InvalidSocket)?
                        .req_relaxed =>
            {
                *state = Some(PatternState::ReadyToRecv);
            }
            Some(PatternState::ReadyToRecv) => return Err(Error::InvalidState),
            None => {}
        }
        Ok(())
    }

    fn ensure_can_recv_for_pattern(&self) -> Result<()> {
        let state = self
            .pattern_state
            .lock()
            .map_err(|_| Error::InvalidSocket)?;
        match *state {
            Some(PatternState::ReadyToSend) => Err(Error::InvalidState),
            _ => Ok(()),
        }
    }

    fn after_pattern_recv(&self) -> Result<()> {
        let mut state = self
            .pattern_state
            .lock()
            .map_err(|_| Error::InvalidSocket)?;
        match *state {
            Some(PatternState::ReadyToRecv) => *state = Some(PatternState::ReadyToSend),
            Some(PatternState::ReadyToSend) => return Err(Error::InvalidState),
            None => {}
        }
        Ok(())
    }
}
