use crate::constants::*;
use crate::context::{
    ContextShared, InprocEndpoint, MessageQueue, SubscriptionSet, WelcomeMessage,
};
use crate::transport::{IpcEndpoint, TcpEndpoint, ZmtpFrame, ZmtpGreeting, ZmtpMetadata};
use crate::{Error, Message, Result};
use libzmq_sys::ipc::{IpcListenerHandle, IpcStreamHandle};
use libzmq_sys::{TcpListenerHandle, TcpStreamHandle};
use std::collections::VecDeque;
use std::convert::TryFrom;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

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
    tcp: Mutex<TcpState>,
    ipc: Mutex<IpcState>,
    monitor: Mutex<Option<MonitorState>>,
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

#[derive(Debug, Default)]
struct TcpState {
    listener: Option<TcpListenerHandle>,
    stream: Option<TcpStreamHandle>,
    bound_endpoint: Option<TcpEndpoint>,
    connected_endpoint: Option<TcpEndpoint>,
    next_reconnect_at: Option<Instant>,
    handshake_started: bool,
    peer_greeting_done: bool,
    peer_ready: bool,
}

#[derive(Debug, Default)]
struct IpcState {
    listener: Option<IpcListenerHandle>,
    stream: Option<IpcStreamHandle>,
    bound_endpoint: Option<IpcEndpoint>,
    connected_endpoint: Option<IpcEndpoint>,
    handshake_started: bool,
    peer_greeting_done: bool,
    peer_ready: bool,
}

#[derive(Debug)]
struct MonitorState {
    endpoint_name: String,
    events: u64,
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
            tcp: Mutex::new(TcpState::default()),
            ipc: Mutex::new(IpcState::default()),
            monitor: Mutex::new(None),
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
        if endpoint.starts_with("tcp://") {
            return self.bind_tcp(endpoint);
        }
        if endpoint.starts_with("ipc://") {
            return self.bind_ipc(endpoint);
        }
        let endpoint_name = endpoint
            .strip_prefix("inproc://")
            .ok_or(Error::NotSupported)?;
        let bound = self.context.bind_inproc(
            endpoint_name,
            self.id,
            Arc::clone(&self.inbox),
            self.socket_type,
            Arc::clone(&self.subscriptions),
            Arc::clone(&self.xpub_welcome),
        )?;
        {
            let mut inproc = self.inproc.lock().map_err(|_| Error::InvalidSocket)?;
            inproc.bound_endpoint_name = Some(endpoint_name.to_string());
            inproc.bound_endpoint = Some(bound);
        }
        self.emit_monitor_event(ZMQ_EVENT_LISTENING, 0, endpoint)?;
        Ok(())
    }

    pub fn unbind(&self, endpoint: &str) -> Result<()> {
        if endpoint.starts_with("tcp://") {
            return self.unbind_tcp(endpoint);
        }
        if endpoint.starts_with("ipc://") {
            return self.unbind_ipc(endpoint);
        }
        let endpoint_name = endpoint
            .strip_prefix("inproc://")
            .ok_or(Error::NotSupported)?;
        {
            let mut inproc = self.inproc.lock().map_err(|_| Error::InvalidSocket)?;
            if inproc.bound_endpoint_name.as_deref() != Some(endpoint_name) {
                return Err(Error::InvalidArgument);
            }
            self.context.unbind_inproc(endpoint_name)?;
            inproc.bound_endpoint_name = None;
            inproc.bound_endpoint = None;
        }
        self.emit_monitor_event(ZMQ_EVENT_CLOSED, 0, endpoint)?;
        Ok(())
    }

    pub fn connect(&self, endpoint: &str) -> Result<()> {
        if endpoint.starts_with("tcp://") {
            return self.connect_tcp(endpoint);
        }
        if endpoint.starts_with("ipc://") {
            return self.connect_ipc(endpoint);
        }
        let endpoint_name = endpoint
            .strip_prefix("inproc://")
            .ok_or(Error::NotSupported)?;
        let bound = self.context.connect_inproc(
            endpoint_name,
            self.id,
            Arc::clone(&self.inbox),
            self.socket_type,
            Arc::clone(&self.subscriptions),
        )?;
        {
            let mut inproc = self.inproc.lock().map_err(|_| Error::InvalidSocket)?;
            inproc.connected_endpoint = Some(endpoint_name.to_string());
            inproc.direct_outbox = bound.map(|bound| bound.binder_inbox());
        }
        self.emit_monitor_event(ZMQ_EVENT_CONNECTED, 0, endpoint)?;
        Ok(())
    }

    pub fn disconnect(&self, endpoint: &str) -> Result<()> {
        if endpoint.starts_with("tcp://") {
            return self.disconnect_tcp(endpoint);
        }
        if endpoint.starts_with("ipc://") {
            return self.disconnect_ipc(endpoint);
        }
        let endpoint_name = endpoint
            .strip_prefix("inproc://")
            .ok_or(Error::NotSupported)?;
        {
            let mut inproc = self.inproc.lock().map_err(|_| Error::InvalidSocket)?;
            if inproc.connected_endpoint.as_deref() != Some(endpoint_name) {
                return Err(Error::InvalidArgument);
            }
            self.context.disconnect_inproc(endpoint_name, self.id)?;
            inproc.connected_endpoint = None;
            inproc.direct_outbox = None;
        }
        self.emit_monitor_event(ZMQ_EVENT_DISCONNECTED, 0, endpoint)?;
        Ok(())
    }

    pub fn monitor(&self, endpoint: &str, events: u64) -> Result<()> {
        let endpoint_name = endpoint
            .strip_prefix("inproc://")
            .ok_or(Error::NotSupported)?;
        if let Some(previous) = self
            .monitor
            .lock()
            .map_err(|_| Error::InvalidSocket)?
            .take()
        {
            let _ = self.context.unbind_inproc(&previous.endpoint_name);
        }
        let monitor_inbox = Arc::new(Mutex::new(VecDeque::new()));
        self.context.bind_inproc(
            endpoint_name,
            0,
            monitor_inbox,
            SocketType::Pair,
            Arc::new(Mutex::new(Vec::new())),
            Arc::new(Mutex::new(None)),
        )?;
        *self.monitor.lock().map_err(|_| Error::InvalidSocket)? = Some(MonitorState {
            endpoint_name: endpoint_name.to_string(),
            events,
        });
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
        if self.has_tcp_transport()? {
            self.send_tcp_frame(message.data())?;
            self.after_pattern_send()?;
            return Ok(size);
        }
        if self.has_ipc_transport()? {
            self.send_ipc_frame(message.data())?;
            self.after_pattern_send()?;
            return Ok(size);
        }
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
        if self.has_tcp_transport()? {
            let message = Message::from_vec(self.recv_tcp_frame()?);
            self.after_pattern_recv()?;
            return Ok(message);
        }
        if self.has_ipc_transport()? {
            let message = Message::from_vec(self.recv_ipc_frame()?);
            self.after_pattern_recv()?;
            return Ok(message);
        }
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
            ZMQ_FD => Ok(-1),
            ZMQ_EVENTS => Ok(self.events()? as i32),
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

    pub fn events(&self) -> Result<i16> {
        let mut events = 0;
        if self.can_recv()
            && !self
                .inbox
                .lock()
                .map_err(|_| Error::InvalidSocket)?
                .is_empty()
        {
            events |= ZMQ_POLLIN as i16;
        }
        if self.can_send() && self.resolve_outboxes(&Message::new()).is_ok() {
            events |= ZMQ_POLLOUT as i16;
        }
        Ok(events)
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

    fn bind_tcp(&self, endpoint: &str) -> Result<()> {
        if !self.supports_stream_transport() {
            return Err(Error::NotSupported);
        }
        let parsed = TcpEndpoint::parse(endpoint)?;
        let listener = TcpListenerHandle::bind(parsed.bind_addr()).map_err(map_io_error)?;
        listener.set_nonblocking(true).map_err(map_io_error)?;
        let mut tcp = self.tcp.lock().map_err(|_| Error::InvalidSocket)?;
        tcp.listener = Some(listener);
        tcp.bound_endpoint = Some(parsed);
        tcp.handshake_started = false;
        tcp.peer_greeting_done = false;
        tcp.peer_ready = false;
        self.emit_monitor_event(ZMQ_EVENT_LISTENING, 0, endpoint)?;
        Ok(())
    }

    fn unbind_tcp(&self, endpoint: &str) -> Result<()> {
        let parsed = TcpEndpoint::parse(endpoint)?;
        let mut tcp = self.tcp.lock().map_err(|_| Error::InvalidSocket)?;
        if tcp.bound_endpoint.as_ref() != Some(&parsed) {
            return Err(Error::InvalidArgument);
        }
        tcp.listener = None;
        tcp.stream = None;
        tcp.bound_endpoint = None;
        tcp.handshake_started = false;
        tcp.peer_greeting_done = false;
        tcp.peer_ready = false;
        self.emit_monitor_event(ZMQ_EVENT_CLOSED, 0, endpoint)?;
        Ok(())
    }

    fn connect_tcp(&self, endpoint: &str) -> Result<()> {
        if !self.supports_stream_transport() {
            return Err(Error::NotSupported);
        }
        let parsed = TcpEndpoint::parse(endpoint)?;
        let connect_addr = parsed.connect_addr()?;
        let stream = match TcpStreamHandle::connect(connect_addr) {
            Ok(stream) => {
                configure_tcp_stream(&stream)?;
                Some(stream)
            }
            Err(error) if is_reconnectable_tcp_error(&error) => None,
            Err(error) => return Err(map_io_error(error)),
        };
        let mut tcp = self.tcp.lock().map_err(|_| Error::InvalidSocket)?;
        tcp.stream = stream;
        tcp.connected_endpoint = Some(parsed);
        tcp.next_reconnect_at = tcp
            .stream
            .is_none()
            .then(|| Instant::now() + Duration::from_millis(100));
        tcp.handshake_started = false;
        tcp.peer_greeting_done = false;
        tcp.peer_ready = false;
        self.emit_monitor_event(ZMQ_EVENT_CONNECTED, 0, endpoint)?;
        Ok(())
    }

    fn disconnect_tcp(&self, endpoint: &str) -> Result<()> {
        let parsed = TcpEndpoint::parse(endpoint)?;
        let mut tcp = self.tcp.lock().map_err(|_| Error::InvalidSocket)?;
        if tcp.connected_endpoint.as_ref() != Some(&parsed) {
            return Err(Error::InvalidArgument);
        }
        tcp.stream = None;
        tcp.connected_endpoint = None;
        tcp.next_reconnect_at = None;
        tcp.handshake_started = false;
        tcp.peer_greeting_done = false;
        tcp.peer_ready = false;
        self.emit_monitor_event(ZMQ_EVENT_DISCONNECTED, 0, endpoint)?;
        Ok(())
    }

    fn has_tcp_transport(&self) -> Result<bool> {
        let tcp = self.tcp.lock().map_err(|_| Error::InvalidSocket)?;
        Ok(tcp.stream.is_some() || tcp.listener.is_some() || tcp.connected_endpoint.is_some())
    }

    fn ensure_tcp_stream(tcp: &mut TcpState) -> Result<&mut TcpStreamHandle> {
        if tcp.stream.is_none() {
            if let Some(endpoint) = tcp.connected_endpoint.as_ref() {
                if tcp
                    .next_reconnect_at
                    .is_some_and(|deadline| Instant::now() < deadline)
                {
                    return Err(Error::Again);
                }
                match TcpStreamHandle::connect(endpoint.connect_addr()?) {
                    Ok(stream) => {
                        configure_tcp_stream(&stream)?;
                        tcp.stream = Some(stream);
                        tcp.next_reconnect_at = None;
                        tcp.handshake_started = false;
                        tcp.peer_greeting_done = false;
                        tcp.peer_ready = false;
                    }
                    Err(error) if is_reconnectable_tcp_error(&error) => {
                        tcp.next_reconnect_at = Some(Instant::now() + Duration::from_millis(100));
                        return Err(Error::Again);
                    }
                    Err(error) => return Err(map_io_error(error)),
                }
            } else {
                let listener = tcp.listener.as_ref().ok_or(Error::Again)?;
                match listener.accept() {
                    Ok(stream) => {
                        configure_tcp_stream(&stream)?;
                        tcp.stream = Some(stream);
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        return Err(Error::Again)
                    }
                    Err(error) => return Err(map_io_error(error)),
                }
            }
        }
        tcp.stream.as_mut().ok_or(Error::Again)
    }

    fn send_tcp_frame(&self, data: &[u8]) -> Result<()> {
        let mut tcp = self.tcp.lock().map_err(|_| Error::InvalidSocket)?;
        Self::ensure_tcp_stream(&mut tcp)?;
        if self.socket_type == SocketType::Stream {
            let stream = tcp.stream.as_mut().ok_or(Error::Again)?;
            return stream.write_all(data).map_err(map_io_error);
        }
        if !tcp.handshake_started {
            let as_server = tcp.bound_endpoint.is_some();
            let stream = tcp.stream.as_mut().ok_or(Error::Again)?;
            tcp.peer_greeting_done = write_zmtp_handshake_tcp(stream, self.socket_type, as_server)?;
            tcp.handshake_started = true;
        }
        let stream = tcp.stream.as_mut().ok_or(Error::Again)?;
        stream
            .write_all(&ZmtpFrame::message(data.to_vec()).encode_v3())
            .map_err(map_io_error)
    }

    fn recv_tcp_frame(&self) -> Result<Vec<u8>> {
        let mut tcp = self.tcp.lock().map_err(|_| Error::InvalidSocket)?;
        Self::ensure_tcp_stream(&mut tcp)?;
        if self.socket_type == SocketType::Stream {
            let stream = tcp.stream.as_mut().ok_or(Error::Again)?;
            return read_raw_tcp(stream);
        }
        if !tcp.handshake_started {
            let as_server = tcp.bound_endpoint.is_some();
            let stream = tcp.stream.as_mut().ok_or(Error::Again)?;
            tcp.peer_greeting_done = write_zmtp_handshake_tcp(stream, self.socket_type, as_server)?;
            tcp.handshake_started = true;
        }
        if !tcp.peer_greeting_done {
            let stream = tcp.stream.as_mut().ok_or(Error::Again)?;
            read_zmtp_greeting_tcp(stream)?;
            tcp.peer_greeting_done = true;
        }
        if !tcp.peer_ready {
            let stream = tcp.stream.as_mut().ok_or(Error::Again)?;
            read_zmtp_peer_ready_tcp(stream)?;
            tcp.peer_ready = true;
        }
        loop {
            let stream = tcp.stream.as_mut().ok_or(Error::Again)?;
            let frame = read_zmtp_frame_tcp(stream)?;
            if !frame.command_frame() {
                return Ok(frame.body().to_vec());
            }
        }
    }

    fn bind_ipc(&self, endpoint: &str) -> Result<()> {
        if !self.supports_stream_transport() {
            return Err(Error::NotSupported);
        }
        let parsed = IpcEndpoint::parse(endpoint)?;
        let listener = IpcListenerHandle::bind(parsed.path()).map_err(map_io_error)?;
        listener.set_nonblocking(true).map_err(map_io_error)?;
        let mut ipc = self.ipc.lock().map_err(|_| Error::InvalidSocket)?;
        ipc.listener = Some(listener);
        ipc.bound_endpoint = Some(parsed);
        ipc.handshake_started = false;
        ipc.peer_greeting_done = false;
        ipc.peer_ready = false;
        self.emit_monitor_event(ZMQ_EVENT_LISTENING, 0, endpoint)?;
        Ok(())
    }

    fn unbind_ipc(&self, endpoint: &str) -> Result<()> {
        let parsed = IpcEndpoint::parse(endpoint)?;
        let mut ipc = self.ipc.lock().map_err(|_| Error::InvalidSocket)?;
        if ipc.bound_endpoint.as_ref() != Some(&parsed) {
            return Err(Error::InvalidArgument);
        }
        ipc.listener = None;
        ipc.stream = None;
        ipc.bound_endpoint = None;
        ipc.handshake_started = false;
        ipc.peer_greeting_done = false;
        ipc.peer_ready = false;
        let _ = std::fs::remove_file(parsed.path());
        self.emit_monitor_event(ZMQ_EVENT_CLOSED, 0, endpoint)?;
        Ok(())
    }

    fn connect_ipc(&self, endpoint: &str) -> Result<()> {
        if !self.supports_stream_transport() {
            return Err(Error::NotSupported);
        }
        let parsed = IpcEndpoint::parse(endpoint)?;
        let stream = IpcStreamHandle::connect(parsed.path()).map_err(map_io_error)?;
        configure_ipc_stream(&stream)?;
        let mut ipc = self.ipc.lock().map_err(|_| Error::InvalidSocket)?;
        ipc.stream = Some(stream);
        ipc.connected_endpoint = Some(parsed);
        ipc.handshake_started = false;
        ipc.peer_greeting_done = false;
        ipc.peer_ready = false;
        self.emit_monitor_event(ZMQ_EVENT_CONNECTED, 0, endpoint)?;
        Ok(())
    }

    fn disconnect_ipc(&self, endpoint: &str) -> Result<()> {
        let parsed = IpcEndpoint::parse(endpoint)?;
        let mut ipc = self.ipc.lock().map_err(|_| Error::InvalidSocket)?;
        if ipc.connected_endpoint.as_ref() != Some(&parsed) {
            return Err(Error::InvalidArgument);
        }
        ipc.stream = None;
        ipc.connected_endpoint = None;
        ipc.handshake_started = false;
        ipc.peer_greeting_done = false;
        ipc.peer_ready = false;
        self.emit_monitor_event(ZMQ_EVENT_DISCONNECTED, 0, endpoint)?;
        Ok(())
    }

    fn has_ipc_transport(&self) -> Result<bool> {
        let ipc = self.ipc.lock().map_err(|_| Error::InvalidSocket)?;
        Ok(ipc.stream.is_some() || ipc.listener.is_some())
    }

    fn supports_stream_transport(&self) -> bool {
        matches!(
            self.socket_type,
            SocketType::Pair
                | SocketType::Push
                | SocketType::Pull
                | SocketType::Req
                | SocketType::Rep
                | SocketType::Stream
        )
    }

    fn ensure_ipc_stream(ipc: &mut IpcState) -> Result<&mut IpcStreamHandle> {
        if ipc.stream.is_none() {
            let listener = ipc.listener.as_ref().ok_or(Error::Again)?;
            match listener.accept() {
                Ok(stream) => {
                    configure_ipc_stream(&stream)?;
                    ipc.stream = Some(stream);
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    return Err(Error::Again)
                }
                Err(error) => return Err(map_io_error(error)),
            }
        }
        ipc.stream.as_mut().ok_or(Error::Again)
    }

    fn send_ipc_frame(&self, data: &[u8]) -> Result<()> {
        let mut ipc = self.ipc.lock().map_err(|_| Error::InvalidSocket)?;
        Self::ensure_ipc_stream(&mut ipc)?;
        if self.socket_type == SocketType::Stream {
            let stream = ipc.stream.as_mut().ok_or(Error::Again)?;
            return stream.write_all(data).map_err(map_io_error);
        }
        if !ipc.handshake_started {
            let as_server = ipc.bound_endpoint.is_some();
            let stream = ipc.stream.as_mut().ok_or(Error::Again)?;
            ipc.peer_greeting_done = write_zmtp_handshake_ipc(stream, self.socket_type, as_server)?;
            ipc.handshake_started = true;
        }
        let stream = ipc.stream.as_mut().ok_or(Error::Again)?;
        stream
            .write_all(&ZmtpFrame::message(data.to_vec()).encode_v3())
            .map_err(map_io_error)
    }

    fn recv_ipc_frame(&self) -> Result<Vec<u8>> {
        let mut ipc = self.ipc.lock().map_err(|_| Error::InvalidSocket)?;
        Self::ensure_ipc_stream(&mut ipc)?;
        if self.socket_type == SocketType::Stream {
            let stream = ipc.stream.as_mut().ok_or(Error::Again)?;
            return read_raw_ipc(stream);
        }
        if !ipc.handshake_started {
            let as_server = ipc.bound_endpoint.is_some();
            let stream = ipc.stream.as_mut().ok_or(Error::Again)?;
            ipc.peer_greeting_done = write_zmtp_handshake_ipc(stream, self.socket_type, as_server)?;
            ipc.handshake_started = true;
        }
        if !ipc.peer_greeting_done {
            let stream = ipc.stream.as_mut().ok_or(Error::Again)?;
            read_zmtp_greeting_ipc(stream)?;
            ipc.peer_greeting_done = true;
        }
        if !ipc.peer_ready {
            let stream = ipc.stream.as_mut().ok_or(Error::Again)?;
            read_zmtp_peer_ready_ipc(stream)?;
            ipc.peer_ready = true;
        }
        loop {
            let stream = ipc.stream.as_mut().ok_or(Error::Again)?;
            let frame = read_zmtp_frame_ipc(stream)?;
            if !frame.command_frame() {
                return Ok(frame.body().to_vec());
            }
        }
    }

    fn emit_monitor_event(&self, event: i32, value: i32, endpoint: &str) -> Result<()> {
        let Some(monitor) = self
            .monitor
            .lock()
            .map_err(|_| Error::InvalidSocket)?
            .as_ref()
            .map(|monitor| MonitorState {
                endpoint_name: monitor.endpoint_name.clone(),
                events: monitor.events,
            })
        else {
            return Ok(());
        };
        if monitor.events & event as u64 == 0 && monitor.events != ZMQ_EVENT_ALL as u64 {
            return Ok(());
        }
        let Some(monitor_endpoint) = self.context.inproc_endpoint(&monitor.endpoint_name)? else {
            return Ok(());
        };
        let Some(outbox) = monitor_endpoint.first_peer()? else {
            return Ok(());
        };

        let mut event_frame = Vec::with_capacity(6);
        event_frame.extend_from_slice(&(event as u16).to_ne_bytes());
        event_frame.extend_from_slice(&value.to_ne_bytes());
        let mut event_message = Message::from_vec(event_frame);
        event_message.set_more(true);
        let endpoint_message = Message::from_vec(endpoint.as_bytes().to_vec());

        let mut queue = outbox.lock().map_err(|_| Error::InvalidSocket)?;
        queue.push_back(event_message);
        queue.push_back(endpoint_message);
        Ok(())
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

fn configure_tcp_stream(stream: &TcpStreamHandle) -> Result<()> {
    stream.set_nonblocking(false).map_err(map_io_error)?;
    stream
        .set_read_timeout(Some(Duration::from_millis(1_000)))
        .map_err(map_io_error)?;
    stream
        .set_write_timeout(Some(Duration::from_millis(1_000)))
        .map_err(map_io_error)
}

fn configure_ipc_stream(stream: &IpcStreamHandle) -> Result<()> {
    stream.set_nonblocking(false).map_err(map_io_error)?;
    stream
        .set_read_timeout(Some(Duration::from_millis(1_000)))
        .map_err(map_io_error)?;
    stream
        .set_write_timeout(Some(Duration::from_millis(1_000)))
        .map_err(map_io_error)
}

fn read_raw_tcp(stream: &mut TcpStreamHandle) -> Result<Vec<u8>> {
    let mut buffer = vec![0u8; 8192];
    let size = stream.read(&mut buffer).map_err(map_io_error)?;
    if size == 0 {
        return Err(Error::Again);
    }
    buffer.truncate(size);
    Ok(buffer)
}

fn read_raw_ipc(stream: &mut IpcStreamHandle) -> Result<Vec<u8>> {
    let mut buffer = vec![0u8; 8192];
    let size = stream.read(&mut buffer).map_err(map_io_error)?;
    if size == 0 {
        return Err(Error::Again);
    }
    buffer.truncate(size);
    Ok(buffer)
}

fn write_zmtp_handshake_tcp(
    stream: &mut TcpStreamHandle,
    socket_type: SocketType,
    as_server: bool,
) -> Result<bool> {
    let greeting = if as_server {
        ZmtpGreeting::null_server()
    } else {
        ZmtpGreeting::null_client()
    };
    if as_server {
        stream.set_read_timeout(None).map_err(map_io_error)?;
    }
    let greeting = greeting.encode();
    stream.write_all(&greeting[..10]).map_err(map_io_error)?;
    let peer_prefix_done = match read_zmtp_peer_greeting_prefix_tcp(stream) {
        Ok(done) => done,
        Err(Error::Again) => false,
        Err(error) => return Err(error),
    };
    stream.write_all(&greeting[10..]).map_err(map_io_error)?;
    if peer_prefix_done {
        read_zmtp_greeting_tail_tcp(stream)?;
    }
    stream
        .write_all(&ZmtpFrame::command(ready_command_body(socket_type)).encode_v3())
        .map_err(map_io_error)?;
    if as_server {
        stream
            .set_read_timeout(Some(Duration::from_millis(1_000)))
            .map_err(map_io_error)?;
    }
    Ok(peer_prefix_done)
}

fn read_zmtp_peer_ready_tcp(stream: &mut TcpStreamHandle) -> Result<()> {
    loop {
        let frame = read_zmtp_frame_tcp(stream)?;
        if frame.command_frame() {
            let _metadata = ZmtpMetadata::decode_ready(frame.body())?;
            return Ok(());
        }
    }
}

fn read_zmtp_greeting_tcp(stream: &mut TcpStreamHandle) -> Result<()> {
    let mut greeting = [0u8; 64];
    stream.read_exact(&mut greeting).map_err(map_io_error)?;
    ZmtpGreeting::decode(&greeting).map(|_| ())
}

fn read_zmtp_greeting_tail_tcp(stream: &mut TcpStreamHandle) -> Result<()> {
    let mut remainder = [0u8; 54];
    stream.read_exact(&mut remainder).map_err(map_io_error)
}

fn read_zmtp_peer_greeting_prefix_tcp(stream: &mut TcpStreamHandle) -> Result<bool> {
    let mut prefix = [0u8; 10];
    stream.read_exact(&mut prefix).map_err(map_io_error)?;
    if prefix[0] == 0xFF && prefix[9] == 0x7F {
        return Ok(true);
    }
    if prefix[0] == 3 && prefix[2..6] == *b"NULL" {
        return Ok(true);
    }
    Err(Error::InvalidArgument)
}

fn read_zmtp_frame_tcp(stream: &mut TcpStreamHandle) -> Result<ZmtpFrame> {
    let encoded = read_zmtp_frame_bytes_tcp(stream)?;
    ZmtpFrame::decode_v3(&encoded)
}

fn read_zmtp_frame_bytes_tcp(stream: &mut TcpStreamHandle) -> Result<Vec<u8>> {
    let mut flags = [0u8; 1];
    stream.read_exact(&mut flags).map_err(map_io_error)?;
    let long = flags[0] & 0x02 != 0;
    let body_len = if long {
        let mut len = [0u8; 8];
        stream.read_exact(&mut len).map_err(map_io_error)?;
        u64::from_be_bytes(len) as usize
    } else {
        let mut len = [0u8; 1];
        stream.read_exact(&mut len).map_err(map_io_error)?;
        len[0] as usize
    };
    let mut encoded = Vec::with_capacity(if long { 9 } else { 2 } + body_len);
    encoded.push(flags[0]);
    if long {
        encoded.extend_from_slice(&(body_len as u64).to_be_bytes());
    } else {
        encoded.push(body_len as u8);
    }
    let start = encoded.len();
    encoded.resize(start + body_len, 0);
    stream
        .read_exact(&mut encoded[start..])
        .map_err(map_io_error)?;
    Ok(encoded)
}

fn write_zmtp_handshake_ipc(
    stream: &mut IpcStreamHandle,
    socket_type: SocketType,
    as_server: bool,
) -> Result<bool> {
    let greeting = if as_server {
        ZmtpGreeting::null_server()
    } else {
        ZmtpGreeting::null_client()
    };
    if as_server {
        stream.set_read_timeout(None).map_err(map_io_error)?;
    }
    let greeting = greeting.encode();
    stream.write_all(&greeting[..10]).map_err(map_io_error)?;
    let peer_prefix_done = match read_zmtp_peer_greeting_prefix_ipc(stream) {
        Ok(done) => done,
        Err(Error::Again) => false,
        Err(error) => return Err(error),
    };
    stream.write_all(&greeting[10..]).map_err(map_io_error)?;
    if peer_prefix_done {
        read_zmtp_greeting_tail_ipc(stream)?;
    }
    stream
        .write_all(&ZmtpFrame::command(ready_command_body(socket_type)).encode_v3())
        .map_err(map_io_error)?;
    if as_server {
        stream
            .set_read_timeout(Some(Duration::from_millis(1_000)))
            .map_err(map_io_error)?;
    }
    Ok(peer_prefix_done)
}

fn read_zmtp_peer_ready_ipc(stream: &mut IpcStreamHandle) -> Result<()> {
    loop {
        let frame = read_zmtp_frame_ipc(stream)?;
        if frame.command_frame() {
            let _metadata = ZmtpMetadata::decode_ready(frame.body())?;
            return Ok(());
        }
    }
}

fn read_zmtp_greeting_ipc(stream: &mut IpcStreamHandle) -> Result<()> {
    let mut greeting = [0u8; 64];
    stream.read_exact(&mut greeting).map_err(map_io_error)?;
    ZmtpGreeting::decode(&greeting).map(|_| ())
}

fn read_zmtp_greeting_tail_ipc(stream: &mut IpcStreamHandle) -> Result<()> {
    let mut remainder = [0u8; 54];
    stream.read_exact(&mut remainder).map_err(map_io_error)
}

fn read_zmtp_peer_greeting_prefix_ipc(stream: &mut IpcStreamHandle) -> Result<bool> {
    let mut prefix = [0u8; 10];
    stream.read_exact(&mut prefix).map_err(map_io_error)?;
    if prefix[0] == 0xFF && prefix[9] == 0x7F {
        return Ok(true);
    }
    if prefix[0] == 3 && prefix[2..6] == *b"NULL" {
        return Ok(true);
    }
    Err(Error::InvalidArgument)
}

fn read_zmtp_frame_ipc(stream: &mut IpcStreamHandle) -> Result<ZmtpFrame> {
    let encoded = read_zmtp_frame_bytes_ipc(stream)?;
    ZmtpFrame::decode_v3(&encoded)
}

fn read_zmtp_frame_bytes_ipc(stream: &mut IpcStreamHandle) -> Result<Vec<u8>> {
    let mut flags = [0u8; 1];
    stream.read_exact(&mut flags).map_err(map_io_error)?;
    let long = flags[0] & 0x02 != 0;
    let body_len = if long {
        let mut len = [0u8; 8];
        stream.read_exact(&mut len).map_err(map_io_error)?;
        u64::from_be_bytes(len) as usize
    } else {
        let mut len = [0u8; 1];
        stream.read_exact(&mut len).map_err(map_io_error)?;
        len[0] as usize
    };
    let mut encoded = Vec::with_capacity(if long { 9 } else { 2 } + body_len);
    encoded.push(flags[0]);
    if long {
        encoded.extend_from_slice(&(body_len as u64).to_be_bytes());
    } else {
        encoded.push(body_len as u8);
    }
    let start = encoded.len();
    encoded.resize(start + body_len, 0);
    stream
        .read_exact(&mut encoded[start..])
        .map_err(map_io_error)?;
    Ok(encoded)
}

fn ready_command_body(socket_type: SocketType) -> Vec<u8> {
    let socket_type = match socket_type {
        SocketType::Pair => "PAIR",
        SocketType::Pull => "PULL",
        SocketType::Push => "PUSH",
        SocketType::Req => "REQ",
        SocketType::Rep => "REP",
        SocketType::Dealer => "DEALER",
        SocketType::Router => "ROUTER",
        SocketType::Pub => "PUB",
        SocketType::Sub => "SUB",
        _ => "PAIR",
    };
    ZmtpMetadata::new([("Socket-Type", socket_type.as_bytes().to_vec())]).encode_ready()
}

fn is_reconnectable_tcp_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::NotConnected
            | io::ErrorKind::TimedOut
    )
}

fn map_io_error(error: io::Error) -> Error {
    match error.kind() {
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut => Error::Again,
        io::ErrorKind::InvalidInput => Error::InvalidArgument,
        io::ErrorKind::Unsupported => Error::NotSupported,
        _ => Error::InvalidSocket,
    }
}
