use crate::constants::*;
use crate::context::{
    ContextShared, InprocEndpoint, MessageQueue, SubscriptionSet, SubscriptionState, WelcomeMessage,
};
use crate::transport::{
    IpcEndpoint, TcpEndpoint, UdpEndpoint, WsEndpoint, ZmtpFrame, ZmtpGreeting, ZmtpMetadata,
};
use crate::{z85_decode, Error, Message, Result, ZapReply, ZapRequest};
use base64::Engine;
#[allow(deprecated)]
use crypto_box::aead::AeadInPlace;
use crypto_box::{
    aead::{Aead, KeyInit},
    PublicKey as CurvePublicKey, SalsaBox, SecretKey as CurveSecretKey, Tag as CurveTag,
};
use crypto_secretbox::XSalsa20Poly1305;
use libzmq_sys::ipc::{IpcListenerHandle, IpcStreamHandle};
use libzmq_sys::{TcpListenerHandle, TcpStreamHandle, UdpSocketHandle};
#[cfg(feature = "wss")]
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
#[cfg(feature = "wss")]
use rustls::{
    ClientConfig, ClientConnection, RootCertStore, ServerConfig, ServerConnection, StreamOwned,
};
use sha1::{Digest, Sha1};
use std::collections::VecDeque;
use std::convert::TryFrom;
use std::io::{self, Read, Write};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(feature = "wss")]
use std::sync::OnceLock;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use zeroize::Zeroize;

const ZMTP_FLAG_LONG_LOCAL: u8 = 0x02;
const ZMTP_FLAG_COMMAND_LOCAL: u8 = 0x04;

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
    udp: Mutex<UdpState>,
    ws: Mutex<WsState>,
    #[cfg(feature = "wss")]
    wss: Mutex<WssState>,
    monitor: Mutex<Option<MonitorState>>,
    inproc_fast_send_enabled: AtomicBool,
    last_recv_more: AtomicBool,
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
    curve_session: Option<CurveSession>,
    gssapi_session: Option<GssapiSession>,
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
    curve_session: Option<CurveSession>,
    gssapi_session: Option<GssapiSession>,
}

#[derive(Debug, Default)]
struct UdpState {
    socket: Option<UdpSocketHandle>,
    bound_endpoint: Option<UdpEndpoint>,
    connected_endpoint: Option<UdpEndpoint>,
    last_peer: Option<SocketAddr>,
}

#[derive(Debug, Default)]
struct WsState {
    listener: Option<TcpListenerHandle>,
    stream: Option<TcpStreamHandle>,
    bound_endpoint: Option<WsEndpoint>,
    connected_endpoint: Option<WsEndpoint>,
    handshake_done: bool,
    client_key: Option<String>,
}

#[cfg(feature = "wss")]
#[derive(Debug, Default)]
struct WssState {
    listener: Option<TcpListenerHandle>,
    stream: Option<WssStream>,
    bound_endpoint: Option<WsEndpoint>,
    connected_endpoint: Option<WsEndpoint>,
    websocket_done: bool,
    client_request_sent: bool,
    client_key: Option<String>,
}

#[cfg(feature = "wss")]
#[derive(Debug)]
enum WssStream {
    Client(StreamOwned<ClientConnection, TcpStreamHandle>),
    Server(StreamOwned<ServerConnection, TcpStreamHandle>),
}

#[cfg(feature = "wss")]
impl Read for WssStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Client(stream) => stream.read(buf),
            Self::Server(stream) => stream.read(buf),
        }
    }
}

#[cfg(feature = "wss")]
impl Write for WssStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Client(stream) => stream.write(buf),
            Self::Server(stream) => stream.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Client(stream) => stream.flush(),
            Self::Server(stream) => stream.flush(),
        }
    }
}

struct CurveSession {
    local_transient_secret: [u8; 32],
    peer_transient_public: [u8; 32],
    send_box: SalsaBox,
    recv_box: SalsaBox,
    #[cfg(feature = "sodium")]
    sodium_key: Option<[u8; 32]>,
    send_nonce: u64,
    recv_nonce: u64,
    send_prefix: &'static [u8; 16],
    recv_prefix: &'static [u8; 16],
}

#[derive(Debug)]
struct GssapiSession {
    #[cfg_attr(not(feature = "gssapi"), allow(dead_code))]
    context: GssapiContext,
}

#[derive(Debug)]
enum GssapiContext {
    #[cfg(feature = "gssapi")]
    Client(libzmq_sys::gssapi::ClientContext),
    #[cfg(feature = "gssapi")]
    Server(libzmq_sys::gssapi::ServerContext),
}

impl std::fmt::Debug for CurveSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CurveSession")
            .field("send_nonce", &self.send_nonce)
            .field("recv_nonce", &self.recv_nonce)
            .field("send_prefix", &self.send_prefix)
            .field("recv_prefix", &self.recv_prefix)
            .finish_non_exhaustive()
    }
}

impl Drop for CurveSession {
    fn drop(&mut self) {
        self.local_transient_secret.zeroize();
        self.peer_transient_public.zeroize();
    }
}

fn curve_session(
    local_transient_secret: [u8; 32],
    peer_transient_public: [u8; 32],
    send_nonce: u64,
    recv_nonce: u64,
    send_prefix: &'static [u8; 16],
    recv_prefix: &'static [u8; 16],
) -> CurveSession {
    let send_box = curve_box_for(&peer_transient_public, &local_transient_secret);
    let recv_box = curve_box_for(&peer_transient_public, &local_transient_secret);
    #[cfg(feature = "sodium")]
    let sodium_key = libzmq_sys::sodium::crypto_box_beforenm_key(
        &peer_transient_public,
        &local_transient_secret,
    );
    CurveSession {
        local_transient_secret,
        peer_transient_public,
        send_box,
        recv_box,
        #[cfg(feature = "sodium")]
        sodium_key,
        send_nonce,
        recv_nonce,
        send_prefix,
        recv_prefix,
    }
}

#[derive(Debug)]
struct MonitorState {
    endpoint_name: String,
    events: u64,
}

#[derive(Debug)]
struct HandshakeResult {
    peer_greeting_done: bool,
    peer_ready: bool,
    curve_session: Option<CurveSession>,
    gssapi_session: Option<GssapiSession>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PlainCredentials {
    username: Vec<u8>,
    password: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CurveCredentials {
    public_key: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GssapiCredentials {
    principal: Vec<u8>,
}

trait GssapiHandshakeIo {
    fn write_zmtp_frame_bytes(&mut self, bytes: &[u8]) -> Result<()>;
    fn read_zmtp_frame(&mut self) -> Result<ZmtpFrame>;
}

impl GssapiHandshakeIo for TcpStreamHandle {
    fn write_zmtp_frame_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.write_all(bytes).map_err(map_io_error)
    }

    fn read_zmtp_frame(&mut self) -> Result<ZmtpFrame> {
        read_zmtp_frame_tcp(self)
    }
}

impl GssapiHandshakeIo for IpcStreamHandle {
    fn write_zmtp_frame_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.write_all(bytes).map_err(map_io_error)
    }

    fn read_zmtp_frame(&mut self) -> Result<ZmtpFrame> {
        read_zmtp_frame_ipc(self)
    }
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
    norm_mode: i32,
    norm_unicast_nack: bool,
    norm_buffer_size: i32,
    norm_segment_size: i32,
    norm_block_size: i32,
    norm_num_parity: i32,
    norm_num_autoparity: i32,
    norm_push: bool,
    security: SecurityOptions,
}

#[derive(Debug, Clone)]
struct SecurityOptions {
    plain_server: bool,
    plain_username: Vec<u8>,
    plain_password: Vec<u8>,
    curve_server: bool,
    curve_publickey: Vec<u8>,
    curve_secretkey: Vec<u8>,
    curve_serverkey: Vec<u8>,
    zap_domain: Vec<u8>,
    gssapi_server: bool,
    gssapi_principal: Vec<u8>,
    gssapi_service_principal: Vec<u8>,
    gssapi_plaintext: bool,
    gssapi_principal_nametype: i32,
    gssapi_service_principal_nametype: i32,
    zap_enforce_domain: bool,
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
            norm_mode: ZMQ_NORM_CC,
            norm_unicast_nack: false,
            norm_buffer_size: 2048,
            norm_segment_size: 1400,
            norm_block_size: 16,
            norm_num_parity: 4,
            norm_num_autoparity: 0,
            norm_push: false,
            security: SecurityOptions::default(),
        }
    }
}

impl Default for SecurityOptions {
    fn default() -> Self {
        Self {
            plain_server: false,
            plain_username: Vec::new(),
            plain_password: Vec::new(),
            curve_server: false,
            curve_publickey: Vec::new(),
            curve_secretkey: Vec::new(),
            curve_serverkey: Vec::new(),
            zap_domain: Vec::new(),
            gssapi_server: false,
            gssapi_principal: Vec::new(),
            gssapi_service_principal: Vec::new(),
            gssapi_plaintext: false,
            gssapi_principal_nametype: ZMQ_GSSAPI_NT_HOSTBASED,
            gssapi_service_principal_nametype: ZMQ_GSSAPI_NT_HOSTBASED,
            zap_enforce_domain: false,
        }
    }
}

impl Drop for SecurityOptions {
    fn drop(&mut self) {
        self.plain_username.zeroize();
        self.plain_password.zeroize();
        self.curve_publickey.zeroize();
        self.curve_secretkey.zeroize();
        self.curve_serverkey.zeroize();
        self.zap_domain.zeroize();
        self.gssapi_principal.zeroize();
        self.gssapi_service_principal.zeroize();
    }
}

impl SecurityOptions {
    fn mechanism(&self) -> i32 {
        if self.gssapi_server
            || !self.gssapi_principal.is_empty()
            || !self.gssapi_service_principal.is_empty()
        {
            ZMQ_GSSAPI
        } else if self.curve_server
            || !self.curve_publickey.is_empty()
            || !self.curve_secretkey.is_empty()
            || !self.curve_serverkey.is_empty()
        {
            ZMQ_CURVE
        } else if self.plain_server
            || !self.plain_username.is_empty()
            || !self.plain_password.is_empty()
        {
            ZMQ_PLAIN
        } else {
            ZMQ_NULL
        }
    }

    fn mechanism_name(&self) -> &'static str {
        match self.mechanism() {
            ZMQ_PLAIN => "PLAIN",
            ZMQ_CURVE => "CURVE",
            ZMQ_GSSAPI => "GSSAPI",
            _ => "NULL",
        }
    }

    fn authorize_plain(&self, credentials: &PlainCredentials) -> Result<()> {
        if !self.plain_username.is_empty() && self.plain_username != credentials.username {
            return Err(Error::InvalidArgument);
        }
        if !self.plain_password.is_empty() && self.plain_password != credentials.password {
            return Err(Error::InvalidArgument);
        }
        Ok(())
    }

    fn authorize_curve(&self, credentials: &CurveCredentials) -> Result<()> {
        if !self.curve_publickey.is_empty() {
            let expected = curve_option_key(&self.curve_publickey)?;
            if expected.as_slice() != credentials.public_key {
                return Err(Error::InvalidArgument);
            }
        }
        Ok(())
    }

    #[cfg(not(feature = "gssapi"))]
    fn authorize_gssapi(&self, credentials: &GssapiCredentials) -> Result<()> {
        if !self.gssapi_principal.is_empty() && self.gssapi_principal != credentials.principal {
            return Err(Error::InvalidArgument);
        }
        Ok(())
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
            subscriptions: Arc::new(Mutex::new(SubscriptionState::default())),
            xpub_welcome: Arc::new(Mutex::new(None)),
            inproc: Mutex::new(InprocState::default()),
            tcp: Mutex::new(TcpState::default()),
            ipc: Mutex::new(IpcState::default()),
            udp: Mutex::new(UdpState::default()),
            ws: Mutex::new(WsState::default()),
            #[cfg(feature = "wss")]
            wss: Mutex::new(WssState::default()),
            monitor: Mutex::new(None),
            inproc_fast_send_enabled: AtomicBool::new(true),
            last_recv_more: AtomicBool::new(false),
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
        if endpoint.starts_with("udp://") {
            return self.bind_udp(endpoint);
        }
        if endpoint.starts_with("ws://") {
            return self.bind_ws(endpoint);
        }
        if endpoint.starts_with("wss://") {
            return self.bind_wss(endpoint);
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
        if endpoint.starts_with("udp://") {
            return self.unbind_udp(endpoint);
        }
        if endpoint.starts_with("ws://") {
            return self.unbind_ws(endpoint);
        }
        if endpoint.starts_with("wss://") {
            return self.unbind_wss(endpoint);
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
        if endpoint.starts_with("udp://") {
            return self.connect_udp(endpoint);
        }
        if endpoint.starts_with("ws://") {
            return self.connect_ws(endpoint);
        }
        if endpoint.starts_with("wss://") {
            return self.connect_wss(endpoint);
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
        if endpoint.starts_with("udp://") {
            return self.disconnect_udp(endpoint);
        }
        if endpoint.starts_with("ws://") {
            return self.disconnect_ws(endpoint);
        }
        if endpoint.starts_with("wss://") {
            return self.disconnect_wss(endpoint);
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
            Arc::new(Mutex::new(SubscriptionState::default())),
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
        let more = message.more();
        let Some(message) = self.try_send_inproc_fast(message)? else {
            self.after_pattern_send(more)?;
            return Ok(size);
        };
        if self.has_udp_transport()? {
            self.send_udp_datagram(&message)?;
            self.after_pattern_send(message.more())?;
            return Ok(size);
        }
        if self.has_ws_transport()? {
            self.send_ws_frame(&message)?;
            self.after_pattern_send(message.more())?;
            return Ok(size);
        }
        if self.has_wss_transport()? {
            self.send_wss_frame(&message)?;
            self.after_pattern_send(message.more())?;
            return Ok(size);
        }
        if self.has_tcp_transport()? {
            self.send_tcp_frame(&message)?;
            self.after_pattern_send(message.more())?;
            return Ok(size);
        }
        if self.has_ipc_transport()? {
            self.send_ipc_frame(&message)?;
            self.after_pattern_send(message.more())?;
            return Ok(size);
        }
        let outboxes = self.resolve_outboxes(&message)?;
        if outboxes.is_empty() {
            if matches!(
                self.socket_type,
                SocketType::Pub | SocketType::Xpub | SocketType::Radio
            ) {
                if self.socket_type == SocketType::Xpub
                    && self
                        .options
                        .lock()
                        .map_err(|_| Error::InvalidSocket)?
                        .xpub_nodrop
                {
                    return Err(Error::Again);
                }
                self.after_pattern_send(message.more())?;
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
        self.after_pattern_send(message.more())?;
        Ok(size)
    }

    pub fn recv(&self, _flags: i32) -> Result<Message> {
        if !self.can_recv() {
            return Err(Error::NotSupported);
        }
        self.ensure_can_recv_for_pattern()?;
        if let Some(message) = self.try_recv_inproc_fast()? {
            self.after_pattern_recv(message.more())?;
            return Ok(message);
        }
        if self.has_udp_transport()? {
            let message = self.recv_udp_datagram()?;
            self.after_pattern_recv(message.more())?;
            return Ok(message);
        }
        if self.has_ws_transport()? {
            let message = self.recv_ws_frame()?;
            self.after_pattern_recv(message.more())?;
            return Ok(message);
        }
        if self.has_wss_transport()? {
            let message = self.recv_wss_frame()?;
            self.after_pattern_recv(message.more())?;
            return Ok(message);
        }
        if self.has_tcp_transport()? {
            let message = self.recv_tcp_frame()?;
            self.after_pattern_recv(message.more())?;
            return Ok(message);
        }
        if self.has_ipc_transport()? {
            let message = self.recv_ipc_frame()?;
            self.after_pattern_recv(message.more())?;
            return Ok(message);
        }
        let mut inbox = self.inbox.lock().map_err(|_| Error::InvalidSocket)?;
        let message = inbox.pop_front().ok_or(Error::Again)?;
        self.last_recv_more.store(message.more(), Ordering::Relaxed);
        if message.routing_id() != 0 {
            *self
                .last_recv_routing_id
                .lock()
                .map_err(|_| Error::InvalidSocket)? = Some(message.routing_id());
        }
        self.after_pattern_recv(message.more())?;
        Ok(message)
    }

    fn try_send_inproc_fast(&self, message: Message) -> Result<Option<Message>> {
        if self.socket_type == SocketType::Pub {
            let bound_endpoint = {
                let inproc = self.inproc.lock().map_err(|_| Error::InvalidSocket)?;
                inproc.bound_endpoint.clone()
            };
            if let Some(bound_endpoint) = bound_endpoint {
                bound_endpoint.send_owned_to_matching_peers(message)?;
                return Ok(None);
            }
        }
        if !matches!(
            self.socket_type,
            SocketType::Pair | SocketType::Push | SocketType::Channel
        ) {
            return Ok(Some(message));
        }
        if !self.inproc_fast_send_enabled.load(Ordering::Relaxed) {
            return Ok(Some(message));
        }
        let outbox = {
            let inproc = self.inproc.lock().map_err(|_| Error::InvalidSocket)?;
            if let Some(outbox) = &inproc.direct_outbox {
                Some(Arc::clone(outbox))
            } else if let Some(bound_endpoint) = &inproc.bound_endpoint {
                match self.socket_type {
                    SocketType::Push => bound_endpoint.next_peer()?,
                    SocketType::Pair | SocketType::Channel => bound_endpoint.first_peer()?,
                    _ => None,
                }
            } else {
                None
            }
        };
        let Some(outbox) = outbox else {
            return Ok(Some(message));
        };
        outbox
            .lock()
            .map_err(|_| Error::InvalidSocket)?
            .push_back(message);
        Ok(None)
    }

    fn try_recv_inproc_fast(&self) -> Result<Option<Message>> {
        let message = self
            .inbox
            .lock()
            .map_err(|_| Error::InvalidSocket)?
            .pop_front();
        if let Some(message) = message.as_ref().filter(|message| {
            message.more()
                || message.routing_id() != 0
                || self.last_recv_more.load(Ordering::Relaxed)
                || matches!(self.socket_type, SocketType::Rep)
        }) {
            self.record_recv_metadata(message)?;
        }
        Ok(message)
    }

    fn record_recv_metadata(&self, message: &Message) -> Result<()> {
        self.last_recv_more.store(message.more(), Ordering::Relaxed);
        if message.routing_id() != 0 {
            *self
                .last_recv_routing_id
                .lock()
                .map_err(|_| Error::InvalidSocket)? = Some(message.routing_id());
        }
        Ok(())
    }

    pub fn subscribe(&self, prefix: &[u8]) -> Result<()> {
        if !matches!(self.socket_type, SocketType::Sub | SocketType::Xsub) {
            return Err(Error::NotSupported);
        }
        let mut subscriptions = self
            .subscriptions
            .lock()
            .map_err(|_| Error::InvalidSocket)?;
        subscriptions.insert(prefix);
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
        subscriptions.remove(prefix);
        Ok(())
    }

    pub fn join(&self, group: &str) -> Result<()> {
        if self.socket_type != SocketType::Dish {
            return Err(Error::NotSupported);
        }
        if group.is_empty()
            || group.len() > ZMQ_GROUP_MAX_LENGTH as usize
            || group.as_bytes().contains(&0)
        {
            return Err(Error::InvalidArgument);
        }
        let mut subscriptions = self
            .subscriptions
            .lock()
            .map_err(|_| Error::InvalidSocket)?;
        subscriptions.insert(group.as_bytes());
        Ok(())
    }

    pub fn leave(&self, group: &str) -> Result<()> {
        if self.socket_type != SocketType::Dish {
            return Err(Error::NotSupported);
        }
        if group.is_empty()
            || group.len() > ZMQ_GROUP_MAX_LENGTH as usize
            || group.as_bytes().contains(&0)
        {
            return Err(Error::InvalidArgument);
        }
        let mut subscriptions = self
            .subscriptions
            .lock()
            .map_err(|_| Error::InvalidSocket)?;
        subscriptions.remove(group.as_bytes());
        Ok(())
    }

    pub fn set_option_i32(&self, option: i32, value: i32) -> Result<()> {
        let mut options = self.options.lock().map_err(|_| Error::InvalidSocket)?;
        match option {
            ZMQ_LINGER => options.linger = value,
            ZMQ_SNDHWM if value >= 0 => {
                options.sndhwm = value;
                self.inproc_fast_send_enabled
                    .store(false, Ordering::Relaxed);
            }
            ZMQ_RCVHWM if value >= 0 => options.rcvhwm = value,
            ZMQ_SNDTIMEO if value >= -1 => options.sndtimeo = value,
            ZMQ_RCVTIMEO if value >= -1 => options.rcvtimeo = value,
            ZMQ_CONFLATE => {
                options.conflate = value != 0;
                self.inproc_fast_send_enabled
                    .store(false, Ordering::Relaxed);
            }
            ZMQ_ROUTER_MANDATORY => options.router_mandatory = value != 0,
            ZMQ_ROUTER_HANDOVER => options.router_handover = value != 0,
            ZMQ_REQ_CORRELATE => options.req_correlate = value != 0,
            ZMQ_REQ_RELAXED => options.req_relaxed = value != 0,
            ZMQ_XPUB_VERBOSE => options.xpub_verbose = value != 0,
            ZMQ_XPUB_VERBOSER => options.xpub_verboser = value != 0,
            ZMQ_XPUB_NODROP => options.xpub_nodrop = value != 0,
            ZMQ_XPUB_MANUAL => options.xpub_manual = value != 0,
            ZMQ_NORM_MODE if (ZMQ_NORM_FIXED..=ZMQ_NORM_CCE_ECNONLY).contains(&value) => {
                options.norm_mode = value;
            }
            ZMQ_NORM_UNICAST_NACK => options.norm_unicast_nack = value != 0,
            ZMQ_NORM_BUFFER_SIZE if value > 0 => options.norm_buffer_size = value,
            ZMQ_NORM_SEGMENT_SIZE if value > 0 => options.norm_segment_size = value,
            ZMQ_NORM_BLOCK_SIZE if value > 0 && value <= 255 => options.norm_block_size = value,
            ZMQ_NORM_NUM_PARITY if (0..255).contains(&value) => options.norm_num_parity = value,
            ZMQ_NORM_NUM_AUTOPARITY if (0..255).contains(&value) => {
                options.norm_num_autoparity = value;
            }
            ZMQ_NORM_PUSH => options.norm_push = value != 0,
            ZMQ_PLAIN_SERVER => options.security.plain_server = value != 0,
            ZMQ_CURVE_SERVER => options.security.curve_server = value != 0,
            ZMQ_GSSAPI_SERVER => options.security.gssapi_server = value != 0,
            ZMQ_GSSAPI_PLAINTEXT => options.security.gssapi_plaintext = value != 0,
            ZMQ_GSSAPI_PRINCIPAL_NAMETYPE if is_valid_gssapi_nametype(value) => {
                options.security.gssapi_principal_nametype = value;
            }
            ZMQ_GSSAPI_SERVICE_PRINCIPAL_NAMETYPE if is_valid_gssapi_nametype(value) => {
                options.security.gssapi_service_principal_nametype = value;
            }
            ZMQ_ZAP_ENFORCE_DOMAIN => options.security.zap_enforce_domain = value != 0,
            ZMQ_SNDHWM | ZMQ_RCVHWM | ZMQ_SNDTIMEO | ZMQ_RCVTIMEO => {
                return Err(Error::InvalidArgument)
            }
            ZMQ_GSSAPI_PRINCIPAL_NAMETYPE | ZMQ_GSSAPI_SERVICE_PRINCIPAL_NAMETYPE => {
                return Err(Error::InvalidArgument)
            }
            ZMQ_NORM_MODE
            | ZMQ_NORM_BUFFER_SIZE
            | ZMQ_NORM_SEGMENT_SIZE
            | ZMQ_NORM_BLOCK_SIZE
            | ZMQ_NORM_NUM_PARITY
            | ZMQ_NORM_NUM_AUTOPARITY => return Err(Error::InvalidArgument),
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
            ZMQ_PLAIN_USERNAME => set_bytes(
                &mut self
                    .options
                    .lock()
                    .map_err(|_| Error::InvalidSocket)?
                    .security
                    .plain_username,
                value,
            ),
            ZMQ_PLAIN_PASSWORD => set_bytes(
                &mut self
                    .options
                    .lock()
                    .map_err(|_| Error::InvalidSocket)?
                    .security
                    .plain_password,
                value,
            ),
            ZMQ_CURVE_PUBLICKEY => set_curve_key(
                &mut self
                    .options
                    .lock()
                    .map_err(|_| Error::InvalidSocket)?
                    .security
                    .curve_publickey,
                value,
            ),
            ZMQ_CURVE_SECRETKEY => set_curve_key(
                &mut self
                    .options
                    .lock()
                    .map_err(|_| Error::InvalidSocket)?
                    .security
                    .curve_secretkey,
                value,
            ),
            ZMQ_CURVE_SERVERKEY => set_curve_key(
                &mut self
                    .options
                    .lock()
                    .map_err(|_| Error::InvalidSocket)?
                    .security
                    .curve_serverkey,
                value,
            ),
            ZMQ_ZAP_DOMAIN => set_bytes(
                &mut self
                    .options
                    .lock()
                    .map_err(|_| Error::InvalidSocket)?
                    .security
                    .zap_domain,
                value,
            ),
            ZMQ_GSSAPI_PRINCIPAL => set_bytes(
                &mut self
                    .options
                    .lock()
                    .map_err(|_| Error::InvalidSocket)?
                    .security
                    .gssapi_principal,
                value,
            ),
            ZMQ_GSSAPI_SERVICE_PRINCIPAL => set_bytes(
                &mut self
                    .options
                    .lock()
                    .map_err(|_| Error::InvalidSocket)?
                    .security
                    .gssapi_service_principal,
                value,
            ),
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
            ZMQ_NORM_MODE => Ok(options.norm_mode),
            ZMQ_NORM_UNICAST_NACK => Ok(i32::from(options.norm_unicast_nack)),
            ZMQ_NORM_BUFFER_SIZE => Ok(options.norm_buffer_size),
            ZMQ_NORM_SEGMENT_SIZE => Ok(options.norm_segment_size),
            ZMQ_NORM_BLOCK_SIZE => Ok(options.norm_block_size),
            ZMQ_NORM_NUM_PARITY => Ok(options.norm_num_parity),
            ZMQ_NORM_NUM_AUTOPARITY => Ok(options.norm_num_autoparity),
            ZMQ_NORM_PUSH => Ok(i32::from(options.norm_push)),
            ZMQ_MECHANISM => Ok(options.security.mechanism()),
            ZMQ_PLAIN_SERVER => Ok(i32::from(options.security.plain_server)),
            ZMQ_CURVE_SERVER => Ok(i32::from(options.security.curve_server)),
            ZMQ_GSSAPI_SERVER => Ok(i32::from(options.security.gssapi_server)),
            ZMQ_GSSAPI_PLAINTEXT => Ok(i32::from(options.security.gssapi_plaintext)),
            ZMQ_GSSAPI_PRINCIPAL_NAMETYPE => Ok(options.security.gssapi_principal_nametype),
            ZMQ_GSSAPI_SERVICE_PRINCIPAL_NAMETYPE => {
                Ok(options.security.gssapi_service_principal_nametype)
            }
            ZMQ_ZAP_ENFORCE_DOMAIN => Ok(i32::from(options.security.zap_enforce_domain)),
            ZMQ_FD => Ok(-1),
            ZMQ_EVENTS => Ok(self.events()? as i32),
            ZMQ_RCVMORE => Ok(i32::from(self.last_recv_more.load(Ordering::Relaxed))),
            ZMQ_THREAD_SAFE => Ok(0),
            _ => Err(Error::InvalidArgument),
        }
    }

    pub fn get_option_bytes(&self, option: i32) -> Result<Vec<u8>> {
        let options = self.options.lock().map_err(|_| Error::InvalidSocket)?;
        match option {
            ZMQ_PLAIN_USERNAME => Ok(options.security.plain_username.clone()),
            ZMQ_PLAIN_PASSWORD => Ok(options.security.plain_password.clone()),
            ZMQ_CURVE_PUBLICKEY => Ok(options.security.curve_publickey.clone()),
            ZMQ_CURVE_SECRETKEY => Ok(options.security.curve_secretkey.clone()),
            ZMQ_CURVE_SERVERKEY => Ok(options.security.curve_serverkey.clone()),
            ZMQ_ZAP_DOMAIN => Ok(options.security.zap_domain.clone()),
            ZMQ_GSSAPI_PRINCIPAL => Ok(options.security.gssapi_principal.clone()),
            ZMQ_GSSAPI_SERVICE_PRINCIPAL => Ok(options.security.gssapi_service_principal.clone()),
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
                SocketType::Pub | SocketType::Xpub | SocketType::Radio => {
                    bound_endpoint.matching_peers(message)
                }
                SocketType::Router | SocketType::Server | SocketType::Peer
                    if message.routing_id() != 0 =>
                {
                    Ok(bound_endpoint
                        .peer_by_id(message.routing_id() as usize)?
                        .into_iter()
                        .collect())
                }
                SocketType::Server | SocketType::Peer => Ok(Vec::new()),
                SocketType::Rep if message.routing_id() != 0 => Ok(bound_endpoint
                    .peer_by_id(message.routing_id() as usize)?
                    .into_iter()
                    .collect()),
                SocketType::Push | SocketType::Scatter => {
                    Ok(bound_endpoint.next_peer()?.into_iter().collect())
                }
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
        tcp.curve_session = None;
        tcp.gssapi_session = None;
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
        tcp.curve_session = None;
        tcp.gssapi_session = None;
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
        tcp.curve_session = None;
        tcp.gssapi_session = None;
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
        tcp.curve_session = None;
        tcp.gssapi_session = None;
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
                        tcp.curve_session = None;
                        tcp.gssapi_session = None;
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

    fn send_tcp_frame(&self, message: &Message) -> Result<()> {
        let mut tcp = self.tcp.lock().map_err(|_| Error::InvalidSocket)?;
        Self::ensure_tcp_stream(&mut tcp)?;
        if self.socket_type == SocketType::Stream {
            let stream = tcp.stream.as_mut().ok_or(Error::Again)?;
            return stream.write_all(message.data()).map_err(map_io_error);
        }
        if !tcp.handshake_started {
            let as_server = tcp.bound_endpoint.is_some();
            let security = self
                .options
                .lock()
                .map_err(|_| Error::InvalidSocket)?
                .security
                .clone();
            let stream = tcp.stream.as_mut().ok_or(Error::Again)?;
            let handshake = write_zmtp_handshake_tcp(
                stream,
                self.socket_type,
                as_server,
                &security,
                &self.context,
            )?;
            tcp.peer_greeting_done = handshake.peer_greeting_done;
            tcp.peer_ready = handshake.peer_ready;
            tcp.curve_session = handshake.curve_session;
            tcp.gssapi_session = handshake.gssapi_session;
            tcp.handshake_started = true;
        }
        if let Some(session) = tcp.curve_session.as_mut() {
            let frame = curve_message_frame(session, message.data(), message.more())?;
            let stream = tcp.stream.as_mut().ok_or(Error::Again)?;
            return stream.write_all(&frame).map_err(map_io_error);
        }
        if let Some(session) = tcp.gssapi_session.as_mut() {
            let frame = gssapi_message_frame(session, message.data(), message.more())?;
            let stream = tcp.stream.as_mut().ok_or(Error::Again)?;
            return stream.write_all(&frame).map_err(map_io_error);
        }
        let stream = tcp.stream.as_mut().ok_or(Error::Again)?;
        stream
            .write_all(
                &ZmtpFrame::message(message.data().to_vec())
                    .with_more(message.more())
                    .encode_v3(),
            )
            .map_err(map_io_error)
    }

    fn recv_tcp_frame(&self) -> Result<Message> {
        let mut tcp = self.tcp.lock().map_err(|_| Error::InvalidSocket)?;
        Self::ensure_tcp_stream(&mut tcp)?;
        if self.socket_type == SocketType::Stream {
            let stream = tcp.stream.as_mut().ok_or(Error::Again)?;
            return Ok(Message::from_vec(read_raw_tcp(stream)?));
        }
        if !tcp.handshake_started {
            let as_server = tcp.bound_endpoint.is_some();
            let security = self
                .options
                .lock()
                .map_err(|_| Error::InvalidSocket)?
                .security
                .clone();
            let stream = tcp.stream.as_mut().ok_or(Error::Again)?;
            let handshake = write_zmtp_handshake_tcp(
                stream,
                self.socket_type,
                as_server,
                &security,
                &self.context,
            )?;
            tcp.peer_greeting_done = handshake.peer_greeting_done;
            tcp.peer_ready = handshake.peer_ready;
            tcp.curve_session = handshake.curve_session;
            tcp.gssapi_session = handshake.gssapi_session;
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
            if tcp.curve_session.is_some() {
                let stream = tcp.stream.as_mut().ok_or(Error::Again)?;
                let body = read_zmtp_frame_body_tcp(stream)?;
                let session = tcp.curve_session.as_mut().ok_or(Error::InvalidSocket)?;
                return curve_message_from_body(session, &body);
            }
            if tcp.gssapi_session.is_some() {
                let stream = tcp.stream.as_mut().ok_or(Error::Again)?;
                let body = read_zmtp_frame_body_tcp(stream)?;
                let session = tcp.gssapi_session.as_mut().ok_or(Error::InvalidSocket)?;
                return gssapi_message_from_body(session, &body);
            }
            let stream = tcp.stream.as_mut().ok_or(Error::Again)?;
            let frame = read_zmtp_frame_tcp(stream)?;
            if !frame.command_frame() {
                let mut message = Message::from_vec(frame.body().to_vec());
                message.set_more(frame.more());
                return Ok(message);
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
        ipc.curve_session = None;
        ipc.gssapi_session = None;
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
        ipc.curve_session = None;
        ipc.gssapi_session = None;
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
        ipc.curve_session = None;
        ipc.gssapi_session = None;
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
        ipc.curve_session = None;
        ipc.gssapi_session = None;
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
                | SocketType::Server
                | SocketType::Client
                | SocketType::Peer
                | SocketType::Stream
                | SocketType::Channel
                | SocketType::Scatter
                | SocketType::Radio
                | SocketType::Dgram
                | SocketType::Gather
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

    fn send_ipc_frame(&self, message: &Message) -> Result<()> {
        let mut ipc = self.ipc.lock().map_err(|_| Error::InvalidSocket)?;
        Self::ensure_ipc_stream(&mut ipc)?;
        if self.socket_type == SocketType::Stream {
            let stream = ipc.stream.as_mut().ok_or(Error::Again)?;
            return stream.write_all(message.data()).map_err(map_io_error);
        }
        if !ipc.handshake_started {
            let as_server = ipc.bound_endpoint.is_some();
            let security = self
                .options
                .lock()
                .map_err(|_| Error::InvalidSocket)?
                .security
                .clone();
            let stream = ipc.stream.as_mut().ok_or(Error::Again)?;
            let handshake = write_zmtp_handshake_ipc(
                stream,
                self.socket_type,
                as_server,
                &security,
                &self.context,
            )?;
            ipc.peer_greeting_done = handshake.peer_greeting_done;
            ipc.peer_ready = handshake.peer_ready;
            ipc.curve_session = handshake.curve_session;
            ipc.gssapi_session = handshake.gssapi_session;
            ipc.handshake_started = true;
        }
        if let Some(session) = ipc.curve_session.as_mut() {
            let frame = curve_message_frame(session, message.data(), message.more())?;
            let stream = ipc.stream.as_mut().ok_or(Error::Again)?;
            return stream.write_all(&frame).map_err(map_io_error);
        }
        if let Some(session) = ipc.gssapi_session.as_mut() {
            let frame = gssapi_message_frame(session, message.data(), message.more())?;
            let stream = ipc.stream.as_mut().ok_or(Error::Again)?;
            return stream.write_all(&frame).map_err(map_io_error);
        }
        let stream = ipc.stream.as_mut().ok_or(Error::Again)?;
        stream
            .write_all(
                &ZmtpFrame::message(message.data().to_vec())
                    .with_more(message.more())
                    .encode_v3(),
            )
            .map_err(map_io_error)
    }

    fn recv_ipc_frame(&self) -> Result<Message> {
        let mut ipc = self.ipc.lock().map_err(|_| Error::InvalidSocket)?;
        Self::ensure_ipc_stream(&mut ipc)?;
        if self.socket_type == SocketType::Stream {
            let stream = ipc.stream.as_mut().ok_or(Error::Again)?;
            return Ok(Message::from_vec(read_raw_ipc(stream)?));
        }
        if !ipc.handshake_started {
            let as_server = ipc.bound_endpoint.is_some();
            let security = self
                .options
                .lock()
                .map_err(|_| Error::InvalidSocket)?
                .security
                .clone();
            let stream = ipc.stream.as_mut().ok_or(Error::Again)?;
            let handshake = write_zmtp_handshake_ipc(
                stream,
                self.socket_type,
                as_server,
                &security,
                &self.context,
            )?;
            ipc.peer_greeting_done = handshake.peer_greeting_done;
            ipc.peer_ready = handshake.peer_ready;
            ipc.curve_session = handshake.curve_session;
            ipc.gssapi_session = handshake.gssapi_session;
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
            if ipc.curve_session.is_some() {
                let stream = ipc.stream.as_mut().ok_or(Error::Again)?;
                let body = read_zmtp_frame_body_ipc(stream)?;
                let session = ipc.curve_session.as_mut().ok_or(Error::InvalidSocket)?;
                return curve_message_from_body(session, &body);
            }
            if ipc.gssapi_session.is_some() {
                let stream = ipc.stream.as_mut().ok_or(Error::Again)?;
                let body = read_zmtp_frame_body_ipc(stream)?;
                let session = ipc.gssapi_session.as_mut().ok_or(Error::InvalidSocket)?;
                return gssapi_message_from_body(session, &body);
            }
            let stream = ipc.stream.as_mut().ok_or(Error::Again)?;
            let frame = read_zmtp_frame_ipc(stream)?;
            if !frame.command_frame() {
                let mut message = Message::from_vec(frame.body().to_vec());
                message.set_more(frame.more());
                return Ok(message);
            }
        }
    }

    fn bind_udp(&self, endpoint: &str) -> Result<()> {
        if self.socket_type != SocketType::Dgram {
            return Err(Error::NotSupported);
        }
        let parsed = UdpEndpoint::parse(endpoint)?;
        let socket = UdpSocketHandle::bind(parsed.bind_addr()).map_err(map_io_error)?;
        if let Some(group) = parsed.multicast_v4() {
            socket
                .join_multicast_v4(group, Ipv4Addr::LOCALHOST)
                .map_err(map_io_error)?;
            socket.set_multicast_loop_v4(true).map_err(map_io_error)?;
        }
        socket.set_nonblocking(true).map_err(map_io_error)?;
        let mut udp = self.udp.lock().map_err(|_| Error::InvalidSocket)?;
        udp.socket = Some(socket);
        udp.bound_endpoint = Some(parsed);
        udp.connected_endpoint = None;
        udp.last_peer = None;
        self.emit_monitor_event(ZMQ_EVENT_LISTENING, 0, endpoint)?;
        Ok(())
    }

    fn unbind_udp(&self, endpoint: &str) -> Result<()> {
        let parsed = UdpEndpoint::parse(endpoint)?;
        let mut udp = self.udp.lock().map_err(|_| Error::InvalidSocket)?;
        if udp.bound_endpoint.as_ref() != Some(&parsed) {
            return Err(Error::InvalidArgument);
        }
        udp.socket = None;
        udp.bound_endpoint = None;
        udp.connected_endpoint = None;
        udp.last_peer = None;
        self.emit_monitor_event(ZMQ_EVENT_CLOSED, 0, endpoint)?;
        Ok(())
    }

    fn connect_udp(&self, endpoint: &str) -> Result<()> {
        if self.socket_type != SocketType::Dgram {
            return Err(Error::NotSupported);
        }
        let parsed = UdpEndpoint::parse(endpoint)?;
        let socket = UdpSocketHandle::bind("0.0.0.0:0").map_err(map_io_error)?;
        if parsed.multicast_v4().is_some() {
            socket
                .set_multicast_if_v4(Ipv4Addr::LOCALHOST)
                .map_err(map_io_error)?;
            socket.set_multicast_loop_v4(true).map_err(map_io_error)?;
            socket.set_multicast_ttl_v4(1).map_err(map_io_error)?;
        }
        socket
            .connect(parsed.connect_addr()?)
            .map_err(map_io_error)?;
        socket.set_nonblocking(true).map_err(map_io_error)?;
        let mut udp = self.udp.lock().map_err(|_| Error::InvalidSocket)?;
        udp.socket = Some(socket);
        udp.bound_endpoint = None;
        udp.connected_endpoint = Some(parsed);
        udp.last_peer = None;
        self.emit_monitor_event(ZMQ_EVENT_CONNECTED, 0, endpoint)?;
        Ok(())
    }

    fn disconnect_udp(&self, endpoint: &str) -> Result<()> {
        let parsed = UdpEndpoint::parse(endpoint)?;
        let mut udp = self.udp.lock().map_err(|_| Error::InvalidSocket)?;
        if udp.connected_endpoint.as_ref() != Some(&parsed) {
            return Err(Error::InvalidArgument);
        }
        udp.socket = None;
        udp.connected_endpoint = None;
        udp.last_peer = None;
        self.emit_monitor_event(ZMQ_EVENT_DISCONNECTED, 0, endpoint)?;
        Ok(())
    }

    fn has_udp_transport(&self) -> Result<bool> {
        let udp = self.udp.lock().map_err(|_| Error::InvalidSocket)?;
        Ok(udp.socket.is_some())
    }

    fn send_udp_datagram(&self, message: &Message) -> Result<()> {
        let udp = self.udp.lock().map_err(|_| Error::InvalidSocket)?;
        let socket = udp.socket.as_ref().ok_or(Error::Again)?;
        let sent = if udp.connected_endpoint.is_some() {
            socket.send(message.data()).map_err(map_io_error)?
        } else if let Some(peer) = udp.last_peer {
            socket.send_to(message.data(), peer).map_err(map_io_error)?
        } else {
            return Err(Error::Again);
        };
        if sent == message.len() {
            Ok(())
        } else {
            Err(Error::Again)
        }
    }

    fn recv_udp_datagram(&self) -> Result<Message> {
        let mut udp = self.udp.lock().map_err(|_| Error::InvalidSocket)?;
        let socket = udp.socket.as_ref().ok_or(Error::Again)?;
        let mut buffer = vec![0; 65_536];
        match socket.recv_from(&mut buffer) {
            Ok((size, peer)) => {
                udp.last_peer = Some(peer);
                buffer.truncate(size);
                Ok(Message::from_vec(buffer))
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Err(Error::Again),
            Err(error) => Err(map_io_error(error)),
        }
    }

    fn bind_ws(&self, endpoint: &str) -> Result<()> {
        if !self.supports_stream_transport() {
            return Err(Error::NotSupported);
        }
        let parsed = WsEndpoint::parse(endpoint)?;
        let listener = TcpListenerHandle::bind(parsed.bind_addr()).map_err(map_io_error)?;
        listener.set_nonblocking(true).map_err(map_io_error)?;
        let mut ws = self.ws.lock().map_err(|_| Error::InvalidSocket)?;
        ws.listener = Some(listener);
        ws.stream = None;
        ws.bound_endpoint = Some(parsed);
        ws.connected_endpoint = None;
        ws.handshake_done = false;
        ws.client_key = None;
        self.emit_monitor_event(ZMQ_EVENT_LISTENING, 0, endpoint)?;
        Ok(())
    }

    fn unbind_ws(&self, endpoint: &str) -> Result<()> {
        let parsed = WsEndpoint::parse(endpoint)?;
        let mut ws = self.ws.lock().map_err(|_| Error::InvalidSocket)?;
        if ws.bound_endpoint.as_ref() != Some(&parsed) {
            return Err(Error::InvalidArgument);
        }
        ws.listener = None;
        ws.stream = None;
        ws.bound_endpoint = None;
        ws.handshake_done = false;
        ws.client_key = None;
        self.emit_monitor_event(ZMQ_EVENT_CLOSED, 0, endpoint)?;
        Ok(())
    }

    fn connect_ws(&self, endpoint: &str) -> Result<()> {
        if !self.supports_stream_transport() {
            return Err(Error::NotSupported);
        }
        let parsed = WsEndpoint::parse(endpoint)?;
        let mut stream = TcpStreamHandle::connect(parsed.connect_addr()?).map_err(map_io_error)?;
        configure_tcp_stream(&stream)?;
        let key = websocket_key()?;
        let request = format!(
            "GET {} HTTP/1.1\r\nHost: {}:{}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {}\r\nSec-WebSocket-Version: 13\r\n\r\n",
            parsed.path(),
            parsed.host(),
            parsed.port(),
            key
        );
        stream.write_all(request.as_bytes()).map_err(map_io_error)?;
        let mut ws = self.ws.lock().map_err(|_| Error::InvalidSocket)?;
        ws.stream = Some(stream);
        ws.listener = None;
        ws.bound_endpoint = None;
        ws.connected_endpoint = Some(parsed);
        ws.handshake_done = false;
        ws.client_key = Some(key);
        self.emit_monitor_event(ZMQ_EVENT_CONNECTED, 0, endpoint)?;
        Ok(())
    }

    fn disconnect_ws(&self, endpoint: &str) -> Result<()> {
        let parsed = WsEndpoint::parse(endpoint)?;
        let mut ws = self.ws.lock().map_err(|_| Error::InvalidSocket)?;
        if ws.connected_endpoint.as_ref() != Some(&parsed) {
            return Err(Error::InvalidArgument);
        }
        ws.stream = None;
        ws.connected_endpoint = None;
        ws.handshake_done = false;
        ws.client_key = None;
        self.emit_monitor_event(ZMQ_EVENT_DISCONNECTED, 0, endpoint)?;
        Ok(())
    }

    fn has_ws_transport(&self) -> Result<bool> {
        let ws = self.ws.lock().map_err(|_| Error::InvalidSocket)?;
        Ok(ws.stream.is_some() || ws.listener.is_some())
    }

    fn ensure_ws_ready<'a>(&self, ws: &'a mut WsState) -> Result<&'a mut TcpStreamHandle> {
        if ws.stream.is_none() {
            let listener = ws.listener.as_ref().ok_or(Error::Again)?;
            match listener.accept() {
                Ok(stream) => {
                    configure_tcp_stream(&stream)?;
                    ws.stream = Some(stream);
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    return Err(Error::Again)
                }
                Err(error) => return Err(map_io_error(error)),
            }
        }
        if !ws.handshake_done {
            let stream = ws.stream.as_mut().ok_or(Error::Again)?;
            if ws.bound_endpoint.is_some() {
                websocket_server_handshake(stream)?;
            } else {
                let key = ws.client_key.as_deref().ok_or(Error::InvalidArgument)?;
                websocket_client_handshake(stream, key)?;
            }
            ws.handshake_done = true;
        }
        ws.stream.as_mut().ok_or(Error::Again)
    }

    fn send_ws_frame(&self, message: &Message) -> Result<()> {
        let mut ws = self.ws.lock().map_err(|_| Error::InvalidSocket)?;
        let mask = ws.connected_endpoint.is_some();
        let stream = if mask && !ws.handshake_done {
            ws.stream.as_mut().ok_or(Error::Again)?
        } else {
            self.ensure_ws_ready(&mut ws)?
        };
        let zmtp = ZmtpFrame::message(message.data().to_vec())
            .with_more(message.more())
            .encode_v3();
        stream
            .write_all(&websocket_binary_frame(&zmtp, mask)?)
            .map_err(map_io_error)
    }

    fn recv_ws_frame(&self) -> Result<Message> {
        let mut ws = self.ws.lock().map_err(|_| Error::InvalidSocket)?;
        let stream = self.ensure_ws_ready(&mut ws)?;
        let payload = read_websocket_binary_frame(stream)?;
        let frame = ZmtpFrame::decode_v3(&payload)?;
        let mut message = Message::from_vec(frame.body().to_vec());
        message.set_more(frame.more());
        Ok(message)
    }

    #[cfg(feature = "wss")]
    fn bind_wss(&self, endpoint: &str) -> Result<()> {
        if !self.supports_stream_transport() {
            return Err(Error::NotSupported);
        }
        let parsed = WsEndpoint::parse_wss(endpoint)?;
        let listener = TcpListenerHandle::bind(parsed.bind_addr()).map_err(map_io_error)?;
        listener.set_nonblocking(true).map_err(map_io_error)?;
        let mut wss = self.wss.lock().map_err(|_| Error::InvalidSocket)?;
        wss.listener = Some(listener);
        wss.stream = None;
        wss.bound_endpoint = Some(parsed);
        wss.connected_endpoint = None;
        wss.websocket_done = false;
        wss.client_request_sent = false;
        wss.client_key = None;
        self.emit_monitor_event(ZMQ_EVENT_LISTENING, 0, endpoint)?;
        Ok(())
    }

    #[cfg(not(feature = "wss"))]
    fn bind_wss(&self, _endpoint: &str) -> Result<()> {
        Err(Error::NotSupported)
    }

    #[cfg(feature = "wss")]
    fn unbind_wss(&self, endpoint: &str) -> Result<()> {
        let parsed = WsEndpoint::parse_wss(endpoint)?;
        let mut wss = self.wss.lock().map_err(|_| Error::InvalidSocket)?;
        if wss.bound_endpoint.as_ref() != Some(&parsed) {
            return Err(Error::InvalidArgument);
        }
        wss.listener = None;
        wss.stream = None;
        wss.bound_endpoint = None;
        wss.websocket_done = false;
        wss.client_request_sent = false;
        wss.client_key = None;
        self.emit_monitor_event(ZMQ_EVENT_CLOSED, 0, endpoint)?;
        Ok(())
    }

    #[cfg(not(feature = "wss"))]
    fn unbind_wss(&self, _endpoint: &str) -> Result<()> {
        Err(Error::NotSupported)
    }

    #[cfg(feature = "wss")]
    fn connect_wss(&self, endpoint: &str) -> Result<()> {
        if !self.supports_stream_transport() {
            return Err(Error::NotSupported);
        }
        let parsed = WsEndpoint::parse_wss(endpoint)?;
        let tcp = TcpStreamHandle::connect(parsed.connect_addr()?).map_err(map_io_error)?;
        configure_tcp_stream(&tcp)?;
        let cert = wss_certificate()?.0;
        let mut roots = RootCertStore::empty();
        roots.add(cert).map_err(|_| Error::InvalidArgument)?;
        let config = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let server_name =
            ServerName::try_from(parsed.host().to_string()).map_err(|_| Error::InvalidArgument)?;
        let connection = ClientConnection::new(Arc::new(config), server_name)
            .map_err(|_| Error::InvalidArgument)?;
        let stream = WssStream::Client(StreamOwned::new(connection, tcp));
        let key = websocket_key()?;
        let mut wss = self.wss.lock().map_err(|_| Error::InvalidSocket)?;
        wss.listener = None;
        wss.stream = Some(stream);
        wss.bound_endpoint = None;
        wss.connected_endpoint = Some(parsed);
        wss.websocket_done = false;
        wss.client_request_sent = false;
        wss.client_key = Some(key);
        self.emit_monitor_event(ZMQ_EVENT_CONNECTED, 0, endpoint)?;
        Ok(())
    }

    #[cfg(not(feature = "wss"))]
    fn connect_wss(&self, _endpoint: &str) -> Result<()> {
        Err(Error::NotSupported)
    }

    #[cfg(feature = "wss")]
    fn disconnect_wss(&self, endpoint: &str) -> Result<()> {
        let parsed = WsEndpoint::parse_wss(endpoint)?;
        let mut wss = self.wss.lock().map_err(|_| Error::InvalidSocket)?;
        if wss.connected_endpoint.as_ref() != Some(&parsed) {
            return Err(Error::InvalidArgument);
        }
        wss.stream = None;
        wss.connected_endpoint = None;
        wss.websocket_done = false;
        wss.client_request_sent = false;
        wss.client_key = None;
        self.emit_monitor_event(ZMQ_EVENT_DISCONNECTED, 0, endpoint)?;
        Ok(())
    }

    #[cfg(not(feature = "wss"))]
    fn disconnect_wss(&self, _endpoint: &str) -> Result<()> {
        Err(Error::NotSupported)
    }

    #[cfg(feature = "wss")]
    fn has_wss_transport(&self) -> Result<bool> {
        let wss = self.wss.lock().map_err(|_| Error::InvalidSocket)?;
        Ok(wss.stream.is_some() || wss.listener.is_some())
    }

    #[cfg(not(feature = "wss"))]
    fn has_wss_transport(&self) -> Result<bool> {
        Ok(false)
    }

    #[cfg(feature = "wss")]
    fn ensure_wss_ready<'a>(&self, wss: &'a mut WssState) -> Result<&'a mut WssStream> {
        if wss.stream.is_none() {
            let listener = wss.listener.as_ref().ok_or(Error::Again)?;
            match listener.accept() {
                Ok(tcp) => {
                    configure_tcp_stream(&tcp)?;
                    let (cert, key) = wss_certificate()?;
                    let config = ServerConfig::builder()
                        .with_no_client_auth()
                        .with_single_cert(vec![cert], key)
                        .map_err(|_| Error::InvalidArgument)?;
                    let connection = ServerConnection::new(Arc::new(config))
                        .map_err(|_| Error::InvalidArgument)?;
                    wss.stream = Some(WssStream::Server(StreamOwned::new(connection, tcp)));
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    return Err(Error::Again)
                }
                Err(error) => return Err(map_io_error(error)),
            }
        }
        if !wss.websocket_done {
            if wss.bound_endpoint.is_some() {
                let stream = wss.stream.as_mut().ok_or(Error::Again)?;
                websocket_server_handshake(stream)?;
            } else {
                write_wss_client_request(wss)?;
                let key = wss.client_key.clone().ok_or(Error::InvalidArgument)?;
                let stream = wss.stream.as_mut().ok_or(Error::Again)?;
                websocket_client_handshake(stream, &key)?;
            }
            wss.websocket_done = true;
        }
        wss.stream.as_mut().ok_or(Error::Again)
    }

    #[cfg(feature = "wss")]
    fn send_wss_frame(&self, message: &Message) -> Result<()> {
        let mut wss = self.wss.lock().map_err(|_| Error::InvalidSocket)?;
        if wss.connected_endpoint.is_some() && !wss.websocket_done {
            write_wss_client_request(&mut wss)?;
        }
        let stream = if wss.connected_endpoint.is_some() && !wss.websocket_done {
            wss.stream.as_mut().ok_or(Error::Again)?
        } else {
            self.ensure_wss_ready(&mut wss)?
        };
        let mask = matches!(stream, WssStream::Client(_));
        let zmtp = ZmtpFrame::message(message.data().to_vec())
            .with_more(message.more())
            .encode_v3();
        stream
            .write_all(&websocket_binary_frame(&zmtp, mask)?)
            .map_err(map_io_error)
    }

    #[cfg(not(feature = "wss"))]
    fn send_wss_frame(&self, _message: &Message) -> Result<()> {
        Err(Error::NotSupported)
    }

    #[cfg(feature = "wss")]
    fn recv_wss_frame(&self) -> Result<Message> {
        let mut wss = self.wss.lock().map_err(|_| Error::InvalidSocket)?;
        let stream = self.ensure_wss_ready(&mut wss)?;
        let payload = read_websocket_binary_frame(stream)?;
        let frame = ZmtpFrame::decode_v3(&payload)?;
        let mut message = Message::from_vec(frame.body().to_vec());
        message.set_more(frame.more());
        Ok(message)
    }

    #[cfg(not(feature = "wss"))]
    fn recv_wss_frame(&self) -> Result<Message> {
        Err(Error::NotSupported)
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
                    SocketType::Router | SocketType::Rep | SocketType::Server | SocketType::Peer
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
                | SocketType::Server
                | SocketType::Client
                | SocketType::Peer
                | SocketType::Pub
                | SocketType::Xpub
                | SocketType::Xsub
                | SocketType::Stream
                | SocketType::Channel
                | SocketType::Scatter
                | SocketType::Radio
                | SocketType::Dgram
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
                | SocketType::Server
                | SocketType::Client
                | SocketType::Peer
                | SocketType::Sub
                | SocketType::Xpub
                | SocketType::Xsub
                | SocketType::Stream
                | SocketType::Channel
                | SocketType::Gather
                | SocketType::Dish
                | SocketType::Dgram
        )
    }

    fn ensure_can_send_for_pattern(&self) -> Result<()> {
        if !matches!(self.socket_type, SocketType::Req | SocketType::Rep) {
            return Ok(());
        }
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

    fn after_pattern_send(&self, more: bool) -> Result<()> {
        if !matches!(self.socket_type, SocketType::Req | SocketType::Rep) {
            return Ok(());
        }
        let mut state = self
            .pattern_state
            .lock()
            .map_err(|_| Error::InvalidSocket)?;
        match *state {
            Some(PatternState::ReadyToSend) if !more => *state = Some(PatternState::ReadyToRecv),
            Some(PatternState::ReadyToSend) => {}
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
        if !matches!(self.socket_type, SocketType::Req | SocketType::Rep) {
            return Ok(());
        }
        let state = self
            .pattern_state
            .lock()
            .map_err(|_| Error::InvalidSocket)?;
        match *state {
            Some(PatternState::ReadyToSend) => Err(Error::InvalidState),
            _ => Ok(()),
        }
    }

    fn after_pattern_recv(&self, more: bool) -> Result<()> {
        if !matches!(self.socket_type, SocketType::Req | SocketType::Rep) {
            return Ok(());
        }
        let mut state = self
            .pattern_state
            .lock()
            .map_err(|_| Error::InvalidSocket)?;
        match *state {
            Some(PatternState::ReadyToRecv) if !more => *state = Some(PatternState::ReadyToSend),
            Some(PatternState::ReadyToRecv) => {}
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

fn websocket_key() -> Result<String> {
    let mut bytes = [0u8; 16];
    fill_random(&mut bytes)?;
    Ok(base64::engine::general_purpose::STANDARD.encode(bytes))
}

#[cfg(feature = "wss")]
fn wss_certificate() -> Result<(CertificateDer<'static>, PrivateKeyDer<'static>)> {
    static CERT: OnceLock<(Vec<u8>, Vec<u8>)> = OnceLock::new();
    let (cert, key) = CERT.get_or_init(|| {
        let certified = rcgen::generate_simple_self_signed(vec![
            "127.0.0.1".to_string(),
            "localhost".to_string(),
        ])
        .expect("self-signed WSS certificate generation should succeed");
        (
            certified.cert.der().as_ref().to_vec(),
            certified.key_pair.serialize_der(),
        )
    });
    Ok((
        CertificateDer::from(cert.clone()),
        PrivateKeyDer::from(PrivatePkcs8KeyDer::from(key.clone())),
    ))
}

fn websocket_accept(key: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(key.as_bytes());
    hasher.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
    base64::engine::general_purpose::STANDARD.encode(hasher.finalize())
}

fn read_http_headers(stream: &mut impl Read) -> Result<String> {
    let mut bytes = Vec::new();
    let mut byte = [0u8; 1];
    while bytes.len() < 8192 {
        stream.read_exact(&mut byte).map_err(map_io_error)?;
        bytes.push(byte[0]);
        if bytes.ends_with(b"\r\n\r\n") {
            return String::from_utf8(bytes).map_err(|_| Error::InvalidArgument);
        }
    }
    Err(Error::InvalidArgument)
}

fn websocket_header_value<'a>(headers: &'a str, name: &str) -> Option<&'a str> {
    headers.lines().find_map(|line| {
        let (stored, value) = line.split_once(':')?;
        stored
            .trim()
            .eq_ignore_ascii_case(name)
            .then_some(value.trim())
    })
}

fn websocket_server_handshake(stream: &mut (impl Read + Write)) -> Result<()> {
    let request = read_http_headers(stream)?;
    if !request.starts_with("GET ") {
        return Err(Error::InvalidArgument);
    }
    let key =
        websocket_header_value(&request, "Sec-WebSocket-Key").ok_or(Error::InvalidArgument)?;
    let response = format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {}\r\n\r\n",
        websocket_accept(key)
    );
    stream.write_all(response.as_bytes()).map_err(map_io_error)
}

fn websocket_client_handshake(stream: &mut impl Read, key: &str) -> Result<()> {
    let response = read_http_headers(stream)?;
    if !response.starts_with("HTTP/1.1 101") {
        return Err(Error::InvalidArgument);
    }
    let accept =
        websocket_header_value(&response, "Sec-WebSocket-Accept").ok_or(Error::InvalidArgument)?;
    if accept != websocket_accept(key) {
        return Err(Error::InvalidArgument);
    }
    Ok(())
}

#[cfg(feature = "wss")]
fn write_wss_client_request(wss: &mut WssState) -> Result<()> {
    if wss.client_request_sent {
        return Ok(());
    }
    let endpoint = wss
        .connected_endpoint
        .as_ref()
        .ok_or(Error::InvalidArgument)?;
    let key = wss.client_key.as_deref().ok_or(Error::InvalidArgument)?;
    let request = format!(
        "GET {} HTTP/1.1\r\nHost: {}:{}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {}\r\nSec-WebSocket-Version: 13\r\n\r\n",
        endpoint.path(),
        endpoint.host(),
        endpoint.port(),
        key
    );
    let stream = wss.stream.as_mut().ok_or(Error::Again)?;
    stream.write_all(request.as_bytes()).map_err(map_io_error)?;
    wss.client_request_sent = true;
    Ok(())
}

fn websocket_binary_frame(payload: &[u8], mask: bool) -> Result<Vec<u8>> {
    let mut frame = Vec::with_capacity(payload.len() + 14);
    frame.push(0x82);
    let mask_bit = if mask { 0x80 } else { 0 };
    if payload.len() <= 125 {
        frame.push(mask_bit | payload.len() as u8);
    } else if payload.len() <= u16::MAX as usize {
        frame.push(mask_bit | 126);
        frame.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    } else {
        frame.push(mask_bit | 127);
        frame.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    }
    if mask {
        let mut key = [0u8; 4];
        fill_random(&mut key)?;
        frame.extend_from_slice(&key);
        frame.extend(
            payload
                .iter()
                .enumerate()
                .map(|(index, byte)| byte ^ key[index % 4]),
        );
    } else {
        frame.extend_from_slice(payload);
    }
    Ok(frame)
}

fn read_websocket_binary_frame(stream: &mut impl Read) -> Result<Vec<u8>> {
    let mut header = [0u8; 2];
    stream.read_exact(&mut header).map_err(map_io_error)?;
    let opcode = header[0] & 0x0f;
    if opcode == 0x08 {
        return Err(Error::Again);
    }
    if opcode != 0x02 {
        return Err(Error::InvalidArgument);
    }
    let masked = header[1] & 0x80 != 0;
    let mut len = (header[1] & 0x7f) as u64;
    if len == 126 {
        let mut bytes = [0u8; 2];
        stream.read_exact(&mut bytes).map_err(map_io_error)?;
        len = u16::from_be_bytes(bytes) as u64;
    } else if len == 127 {
        let mut bytes = [0u8; 8];
        stream.read_exact(&mut bytes).map_err(map_io_error)?;
        len = u64::from_be_bytes(bytes);
    }
    let mut mask = [0u8; 4];
    if masked {
        stream.read_exact(&mut mask).map_err(map_io_error)?;
    }
    let mut payload = vec![0; len as usize];
    stream.read_exact(&mut payload).map_err(map_io_error)?;
    if masked {
        for (index, byte) in payload.iter_mut().enumerate() {
            *byte ^= mask[index % 4];
        }
    }
    Ok(payload)
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
    security: &SecurityOptions,
    context: &ContextShared,
) -> Result<HandshakeResult> {
    let greeting = ZmtpGreeting::new(security.mechanism_name(), as_server);
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
    let mut peer_ready = false;
    let mut curve_session = None;
    let mut gssapi_session = None;
    match security.mechanism() {
        ZMQ_PLAIN => {
            peer_ready =
                write_plain_handshake_tcp(stream, socket_type, as_server, security, context)?;
        }
        ZMQ_CURVE => {
            let handshake =
                write_curve_handshake_tcp(stream, socket_type, as_server, security, context)?;
            peer_ready = handshake.peer_ready;
            curve_session = handshake.curve_session;
        }
        ZMQ_GSSAPI => {
            let handshake =
                write_gssapi_handshake_tcp(stream, socket_type, as_server, security, context)?;
            peer_ready = handshake.peer_ready;
            gssapi_session = handshake.gssapi_session;
        }
        _ => {
            stream
                .write_all(&ZmtpFrame::command(ready_command_body(socket_type)).encode_v3())
                .map_err(map_io_error)?;
        }
    }
    if as_server {
        stream
            .set_read_timeout(Some(Duration::from_millis(1_000)))
            .map_err(map_io_error)?;
    }
    Ok(HandshakeResult {
        peer_greeting_done: peer_prefix_done,
        peer_ready,
        curve_session,
        gssapi_session,
    })
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

fn read_zmtp_frame_body_tcp(stream: &mut TcpStreamHandle) -> Result<Vec<u8>> {
    read_zmtp_frame_body(stream)
}

fn write_zmtp_handshake_ipc(
    stream: &mut IpcStreamHandle,
    socket_type: SocketType,
    as_server: bool,
    security: &SecurityOptions,
    context: &ContextShared,
) -> Result<HandshakeResult> {
    let greeting = ZmtpGreeting::new(security.mechanism_name(), as_server);
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
    let mut peer_ready = false;
    let mut curve_session = None;
    let mut gssapi_session = None;
    match security.mechanism() {
        ZMQ_PLAIN => {
            peer_ready =
                write_plain_handshake_ipc(stream, socket_type, as_server, security, context)?;
        }
        ZMQ_CURVE => {
            let handshake =
                write_curve_handshake_ipc(stream, socket_type, as_server, security, context)?;
            peer_ready = handshake.peer_ready;
            curve_session = handshake.curve_session;
        }
        ZMQ_GSSAPI => {
            let handshake =
                write_gssapi_handshake_ipc(stream, socket_type, as_server, security, context)?;
            peer_ready = handshake.peer_ready;
            gssapi_session = handshake.gssapi_session;
        }
        _ => {
            stream
                .write_all(&ZmtpFrame::command(ready_command_body(socket_type)).encode_v3())
                .map_err(map_io_error)?;
        }
    }
    if as_server {
        stream
            .set_read_timeout(Some(Duration::from_millis(1_000)))
            .map_err(map_io_error)?;
    }
    Ok(HandshakeResult {
        peer_greeting_done: peer_prefix_done,
        peer_ready,
        curve_session,
        gssapi_session,
    })
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

fn read_zmtp_frame_body_ipc(stream: &mut IpcStreamHandle) -> Result<Vec<u8>> {
    let mut flags = [0u8; 1];
    stream.read_exact(&mut flags).map_err(map_io_error)?;
    let long = flags[0] & ZMTP_FLAG_LONG_LOCAL != 0;
    let body_len = if long {
        let mut len = [0u8; 8];
        stream.read_exact(&mut len).map_err(map_io_error)?;
        u64::from_be_bytes(len) as usize
    } else {
        let mut len = [0u8; 1];
        stream.read_exact(&mut len).map_err(map_io_error)?;
        len[0] as usize
    };
    let mut body = vec![0; body_len];
    stream.read_exact(&mut body).map_err(map_io_error)?;
    Ok(body)
}

fn read_zmtp_frame_body(stream: &mut impl Read) -> Result<Vec<u8>> {
    let mut flags = [0u8; 1];
    stream.read_exact(&mut flags).map_err(map_io_error)?;
    let long = flags[0] & ZMTP_FLAG_LONG_LOCAL != 0;
    let body_len = if long {
        let mut len = [0u8; 8];
        stream.read_exact(&mut len).map_err(map_io_error)?;
        u64::from_be_bytes(len) as usize
    } else {
        let mut len = [0u8; 1];
        stream.read_exact(&mut len).map_err(map_io_error)?;
        len[0] as usize
    };
    let mut body = vec![0; body_len];
    stream.read_exact(&mut body).map_err(map_io_error)?;
    Ok(body)
}

fn write_plain_handshake_tcp(
    stream: &mut TcpStreamHandle,
    socket_type: SocketType,
    as_server: bool,
    security: &SecurityOptions,
    context: &ContextShared,
) -> Result<bool> {
    if as_server {
        let credentials = parse_plain_hello(&read_zmtp_frame_tcp(stream)?)?;
        authorize_plain(context, security, &credentials)?;
        stream
            .write_all(&ZmtpFrame::command(command_body("WELCOME", [])).encode_v3())
            .map_err(map_io_error)?;
        expect_command(&read_zmtp_frame_tcp(stream)?, "INITIATE")?;
        stream
            .write_all(&ZmtpFrame::command(ready_command_body(socket_type)).encode_v3())
            .map_err(map_io_error)?;
        Ok(true)
    } else {
        stream
            .write_all(&ZmtpFrame::command(plain_hello_body(security)).encode_v3())
            .map_err(map_io_error)?;
        expect_command(&read_zmtp_frame_tcp(stream)?, "WELCOME")?;
        stream
            .write_all(&ZmtpFrame::command(plain_initiate_body(socket_type)).encode_v3())
            .map_err(map_io_error)?;
        Ok(false)
    }
}

fn write_plain_handshake_ipc(
    stream: &mut IpcStreamHandle,
    socket_type: SocketType,
    as_server: bool,
    security: &SecurityOptions,
    context: &ContextShared,
) -> Result<bool> {
    if as_server {
        let credentials = parse_plain_hello(&read_zmtp_frame_ipc(stream)?)?;
        authorize_plain(context, security, &credentials)?;
        stream
            .write_all(&ZmtpFrame::command(command_body("WELCOME", [])).encode_v3())
            .map_err(map_io_error)?;
        expect_command(&read_zmtp_frame_ipc(stream)?, "INITIATE")?;
        stream
            .write_all(&ZmtpFrame::command(ready_command_body(socket_type)).encode_v3())
            .map_err(map_io_error)?;
        Ok(true)
    } else {
        stream
            .write_all(&ZmtpFrame::command(plain_hello_body(security)).encode_v3())
            .map_err(map_io_error)?;
        expect_command(&read_zmtp_frame_ipc(stream)?, "WELCOME")?;
        stream
            .write_all(&ZmtpFrame::command(plain_initiate_body(socket_type)).encode_v3())
            .map_err(map_io_error)?;
        Ok(false)
    }
}

fn authorize_plain(
    context: &ContextShared,
    security: &SecurityOptions,
    credentials: &PlainCredentials,
) -> Result<()> {
    if context.inproc_endpoint("zeromq.zap.01")?.is_some() {
        return authorize_plain_via_zap(context, security, credentials);
    }
    security.authorize_plain(credentials)
}

fn authorize_plain_via_zap(
    context: &ContextShared,
    security: &SecurityOptions,
    credentials: &PlainCredentials,
) -> Result<()> {
    let peer_id = context.next_transient_socket_id();
    let inbox = Arc::new(Mutex::new(VecDeque::new()));
    let subscriptions = Arc::new(Mutex::new(SubscriptionState::default()));
    let endpoint = context
        .connect_inproc(
            "zeromq.zap.01",
            peer_id,
            Arc::clone(&inbox),
            SocketType::Req,
            subscriptions,
        )?
        .ok_or(Error::Again)?;

    let request = ZapRequest::new(
        peer_id.to_string(),
        security.zap_domain.clone(),
        Vec::new(),
        Vec::new(),
        "PLAIN",
        [credentials.username.clone(), credentials.password.clone()],
    );
    let frames = request.encode();
    {
        let binder_inbox = endpoint.binder_inbox();
        let mut queue = binder_inbox.lock().map_err(|_| Error::InvalidSocket)?;
        let last = frames.len().saturating_sub(1);
        for (index, frame) in frames.into_iter().enumerate() {
            let mut message = Message::from_vec(frame);
            message.set_routing_id(peer_id as u32);
            message.set_more(index != last);
            queue.push_back(message);
        }
    }

    let reply = wait_zap_reply(&inbox);
    let _ = context.disconnect_inproc("zeromq.zap.01", peer_id);
    let reply = reply?;
    if reply.status_code.starts_with('2') {
        Ok(())
    } else {
        Err(Error::InvalidArgument)
    }
}

fn write_curve_handshake_tcp(
    stream: &mut TcpStreamHandle,
    socket_type: SocketType,
    as_server: bool,
    security: &SecurityOptions,
    context: &ContextShared,
) -> Result<HandshakeResult> {
    if as_server {
        let mut hello = parse_curve_hello(&read_zmtp_frame_tcp(stream)?, security)?;
        let welcome = curve_welcome_body(security, &hello)?;
        stream
            .write_all(&ZmtpFrame::command(welcome).encode_v3())
            .map_err(map_io_error)?;
        let initiate = parse_curve_initiate(&read_zmtp_frame_tcp(stream)?, security, &mut hello)?;
        let credentials = CurveCredentials {
            public_key: initiate.client_public_key.to_vec(),
        };
        authorize_curve(context, security, &credentials)?;
        let mut session = hello.session();
        stream
            .write_all(
                &ZmtpFrame::command(curve_ready_body(socket_type, &mut session)?).encode_v3(),
            )
            .map_err(map_io_error)?;
        Ok(HandshakeResult {
            peer_greeting_done: false,
            peer_ready: true,
            curve_session: Some(session),
            gssapi_session: None,
        })
    } else {
        let mut client = CurveClientHandshake::new(security)?;
        stream
            .write_all(&ZmtpFrame::command(client.hello_body()?).encode_v3())
            .map_err(map_io_error)?;
        client.process_welcome(&read_zmtp_frame_tcp(stream)?)?;
        stream
            .write_all(&ZmtpFrame::command(client.initiate_body(socket_type)?).encode_v3())
            .map_err(map_io_error)?;
        client.process_ready(&read_zmtp_frame_tcp(stream)?)?;
        Ok(HandshakeResult {
            peer_greeting_done: false,
            peer_ready: true,
            curve_session: Some(client.session()?),
            gssapi_session: None,
        })
    }
}

fn write_curve_handshake_ipc(
    stream: &mut IpcStreamHandle,
    socket_type: SocketType,
    as_server: bool,
    security: &SecurityOptions,
    context: &ContextShared,
) -> Result<HandshakeResult> {
    if as_server {
        let mut hello = parse_curve_hello(&read_zmtp_frame_ipc(stream)?, security)?;
        let welcome = curve_welcome_body(security, &hello)?;
        stream
            .write_all(&ZmtpFrame::command(welcome).encode_v3())
            .map_err(map_io_error)?;
        let initiate = parse_curve_initiate(&read_zmtp_frame_ipc(stream)?, security, &mut hello)?;
        let credentials = CurveCredentials {
            public_key: initiate.client_public_key.to_vec(),
        };
        authorize_curve(context, security, &credentials)?;
        let mut session = hello.session();
        stream
            .write_all(
                &ZmtpFrame::command(curve_ready_body(socket_type, &mut session)?).encode_v3(),
            )
            .map_err(map_io_error)?;
        Ok(HandshakeResult {
            peer_greeting_done: false,
            peer_ready: true,
            curve_session: Some(session),
            gssapi_session: None,
        })
    } else {
        let mut client = CurveClientHandshake::new(security)?;
        stream
            .write_all(&ZmtpFrame::command(client.hello_body()?).encode_v3())
            .map_err(map_io_error)?;
        client.process_welcome(&read_zmtp_frame_ipc(stream)?)?;
        stream
            .write_all(&ZmtpFrame::command(client.initiate_body(socket_type)?).encode_v3())
            .map_err(map_io_error)?;
        client.process_ready(&read_zmtp_frame_ipc(stream)?)?;
        Ok(HandshakeResult {
            peer_greeting_done: false,
            peer_ready: true,
            curve_session: Some(client.session()?),
            gssapi_session: None,
        })
    }
}

fn authorize_curve(
    context: &ContextShared,
    security: &SecurityOptions,
    credentials: &CurveCredentials,
) -> Result<()> {
    if context.inproc_endpoint("zeromq.zap.01")?.is_some() {
        return authorize_curve_via_zap(context, security, credentials);
    }
    security.authorize_curve(credentials)
}

fn authorize_curve_via_zap(
    context: &ContextShared,
    security: &SecurityOptions,
    credentials: &CurveCredentials,
) -> Result<()> {
    let peer_id = context.next_transient_socket_id();
    let inbox = Arc::new(Mutex::new(VecDeque::new()));
    let subscriptions = Arc::new(Mutex::new(SubscriptionState::default()));
    let endpoint = context
        .connect_inproc(
            "zeromq.zap.01",
            peer_id,
            Arc::clone(&inbox),
            SocketType::Req,
            subscriptions,
        )?
        .ok_or(Error::Again)?;

    let request = ZapRequest::new(
        peer_id.to_string(),
        security.zap_domain.clone(),
        Vec::new(),
        Vec::new(),
        "CURVE",
        [credentials.public_key.clone()],
    );
    send_zap_request(&endpoint, peer_id, request.encode())?;
    let reply = wait_zap_reply(&inbox);
    let _ = context.disconnect_inproc("zeromq.zap.01", peer_id);
    let reply = reply?;
    if reply.status_code.starts_with('2') {
        Ok(())
    } else {
        Err(Error::InvalidArgument)
    }
}

#[derive(Debug)]
struct CurveClientHandshake {
    public_key: [u8; 32],
    secret_key: [u8; 32],
    server_key: [u8; 32],
    transient_secret: [u8; 32],
    transient_public: [u8; 32],
    server_transient_public: Option<[u8; 32]>,
    cookie: Option<[u8; 96]>,
    send_nonce: u64,
    recv_nonce: u64,
}

#[derive(Debug)]
struct CurveServerHandshake {
    server_public_key: [u8; 32],
    server_secret_key: [u8; 32],
    transient_secret: [u8; 32],
    transient_public: [u8; 32],
    client_transient_public: [u8; 32],
    cookie_key: [u8; 32],
    peer_nonce: u64,
}

#[derive(Debug)]
struct CurveInitiate {
    client_public_key: [u8; 32],
}

impl CurveClientHandshake {
    fn new(security: &SecurityOptions) -> Result<Self> {
        let secret_key = curve_option_key(&security.curve_secretkey)?;
        let public_key = if security.curve_publickey.is_empty() {
            curve_public_from_secret(&secret_key)
        } else {
            curve_option_key(&security.curve_publickey)?
        };
        let server_key = curve_option_key(&security.curve_serverkey)?;
        let transient_secret = random_array()?;
        let transient_public = curve_public_from_secret(&transient_secret);
        Ok(Self {
            public_key,
            secret_key,
            server_key,
            transient_secret,
            transient_public,
            server_transient_public: None,
            cookie: None,
            send_nonce: 1,
            recv_nonce: 0,
        })
    }

    fn hello_body(&mut self) -> Result<Vec<u8>> {
        let nonce_number = self.next_send_nonce();
        let nonce = curve_nonce(b"CurveZMQHELLO---", nonce_number);
        let signature =
            curve_box_encrypt(&[0u8; 64], &nonce, &self.server_key, &self.transient_secret)?;
        if signature.len() != 80 {
            return Err(Error::InvalidArgument);
        }
        let mut tail = Vec::with_capacity(194);
        tail.extend_from_slice(&[1, 0]);
        tail.extend_from_slice(&[0u8; 72]);
        tail.extend_from_slice(&self.transient_public);
        tail.extend_from_slice(&nonce_number.to_be_bytes());
        tail.extend_from_slice(&signature);
        Ok(command_body("HELLO", tail))
    }

    fn process_welcome(&mut self, frame: &ZmtpFrame) -> Result<()> {
        let tail = command_tail(frame, "WELCOME")?;
        if tail.len() != 160 {
            return Err(Error::InvalidArgument);
        }
        let nonce = curve_long_nonce(b"WELCOME-", tail[..16].try_into().unwrap());
        let plaintext = curve_box_decrypt(
            &tail[16..],
            &nonce,
            &self.server_key,
            &self.transient_secret,
        )?;
        if plaintext.len() != 128 {
            return Err(Error::InvalidArgument);
        }
        let mut server_transient_public = [0u8; 32];
        server_transient_public.copy_from_slice(&plaintext[..32]);
        let mut cookie = [0u8; 96];
        cookie.copy_from_slice(&plaintext[32..128]);
        self.server_transient_public = Some(server_transient_public);
        self.cookie = Some(cookie);
        Ok(())
    }

    fn initiate_body(&mut self, socket_type: SocketType) -> Result<Vec<u8>> {
        let server_transient_public = self.server_transient_public.ok_or(Error::InvalidArgument)?;
        let cookie = self.cookie.ok_or(Error::InvalidArgument)?;
        let mut vouch_nonce_tail = [0u8; 16];
        fill_random(&mut vouch_nonce_tail)?;
        let vouch_nonce = curve_long_nonce(b"VOUCH---", vouch_nonce_tail);
        let mut vouch_plaintext = Vec::with_capacity(64);
        vouch_plaintext.extend_from_slice(&self.transient_public);
        vouch_plaintext.extend_from_slice(&self.server_key);
        let vouch_box = curve_box_encrypt(
            &vouch_plaintext,
            &vouch_nonce,
            &server_transient_public,
            &self.secret_key,
        )?;
        if vouch_box.len() != 80 {
            return Err(Error::InvalidArgument);
        }

        let mut plaintext = Vec::with_capacity(128 + socket_type_metadata(socket_type).len());
        plaintext.extend_from_slice(&self.public_key);
        plaintext.extend_from_slice(&vouch_nonce_tail);
        plaintext.extend_from_slice(&vouch_box);
        plaintext.extend_from_slice(&socket_type_metadata(socket_type));

        let nonce_number = self.next_send_nonce();
        let nonce = curve_nonce(b"CurveZMQINITIATE", nonce_number);
        let initiate_box = curve_box_encrypt(
            &plaintext,
            &nonce,
            &server_transient_public,
            &self.transient_secret,
        )?;
        let mut tail = Vec::with_capacity(104 + initiate_box.len());
        tail.extend_from_slice(&cookie);
        tail.extend_from_slice(&nonce_number.to_be_bytes());
        tail.extend_from_slice(&initiate_box);
        Ok(command_body("INITIATE", tail))
    }

    fn process_ready(&mut self, frame: &ZmtpFrame) -> Result<()> {
        let tail = command_tail(frame, "READY")?;
        if tail.len() < 24 {
            return Err(Error::InvalidArgument);
        }
        let nonce_number = u64::from_be_bytes(tail[..8].try_into().unwrap());
        if nonce_number <= self.recv_nonce {
            return Err(Error::InvalidArgument);
        }
        self.recv_nonce = nonce_number;
        let nonce = curve_nonce(b"CurveZMQREADY---", nonce_number);
        let server_transient_public = self.server_transient_public.ok_or(Error::InvalidArgument)?;
        let plaintext = curve_box_decrypt(
            &tail[8..],
            &nonce,
            &server_transient_public,
            &self.transient_secret,
        )?;
        let _ = ZmtpMetadata::decode_ready(&ready_command_body_from_metadata(plaintext))?;
        Ok(())
    }

    fn session(self) -> Result<CurveSession> {
        Ok(curve_session(
            self.transient_secret,
            self.server_transient_public.ok_or(Error::InvalidArgument)?,
            self.send_nonce,
            self.recv_nonce,
            b"CurveZMQMESSAGEC",
            b"CurveZMQMESSAGES",
        ))
    }

    fn next_send_nonce(&mut self) -> u64 {
        let nonce = self.send_nonce;
        self.send_nonce = self.send_nonce.saturating_add(1);
        nonce
    }
}

impl Drop for CurveClientHandshake {
    fn drop(&mut self) {
        self.secret_key.zeroize();
        self.transient_secret.zeroize();
        self.cookie.zeroize();
    }
}

impl CurveServerHandshake {
    fn session(self) -> CurveSession {
        curve_session(
            self.transient_secret,
            self.client_transient_public,
            1,
            self.peer_nonce,
            b"CurveZMQMESSAGES",
            b"CurveZMQMESSAGEC",
        )
    }
}

impl Drop for CurveServerHandshake {
    fn drop(&mut self) {
        self.server_secret_key.zeroize();
        self.transient_secret.zeroize();
        self.cookie_key.zeroize();
    }
}

fn parse_curve_hello(
    frame: &ZmtpFrame,
    security: &SecurityOptions,
) -> Result<CurveServerHandshake> {
    let tail = command_tail(frame, "HELLO")?;
    if tail.len() != 194 || tail[..2] != [1, 0] {
        return Err(Error::InvalidArgument);
    }
    let server_secret_key = curve_option_key(&security.curve_secretkey)?;
    let server_public_key = curve_public_from_secret(&server_secret_key);
    let mut client_transient_public = [0u8; 32];
    client_transient_public.copy_from_slice(&tail[74..106]);
    let nonce_number = u64::from_be_bytes(tail[106..114].try_into().unwrap());
    let nonce = curve_nonce(b"CurveZMQHELLO---", nonce_number);
    let plaintext = curve_box_decrypt(
        &tail[114..194],
        &nonce,
        &client_transient_public,
        &server_secret_key,
    )?;
    if plaintext != [0u8; 64] {
        return Err(Error::InvalidArgument);
    }
    let transient_secret = random_array()?;
    let transient_public = curve_public_from_secret(&transient_secret);
    let cookie_key = random_array()?;
    Ok(CurveServerHandshake {
        server_public_key,
        server_secret_key,
        transient_secret,
        transient_public,
        client_transient_public,
        cookie_key,
        peer_nonce: nonce_number,
    })
}

fn curve_welcome_body(security: &SecurityOptions, hello: &CurveServerHandshake) -> Result<Vec<u8>> {
    let mut cookie_nonce_tail = [0u8; 16];
    fill_random(&mut cookie_nonce_tail)?;
    let cookie_nonce = curve_long_nonce(b"COOKIE--", cookie_nonce_tail);
    let mut cookie_plaintext = Vec::with_capacity(64);
    cookie_plaintext.extend_from_slice(&hello.client_transient_public);
    cookie_plaintext.extend_from_slice(&hello.transient_secret);
    let cookie_box = curve_secretbox_encrypt(&cookie_plaintext, &cookie_nonce, &hello.cookie_key)?;
    if cookie_box.len() != 80 {
        return Err(Error::InvalidArgument);
    }
    let mut cookie = Vec::with_capacity(96);
    cookie.extend_from_slice(&cookie_nonce_tail);
    cookie.extend_from_slice(&cookie_box);

    let mut welcome_nonce_tail = [0u8; 16];
    fill_random(&mut welcome_nonce_tail)?;
    let welcome_nonce = curve_long_nonce(b"WELCOME-", welcome_nonce_tail);
    let mut plaintext = Vec::with_capacity(128);
    plaintext.extend_from_slice(&hello.transient_public);
    plaintext.extend_from_slice(&cookie);
    let server_secret_key = curve_option_key(&security.curve_secretkey)?;
    let welcome_box = curve_box_encrypt(
        &plaintext,
        &welcome_nonce,
        &hello.client_transient_public,
        &server_secret_key,
    )?;
    if welcome_box.len() != 144 {
        return Err(Error::InvalidArgument);
    }
    let mut tail = Vec::with_capacity(160);
    tail.extend_from_slice(&welcome_nonce_tail);
    tail.extend_from_slice(&welcome_box);
    Ok(command_body("WELCOME", tail))
}

fn parse_curve_initiate(
    frame: &ZmtpFrame,
    _security: &SecurityOptions,
    hello: &mut CurveServerHandshake,
) -> Result<CurveInitiate> {
    let tail = command_tail(frame, "INITIATE")?;
    if tail.len() < 248 {
        return Err(Error::InvalidArgument);
    }
    let cookie_nonce = curve_long_nonce(b"COOKIE--", tail[..16].try_into().unwrap());
    let cookie_plaintext =
        curve_secretbox_decrypt(&tail[16..96], &cookie_nonce, &hello.cookie_key)?;
    if cookie_plaintext.len() != 64
        || cookie_plaintext[..32] != hello.client_transient_public
        || cookie_plaintext[32..64] != hello.transient_secret
    {
        return Err(Error::InvalidArgument);
    }

    let nonce_number = u64::from_be_bytes(tail[96..104].try_into().unwrap());
    if nonce_number <= hello.peer_nonce {
        return Err(Error::InvalidArgument);
    }
    hello.peer_nonce = nonce_number;
    let nonce = curve_nonce(b"CurveZMQINITIATE", nonce_number);
    let plaintext = curve_box_decrypt(
        &tail[104..],
        &nonce,
        &hello.client_transient_public,
        &hello.transient_secret,
    )?;
    if plaintext.len() < 128 {
        return Err(Error::InvalidArgument);
    }
    let mut client_public_key = [0u8; 32];
    client_public_key.copy_from_slice(&plaintext[..32]);
    let vouch_nonce = curve_long_nonce(b"VOUCH---", plaintext[32..48].try_into().unwrap());
    let vouch_plaintext = curve_box_decrypt(
        &plaintext[48..128],
        &vouch_nonce,
        &client_public_key,
        &hello.transient_secret,
    )?;
    if vouch_plaintext.len() != 64
        || vouch_plaintext[..32] != hello.client_transient_public
        || vouch_plaintext[32..64] != hello.server_public_key
    {
        return Err(Error::InvalidArgument);
    }
    let _ =
        ZmtpMetadata::decode_ready(&ready_command_body_from_metadata(plaintext[128..].to_vec()))?;
    Ok(CurveInitiate { client_public_key })
}

fn curve_ready_body(socket_type: SocketType, session: &mut CurveSession) -> Result<Vec<u8>> {
    let nonce_number = session.next_send_nonce();
    let nonce = curve_nonce(b"CurveZMQREADY---", nonce_number);
    let ciphertext = curve_box_encrypt(
        &socket_type_metadata(socket_type),
        &nonce,
        &session.peer_transient_public,
        &session.local_transient_secret,
    )?;
    let mut tail = Vec::with_capacity(8 + ciphertext.len());
    tail.extend_from_slice(&nonce_number.to_be_bytes());
    tail.extend_from_slice(&ciphertext);
    Ok(command_body("READY", tail))
}

fn curve_message_frame(session: &mut CurveSession, data: &[u8], more: bool) -> Result<Vec<u8>> {
    let nonce_number = session.next_send_nonce();
    let nonce = curve_nonce(session.send_prefix, nonce_number);
    let mut payload = Vec::with_capacity(1 + data.len());
    payload.push(u8::from(more));
    payload.extend_from_slice(data);

    #[cfg(feature = "sodium")]
    if let Some(key) = &session.sodium_key {
        return curve_message_frame_sodium(&payload, &nonce, nonce_number, key);
    }

    let ciphertext = curve_message_encrypt(session, payload, &nonce)?;
    let body_len = 1 + "MESSAGE".len() + 8 + ciphertext.len();
    let mut frame = Vec::with_capacity(9 + body_len);
    if body_len <= u8::MAX as usize {
        frame.push(ZMTP_FLAG_COMMAND_LOCAL);
        frame.push(body_len as u8);
    } else {
        frame.push(ZMTP_FLAG_COMMAND_LOCAL | ZMTP_FLAG_LONG_LOCAL);
        frame.extend_from_slice(&(body_len as u64).to_be_bytes());
    }
    frame.push("MESSAGE".len() as u8);
    frame.extend_from_slice(b"MESSAGE");
    frame.extend_from_slice(&nonce_number.to_be_bytes());
    frame.extend_from_slice(&ciphertext);
    Ok(frame)
}

#[cfg(feature = "sodium")]
fn curve_message_frame_sodium(
    payload: &[u8],
    nonce: &[u8; 24],
    nonce_number: u64,
    key: &[u8; 32],
) -> Result<Vec<u8>> {
    let body_len = 1 + "MESSAGE".len() + 8 + 16 + payload.len();
    let mut frame = Vec::with_capacity(9 + body_len);
    if body_len <= u8::MAX as usize {
        frame.push(ZMTP_FLAG_COMMAND_LOCAL);
        frame.push(body_len as u8);
    } else {
        frame.push(ZMTP_FLAG_COMMAND_LOCAL | ZMTP_FLAG_LONG_LOCAL);
        frame.extend_from_slice(&(body_len as u64).to_be_bytes());
    }
    frame.push("MESSAGE".len() as u8);
    frame.extend_from_slice(b"MESSAGE");
    frame.extend_from_slice(&nonce_number.to_be_bytes());
    let ciphertext_start = frame.len();
    frame.resize(ciphertext_start + 16 + payload.len(), 0);
    libzmq_sys::sodium::crypto_box_easy_afternm_encrypt_into(
        &mut frame[ciphertext_start..],
        payload,
        nonce,
        key,
    )
    .ok_or(Error::InvalidArgument)?;
    Ok(frame)
}

fn curve_message_from_body(session: &mut CurveSession, body: &[u8]) -> Result<Message> {
    let tail = curve_message_tail(body)?;
    if tail.len() < 25 {
        return Err(Error::InvalidArgument);
    }
    let nonce_number = u64::from_be_bytes(tail[..8].try_into().unwrap());
    if nonce_number <= session.recv_nonce {
        return Err(Error::InvalidArgument);
    }
    session.recv_nonce = nonce_number;
    let nonce = curve_nonce(session.recv_prefix, nonce_number);
    let plaintext = curve_message_decrypt(session, &tail[8..], &nonce)?;
    if plaintext.is_empty() || plaintext[0] & !0x01 != 0 {
        return Err(Error::InvalidArgument);
    }
    let mut message = Message::from_vec(plaintext[1..].to_vec());
    message.set_more(plaintext[0] & 0x01 != 0);
    Ok(message)
}

fn curve_message_tail(body: &[u8]) -> Result<&[u8]> {
    if body.len() < 8 || body[0] != 7 || &body[1..8] != b"MESSAGE" {
        return Err(Error::InvalidArgument);
    }
    Ok(&body[8..])
}

impl CurveSession {
    fn next_send_nonce(&mut self) -> u64 {
        let nonce = self.send_nonce;
        self.send_nonce = self.send_nonce.saturating_add(1);
        nonce
    }
}

fn curve_option_key(value: &[u8]) -> Result<[u8; 32]> {
    let bytes = if value.len() == 40 {
        z85_decode(std::str::from_utf8(value).map_err(|_| Error::InvalidArgument)?)?
    } else {
        value.to_vec()
    };
    if bytes.len() != 32 {
        return Err(Error::InvalidArgument);
    }
    let mut key = [0u8; 32];
    key.copy_from_slice(&bytes);
    Ok(key)
}

fn curve_public_from_secret(secret: &[u8; 32]) -> [u8; 32] {
    CurveSecretKey::from(*secret).public_key().to_bytes()
}

fn curve_box_for(peer_public: &[u8; 32], local_secret: &[u8; 32]) -> SalsaBox {
    SalsaBox::new(
        &CurvePublicKey::from(*peer_public),
        &CurveSecretKey::from(*local_secret),
    )
}

fn curve_box_encrypt(
    plaintext: &[u8],
    nonce: &[u8; 24],
    recipient_public: &[u8; 32],
    sender_secret: &[u8; 32],
) -> Result<Vec<u8>> {
    let cipher = SalsaBox::new(
        &CurvePublicKey::from(*recipient_public),
        &CurveSecretKey::from(*sender_secret),
    );
    let nonce =
        crypto_box::Nonce::try_from(nonce.as_slice()).map_err(|_| Error::InvalidArgument)?;
    cipher
        .encrypt(&nonce, plaintext)
        .map_err(|_| Error::InvalidArgument)
}

fn curve_box_decrypt(
    ciphertext: &[u8],
    nonce: &[u8; 24],
    sender_public: &[u8; 32],
    recipient_secret: &[u8; 32],
) -> Result<Vec<u8>> {
    let cipher = SalsaBox::new(
        &CurvePublicKey::from(*sender_public),
        &CurveSecretKey::from(*recipient_secret),
    );
    let nonce =
        crypto_box::Nonce::try_from(nonce.as_slice()).map_err(|_| Error::InvalidArgument)?;
    cipher
        .decrypt(&nonce, ciphertext)
        .map_err(|_| Error::InvalidArgument)
}

fn curve_message_encrypt(
    session: &CurveSession,
    mut plaintext: Vec<u8>,
    nonce: &[u8; 24],
) -> Result<Vec<u8>> {
    #[cfg(feature = "sodium")]
    if let Some(key) = &session.sodium_key {
        return libzmq_sys::sodium::crypto_box_easy_afternm_encrypt(&plaintext, nonce, key)
            .ok_or(Error::InvalidArgument);
    }

    let tag = curve_box_encrypt_in_place(&session.send_box, &mut plaintext, nonce)?;
    let mut ciphertext = Vec::with_capacity(tag.len() + plaintext.len());
    ciphertext.extend_from_slice(tag.as_slice());
    ciphertext.extend_from_slice(&plaintext);
    Ok(ciphertext)
}

fn curve_message_decrypt(
    session: &CurveSession,
    ciphertext: &[u8],
    nonce: &[u8; 24],
) -> Result<Vec<u8>> {
    #[cfg(feature = "sodium")]
    if let Some(key) = &session.sodium_key {
        return libzmq_sys::sodium::crypto_box_easy_afternm_decrypt(ciphertext, nonce, key)
            .ok_or(Error::InvalidArgument);
    }

    curve_box_decrypt_in_place(&session.recv_box, ciphertext, nonce)
}

#[allow(deprecated)]
fn curve_box_encrypt_in_place(
    cipher: &SalsaBox,
    plaintext: &mut [u8],
    nonce: &[u8; 24],
) -> Result<CurveTag> {
    let nonce =
        crypto_box::Nonce::try_from(nonce.as_slice()).map_err(|_| Error::InvalidArgument)?;
    cipher
        .encrypt_in_place_detached(&nonce, &[], plaintext)
        .map_err(|_| Error::InvalidArgument)
}

#[allow(deprecated)]
fn curve_box_decrypt_in_place(
    cipher: &SalsaBox,
    ciphertext: &[u8],
    nonce: &[u8; 24],
) -> Result<Vec<u8>> {
    if ciphertext.len() < 16 {
        return Err(Error::InvalidArgument);
    }
    let nonce =
        crypto_box::Nonce::try_from(nonce.as_slice()).map_err(|_| Error::InvalidArgument)?;
    let tag = CurveTag::from_slice(&ciphertext[..16]);
    let mut plaintext = ciphertext[16..].to_vec();
    cipher
        .decrypt_in_place_detached(&nonce, &[], &mut plaintext, tag)
        .map_err(|_| Error::InvalidArgument)?;
    Ok(plaintext)
}

fn curve_secretbox_encrypt(plaintext: &[u8], nonce: &[u8; 24], key: &[u8; 32]) -> Result<Vec<u8>> {
    let key =
        crypto_secretbox::Key::try_from(key.as_slice()).map_err(|_| Error::InvalidArgument)?;
    let nonce =
        crypto_secretbox::Nonce::try_from(nonce.as_slice()).map_err(|_| Error::InvalidArgument)?;
    let cipher = XSalsa20Poly1305::new(&key);
    cipher
        .encrypt(&nonce, plaintext)
        .map_err(|_| Error::InvalidArgument)
}

fn curve_secretbox_decrypt(ciphertext: &[u8], nonce: &[u8; 24], key: &[u8; 32]) -> Result<Vec<u8>> {
    let key =
        crypto_secretbox::Key::try_from(key.as_slice()).map_err(|_| Error::InvalidArgument)?;
    let nonce =
        crypto_secretbox::Nonce::try_from(nonce.as_slice()).map_err(|_| Error::InvalidArgument)?;
    let cipher = XSalsa20Poly1305::new(&key);
    cipher
        .decrypt(&nonce, ciphertext)
        .map_err(|_| Error::InvalidArgument)
}

fn curve_nonce(prefix: &'static [u8; 16], nonce: u64) -> [u8; 24] {
    let mut bytes = [0u8; 24];
    bytes[..16].copy_from_slice(prefix);
    bytes[16..].copy_from_slice(&nonce.to_be_bytes());
    bytes
}

fn curve_long_nonce(prefix: &[u8; 8], tail: [u8; 16]) -> [u8; 24] {
    let mut bytes = [0u8; 24];
    bytes[..8].copy_from_slice(prefix);
    bytes[8..].copy_from_slice(&tail);
    bytes
}

fn random_array<const N: usize>() -> Result<[u8; N]> {
    let mut bytes = [0u8; N];
    fill_random(&mut bytes)?;
    Ok(bytes)
}

fn fill_random(bytes: &mut [u8]) -> Result<()> {
    getrandom::getrandom(bytes).map_err(|_| Error::InvalidSocket)
}

fn socket_type_metadata(socket_type: SocketType) -> Vec<u8> {
    ready_command_body(socket_type)
        .into_iter()
        .skip(6)
        .collect()
}

fn ready_command_body_from_metadata(metadata: Vec<u8>) -> Vec<u8> {
    command_body("READY", metadata)
}

fn write_gssapi_handshake_tcp(
    stream: &mut TcpStreamHandle,
    socket_type: SocketType,
    as_server: bool,
    security: &SecurityOptions,
    context: &ContextShared,
) -> Result<HandshakeResult> {
    #[cfg(feature = "gssapi")]
    {
        write_real_gssapi_handshake(stream, socket_type, as_server, security, context)
    }

    #[cfg(not(feature = "gssapi"))]
    {
        write_placeholder_gssapi_handshake(stream, socket_type, as_server, security, context)
    }
}

#[cfg(not(feature = "gssapi"))]
fn write_placeholder_gssapi_handshake(
    stream: &mut impl GssapiHandshakeIo,
    socket_type: SocketType,
    as_server: bool,
    security: &SecurityOptions,
    context: &ContextShared,
) -> Result<HandshakeResult> {
    if as_server {
        let credentials = parse_gssapi_initiate(&stream.read_zmtp_frame()?)?;
        authorize_gssapi(context, security, &credentials)?;
        stream.write_zmtp_frame_bytes(
            &ZmtpFrame::command(ready_command_body(socket_type)).encode_v3(),
        )?;
        expect_command(&stream.read_zmtp_frame()?, "READY")?;
    } else {
        stream.write_zmtp_frame_bytes(
            &ZmtpFrame::command(gssapi_placeholder_initiate_body(security)).encode_v3(),
        )?;
        expect_command(&stream.read_zmtp_frame()?, "READY")?;
        stream.write_zmtp_frame_bytes(
            &ZmtpFrame::command(ready_command_body(socket_type)).encode_v3(),
        )?;
    }

    Ok(HandshakeResult {
        peer_greeting_done: false,
        peer_ready: true,
        curve_session: None,
        gssapi_session: None,
    })
}

fn write_gssapi_handshake_ipc(
    stream: &mut IpcStreamHandle,
    socket_type: SocketType,
    as_server: bool,
    security: &SecurityOptions,
    context: &ContextShared,
) -> Result<HandshakeResult> {
    #[cfg(feature = "gssapi")]
    {
        write_real_gssapi_handshake(stream, socket_type, as_server, security, context)
    }

    #[cfg(not(feature = "gssapi"))]
    {
        write_placeholder_gssapi_handshake(stream, socket_type, as_server, security, context)
    }
}

#[cfg(feature = "gssapi")]
fn write_real_gssapi_handshake(
    stream: &mut impl GssapiHandshakeIo,
    socket_type: SocketType,
    as_server: bool,
    security: &SecurityOptions,
    context: &ContextShared,
) -> Result<HandshakeResult> {
    if as_server {
        let principal = (!security.gssapi_principal.is_empty()).then_some((
            security.gssapi_principal.as_slice(),
            gssapi_name_type(security.gssapi_principal_nametype)?,
        ));
        let mut server = libzmq_sys::gssapi::ServerContext::new(principal)
            .map_err(|_| Error::InvalidArgument)?;
        loop {
            let token = parse_gssapi_initiate_token(&stream.read_zmtp_frame()?)?;
            match server.accept(&token).map_err(|_| Error::InvalidArgument)? {
                libzmq_sys::gssapi::Step::Continue(reply) => {
                    if reply.is_empty() {
                        return Err(Error::InvalidArgument);
                    }
                    stream.write_zmtp_frame_bytes(
                        &ZmtpFrame::command(gssapi_initiate_body(&reply)).encode_v3(),
                    )?;
                }
                libzmq_sys::gssapi::Step::Complete(reply) => {
                    if !reply.is_empty() {
                        stream.write_zmtp_frame_bytes(
                            &ZmtpFrame::command(gssapi_initiate_body(&reply)).encode_v3(),
                        )?;
                    }
                    break;
                }
            }
        }
        if context.inproc_endpoint("zeromq.zap.01")?.is_some() {
            let credentials = GssapiCredentials {
                principal: server
                    .source_principal()
                    .map_err(|_| Error::InvalidArgument)?,
            };
            authorize_gssapi_via_zap(context, security, &credentials)?;
        }
        let mut session = GssapiSession {
            context: GssapiContext::Server(server),
        };
        stream.write_zmtp_frame_bytes(
            &ZmtpFrame::command(gssapi_ready_body(socket_type, &mut session, security)?)
                .encode_v3(),
        )?;
        gssapi_expect_ready(&stream.read_zmtp_frame()?, &mut session, security)?;
        Ok(HandshakeResult {
            peer_greeting_done: false,
            peer_ready: true,
            curve_session: None,
            gssapi_session: (!security.gssapi_plaintext).then_some(session),
        })
    } else {
        if security.gssapi_service_principal.is_empty() {
            return Err(Error::InvalidArgument);
        }
        let principal = (!security.gssapi_principal.is_empty()).then_some((
            security.gssapi_principal.as_slice(),
            gssapi_name_type(security.gssapi_principal_nametype)?,
        ));
        let mut client = libzmq_sys::gssapi::ClientContext::new(
            &security.gssapi_service_principal,
            gssapi_name_type(security.gssapi_service_principal_nametype)?,
            principal,
        )
        .map_err(|_| Error::InvalidArgument)?;
        let mut complete = false;
        let mut token = match client.initiate(None).map_err(|_| Error::InvalidArgument)? {
            libzmq_sys::gssapi::Step::Continue(token) => token,
            libzmq_sys::gssapi::Step::Complete(token) => {
                complete = true;
                token
            }
        };
        loop {
            if token.is_empty() {
                return Err(Error::InvalidArgument);
            }
            stream.write_zmtp_frame_bytes(
                &ZmtpFrame::command(gssapi_initiate_body(&token)).encode_v3(),
            )?;
            if complete {
                break;
            }
            let reply = parse_gssapi_initiate_token(&stream.read_zmtp_frame()?)?;
            match client
                .initiate(Some(&reply))
                .map_err(|_| Error::InvalidArgument)?
            {
                libzmq_sys::gssapi::Step::Continue(next) => token = next,
                libzmq_sys::gssapi::Step::Complete(next) => {
                    complete = true;
                    token = next;
                }
            }
        }
        let mut session = GssapiSession {
            context: GssapiContext::Client(client),
        };
        gssapi_expect_ready(&stream.read_zmtp_frame()?, &mut session, security)?;
        stream.write_zmtp_frame_bytes(
            &ZmtpFrame::command(gssapi_ready_body(socket_type, &mut session, security)?)
                .encode_v3(),
        )?;
        Ok(HandshakeResult {
            peer_greeting_done: false,
            peer_ready: true,
            curve_session: None,
            gssapi_session: (!security.gssapi_plaintext).then_some(session),
        })
    }
}

#[cfg(not(feature = "gssapi"))]
fn authorize_gssapi(
    context: &ContextShared,
    security: &SecurityOptions,
    credentials: &GssapiCredentials,
) -> Result<()> {
    if context.inproc_endpoint("zeromq.zap.01")?.is_some() {
        return authorize_gssapi_via_zap(context, security, credentials);
    }
    security.authorize_gssapi(credentials)
}

fn authorize_gssapi_via_zap(
    context: &ContextShared,
    security: &SecurityOptions,
    credentials: &GssapiCredentials,
) -> Result<()> {
    let peer_id = context.next_transient_socket_id();
    let inbox = Arc::new(Mutex::new(VecDeque::new()));
    let subscriptions = Arc::new(Mutex::new(SubscriptionState::default()));
    let endpoint = context
        .connect_inproc(
            "zeromq.zap.01",
            peer_id,
            Arc::clone(&inbox),
            SocketType::Req,
            subscriptions,
        )?
        .ok_or(Error::Again)?;
    let request = ZapRequest::new(
        peer_id.to_string(),
        security.zap_domain.clone(),
        Vec::new(),
        Vec::new(),
        "GSSAPI",
        [credentials.principal.clone()],
    );
    send_zap_request(&endpoint, peer_id, request.encode())?;
    let reply = wait_zap_reply(&inbox);
    let _ = context.disconnect_inproc("zeromq.zap.01", peer_id);
    let reply = reply?;
    if reply.status_code.starts_with('2') {
        Ok(())
    } else {
        Err(Error::InvalidArgument)
    }
}

#[cfg(not(feature = "gssapi"))]
fn gssapi_placeholder_initiate_body(security: &SecurityOptions) -> Vec<u8> {
    let mut token = Vec::with_capacity(4 + security.gssapi_principal.len());
    token.extend_from_slice(&(security.gssapi_principal.len() as u32).to_be_bytes());
    token.extend_from_slice(&security.gssapi_principal);
    command_body("INITIATE", token)
}

#[cfg(feature = "gssapi")]
fn gssapi_initiate_body(token: &[u8]) -> Vec<u8> {
    let mut tail = Vec::with_capacity(4 + token.len());
    tail.extend_from_slice(&(token.len() as u32).to_be_bytes());
    tail.extend_from_slice(token);
    command_body("INITIATE", tail)
}

#[cfg(feature = "gssapi")]
fn parse_gssapi_initiate_token(frame: &ZmtpFrame) -> Result<Vec<u8>> {
    let tail = command_tail(frame, "INITIATE")?;
    parse_gssapi_token_tail(tail)
}

#[cfg(feature = "gssapi")]
fn gssapi_ready_body(
    socket_type: SocketType,
    session: &mut GssapiSession,
    security: &SecurityOptions,
) -> Result<Vec<u8>> {
    let ready = ready_command_body(socket_type);
    if security.gssapi_plaintext {
        Ok(ready)
    } else {
        gssapi_wrapped_body(session, 0x02, &ready)
    }
}

#[cfg(feature = "gssapi")]
fn gssapi_expect_ready(
    frame: &ZmtpFrame,
    session: &mut GssapiSession,
    security: &SecurityOptions,
) -> Result<()> {
    if security.gssapi_plaintext {
        return expect_command(frame, "READY");
    }
    let (flags, plaintext) = gssapi_unwrap_body(session, frame.body())?;
    if flags & 0x02 == 0 {
        return Err(Error::InvalidArgument);
    }
    command_tail_body(&plaintext, "READY").map(|_| ())
}

#[cfg(feature = "gssapi")]
fn gssapi_message_frame(session: &mut GssapiSession, data: &[u8], more: bool) -> Result<Vec<u8>> {
    let body = gssapi_wrapped_body(session, u8::from(more), data)?;
    Ok(ZmtpFrame::command(body).encode_v3())
}

#[cfg(feature = "gssapi")]
fn gssapi_message_from_body(session: &mut GssapiSession, body: &[u8]) -> Result<Message> {
    let (flags, plaintext) = gssapi_unwrap_body(session, body)?;
    if flags & !0x01 != 0 {
        return Err(Error::InvalidArgument);
    }
    let mut message = Message::from_vec(plaintext);
    message.set_more(flags & 0x01 != 0);
    Ok(message)
}

#[cfg(not(feature = "gssapi"))]
fn gssapi_message_frame(
    _session: &mut GssapiSession,
    _data: &[u8],
    _more: bool,
) -> Result<Vec<u8>> {
    Err(Error::NotSupported)
}

#[cfg(not(feature = "gssapi"))]
fn gssapi_message_from_body(_session: &mut GssapiSession, _body: &[u8]) -> Result<Message> {
    Err(Error::NotSupported)
}

#[cfg(feature = "gssapi")]
fn gssapi_wrapped_body(session: &mut GssapiSession, flags: u8, data: &[u8]) -> Result<Vec<u8>> {
    let mut plaintext = Vec::with_capacity(1 + data.len());
    plaintext.push(flags);
    plaintext.extend_from_slice(data);
    let wrapped = session.wrap(&plaintext)?;
    let mut tail = Vec::with_capacity(4 + wrapped.len());
    tail.extend_from_slice(&(wrapped.len() as u32).to_be_bytes());
    tail.extend_from_slice(&wrapped);
    Ok(command_body("MESSAGE", tail))
}

#[cfg(feature = "gssapi")]
fn gssapi_unwrap_body(session: &mut GssapiSession, body: &[u8]) -> Result<(u8, Vec<u8>)> {
    let token = parse_gssapi_token_tail(command_tail_body(body, "MESSAGE")?)?;
    let plaintext = session.unwrap(&token)?;
    if plaintext.is_empty() {
        return Err(Error::InvalidArgument);
    }
    Ok((plaintext[0], plaintext[1..].to_vec()))
}

#[cfg(feature = "gssapi")]
fn parse_gssapi_token_tail(tail: &[u8]) -> Result<Vec<u8>> {
    if tail.len() < 4 {
        return Err(Error::InvalidArgument);
    }
    let len = u32::from_be_bytes(tail[..4].try_into().unwrap()) as usize;
    if tail.len() != 4 + len {
        return Err(Error::InvalidArgument);
    }
    Ok(tail[4..].to_vec())
}

#[cfg(feature = "gssapi")]
impl GssapiSession {
    fn wrap(&mut self, plaintext: &[u8]) -> Result<Vec<u8>> {
        match &mut self.context {
            GssapiContext::Client(context) => context.wrap(plaintext),
            GssapiContext::Server(context) => context.wrap(plaintext),
        }
        .map_err(|_| Error::InvalidArgument)
    }

    fn unwrap(&mut self, ciphertext: &[u8]) -> Result<Vec<u8>> {
        match &mut self.context {
            GssapiContext::Client(context) => context.unwrap(ciphertext),
            GssapiContext::Server(context) => context.unwrap(ciphertext),
        }
        .map_err(|_| Error::InvalidArgument)
    }
}

#[cfg(feature = "gssapi")]
fn gssapi_name_type(value: i32) -> Result<libzmq_sys::gssapi::NameType> {
    match value {
        ZMQ_GSSAPI_NT_HOSTBASED => Ok(libzmq_sys::gssapi::NameType::HostBased),
        ZMQ_GSSAPI_NT_USER_NAME => Ok(libzmq_sys::gssapi::NameType::UserName),
        ZMQ_GSSAPI_NT_KRB5_PRINCIPAL => Ok(libzmq_sys::gssapi::NameType::Krb5Principal),
        _ => Err(Error::InvalidArgument),
    }
}

#[cfg(not(feature = "gssapi"))]
fn parse_gssapi_initiate(frame: &ZmtpFrame) -> Result<GssapiCredentials> {
    let tail = command_tail(frame, "INITIATE")?;
    if tail.len() < 4 {
        return Err(Error::InvalidArgument);
    }
    let len = u32::from_be_bytes(tail[..4].try_into().unwrap()) as usize;
    if tail.len() != 4 + len {
        return Err(Error::InvalidArgument);
    }
    Ok(GssapiCredentials {
        principal: tail[4..].to_vec(),
    })
}

fn send_zap_request(endpoint: &InprocEndpoint, peer_id: usize, frames: Vec<Vec<u8>>) -> Result<()> {
    let binder_inbox = endpoint.binder_inbox();
    let mut queue = binder_inbox.lock().map_err(|_| Error::InvalidSocket)?;
    let last = frames.len().saturating_sub(1);
    for (index, frame) in frames.into_iter().enumerate() {
        let mut message = Message::from_vec(frame);
        message.set_routing_id(peer_id as u32);
        message.set_more(index != last);
        queue.push_back(message);
    }
    Ok(())
}

fn wait_zap_reply(inbox: &MessageQueue) -> Result<ZapReply> {
    let deadline = Instant::now() + Duration::from_millis(1_000);
    let mut frames = Vec::new();
    while Instant::now() < deadline {
        let message = inbox.lock().map_err(|_| Error::InvalidSocket)?.pop_front();
        if let Some(message) = message {
            let more = message.more();
            frames.push(message.data().to_vec());
            if !more {
                return ZapReply::decode(&frames);
            }
        } else {
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    Err(Error::Again)
}

fn plain_hello_body(security: &SecurityOptions) -> Vec<u8> {
    let mut tail = Vec::new();
    tail.push(security.plain_username.len().min(u8::MAX as usize) as u8);
    tail.extend_from_slice(
        &security.plain_username[..security.plain_username.len().min(u8::MAX as usize)],
    );
    tail.push(security.plain_password.len().min(u8::MAX as usize) as u8);
    tail.extend_from_slice(
        &security.plain_password[..security.plain_password.len().min(u8::MAX as usize)],
    );
    command_body("HELLO", tail)
}

fn parse_plain_hello(frame: &ZmtpFrame) -> Result<PlainCredentials> {
    let tail = command_tail(frame, "HELLO")?;
    if tail.is_empty() {
        return Err(Error::InvalidArgument);
    }
    let username_len = tail[0] as usize;
    if tail.len() < 1 + username_len + 1 {
        return Err(Error::InvalidArgument);
    }
    let username_start = 1;
    let username_end = username_start + username_len;
    let password_len = tail[username_end] as usize;
    let password_start = username_end + 1;
    let password_end = password_start + password_len;
    if tail.len() != password_end {
        return Err(Error::InvalidArgument);
    }
    Ok(PlainCredentials {
        username: tail[username_start..username_end].to_vec(),
        password: tail[password_start..password_end].to_vec(),
    })
}

fn plain_initiate_body(socket_type: SocketType) -> Vec<u8> {
    let ready = ready_command_body(socket_type);
    command_body("INITIATE", ready.into_iter().skip(6))
}

fn command_body(name: &str, tail: impl IntoIterator<Item = u8>) -> Vec<u8> {
    let mut body = Vec::new();
    body.push(name.len().min(u8::MAX as usize) as u8);
    body.extend_from_slice(&name.as_bytes()[..name.len().min(u8::MAX as usize)]);
    body.extend(tail);
    body
}

fn expect_command(frame: &ZmtpFrame, expected: &str) -> Result<()> {
    command_tail(frame, expected).map(|_| ())
}

fn command_tail<'a>(frame: &'a ZmtpFrame, expected: &str) -> Result<&'a [u8]> {
    if !frame.command_frame() {
        return Err(Error::InvalidArgument);
    }
    command_tail_body(frame.body(), expected)
}

fn command_tail_body<'a>(body: &'a [u8], expected: &str) -> Result<&'a [u8]> {
    if body.is_empty() {
        return Err(Error::InvalidArgument);
    }
    let name_len = body[0] as usize;
    if body.len() < 1 + name_len || &body[1..1 + name_len] != expected.as_bytes() {
        return Err(Error::InvalidArgument);
    }
    Ok(&body[1 + name_len..])
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
        SocketType::Server => "SERVER",
        SocketType::Client => "CLIENT",
        SocketType::Peer => "PEER",
        SocketType::Dgram => "DGRAM",
        SocketType::Pub => "PUB",
        SocketType::Sub => "SUB",
        SocketType::Channel => "CHANNEL",
        SocketType::Scatter => "SCATTER",
        SocketType::Gather => "GATHER",
        SocketType::Radio => "RADIO",
        SocketType::Dish => "DISH",
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

fn set_bytes(target: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    target.clear();
    target.extend_from_slice(value);
    Ok(())
}

fn set_curve_key(target: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    if !(value.len() == 32 || value.len() == 40) {
        return Err(Error::InvalidArgument);
    }
    set_bytes(target, value)
}

fn is_valid_gssapi_nametype(value: i32) -> bool {
    matches!(
        value,
        ZMQ_GSSAPI_NT_HOSTBASED | ZMQ_GSSAPI_NT_USER_NAME | ZMQ_GSSAPI_NT_KRB5_PRINCIPAL
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
