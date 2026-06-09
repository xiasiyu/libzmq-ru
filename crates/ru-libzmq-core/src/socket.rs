use crate::constants::*;
use crate::context::{ContextShared, InprocEndpoint, MessageQueue};
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
    inproc: Mutex<InprocState>,
    last_recv_more: Mutex<bool>,
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
            inproc: Mutex::new(InprocState::default()),
            last_recv_more: Mutex::new(false),
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
        let bound = self
            .context
            .bind_inproc(endpoint, Arc::clone(&self.inbox))?;
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
        let bound = self
            .context
            .connect_inproc(endpoint, Arc::clone(&self.inbox))?;
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
        self.context.disconnect_inproc(endpoint, &self.inbox)?;
        inproc.connected_endpoint = None;
        inproc.direct_outbox = None;
        Ok(())
    }

    pub fn send(&self, mut message: Message, flags: i32) -> Result<usize> {
        let size = message.len();
        message.set_more(flags & ZMQ_SNDMORE != 0);
        let outbox = self.resolve_outbox()?;
        let options = self
            .options
            .lock()
            .map_err(|_| Error::InvalidSocket)?
            .clone();
        let mut queue = outbox.lock().map_err(|_| Error::InvalidSocket)?;
        if options.conflate {
            queue.clear();
        } else if options.sndhwm > 0 && queue.len() >= options.sndhwm as usize {
            return Err(Error::Again);
        }
        queue.push_back(message);
        Ok(size)
    }

    pub fn recv(&self, _flags: i32) -> Result<Message> {
        let mut inbox = self.inbox.lock().map_err(|_| Error::InvalidSocket)?;
        let message = inbox.pop_front().ok_or(Error::Again)?;
        *self
            .last_recv_more
            .lock()
            .map_err(|_| Error::InvalidSocket)? = message.more();
        Ok(message)
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
            ZMQ_SNDHWM | ZMQ_RCVHWM | ZMQ_SNDTIMEO | ZMQ_RCVTIMEO => {
                return Err(Error::InvalidArgument)
            }
            _ => return Err(Error::InvalidArgument),
        }
        Ok(())
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

    fn resolve_outbox(&self) -> Result<MessageQueue> {
        let inproc = self.inproc.lock().map_err(|_| Error::InvalidSocket)?;
        if let Some(outbox) = &inproc.direct_outbox {
            return Ok(Arc::clone(outbox));
        }
        if let Some(endpoint) = &inproc.connected_endpoint {
            if let Some(bound_endpoint) = self.context.inproc_endpoint(endpoint)? {
                return Ok(bound_endpoint.binder_inbox());
            }
        }
        if let Some(bound_endpoint) = &inproc.bound_endpoint {
            if let Some(peer) = bound_endpoint.first_peer()? {
                return Ok(peer);
            }
        }
        Err(Error::Again)
    }
}
