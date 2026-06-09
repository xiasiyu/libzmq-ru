use crate::constants::*;
use crate::{Error, Message, Result};
use std::convert::TryFrom;
use std::sync::Mutex;

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
}

#[derive(Debug, Clone)]
struct SocketOptions {
    linger: i32,
    sndhwm: i32,
    rcvhwm: i32,
    sndtimeo: i32,
    rcvtimeo: i32,
}

impl Default for SocketOptions {
    fn default() -> Self {
        Self {
            linger: -1,
            sndhwm: 1000,
            rcvhwm: 1000,
            sndtimeo: -1,
            rcvtimeo: -1,
        }
    }
}

impl Socket {
    pub(crate) fn new(id: usize, socket_type: SocketType) -> Self {
        Self {
            id,
            socket_type,
            options: Mutex::new(SocketOptions::default()),
        }
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn socket_type(&self) -> SocketType {
        self.socket_type
    }

    pub fn bind(&self, _endpoint: &str) -> Result<()> {
        Err(Error::NotImplemented("socket bind"))
    }

    pub fn connect(&self, _endpoint: &str) -> Result<()> {
        Err(Error::NotImplemented("socket connect"))
    }

    pub fn send(&self, _message: Message, _flags: i32) -> Result<usize> {
        Err(Error::NotImplemented("socket send"))
    }

    pub fn recv(&self, _flags: i32) -> Result<Message> {
        Err(Error::NotImplemented("socket recv"))
    }

    pub fn set_option_i32(&self, option: i32, value: i32) -> Result<()> {
        let mut options = self.options.lock().map_err(|_| Error::InvalidSocket)?;
        match option {
            ZMQ_LINGER => options.linger = value,
            ZMQ_SNDHWM if value >= 0 => options.sndhwm = value,
            ZMQ_RCVHWM if value >= 0 => options.rcvhwm = value,
            ZMQ_SNDTIMEO if value >= -1 => options.sndtimeo = value,
            ZMQ_RCVTIMEO if value >= -1 => options.rcvtimeo = value,
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
            ZMQ_RCVMORE => Ok(0),
            ZMQ_THREAD_SAFE => Ok(0),
            _ => Err(Error::InvalidArgument),
        }
    }
}
