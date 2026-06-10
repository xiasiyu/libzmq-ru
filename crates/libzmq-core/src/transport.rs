use crate::{Error, Result};
use std::net::SocketAddr;
use std::str::FromStr;

pub const ZMTP_GREETING_SIZE: usize = 64;
const ZMTP_FLAG_MORE: u8 = 0x01;
const ZMTP_FLAG_LONG: u8 = 0x02;
const ZMTP_FLAG_COMMAND: u8 = 0x04;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Endpoint {
    Inproc(String),
    Tcp(TcpEndpoint),
    Ipc(IpcEndpoint),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpEndpoint {
    host: String,
    port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IpcEndpoint {
    path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZmtpGreeting {
    major: u8,
    minor: u8,
    mechanism: String,
    as_server: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZmtpFrame {
    body: Vec<u8>,
    more: bool,
    command: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZmtpMetadata {
    properties: Vec<(String, Vec<u8>)>,
}

impl Endpoint {
    pub fn parse(endpoint: &str) -> Result<Self> {
        if let Some(name) = endpoint.strip_prefix("inproc://") {
            if name.is_empty() {
                return Err(Error::InvalidArgument);
            }
            return Ok(Self::Inproc(name.to_string()));
        }
        if endpoint.starts_with("tcp://") {
            return TcpEndpoint::parse(endpoint).map(Self::Tcp);
        }
        if endpoint.starts_with("ipc://") {
            return IpcEndpoint::parse(endpoint).map(Self::Ipc);
        }
        Err(Error::NotSupported)
    }
}

impl TcpEndpoint {
    pub fn parse(endpoint: &str) -> Result<Self> {
        let authority = endpoint
            .strip_prefix("tcp://")
            .ok_or(Error::InvalidArgument)?;
        let (host, port) = split_host_port(authority)?;
        if host.is_empty() {
            return Err(Error::InvalidArgument);
        }
        Ok(Self {
            host: host.to_string(),
            port,
        })
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn bind_addr(&self) -> String {
        let host = if self.host == "*" {
            "0.0.0.0"
        } else {
            &self.host
        };
        format_host_port(host, self.port)
    }

    pub fn connect_addr(&self) -> Result<String> {
        if self.host == "*" {
            return Err(Error::InvalidArgument);
        }
        Ok(format_host_port(&self.host, self.port))
    }
}

impl IpcEndpoint {
    pub fn parse(endpoint: &str) -> Result<Self> {
        let path = endpoint
            .strip_prefix("ipc://")
            .ok_or(Error::InvalidArgument)?;
        if path.is_empty() {
            return Err(Error::InvalidArgument);
        }
        Ok(Self {
            path: path.to_string(),
        })
    }

    pub fn path(&self) -> &str {
        &self.path
    }
}

impl ZmtpGreeting {
    pub fn null_client() -> Self {
        Self {
            major: 3,
            minor: 1,
            mechanism: "NULL".to_string(),
            as_server: false,
        }
    }

    pub fn null_server() -> Self {
        Self {
            as_server: true,
            ..Self::null_client()
        }
    }

    pub fn major(&self) -> u8 {
        self.major
    }

    pub fn minor(&self) -> u8 {
        self.minor
    }

    pub fn mechanism(&self) -> &str {
        &self.mechanism
    }

    pub fn as_server(&self) -> bool {
        self.as_server
    }

    pub fn encode(&self) -> [u8; ZMTP_GREETING_SIZE] {
        let mut bytes = [0u8; ZMTP_GREETING_SIZE];
        bytes[0] = 0xFF;
        bytes[8] = 0x01;
        bytes[9] = 0x7F;
        bytes[10] = self.major;
        bytes[11] = self.minor;
        let mechanism = self.mechanism.as_bytes();
        let len = mechanism.len().min(20);
        bytes[12..12 + len].copy_from_slice(&mechanism[..len]);
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != ZMTP_GREETING_SIZE || bytes[0] != 0xFF || bytes[9] != 0x7F {
            return Err(Error::InvalidArgument);
        }
        let mechanism = bytes[12..32]
            .split(|byte| *byte == 0)
            .next()
            .unwrap_or_default();
        let mechanism = std::str::from_utf8(mechanism).map_err(|_| Error::InvalidArgument)?;
        Ok(Self {
            major: bytes[10],
            minor: bytes[11],
            mechanism: mechanism.to_string(),
            as_server: bytes[32] != 0,
        })
    }
}

impl ZmtpFrame {
    pub fn message(body: impl Into<Vec<u8>>) -> Self {
        Self {
            body: body.into(),
            more: false,
            command: false,
        }
    }

    pub fn command(body: impl Into<Vec<u8>>) -> Self {
        Self {
            body: body.into(),
            more: false,
            command: true,
        }
    }

    pub fn with_more(mut self, more: bool) -> Self {
        self.more = more;
        self
    }

    pub fn body(&self) -> &[u8] {
        &self.body
    }

    pub fn more(&self) -> bool {
        self.more
    }

    pub fn command_frame(&self) -> bool {
        self.command
    }

    pub fn encode_v1(&self) -> Vec<u8> {
        self.encode_with_command_bit(false)
    }

    pub fn decode_v1(bytes: &[u8]) -> Result<Self> {
        Self::decode_with_command_bit(bytes, false)
    }

    pub fn encode_v2(&self) -> Vec<u8> {
        self.encode_with_command_bit(false)
    }

    pub fn decode_v2(bytes: &[u8]) -> Result<Self> {
        Self::decode_with_command_bit(bytes, false)
    }

    pub fn encode_v3(&self) -> Vec<u8> {
        self.encode_with_command_bit(true)
    }

    pub fn decode_v3(bytes: &[u8]) -> Result<Self> {
        Self::decode_with_command_bit(bytes, true)
    }

    fn encode_with_command_bit(&self, include_command: bool) -> Vec<u8> {
        let mut flags = 0;
        if self.more {
            flags |= ZMTP_FLAG_MORE;
        }
        if include_command && self.command {
            flags |= ZMTP_FLAG_COMMAND;
        }
        let len = self.body.len();
        let mut bytes = Vec::with_capacity(10 + len);
        if len <= u8::MAX as usize {
            bytes.push(flags);
            bytes.push(len as u8);
        } else {
            bytes.push(flags | ZMTP_FLAG_LONG);
            bytes.extend_from_slice(&(len as u64).to_be_bytes());
        }
        bytes.extend_from_slice(&self.body);
        bytes
    }

    fn decode_with_command_bit(bytes: &[u8], include_command: bool) -> Result<Self> {
        if bytes.len() < 2 {
            return Err(Error::InvalidArgument);
        }
        let flags = bytes[0];
        let (header_len, body_len) = if flags & ZMTP_FLAG_LONG != 0 {
            if bytes.len() < 9 {
                return Err(Error::InvalidArgument);
            }
            let mut len = [0u8; 8];
            len.copy_from_slice(&bytes[1..9]);
            (9, u64::from_be_bytes(len) as usize)
        } else {
            (2, bytes[1] as usize)
        };
        if bytes.len() != header_len + body_len {
            return Err(Error::InvalidArgument);
        }
        Ok(Self {
            body: bytes[header_len..].to_vec(),
            more: flags & ZMTP_FLAG_MORE != 0,
            command: include_command && flags & ZMTP_FLAG_COMMAND != 0,
        })
    }
}

impl ZmtpMetadata {
    pub fn new(
        properties: impl IntoIterator<Item = (impl Into<String>, impl Into<Vec<u8>>)>,
    ) -> Self {
        Self {
            properties: properties
                .into_iter()
                .map(|(name, value)| (name.into(), value.into()))
                .collect(),
        }
    }

    pub fn encode_ready(&self) -> Vec<u8> {
        let mut body = Vec::new();
        body.push(5);
        body.extend_from_slice(b"READY");
        for (name, value) in &self.properties {
            body.push(name.len().min(u8::MAX as usize) as u8);
            body.extend_from_slice(&name.as_bytes()[..name.len().min(u8::MAX as usize)]);
            body.extend_from_slice(&(value.len() as u32).to_be_bytes());
            body.extend_from_slice(value);
        }
        body
    }

    pub fn decode_ready(body: &[u8]) -> Result<Self> {
        if body.len() < 6 || body[0] != 5 || &body[1..6] != b"READY" {
            return Err(Error::InvalidArgument);
        }
        let mut offset = 6;
        let mut properties = Vec::new();
        while offset < body.len() {
            let name_len = body[offset] as usize;
            offset += 1;
            if body.len() < offset + name_len + 4 {
                return Err(Error::InvalidArgument);
            }
            let name = std::str::from_utf8(&body[offset..offset + name_len])
                .map_err(|_| Error::InvalidArgument)?
                .to_string();
            offset += name_len;
            let mut value_len = [0u8; 4];
            value_len.copy_from_slice(&body[offset..offset + 4]);
            offset += 4;
            let value_len = u32::from_be_bytes(value_len) as usize;
            if body.len() < offset + value_len {
                return Err(Error::InvalidArgument);
            }
            properties.push((name, body[offset..offset + value_len].to_vec()));
            offset += value_len;
        }
        Ok(Self { properties })
    }

    pub fn get(&self, name: &str) -> Option<&[u8]> {
        self.properties
            .iter()
            .find_map(|(stored, value)| (stored == name).then_some(value.as_slice()))
    }
}

fn split_host_port(authority: &str) -> Result<(&str, u16)> {
    if let Ok(addr) = SocketAddr::from_str(authority) {
        return Ok((
            match addr {
                SocketAddr::V4(addr) => authority
                    .strip_suffix(&format!(":{}", addr.port()))
                    .unwrap_or_default(),
                SocketAddr::V6(_) => authority
                    .rsplit_once(']')
                    .map(|(host, _)| host.trim_start_matches('['))
                    .unwrap_or_default(),
            },
            addr.port(),
        ));
    }

    let (host, port) = authority.rsplit_once(':').ok_or(Error::InvalidArgument)?;
    let port = port.parse::<u16>().map_err(|_| Error::InvalidArgument)?;
    Ok((host, port))
}

fn format_host_port(host: &str, port: u16) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_tcp_endpoints() {
        let endpoint = TcpEndpoint::parse("tcp://127.0.0.1:5555").unwrap();
        assert_eq!(endpoint.host(), "127.0.0.1");
        assert_eq!(endpoint.port(), 5555);
        assert_eq!(endpoint.connect_addr().unwrap(), "127.0.0.1:5555");

        let wildcard = TcpEndpoint::parse("tcp://*:0").unwrap();
        assert_eq!(wildcard.bind_addr(), "0.0.0.0:0");
        assert_eq!(wildcard.connect_addr(), Err(Error::InvalidArgument));

        let ipv6 = TcpEndpoint::parse("tcp://[::1]:5555").unwrap();
        assert_eq!(ipv6.host(), "::1");
        assert_eq!(ipv6.connect_addr().unwrap(), "[::1]:5555");
    }

    #[test]
    fn parses_ipc_and_inproc_endpoints() {
        let ipc = IpcEndpoint::parse("ipc:///tmp/libzmq.sock").unwrap();
        assert_eq!(ipc.path(), "/tmp/libzmq.sock");
        assert!(matches!(
            Endpoint::parse("inproc://name").unwrap(),
            Endpoint::Inproc(name) if name == "name"
        ));
        assert_eq!(Endpoint::parse("ipc://"), Err(Error::InvalidArgument));
    }

    #[test]
    fn zmtp_greeting_round_trips() {
        let encoded = ZmtpGreeting::null_server().encode();
        let decoded = ZmtpGreeting::decode(&encoded).unwrap();
        assert_eq!(decoded.major(), 3);
        assert_eq!(decoded.minor(), 1);
        assert_eq!(decoded.mechanism(), "NULL");
        assert_eq!(decoded.mechanism(), "NULL");
    }

    #[test]
    fn zmtp_v1_and_v2_frames_round_trip_short_and_long_bodies() {
        let short = ZmtpFrame::message(b"hello".to_vec()).with_more(true);
        let decoded = ZmtpFrame::decode_v2(&short.encode_v2()).unwrap();
        assert_eq!(decoded.body(), b"hello");
        assert!(decoded.more());

        let long_body = vec![7u8; 300];
        let long = ZmtpFrame::message(long_body.clone());
        let encoded = long.encode_v1();
        assert_eq!(encoded[0] & ZMTP_FLAG_LONG, ZMTP_FLAG_LONG);
        let decoded = ZmtpFrame::decode_v1(&encoded).unwrap();
        assert_eq!(decoded.body(), long_body.as_slice());
    }

    #[test]
    fn zmtp_v3_command_frames_round_trip() {
        let frame = ZmtpFrame::command(b"READY".to_vec());
        let encoded = frame.encode_v3();
        assert_eq!(encoded[0] & ZMTP_FLAG_COMMAND, ZMTP_FLAG_COMMAND);
        let decoded = ZmtpFrame::decode_v3(&encoded).unwrap();
        assert!(decoded.command_frame());
        assert_eq!(decoded.body(), b"READY");
    }

    #[test]
    fn zmtp_ready_metadata_round_trips() {
        let metadata = ZmtpMetadata::new([("Socket-Type", b"PAIR".to_vec())]);
        let decoded = ZmtpMetadata::decode_ready(&metadata.encode_ready()).unwrap();
        assert_eq!(decoded.get("Socket-Type"), Some(b"PAIR".as_slice()));
        assert_eq!(
            ZmtpMetadata::decode_ready(b"HELLO"),
            Err(Error::InvalidArgument)
        );
    }
}
