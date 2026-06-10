use crate::{Error, Result};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::str::FromStr;

pub const ZMTP_GREETING_SIZE: usize = 64;
const ZMTP_FLAG_MORE: u8 = 0x01;
const ZMTP_FLAG_LONG: u8 = 0x02;
const ZMTP_FLAG_COMMAND: u8 = 0x04;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Endpoint {
    Inproc(String),
    Tcp(TcpEndpoint),
    Udp(UdpEndpoint),
    Ws(WsEndpoint),
    Wss(WsEndpoint),
    Ipc(IpcEndpoint),
    Norm(NormEndpoint),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpEndpoint {
    host: String,
    port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdpEndpoint {
    host: String,
    port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WsEndpoint {
    host: String,
    port: u16,
    path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IpcEndpoint {
    path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormEndpoint {
    interface: Option<String>,
    address: String,
    port: u16,
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
        if endpoint.starts_with("udp://") {
            return UdpEndpoint::parse(endpoint).map(Self::Udp);
        }
        if endpoint.starts_with("ws://") {
            return WsEndpoint::parse(endpoint).map(Self::Ws);
        }
        if endpoint.starts_with("wss://") {
            return WsEndpoint::parse_wss(endpoint).map(Self::Wss);
        }
        if endpoint.starts_with("ipc://") {
            return IpcEndpoint::parse(endpoint).map(Self::Ipc);
        }
        if endpoint.starts_with("norm://") {
            return NormEndpoint::parse(endpoint).map(Self::Norm);
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

impl UdpEndpoint {
    pub fn parse(endpoint: &str) -> Result<Self> {
        let authority = endpoint
            .strip_prefix("udp://")
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

    pub fn bind_addr(&self) -> String {
        let host = if self.host == "*" || self.multicast_v4().is_some() {
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

    pub fn multicast_v4(&self) -> Option<Ipv4Addr> {
        match self.host.parse::<IpAddr>().ok()? {
            IpAddr::V4(addr) if addr.is_multicast() => Some(addr),
            _ => None,
        }
    }
}

impl WsEndpoint {
    pub fn parse(endpoint: &str) -> Result<Self> {
        Self::parse_with_prefix(endpoint, "ws://")
    }

    pub fn parse_wss(endpoint: &str) -> Result<Self> {
        Self::parse_with_prefix(endpoint, "wss://")
    }

    fn parse_with_prefix(endpoint: &str, prefix: &str) -> Result<Self> {
        let authority = endpoint
            .strip_prefix(prefix)
            .ok_or(Error::InvalidArgument)?;
        let (authority, path) = authority.split_once('/').unwrap_or((authority, ""));
        let (host, port) = split_host_port(authority)?;
        if host.is_empty() {
            return Err(Error::InvalidArgument);
        }
        Ok(Self {
            host: host.to_string(),
            port,
            path: format!("/{path}"),
        })
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

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn path(&self) -> &str {
        &self.path
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

impl NormEndpoint {
    pub fn parse(endpoint: &str) -> Result<Self> {
        let authority = endpoint
            .strip_prefix("norm://")
            .ok_or(Error::InvalidArgument)?;
        let (interface, authority) = match authority.split_once(';') {
            Some((interface, rest)) if !interface.is_empty() && !rest.is_empty() => {
                (Some(interface.to_string()), rest)
            }
            Some(_) => return Err(Error::InvalidArgument),
            None => (None, authority),
        };
        let (address, port) = split_host_port(authority)?;
        if address.is_empty() || address == "*" {
            return Err(Error::InvalidArgument);
        }
        Ok(Self {
            interface,
            address: address.to_string(),
            port,
        })
    }

    pub fn interface(&self) -> Option<&str> {
        self.interface.as_deref()
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub fn port(&self) -> u16 {
        self.port
    }
}

impl ZmtpGreeting {
    pub fn new(mechanism: impl Into<String>, as_server: bool) -> Self {
        Self {
            major: 3,
            minor: 1,
            mechanism: mechanism.into(),
            as_server,
        }
    }

    pub fn null_client() -> Self {
        Self::new("NULL", false)
    }

    pub fn null_server() -> Self {
        Self::new("NULL", true)
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
        bytes[32] = u8::from(self.as_server);
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
    fn parses_udp_endpoints() {
        let endpoint = UdpEndpoint::parse("udp://127.0.0.1:5555").unwrap();
        assert_eq!(endpoint.connect_addr().unwrap(), "127.0.0.1:5555");

        let wildcard = UdpEndpoint::parse("udp://*:0").unwrap();
        assert_eq!(wildcard.bind_addr(), "0.0.0.0:0");
        assert_eq!(wildcard.connect_addr(), Err(Error::InvalidArgument));
        assert!(matches!(
            Endpoint::parse("udp://127.0.0.1:5555").unwrap(),
            Endpoint::Udp(_)
        ));

        let multicast = UdpEndpoint::parse("udp://239.255.0.1:5555").unwrap();
        assert_eq!(multicast.bind_addr(), "0.0.0.0:5555");
        assert_eq!(multicast.connect_addr().unwrap(), "239.255.0.1:5555");
        assert_eq!(multicast.multicast_v4().unwrap().octets(), [239, 255, 0, 1]);
    }

    #[test]
    fn parses_ws_endpoints() {
        let endpoint = WsEndpoint::parse("ws://127.0.0.1:8080/socket").unwrap();
        assert_eq!(endpoint.host(), "127.0.0.1");
        assert_eq!(endpoint.port(), 8080);
        assert_eq!(endpoint.path(), "/socket");
        assert_eq!(endpoint.connect_addr().unwrap(), "127.0.0.1:8080");

        let default_path = WsEndpoint::parse("ws://127.0.0.1:8080").unwrap();
        assert_eq!(default_path.path(), "/");

        let wildcard = WsEndpoint::parse("ws://*:8080").unwrap();
        assert_eq!(wildcard.bind_addr(), "0.0.0.0:8080");
        assert_eq!(wildcard.connect_addr(), Err(Error::InvalidArgument));
        assert!(matches!(
            Endpoint::parse("ws://127.0.0.1:8080/socket").unwrap(),
            Endpoint::Ws(_)
        ));
        assert!(matches!(
            Endpoint::parse("wss://127.0.0.1:8443/socket").unwrap(),
            Endpoint::Wss(_)
        ));
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
    fn parses_norm_endpoints() {
        let endpoint = NormEndpoint::parse("norm://127.0.0.1:5555").unwrap();
        assert_eq!(endpoint.interface(), None);
        assert_eq!(endpoint.address(), "127.0.0.1");
        assert_eq!(endpoint.port(), 5555);

        let with_interface = NormEndpoint::parse("norm://en0;224.1.2.3:6000").unwrap();
        assert_eq!(with_interface.interface(), Some("en0"));
        assert_eq!(with_interface.address(), "224.1.2.3");
        assert_eq!(with_interface.port(), 6000);
        assert!(matches!(
            Endpoint::parse("norm://127.0.0.1:5555").unwrap(),
            Endpoint::Norm(_)
        ));
        assert_eq!(
            NormEndpoint::parse("norm://*:5555"),
            Err(Error::InvalidArgument)
        );
        assert_eq!(
            NormEndpoint::parse("norm://en0;"),
            Err(Error::InvalidArgument)
        );
    }

    #[test]
    fn unsupported_optional_transport_schemes_are_explicit() {
        for endpoint in [
            "pgm://127.0.0.1:5555",
            "epgm://127.0.0.1:5555",
            "tipc://{5560,0,0}",
            "vmci://1:5555",
            "vsock://2:5555",
        ] {
            assert_eq!(Endpoint::parse(endpoint), Err(Error::NotSupported));
        }
    }

    #[test]
    fn zmtp_greeting_round_trips() {
        let encoded = ZmtpGreeting::null_server().encode();
        let decoded = ZmtpGreeting::decode(&encoded).unwrap();
        assert_eq!(decoded.major(), 3);
        assert_eq!(decoded.minor(), 1);
        assert_eq!(decoded.mechanism(), "NULL");
        assert!(decoded.as_server());
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
