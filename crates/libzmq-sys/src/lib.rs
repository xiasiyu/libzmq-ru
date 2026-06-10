//! Platform and third-party FFI isolation for libzmq.
//!
//! Business logic must not call OS or third-party C APIs directly. Future socket,
//! poller, GSSAPI, OpenPGM, and NORM bindings belong in this crate or modules
//! below it.

pub mod platform {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Family {
        Unix,
        Windows,
    }

    pub fn family() -> Family {
        if cfg!(windows) {
            Family::Windows
        } else {
            Family::Unix
        }
    }
}

use std::io;
use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream, ToSocketAddrs, UdpSocket};
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::time::Duration;

pub const POLLIN: i16 = 0x001;
pub const POLLOUT: i16 = 0x004;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddressFamily {
    Inet,
    Inet6,
    Unix,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SockAddr {
    Inet(SocketAddr),
    UnixPath(Vec<u8>),
}

#[derive(Debug)]
pub struct TcpListenerHandle {
    listener: TcpListener,
}

#[derive(Debug)]
pub struct TcpStreamHandle {
    stream: TcpStream,
}

#[derive(Debug)]
pub struct UdpSocketHandle {
    socket: UdpSocket,
}

impl SockAddr {
    pub fn family(&self) -> AddressFamily {
        match self {
            Self::Inet(addr) if matches!(addr.ip(), IpAddr::V6(_)) => AddressFamily::Inet6,
            Self::Inet(_) => AddressFamily::Inet,
            Self::UnixPath(_) => AddressFamily::Unix,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PollEvent {
    pub fd: RawHandle,
    pub events: i16,
    pub revents: i16,
}

impl PollEvent {
    pub fn new(fd: RawHandle, events: i16) -> Self {
        Self {
            fd,
            events,
            revents: 0,
        }
    }
}

impl TcpListenerHandle {
    pub fn bind(addr: impl ToSocketAddrs) -> io::Result<Self> {
        Ok(Self {
            listener: TcpListener::bind(addr)?,
        })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.listener.set_nonblocking(nonblocking)
    }

    pub fn accept(&self) -> io::Result<TcpStreamHandle> {
        let (stream, _) = self.listener.accept()?;
        Ok(TcpStreamHandle { stream })
    }
}

impl TcpStreamHandle {
    pub fn connect(addr: impl ToSocketAddrs) -> io::Result<Self> {
        Ok(Self {
            stream: TcpStream::connect(addr)?,
        })
    }

    pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.stream.set_nonblocking(nonblocking)
    }

    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.stream.set_read_timeout(timeout)
    }

    pub fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.stream.set_write_timeout(timeout)
    }

    pub fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.stream.write_all(bytes)
    }

    pub fn read_exact(&mut self, bytes: &mut [u8]) -> io::Result<()> {
        self.stream.read_exact(bytes)
    }

    pub fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.stream.read(bytes)
    }
}

impl Read for TcpStreamHandle {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.stream.read(buf)
    }
}

impl Write for TcpStreamHandle {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.stream.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.stream.flush()
    }
}

impl UdpSocketHandle {
    pub fn bind(addr: impl ToSocketAddrs) -> io::Result<Self> {
        Ok(Self {
            socket: UdpSocket::bind(addr)?,
        })
    }

    pub fn connect(&self, addr: impl ToSocketAddrs) -> io::Result<()> {
        self.socket.connect(addr)
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.socket.set_nonblocking(nonblocking)
    }

    pub fn join_multicast_v4(&self, group: Ipv4Addr, interface: Ipv4Addr) -> io::Result<()> {
        self.socket.join_multicast_v4(&group, &interface)
    }

    pub fn set_multicast_loop_v4(&self, enabled: bool) -> io::Result<()> {
        self.socket.set_multicast_loop_v4(enabled)
    }

    pub fn set_multicast_ttl_v4(&self, ttl: u32) -> io::Result<()> {
        self.socket.set_multicast_ttl_v4(ttl)
    }

    #[cfg(unix)]
    pub fn set_multicast_if_v4(&self, interface: Ipv4Addr) -> io::Result<()> {
        let addr = libc::in_addr {
            s_addr: u32::from_ne_bytes(interface.octets()),
        };
        // SAFETY: `self.socket.as_raw_fd()` is a valid UDP socket fd, and `addr` points to a
        // properly initialized `in_addr` for the duration of the call.
        let rc = unsafe {
            libc::setsockopt(
                self.socket.as_raw_fd(),
                libc::IPPROTO_IP,
                libc::IP_MULTICAST_IF,
                (&addr as *const libc::in_addr).cast(),
                std::mem::size_of::<libc::in_addr>() as libc::socklen_t,
            )
        };
        if rc == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[cfg(windows)]
    pub fn set_multicast_if_v4(&self, _interface: Ipv4Addr) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "setting IPv4 multicast interface is not implemented on Windows",
        ))
    }

    pub fn send(&self, bytes: &[u8]) -> io::Result<usize> {
        self.socket.send(bytes)
    }

    pub fn send_to(&self, bytes: &[u8], addr: SocketAddr) -> io::Result<usize> {
        self.socket.send_to(bytes, addr)
    }

    pub fn recv_from(&self, bytes: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.socket.recv_from(bytes)
    }
}

#[cfg(unix)]
pub mod ipc {
    use std::io::{self, Read, Write};
    use std::os::unix::net::{UnixListener, UnixStream};
    use std::path::Path;

    #[derive(Debug)]
    pub struct IpcListenerHandle {
        listener: UnixListener,
    }

    #[derive(Debug)]
    pub struct IpcStreamHandle {
        stream: UnixStream,
    }

    impl IpcListenerHandle {
        pub fn bind(path: impl AsRef<Path>) -> io::Result<Self> {
            let path = path.as_ref();
            if path.exists() {
                std::fs::remove_file(path)?;
            }
            Ok(Self {
                listener: UnixListener::bind(path)?,
            })
        }

        pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
            self.listener.set_nonblocking(nonblocking)
        }

        pub fn accept(&self) -> io::Result<IpcStreamHandle> {
            let (stream, _) = self.listener.accept()?;
            Ok(IpcStreamHandle { stream })
        }
    }

    impl IpcStreamHandle {
        pub fn connect(path: impl AsRef<Path>) -> io::Result<Self> {
            Ok(Self {
                stream: UnixStream::connect(path)?,
            })
        }

        pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
            self.stream.set_nonblocking(nonblocking)
        }

        pub fn set_read_timeout(&self, timeout: Option<std::time::Duration>) -> io::Result<()> {
            self.stream.set_read_timeout(timeout)
        }

        pub fn set_write_timeout(&self, timeout: Option<std::time::Duration>) -> io::Result<()> {
            self.stream.set_write_timeout(timeout)
        }

        pub fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
            self.stream.write_all(bytes)
        }

        pub fn read_exact(&mut self, bytes: &mut [u8]) -> io::Result<()> {
            self.stream.read_exact(bytes)
        }

        pub fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
            self.stream.read(bytes)
        }
    }
}

#[cfg(windows)]
pub mod ipc {
    use std::io;

    #[derive(Debug)]
    pub struct IpcListenerHandle;

    #[derive(Debug)]
    pub struct IpcStreamHandle;

    impl IpcListenerHandle {
        pub fn bind(_path: impl AsRef<std::path::Path>) -> io::Result<Self> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "IPC listener is unavailable on Windows in this layer",
            ))
        }

        pub fn set_nonblocking(&self, _nonblocking: bool) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "IPC listener is unavailable on Windows",
            ))
        }

        pub fn accept(&self) -> io::Result<IpcStreamHandle> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "IPC listener is unavailable on Windows",
            ))
        }
    }

    impl IpcStreamHandle {
        pub fn connect(_path: impl AsRef<std::path::Path>) -> io::Result<Self> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "IPC connecter is unavailable on Windows in this layer",
            ))
        }

        pub fn set_nonblocking(&self, _nonblocking: bool) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "IPC stream is unavailable on Windows",
            ))
        }

        pub fn set_read_timeout(&self, _timeout: Option<std::time::Duration>) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "IPC stream is unavailable on Windows",
            ))
        }

        pub fn set_write_timeout(&self, _timeout: Option<std::time::Duration>) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "IPC stream is unavailable on Windows",
            ))
        }

        pub fn write_all(&mut self, _bytes: &[u8]) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "IPC stream is unavailable on Windows",
            ))
        }

        pub fn read_exact(&mut self, _bytes: &mut [u8]) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "IPC stream is unavailable on Windows",
            ))
        }

        pub fn read(&mut self, _bytes: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "IPC stream is unavailable on Windows",
            ))
        }
    }
}

#[cfg(unix)]
mod imp {
    use super::{io, Duration, PollEvent};
    use std::mem::MaybeUninit;
    use std::os::fd::RawFd;

    pub type RawHandle = RawFd;

    #[derive(Debug)]
    pub struct OwnedHandle {
        fd: RawFd,
    }

    impl OwnedHandle {
        pub fn new(fd: RawFd) -> io::Result<Self> {
            if fd < 0 {
                Err(io::Error::from_raw_os_error(libc::EBADF))
            } else {
                Ok(Self { fd })
            }
        }

        pub fn raw(&self) -> RawFd {
            self.fd
        }

        pub fn into_raw(mut self) -> RawFd {
            let fd = self.fd;
            self.fd = -1;
            fd
        }

        pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
            set_nonblocking(self.fd, nonblocking)
        }
    }

    impl Drop for OwnedHandle {
        fn drop(&mut self) {
            if self.fd >= 0 {
                // SAFETY: `fd` is owned by this RAII wrapper and is closed at most once.
                unsafe { libc::close(self.fd) };
            }
        }
    }

    #[derive(Debug)]
    pub struct SignalPair {
        reader: OwnedHandle,
        writer: OwnedHandle,
    }

    impl SignalPair {
        pub fn new() -> io::Result<Self> {
            let mut fds = [0; 2];
            // SAFETY: `fds` points to two valid integers for libc to initialize.
            let rc =
                unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()) };
            if rc != 0 {
                return Err(io::Error::last_os_error());
            }
            let pair = Self {
                reader: OwnedHandle::new(fds[0])?,
                writer: OwnedHandle::new(fds[1])?,
            };
            pair.reader.set_nonblocking(true)?;
            pair.writer.set_nonblocking(true)?;
            Ok(pair)
        }

        pub fn reader(&self) -> RawFd {
            self.reader.raw()
        }

        pub fn writer(&self) -> RawFd {
            self.writer.raw()
        }

        pub fn signal(&self) -> io::Result<()> {
            let byte = [1u8];
            // SAFETY: `writer` is a valid owned socket fd and the byte slice is valid for length 1.
            let rc = unsafe { libc::write(self.writer.raw(), byte.as_ptr().cast(), byte.len()) };
            if rc < 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        }

        pub fn drain(&self) -> io::Result<usize> {
            let mut total = 0;
            let mut buf = [0u8; 64];
            loop {
                // SAFETY: `reader` is a valid owned socket fd and `buf` is writable for `buf.len()` bytes.
                let rc =
                    unsafe { libc::read(self.reader.raw(), buf.as_mut_ptr().cast(), buf.len()) };
                if rc > 0 {
                    total += rc as usize;
                    continue;
                }
                if rc == 0 {
                    return Ok(total);
                }
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::WouldBlock {
                    return Ok(total);
                }
                return Err(err);
            }
        }
    }

    pub fn set_nonblocking(fd: RawFd, nonblocking: bool) -> io::Result<()> {
        // SAFETY: `fd` is supplied by caller and `F_GETFL` does not require an additional argument.
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        if flags < 0 {
            return Err(io::Error::last_os_error());
        }
        let updated = if nonblocking {
            flags | libc::O_NONBLOCK
        } else {
            flags & !libc::O_NONBLOCK
        };
        // SAFETY: `fd` is supplied by caller and `updated` is a valid file status flag set.
        let rc = unsafe { libc::fcntl(fd, libc::F_SETFL, updated) };
        if rc < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub fn pipe() -> io::Result<(OwnedHandle, OwnedHandle)> {
        let mut fds = [0; 2];
        // SAFETY: `fds` points to two valid integers for libc to initialize.
        let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok((OwnedHandle::new(fds[0])?, OwnedHandle::new(fds[1])?))
    }

    pub fn poll(events: &mut [PollEvent], timeout: Option<Duration>) -> io::Result<usize> {
        let timeout_ms = timeout.map(duration_to_ms).unwrap_or(-1);
        let mut pollfds: Vec<libc::pollfd> = events
            .iter()
            .map(|event| libc::pollfd {
                fd: event.fd,
                events: event.events,
                revents: 0,
            })
            .collect();
        // SAFETY: `pollfds` points to `pollfds.len()` initialized pollfd entries.
        let rc = unsafe {
            libc::poll(
                pollfds.as_mut_ptr(),
                pollfds.len() as libc::nfds_t,
                timeout_ms,
            )
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        for (event, pollfd) in events.iter_mut().zip(pollfds.iter()) {
            event.revents = pollfd.revents;
        }
        Ok(rc as usize)
    }

    pub fn select_read(fd: RawFd, timeout: Option<Duration>) -> io::Result<bool> {
        let mut readfds = MaybeUninit::<libc::fd_set>::uninit();
        // SAFETY: `readfds` is allocated and then initialized through FD_ZERO/FD_SET before use.
        unsafe {
            libc::FD_ZERO(readfds.as_mut_ptr());
            libc::FD_SET(fd, readfds.as_mut_ptr());
        }
        let mut timeout_value = timeout.map(duration_to_timeval);
        let timeout_ptr = timeout_value
            .as_mut()
            .map_or(std::ptr::null_mut(), |value| value as *mut libc::timeval);
        // SAFETY: `readfds` is initialized, other fd sets are null, and `timeout_ptr` is null or valid.
        let rc = unsafe {
            libc::select(
                fd + 1,
                readfds.as_mut_ptr(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                timeout_ptr,
            )
        };
        if rc < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(rc > 0)
        }
    }

    pub fn create_tcp_socket() -> io::Result<OwnedHandle> {
        // SAFETY: Arguments are valid constants for creating a TCP socket.
        let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_STREAM, 0) };
        OwnedHandle::new(fd)
    }

    pub fn create_ipc_socket() -> io::Result<OwnedHandle> {
        // SAFETY: Arguments are valid constants for creating a Unix domain stream socket.
        let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
        OwnedHandle::new(fd)
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn create_epoll() -> io::Result<OwnedHandle> {
        // SAFETY: `EPOLL_CLOEXEC` is a valid epoll_create1 flag.
        let fd = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
        OwnedHandle::new(fd)
    }

    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    pub fn create_epoll() -> io::Result<OwnedHandle> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "epoll is unavailable on this platform",
        ))
    }

    #[cfg(any(
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "dragonfly"
    ))]
    pub fn create_kqueue() -> io::Result<OwnedHandle> {
        // SAFETY: `kqueue` takes no arguments and returns a new kernel queue fd on success.
        let fd = unsafe { libc::kqueue() };
        OwnedHandle::new(fd)
    }

    #[cfg(not(any(
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "dragonfly"
    )))]
    pub fn create_kqueue() -> io::Result<OwnedHandle> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "kqueue is unavailable on this platform",
        ))
    }

    fn duration_to_ms(duration: Duration) -> i32 {
        i32::try_from(duration.as_millis()).unwrap_or(i32::MAX)
    }

    fn duration_to_timeval(duration: Duration) -> libc::timeval {
        libc::timeval {
            tv_sec: duration.as_secs() as libc::time_t,
            tv_usec: duration.subsec_micros() as libc::suseconds_t,
        }
    }
}

#[cfg(windows)]
mod imp {
    use super::{io, Duration, PollEvent, POLLIN};
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::os::windows::io::{AsRawSocket, IntoRawSocket, RawSocket};
    use std::time::Instant;
    use windows_sys::Win32::Networking::WinSock::{
        closesocket, ioctlsocket, FIONBIO, POLLRDNORM, POLLWRNORM, SOCKET_ERROR,
    };

    pub type RawHandle = RawSocket;
    const INVALID_SOCKET_VALUE: RawSocket = !0;

    #[derive(Debug)]
    pub struct OwnedHandle {
        socket: RawSocket,
    }

    impl OwnedHandle {
        pub fn new(socket: RawSocket) -> io::Result<Self> {
            if socket == INVALID_SOCKET_VALUE {
                Err(io::Error::last_os_error())
            } else {
                Ok(Self { socket })
            }
        }

        pub fn raw(&self) -> RawSocket {
            self.socket
        }

        pub fn into_raw(mut self) -> RawSocket {
            let socket = self.socket;
            self.socket = INVALID_SOCKET_VALUE;
            socket
        }

        pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
            set_nonblocking(self.socket, nonblocking)
        }
    }

    impl Drop for OwnedHandle {
        fn drop(&mut self) {
            if self.socket != INVALID_SOCKET_VALUE {
                // SAFETY: `socket` is owned by this RAII wrapper and is closed at most once.
                unsafe { closesocket(self.socket as usize) };
            }
        }
    }

    #[derive(Debug)]
    pub struct SignalPair {
        reader: TcpStream,
        writer: TcpStream,
    }

    impl SignalPair {
        pub fn new() -> io::Result<Self> {
            let listener = TcpListener::bind(("127.0.0.1", 0))?;
            let writer = TcpStream::connect(listener.local_addr()?)?;
            let (reader, _) = listener.accept()?;
            reader.set_nonblocking(true)?;
            writer.set_nonblocking(true)?;
            Ok(Self { reader, writer })
        }

        pub fn reader(&self) -> RawSocket {
            self.reader.as_raw_socket()
        }

        pub fn writer(&self) -> RawSocket {
            self.writer.as_raw_socket()
        }

        pub fn signal(&self) -> io::Result<()> {
            let mut writer = &self.writer;
            writer.write_all(&[1])
        }

        pub fn drain(&self) -> io::Result<usize> {
            let mut total = 0;
            let mut buf = [0u8; 64];
            loop {
                let mut reader = &self.reader;
                match reader.read(&mut buf) {
                    Ok(0) => return Ok(total),
                    Ok(n) => total += n,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(total),
                    Err(error) => return Err(error),
                }
            }
        }
    }

    pub fn set_nonblocking(socket: RawSocket, nonblocking: bool) -> io::Result<()> {
        let mut mode = u32::from(nonblocking);
        // SAFETY: `socket` is supplied by caller and `mode` points to a valid u_long value.
        let rc = unsafe { ioctlsocket(socket as usize, FIONBIO, &mut mode) };
        if rc == SOCKET_ERROR {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub fn pipe() -> io::Result<(OwnedHandle, OwnedHandle)> {
        let pair = SignalPair::new()?;
        Ok((
            OwnedHandle::new(pair.reader.into_raw_socket())?,
            OwnedHandle::new(pair.writer.into_raw_socket())?,
        ))
    }

    pub fn poll(events: &mut [PollEvent], timeout: Option<Duration>) -> io::Result<usize> {
        let timeout_at = timeout.map(|timeout| Instant::now() + timeout);
        let mut ready = 0;
        for event in events.iter_mut() {
            event.revents = 0;
            if event.events & POLLIN != 0 {
                event.revents |= POLLRDNORM as i16;
            }
            if event.events & super::POLLOUT != 0 {
                event.revents |= POLLWRNORM as i16;
            }
            if event.revents != 0 {
                ready += 1;
            }
        }
        if ready == 0 {
            if let Some(timeout_at) = timeout_at {
                let now = Instant::now();
                if timeout_at > now {
                    std::thread::sleep(timeout_at - now);
                }
            }
        }
        Ok(ready)
    }

    pub fn select_read(_fd: RawSocket, timeout: Option<Duration>) -> io::Result<bool> {
        if let Some(timeout) = timeout {
            std::thread::sleep(timeout);
        }
        Ok(false)
    }

    pub fn create_tcp_socket() -> io::Result<OwnedHandle> {
        let stream = TcpStream::connect(("127.0.0.1", 9))?;
        OwnedHandle::new(stream.into_raw_socket())
    }

    pub fn create_ipc_socket() -> io::Result<OwnedHandle> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "IPC sockets are unavailable on Windows in this layer",
        ))
    }

    pub fn create_epoll() -> io::Result<OwnedHandle> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "epoll is unavailable on Windows",
        ))
    }

    pub fn create_kqueue() -> io::Result<OwnedHandle> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "kqueue is unavailable on Windows",
        ))
    }
}

pub use imp::{
    create_epoll, create_ipc_socket, create_kqueue, create_tcp_socket, pipe, poll, select_read,
    set_nonblocking,
};
pub use imp::{OwnedHandle, RawHandle, SignalPair};

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddrV4};
    use std::thread;

    #[test]
    fn sockaddr_reports_family() {
        let inet = SockAddr::Inet(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 5555).into());
        assert_eq!(inet.family(), AddressFamily::Inet);
        let unix = SockAddr::UnixPath(b"/tmp/libzmq-test.sock".to_vec());
        assert_eq!(unix.family(), AddressFamily::Unix);
    }

    #[test]
    fn invalid_handle_is_rejected() {
        #[cfg(unix)]
        assert!(OwnedHandle::new(-1).is_err());

        #[cfg(windows)]
        assert!(OwnedHandle::new(!0).is_err());
    }

    #[test]
    fn pipe_handles_support_nonblocking() {
        let (reader, writer) = pipe().expect("pipe should be created");
        reader
            .set_nonblocking(true)
            .expect("reader should become nonblocking");
        writer
            .set_nonblocking(true)
            .expect("writer should become nonblocking");
    }

    #[test]
    fn signaler_wakes_poll_and_select() {
        let signaler = SignalPair::new().expect("signaler should be created");
        let mut events = [PollEvent::new(signaler.reader(), POLLIN)];
        assert_eq!(
            poll(&mut events, Some(Duration::from_millis(0))).unwrap(),
            0
        );
        signaler.signal().expect("signal should write a byte");
        assert_eq!(
            poll(&mut events, Some(Duration::from_millis(100))).unwrap(),
            1
        );
        assert!(events[0].revents & POLLIN != 0);
        assert!(select_read(signaler.reader(), Some(Duration::from_millis(100))).unwrap());
        assert!(signaler.drain().unwrap() > 0);
    }

    #[test]
    fn tcp_and_ipc_socket_syscalls_are_wrapped() {
        let tcp = create_tcp_socket().expect("TCP socket should be created");
        tcp.set_nonblocking(true)
            .expect("TCP socket should become nonblocking");

        #[cfg(unix)]
        {
            let ipc = create_ipc_socket().expect("IPC socket should be created");
            ipc.set_nonblocking(true)
                .expect("IPC socket should become nonblocking");
        }
    }

    #[test]
    fn tcp_listener_and_connecter_exchange_bytes() {
        let listener = TcpListenerHandle::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let mut stream = listener.accept().unwrap();
            let mut bytes = [0u8; 4];
            stream.read_exact(&mut bytes).unwrap();
            assert_eq!(&bytes, b"ping");
            stream.write_all(b"pong").unwrap();
        });

        let mut client = TcpStreamHandle::connect(addr).unwrap();
        client.write_all(b"ping").unwrap();
        let mut bytes = [0u8; 4];
        client.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"pong");
        server.join().unwrap();
    }

    #[test]
    fn udp_sockets_exchange_datagrams() {
        let server = UdpSocketHandle::bind("127.0.0.1:0").unwrap();
        let client = UdpSocketHandle::bind("127.0.0.1:0").unwrap();
        let server_addr = server.local_addr().unwrap();
        client.connect(server_addr).unwrap();

        assert_eq!(client.send(b"ping").unwrap(), 4);
        let mut bytes = [0u8; 8];
        let (size, peer) = server.recv_from(&mut bytes).unwrap();
        assert_eq!(&bytes[..size], b"ping");

        assert_eq!(server.send_to(b"pong", peer).unwrap(), 4);
        let (size, _) = client.recv_from(&mut bytes).unwrap();
        assert_eq!(&bytes[..size], b"pong");
    }

    #[cfg(unix)]
    #[test]
    fn ipc_listener_and_connecter_exchange_bytes() {
        let path = std::env::temp_dir().join(format!(
            "libzmq-ipc-{}-{}.sock",
            std::process::id(),
            "exchange"
        ));
        let listener = ipc::IpcListenerHandle::bind(&path).unwrap();
        let server = thread::spawn(move || {
            let mut stream = listener.accept().unwrap();
            let mut bytes = [0u8; 4];
            stream.read_exact(&mut bytes).unwrap();
            assert_eq!(&bytes, b"ping");
            stream.write_all(b"pong").unwrap();
        });

        let mut client = ipc::IpcStreamHandle::connect(&path).unwrap();
        client.write_all(b"ping").unwrap();
        let mut bytes = [0u8; 4];
        client.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"pong");
        server.join().unwrap();
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn native_poll_backends_are_constructible_or_explicitly_unsupported() {
        match create_epoll() {
            Ok(handle) => drop(handle),
            Err(error) => assert_eq!(error.kind(), io::ErrorKind::Unsupported),
        }
        match create_kqueue() {
            Ok(handle) => drop(handle),
            Err(error) => assert_eq!(error.kind(), io::ErrorKind::Unsupported),
        }
    }
}
