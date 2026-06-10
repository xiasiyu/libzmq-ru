pub mod trace;

pub use trace::{Observation, Operation, TraceCase};

#[cfg(test)]
mod tests {
    use libzmq_core::ZmtpGreeting;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::thread;

    #[test]
    fn ordinary_tcp_sockets_exchange_zmtp_greetings() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut greeting = [0u8; 64];
            stream.read_exact(&mut greeting).unwrap();
            let client_greeting = ZmtpGreeting::decode(&greeting).unwrap();
            assert_eq!(client_greeting.mechanism(), "NULL");
            stream
                .write_all(&ZmtpGreeting::null_server().encode())
                .unwrap();
        });

        let mut client = TcpStream::connect(addr).unwrap();
        client
            .write_all(&ZmtpGreeting::null_client().encode())
            .unwrap();
        let mut greeting = [0u8; 64];
        client.read_exact(&mut greeting).unwrap();
        let server_greeting = ZmtpGreeting::decode(&greeting).unwrap();
        assert_eq!(server_greeting.mechanism(), "NULL");
        server.join().unwrap();
    }
}
