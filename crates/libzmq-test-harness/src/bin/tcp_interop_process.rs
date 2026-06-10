use std::env;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn main() {
    let current = env::current_exe().expect("current executable path is available");
    let bin_dir = current.parent().expect("current executable has a parent");
    println!("{{\"case\":\"tcp_process_interop\"}}");
    run_case(
        bin_dir.join(exe_name("cpp_tcp_pair_peer")),
        bin_dir.join(exe_name("rust_tcp_pair_peer")),
        "cpp_server_rust_client",
        "hello",
    );
    run_case(
        bin_dir.join(exe_name("rust_tcp_pair_peer")),
        bin_dir.join(exe_name("cpp_tcp_pair_peer")),
        "rust_server_cpp_client",
        "world",
    );
}

fn run_case(server_bin: PathBuf, client_bin: PathBuf, name: &str, payload: &str) {
    let endpoint = format!("tcp://127.0.0.1:{}", unused_tcp_port());
    let mut server = Command::new(&server_bin)
        .arg("server")
        .arg(&endpoint)
        .arg(payload)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|error| panic!("failed to spawn {}: {error}", server_bin.display()));
    std::thread::sleep(Duration::from_millis(150));
    let client = Command::new(&client_bin)
        .arg("client")
        .arg(&endpoint)
        .arg(payload)
        .output()
        .unwrap_or_else(|error| panic!("failed to run {}: {error}", client_bin.display()));
    if !client.status.success() {
        let _ = server.kill();
        panic!("client failed: {}", String::from_utf8_lossy(&client.stderr));
    }
    let output = wait_child(server);
    if !output.status.success() {
        println!(
            "{{\"observation\":{{\"type\":\"{name}\",\"blocked\":true,\"error\":\"{}\"}}}}",
            json_escape(String::from_utf8_lossy(&output.stderr).trim())
        );
        return;
    }
    let data = String::from_utf8(output.stdout).unwrap();
    assert_eq!(data.trim(), payload);
    println!("{{\"observation\":{{\"type\":\"{name}\",\"data\":\"{payload}\"}}}}");
}

fn json_escape(input: &str) -> String {
    input.replace('\\', "\\\\").replace('"', "\\\"")
}

fn wait_child(mut child: Child) -> std::process::Output {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if child.try_wait().unwrap().is_some() {
            return child.wait_with_output().unwrap();
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    let _ = child.kill();
    child.wait_with_output().unwrap()
}

fn unused_tcp_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn exe_name(base: &str) -> String {
    if cfg!(windows) {
        format!("{base}.exe")
    } else {
        base.to_string()
    }
}
