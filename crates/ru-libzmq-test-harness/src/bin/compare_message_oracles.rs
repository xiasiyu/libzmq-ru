use std::env;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    let current = env::current_exe().expect("current executable path is available");
    let bin_dir = current
        .parent()
        .expect("current executable has a parent directory");

    let cpp = run(bin_dir.join(exe_name("cpp-message-oracle")));
    let rust = run(bin_dir.join(exe_name("rust-message-oracle")));

    if cpp != rust {
        eprintln!("message oracle traces differ");
        eprintln!("--- cpp ---\n{cpp}");
        eprintln!("--- rust ---\n{rust}");
        std::process::exit(1);
    }

    print!("{rust}");
}

fn run(path: PathBuf) -> String {
    let output = Command::new(&path)
        .output()
        .unwrap_or_else(|error| panic!("failed to run {}: {error}", path.display()));
    if !output.status.success() {
        panic!(
            "{} exited with status {:?}: {}",
            path.display(),
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    String::from_utf8(output.stdout).expect("oracle output is UTF-8")
}

fn exe_name(base: &str) -> String {
    if cfg!(windows) {
        format!("{base}.exe")
    } else {
        base.to_string()
    }
}
