use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

const DEFAULT_THRESHOLD: f64 = 8.0;
const FULL_THRESHOLD: f64 = 10.0;

#[derive(Debug, Default)]
struct Stats {
    files: usize,
    code_lines: usize,
    unsafe_lines: usize,
    unsafe_tokens: usize,
    disallowed: Vec<UnsafeHit>,
}

#[derive(Debug)]
struct UnsafeHit {
    path: PathBuf,
    line: usize,
    text: String,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("unsafe-report: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    let write_path = match args.as_slice() {
        [] => None,
        [flag, path] if flag == "--write" => Some(PathBuf::from(path)),
        _ => return Err("usage: unsafe-report [--write <path>]".to_string()),
    };

    let root = workspace_root()?;
    let production = scan_roots(&root, production_roots(), true)?;
    let workspace = scan_roots(&root, workspace_roots(), false)?;
    let report = render_report(&production, &workspace);

    if let Some(path) = write_path {
        let path = root.join(path);
        fs::write(&path, report).map_err(|error| format!("write {}: {error}", path.display()))?;
    } else {
        print!("{report}");
    }

    let default_percentage = production.percentage();
    let full_percentage = production.percentage();
    if default_percentage > DEFAULT_THRESHOLD {
        return Err(format!(
            "default production unsafe ratio {default_percentage:.2}% exceeds {DEFAULT_THRESHOLD:.2}%"
        ));
    }
    if full_percentage > FULL_THRESHOLD {
        return Err(format!(
            "full production unsafe ratio {full_percentage:.2}% exceeds {FULL_THRESHOLD:.2}%"
        ));
    }
    if !production.disallowed.is_empty() {
        return Err(format!(
            "{} unsafe production locations are outside approved islands",
            production.disallowed.len()
        ));
    }
    Ok(())
}

fn workspace_root() -> Result<PathBuf, String> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .ancestors()
        .nth(2)
        .map(Path::to_path_buf)
        .ok_or_else(|| {
            format!(
                "cannot derive workspace root from {}",
                manifest_dir.display()
            )
        })
}

fn production_roots() -> &'static [&'static str] {
    &[
        "crates/libzmq-core/src",
        "crates/libzmq/src",
        "crates/libzmq-ffi/src",
        "crates/libzmq-sys/src",
    ]
}

fn workspace_roots() -> &'static [&'static str] {
    &[
        "crates/libzmq-core/src",
        "crates/libzmq/src",
        "crates/libzmq-ffi/src",
        "crates/libzmq-sys/src",
        "crates/libzmq-test-harness/src",
    ]
}

fn approved_unsafe_prefixes() -> &'static [&'static str] {
    &[
        "crates/libzmq-ffi/src/",
        "crates/libzmq-sys/src/",
        "crates/libzmq-test-harness/src/bin/",
    ]
}

fn scan_roots(root: &Path, roots: &[&str], enforce_approved: bool) -> Result<Stats, String> {
    let mut stats = Stats::default();
    for relative in roots {
        collect_rs_files(&root.join(relative), &mut |path| {
            scan_file(root, path, enforce_approved, &mut stats)
        })
        .map_err(|error| format!("scan {relative}: {error}"))?;
    }
    Ok(stats)
}

fn collect_rs_files(
    dir: &Path,
    visit: &mut impl FnMut(&Path) -> Result<(), String>,
) -> io::Result<()> {
    let mut entries = fs::read_dir(dir)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.path());
    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            collect_rs_files(&path, visit)?;
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            visit(&path).map_err(io::Error::other)?;
        }
    }
    Ok(())
}

fn scan_file(
    root: &Path,
    path: &Path,
    enforce_approved: bool,
    stats: &mut Stats,
) -> Result<(), String> {
    let source =
        fs::read_to_string(path).map_err(|error| format!("read {}: {error}", path.display()))?;
    let relative = path
        .strip_prefix(root)
        .map_err(|error| format!("strip {}: {error}", path.display()))?;
    stats.files += 1;
    for (line_index, line) in source.lines().enumerate() {
        let code = strip_line_comment(line).trim();
        if code.is_empty() {
            continue;
        }
        stats.code_lines += 1;
        let tokens = count_token(code, "unsafe");
        if tokens == 0 {
            continue;
        }
        stats.unsafe_lines += 1;
        stats.unsafe_tokens += tokens;
        if enforce_approved && !is_approved(relative) {
            stats.disallowed.push(UnsafeHit {
                path: relative.to_path_buf(),
                line: line_index + 1,
                text: code.to_string(),
            });
        }
    }
    Ok(())
}

fn strip_line_comment(line: &str) -> &str {
    line.split_once("//")
        .map(|(code, _comment)| code)
        .unwrap_or(line)
}

fn count_token(line: &str, token: &str) -> usize {
    line.match_indices(token)
        .filter(|(index, _)| {
            let before = line[..*index].chars().next_back();
            let after = line[*index + token.len()..].chars().next();
            !before.is_some_and(is_ident) && !after.is_some_and(is_ident)
        })
        .count()
}

fn is_ident(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}

fn is_approved(relative: &Path) -> bool {
    let relative = relative.to_string_lossy();
    approved_unsafe_prefixes()
        .iter()
        .any(|prefix| relative.starts_with(prefix))
}

fn render_report(production: &Stats, workspace: &Stats) -> String {
    let mut report = String::new();
    report.push_str("# Unsafe Report\n\n");
    report.push_str("Generated by `cargo run -p libzmq-test-harness --bin unsafe-report -- --write docs/unsafe-report.md`.\n\n");
    report.push_str("## Production Source\n\n");
    report.push_str(&render_stats_table(production));
    report.push_str("\nApproved production unsafe islands:\n\n");
    report.push_str("- `crates/libzmq-ffi/src/` for C ABI pointer handling.\n");
    report.push_str("- `crates/libzmq-sys/src/` for OS/syscall boundaries.\n\n");
    report.push_str(&format!(
        "Default feature gate: {:.2}% <= {:.2}%: {}.\n\n",
        production.percentage(),
        DEFAULT_THRESHOLD,
        pass_fail(production.percentage() <= DEFAULT_THRESHOLD)
    ));
    report.push_str(&format!(
        "Full feature handwritten gate: {:.2}% <= {:.2}%: {}.\n\n",
        production.percentage(),
        FULL_THRESHOLD,
        pass_fail(production.percentage() <= FULL_THRESHOLD)
    ));
    report.push_str(&format!(
        "Disallowed production unsafe locations: {}.\n\n",
        production.disallowed.len()
    ));
    if !production.disallowed.is_empty() {
        for hit in &production.disallowed {
            report.push_str(&format!(
                "- `{}:{}` `{}`\n",
                hit.path.display(),
                hit.line,
                hit.text
            ));
        }
        report.push('\n');
    }

    report.push_str("## Workspace Source\n\n");
    report.push_str(&render_stats_table(workspace));
    report.push_str("\nWorkspace counts include test harness C++ oracle/interop binaries; those are not part of production unsafe percentage gates.\n");
    report
}

fn render_stats_table(stats: &Stats) -> String {
    format!(
        "| Metric | Value |\n| --- | ---: |\n| Rust source files | {} |\n| Nonblank code lines | {} |\n| Lines containing `unsafe` | {} |\n| `unsafe` tokens | {} |\n| Unsafe line ratio | {:.2}% |\n",
        stats.files,
        stats.code_lines,
        stats.unsafe_lines,
        stats.unsafe_tokens,
        stats.percentage()
    )
}

fn pass_fail(pass: bool) -> &'static str {
    if pass {
        "PASS"
    } else {
        "FAIL"
    }
}

impl Stats {
    fn percentage(&self) -> f64 {
        if self.code_lines == 0 {
            0.0
        } else {
            self.unsafe_lines as f64 * 100.0 / self.code_lines as f64
        }
    }
}
