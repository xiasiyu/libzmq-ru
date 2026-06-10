fn main() {
    if std::env::var_os("CARGO_FEATURE_SODIUM").is_some()
        && pkg_config::probe_library("libsodium").is_err()
    {
        println!("cargo:rustc-link-lib=sodium");
    }
}
