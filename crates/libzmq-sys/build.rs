fn main() {
    if std::env::var_os("CARGO_FEATURE_SODIUM").is_some()
        && pkg_config::probe_library("libsodium").is_err()
    {
        println!("cargo:rustc-link-lib=sodium");
    }

    if std::env::var_os("CARGO_FEATURE_GSSAPI").is_some() {
        if cfg!(target_os = "macos") {
            println!("cargo:rustc-link-lib=framework=GSS");
        } else if pkg_config::probe_library("krb5-gssapi").is_err() {
            println!("cargo:rustc-link-lib=gssapi_krb5");
        }
    }

    if std::env::var_os("CARGO_FEATURE_NORM").is_some()
        && pkg_config::probe_library("norm").is_err()
    {
        println!("cargo:rustc-link-lib=norm");
    }
}
