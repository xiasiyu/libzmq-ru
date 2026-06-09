//! Platform and third-party FFI isolation for ru-libzmq.
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
