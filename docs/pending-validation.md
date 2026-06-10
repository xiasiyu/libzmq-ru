# Pending Validation

This document tracks release blockers and validation items that cannot be truthfully marked complete in the current local environment.

## Environment-Dependent Validation

- Real GSSAPI original-C++ interop: requires a configured Kerberos realm, principal/keytab, or credential cache.
- Windows DLL export validation: requires a Windows-capable runner.
- Linux all-features cross-check: requires the Linux C toolchain needed by WSS crypto provider dependencies.

## Incomplete Feature Scope

- `norm://` socket transport: sys-level NORM data-object send/receive is proven, but full socket transport still needs original libzmq stream/event-loop semantics.
- `pgm://` and `epgm://` transports: require OpenPGM availability and implementation validation.
- `tipc://`, `vmci://`, and `vsock://` transports: require corresponding platform/kernel support and implementation validation.

## Test Infrastructure Gaps

- Fuzz smoke targets: `fuzz/` currently contains only the target plan and needs executable fuzz targets before release validation can pass.

## Recently Resolved

- NULL/PLAIN/CURVE TCP original-C++ interop now passes in same-process and process-isolated harnesses against CURVE-capable oracle builds.
