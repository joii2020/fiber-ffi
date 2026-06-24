# fiber-ffi

Standalone FFI wrapper for `fiber`, intended for Android and iOS integration.

This repository was migrated from:

`\\wsl.localhost\Debian\home\joii\code\fiber-dev\fiber\crates\fiber-ffi`

## Layout

- `src/lib.rs`: Rust FFI implementation.
- `include/fiber_ffi.h`: C ABI header for mobile clients.
- `Cargo.toml`: standalone crate manifest.

## API shape

The public entry points use C structs for common channel, payment, and invoice
operations, for example `fiber_open_channel`,
`fiber_open_channel_with_external_funding`, `fiber_submit_signed_funding_tx`,
`fiber_send_payment`, `fiber_build_router`, `fiber_send_payment_with_router`,
and `fiber_new_invoice`.

Use the `FIBER_*_OPTIONS_INIT` macros, or set `struct_size` to
`sizeof(Fiber...Options)` and `flags` to `0`, before passing a native options
struct. Use `FiberU128 { .low = ..., .high = ... }` for `u128` amounts.
Optional fields use a `has_*` flag next to the value; leave the flag as zero to
use the Fiber default. Small option sets such as invoice currency and payment
status filters are `int32_t` constants instead of C enums, so invalid values are
reported as `FIBER_FFI_STATUS_INVALID_ARGUMENT`. Complex CKB objects such as
`Script`, route hints, and custom payment records are still accepted as JSON
sub-objects through dedicated `*_json` fields, so callers no longer need to
build the full RPC request JSON.

## Dependency

The original crate depended on `fnn` through the fiber workspace path:

```toml
fnn = { path = "../fiber-lib" }
```

This standalone repository now depends on the upstream fiber repository instead:

```toml
fnn = { git = "https://github.com/nervosnetwork/fiber.git", branch = "develop" }
```

If the FFI implementation needs APIs that only exist in a fork or local branch, point this dependency to that fork/branch/revision.

## Build

Android uses the `cdylib` output. The crate enables vendored OpenSSL so local
builds do not depend on a system OpenSSL installation.

Point `ANDROID_NDK_HOME` or `NDK_HOME` to the Android NDK.

Windows:

```bat
scripts\build-android.bat
```

The Windows batch script expects MSYS2 build tools at `C:\msys64\usr\bin` so
vendored OpenSSL can run a Unix-style `perl`. Install MSYS2 packages if needed:

```sh
pacman -S perl make
```

WSL, Linux, macOS, or Git Bash:

```sh
make build-android
```

The build uses `--no-default-features --features sqlite,watchtower` to avoid the
RocksDB/libclang toolchain requirement.
