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

## Preparing CKB before Fiber starts

`fiber_prepare_ckb` gives mobile applications an asynchronous pre-start step.
The function always exists, regardless of the selected Cargo features, and its
completion callback is never invoked inline on the calling stack. Android and
iOS bridges should dispatch it onto their main event loop before updating UI.

- With `disable-ckb-rpc`, it starts the embedded CKB Light Client, waits for its
  verified chain data and required scripts, and keeps the prepared instance
  alive. The next `fiber_start` with the same `config_path` and
  `database_prefix` reuses that instance.
- Without `disable-ckb-rpc`, it asynchronously succeeds with
  `{"ready":true,"mode":"external_rpc","skipped":true}` and leaves the
  existing external-RPC startup behavior unchanged.

Applications must keep the config file contents, config path, database
directory, and CKB funding key unchanged between preparation and `fiber_start`.
If Fiber is started while CKB is still preparing, the start call fails and the
application should wait for the completion callback before retrying.

For a public-testnet demo, set `ckb_light_client.history_start_block` to a
trusted block at or before both the wallet's earliest relevant cell and every
configured Type ID deployment. Scanning from `0x0` is correct for recovery but
can take far longer than an interactive demo; moving the start forward without
checking those two bounds can hide spendable cells or contract dependencies.

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

The crate emits a `cdylib`. Mobile builds disable default features to avoid the
RocksDB/libclang toolchain requirement.

### iOS

Prerequisites:

- macOS with full Xcode installed. Command Line Tools alone are not enough
  because the iPhoneOS and iPhoneSimulator SDKs are required.
- Select Xcode as the active developer directory if needed:

  ```sh
  sudo xcode-select -s /Applications/Xcode.app/Contents/Developer
  ```

Build the raw dynamic library for device:

```sh
make build-ios
```

Build the raw dynamic library for Apple Silicon simulator:

```sh
make build-ios-sim
```

The outputs are:

```text
target/aarch64-apple-ios/release/libfiber_ffi.dylib
target/aarch64-apple-ios-sim/release/libfiber_ffi.dylib
```

Copy or package these outputs from the app/demo repository as needed. The C ABI
header is `include/fiber_ffi.h`.

### Android

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

For the Android `Prepare CKB` demo with no full-node RPC dependency, build the
library with the embedded portable Light Client enabled:

```sh
make build-android MOBILE_FEATURES="sqlite ckb-light-client-portable"
```

Copy the resulting `libfiber_ffi.so` into
`demos/android/app/src/main/jniLibs/arm64-v8a/` before building the APK. A
library built with the default `sqlite` feature still exposes
`fiber_prepare_ckb`, but reports `mode: external_rpc` and `skipped: true`.
