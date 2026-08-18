# fiber-ffi

Standalone native Rust and C FFI wrapper for `fiber`, intended for Rust,
Android, and iOS integration.

This repository was migrated from:

`\\wsl.localhost\Debian\home\joii\code\fiber-dev\fiber\crates\fiber-ffi`

## Layout

- `src/native.rs`: safe, typed Rust API used by native callers and the C adapter.
- `src/lib.rs`: C ABI adapter plus the internal node runtime.
- `include/fiber_ffi.h`: C ABI header for mobile clients.
- `Cargo.toml`: standalone crate manifest.

## Rust native API

Rust callers depend on this package as a normal Cargo dependency and use
`fiber_ffi::native`. `FiberNode` owns a dedicated runtime thread, while its RPC
methods are async and use Fiber's typed request and response values directly:

```rust,no_run
use fiber_ffi::native::{FiberNode, StartOptions};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let node = FiberNode::start(StartOptions::new("config.yml"))?;
let info = node.node_info().await?;
println!("node: {:?}", info.node_id);
node.stop()?;
# Ok(())
# }
```

`native::types` re-exports the channel, payment, invoice, peer, hash, and event
types used by the API. CKB funding-address derivation, wallet-history discovery,
preparation progress, readiness, and balance are available without crossing a
C ABI boundary.

## C API shape

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
External wallet-history discovery is deliberately separate from preparation:

1. `fiber_ckb_funding_address` locally derives the configured funding address.
2. `fiber_ckb_discover_history_start_block` accepts an explicit RPC URL and
   exactly one of `lock_args`, `pubkey`, or `address`, then returns a conservative
   height. It neither reads nor writes Light Client state.
3. `fiber_prepare_ckb_with_history_start_block` accepts that height, persists a
   wallet-bound birthday, and prepares the Light Client without contacting the
   external RPC. Later starts can call `fiber_prepare_ckb` and reuse the saved
   value.

The prepare functions always exist, regardless of the selected Cargo features,
and their callback is never invoked inline on the calling stack. Android and iOS
bridges should dispatch it onto their main event loop before updating UI.

- With `disable-ckb-rpc`, it starts the embedded CKB Light Client, waits for its
  verified chain data and required scripts, and keeps the prepared instance
  alive. The next `fiber_start` with the same `config_path` and
  `database_prefix` reuses that instance.
- Without `disable-ckb-rpc`, it asynchronously succeeds with
  `{"ready":true,"mode":"external_rpc","skipped":true,"status":"ready"}`
  and leaves the existing external-RPC startup behavior unchanged.

With the embedded Light Client, the callback reports progress as JSON. Its
`status` is `initializing`, `wallet_birthday`, `connecting`, `syncing_headers`, or
`syncing_scripts`; the terminal status is `ready` or `failed`. Connecting and
header updates include peer and tip information, while script updates include
the target tip and slowest scanned height. Callers must keep `user_data` alive
until the terminal callback returns. `ready` remains available for callers that
only need to distinguish terminal success.

Applications must keep the config file contents, config path, database
directory, CKB funding key, and effective history height unchanged between
preparation and `fiber_start`.
If Fiber is started while CKB is still preparing, the start call fails and the
application should wait for the terminal callback before retrying.

After Fiber starts, `fiber_ckb_readiness` reports the live chain/indexer state.
`fiber_ckb_balance` derives the configured funding lock and queries its cells
through the same CKB backend. It returns the current indexed height, lag, base
CKB Cell count, decimal `capacity_shannons`, and human-readable
`capacity_ckb`, together with the chain-aware CKB `address` and hexadecimal
`lock_args`. The total excludes typed and non-empty-data Cells so UDT storage
capacity is not presented as CKB channel funding, but it can include inputs
reserved by an in-flight transaction; callers must still retain room for a
change Cell and transaction fees.
`fiber_open_channel` performs the same check internally and returns
`FIBER_FFI_STATUS_NOT_READY` without sending an open-channel message when the
CKB tip is stale, the indexer tip is unavailable, or any required script trails
the configured operational lag limit. Applications can call the readiness API
first to show sync progress instead of presenting a channel-opening form that
cannot yet succeed.
When the index is behind, the JSON also includes `wait_estimate` with a lower
and upper number of seconds, a three-second retry suggestion, and a confidence
level. The first estimate is a conservative protocol-based range; repeated
queries use observed index progress and report `stalled` if it does not move for
one complete 60-second peer-request window. A catch-up sample is retained so the
next block does not immediately fall back to low confidence.

The pure C demo in [`examples/c-cli`](examples/c-cli) exposes numbered Peer,
Channel, and Pay menus on Linux and macOS and links against the generated
`libfiber_ffi` dynamic library. It sets `FIBER_FFI_LOG_FILE` so library logs do
not interfere with the interactive menu; other native clients may use the same
environment variable before the first FFI call. On the first start, the CLI
calls the independent discovery API with `--ckb-discovery-rpc` to find the
earliest live base-CKB Cell for the exact funding lock (or the Indexer tip for an
empty wallet), subtracts a safety window, and passes the returned height into
the prepare API. Prepare atomically stores a network- and wallet-bound birthday
under the data directory without using the external RPC. Later starts reuse that
value and never advance it automatically. The Light Client persists required-script
progress and reuses it when the required scripts and wallet birthday are
unchanged; the first run after upgrading safely removes transient scripts left
by older transaction queries. Scripts added while following an already tracked
channel transaction chain are persisted and retained across later restarts;
unrelated `get_cells`, `get_transactions`, and `get_transaction` calls cannot
silently expand the historical scan range. Explicit CellDeps in its trusted
release config are treated as pinned dependencies: their producing transactions
remain Light-Client verified, while their liveness assertion comes from the
trusted configuration so contract deployment history is not scanned. Wallet
inputs are never covered by this shortcut. The standard secp256k1 dep-group and
its members are pinned independently because their outpoints and contents are
committed by the already verified genesis block.

`ckb_light_client.startup_script_lag_tolerance` optionally permits startup when
the persisted required-script index is within that many blocks of the fixed,
verified startup tip. Synchronization continues in the background and RPC calls
that require missing index data still return not-ready. It defaults to zero;
the C CLI test configuration keeps this strict default so funding operations are
not exposed before the required scripts catch up.
`ckb_light_client.operational_lag_tolerance` controls the maximum distance
between the newest verified Light Client header and the fully indexed snapshot
used by Fiber. The embedded RPC gateway exposes that snapshot consistently as
both its chain and indexer tip, so new headers cannot move the target halfway
through funding-cell selection or transaction validation. It defaults to zero;
the C CLI test configuration explicitly uses six blocks.
`ckb_light_client.startup_min_peers` likewise controls the startup connection
threshold while the Light Client continues toward its normal outbound maximum;
it defaults to four and the C CLI test configuration uses two.
`ckb_light_client.preferred_peers` may list up to eight complete CKB multiaddrs
(including `/p2p/<peer-id>`). The network actively maintains those connections
while still using bundled bootnodes and discovery to find independent peers.

For bidirectionally funded channels, an old Cell supplied by the peer may use a
script that the local Light Client has never tracked. Set
`ckb_light_client.peer_funding_liveness_rpc_url` to a same-chain CKB full-node
RPC to validate it without adding that script and scanning its complete history
during Fiber's RPC timeout. The gateway asks this RPC only whether such an input is currently
`live`; it ignores the returned Cell, obtains the producing transaction and Cell
contents through the Light Client, keeps CellDeps on Light-Client verification,
and tracks final funding-transaction confirmation through the Light Client. A
false answer can therefore reject or delay a channel, while a false `live`
answer can only let an invalid transaction reach normal validation/broadcast;
it cannot replace locally verified transaction data.

The discovery options default to a 1000-block safety window and reject an
Indexer more than 100 blocks behind its node. The RPC must match the target
chain. An explicit `ckb_light_client.history_start_block` remains available for
recovery; prepare always keeps the earliest applicable configured, discovered,
legacy, or persisted height. Without any supplied or persisted height, scanning
safely starts from `0x0`.

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

The crate emits both a `cdylib` and an `rlib` from the same source. The dynamic
library keeps the existing C ABI for mobile clients; the `rlib` is the artifact
Cargo uses when another Rust crate depends on `fiber-ffi`. Mobile builds disable
default features to avoid the RocksDB/libclang toolchain requirement.

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
