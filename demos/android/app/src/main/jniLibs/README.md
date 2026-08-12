# Fiber native library placement

Current app build is restricted to `arm64-v8a` because the bundled Fiber FFI library is AArch64:

- `arm64-v8a/libfiber_ffi.so`

If you add more ABI builds later, place each `libfiber_ffi.so` under the matching ABI folder and update `abiFilters` in `app/build.gradle.kts`.

Android devices and emulators that use 16 KB memory pages require every native library to be linked with 16 KB LOAD segment alignment. Build the Rust FFI library with a 16 KB max page size before copying it here, for example:

```sh
RUSTFLAGS="-C link-arg=-Wl,-z,max-page-size=16384" cargo build --target aarch64-linux-android --release
```

Without this, `System.loadLibrary("fiber_ffi")` fails on 16 KB page-size devices with an error like:

```text
program alignment (4096) cannot be smaller than system page size (16384)
```

The JNI bridge links `libfiber_ffi.so` directly and calls the exported functions from `fiber_ffi.h`:

- `fiber_prepare_ckb(...)`
- `fiber_start(const FiberStartOptions *options, FiberHandle **out_handle)`
- `fiber_stop(FiberHandle *handle)`

To exercise the `Prepare CKB` button with the embedded Light Client rather than
the asynchronous external-RPC no-op, build from the repository root with:

```sh
make build-android MOBILE_FEATURES="sqlite ckb-light-client-portable"
```

The bundled testnet config starts its first-run scan at `0x125bae1`, the block
containing the oldest configured Fiber contract dependency. This is suitable
for a fresh demo wallet. Move `ckb_light_client.history_start_block` back to the
earliest relevant block before importing an older funded wallet.
