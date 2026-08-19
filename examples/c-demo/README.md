# fiber-ffi C background demo

This is a small C example that starts a testnet Fiber node and keeps it running
so that the node can be controlled with `fnn-cli` over JSON-RPC. It has no
interactive menu of its own.

The startup sequence is:

```text
read <data>/ckb/key
  -> query an external CKB RPC/Indexer for a safe wallet history height
  -> start and synchronize the embedded CKB Light Client
  -> start Fiber and its RPC server
  -> wait for SIGINT or SIGTERM
```

On later runs, the persisted `ckb/wallet-birthday.json` is reused instead of
advancing the history start. This prevents old wallet Cells from being skipped.

## Build and run

From the repository root:

```bash
make -C examples/c-demo
make -C examples/c-demo run
```

The first command creates a test-only key at
`examples/c-demo/data/ckb/key` if it is missing. An existing key is never
overwritten. On first use Fiber encrypts a plaintext 64-character hex key with
`FIBER_SECRET_KEY_PASSWORD`; all later runs must use the same password.

The fixed password supplied by `make run` is for disposable testnet data only.
For an existing wallet or any non-demo use, place its key under the selected
data directory and set a real password before the first run:

```bash
FIBER_SECRET_KEY_PASSWORD='replace-this' \
  make -C examples/c-demo run
```

The generated executable and dynamic library are:

```text
target/c-demo/fiber-ffi-c-demo
target/release/libfiber_ffi.so       # Linux
target/release/libfiber_ffi.dylib    # macOS
```

Use another data directory or CKB RPC/Indexer with `DEMO_ARGS`:

```bash
make -C examples/c-demo run \
  DEMO_ARGS='--data /path/to/node --ckb-rpc https://testnet.ckbapp.dev/'
```

The external RPC is used only for first-run wallet-history discovery. Chain and
Indexer tips are checked, and the returned start height includes a safety
window. Fiber's normal chain access then uses the embedded Light Client rather
than that external RPC.

## Control the running node

The default configuration exposes Fiber RPC only on loopback:

```bash
fnn-cli -u http://127.0.0.1:8227 info
fnn-cli -u http://127.0.0.1:8227 peer list_peers
fnn-cli -u http://127.0.0.1:8227 \
  peer connect_peer --address /ip4/1.2.3.4/tcp/8228/p2p/QmPeer...
fnn-cli -u http://127.0.0.1:8227 channel list_channels
fnn-cli -u http://127.0.0.1:8227 \
  channel open_channel --pubkey 02abc... --funding-amount 50000000000
```

Run `fnn-cli --help` and each subcommand's `--help` for the complete parameter
list. Press Ctrl-C in the demo process (or send `SIGTERM`) for a graceful Fiber,
RPC, and Light Client shutdown.

Library logs and Fiber events are appended to
`examples/c-demo/data/fiber-ffi.log` by default.
