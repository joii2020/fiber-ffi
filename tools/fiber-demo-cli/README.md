# fiber-demo-cli

`fiber-demo-cli` is the native Rust demonstration program for `fiber-ffi`. It
links the crate as an `rlib` and calls the safe, typed
`fiber_ffi::native::FiberNode` API.

Its startup sequence and interactive features are:

```text
funding address and first-run wallet birthday discovery
  -> embedded CKB Light Client preparation and synchronization
  -> Fiber node startup
  -> node information and CKB balance
  -> Peer / Channel / Pay menus
```

## Build and run

Run from the repository root:

```bash
make -C tools/fiber-demo-cli
make -C tools/fiber-demo-cli run
```

The first command only builds the executable:

```text
target/release/fiber-demo-cli
```

The CLI does not generate a wallet key. Before the first run, place the
64-character hexadecimal private key for an existing, funded CKB testnet wallet
at `tools/fiber-demo-cli/data/ckb/key` and restrict access to the file. For
example:

```bash
mkdir -p tools/fiber-demo-cli/data/ckb
install -m 600 /path/to/funded-testnet-key tools/fiber-demo-cli/data/ckb/key
```

If the key is missing, the CLI exits immediately. A newly generated key would
have no CKB, so it could neither open a funded channel nor cover change-Cell
capacity and transaction fees.

The Makefile enables `sqlite,ckb-light-client-portable`, which statically links
the `fiber-ffi` Rust library and enables the embedded CKB Light Client. Override
the feature list with `FIBER_FEATURES` when needed.

The default password `fiber-ffi-rust-cli-demo-password` is only for temporary
testnet data. To select the password before the wallet is first opened:

```bash
FIBER_SECRET_KEY_PASSWORD='replace-this' make -C tools/fiber-demo-cli run
```

Later runs must use the same password. To pass CLI options:

```bash
make -C tools/fiber-demo-cli run \
  CLI_ARGS='--data ./tools/fiber-demo-cli/data --log-level info,fiber_ffi=debug'
```

Use `target/release/fiber-demo-cli --help` for all options. The default config
is testnet-only; do not use the fixed password in production.
