# fiber-ffi C CLI example

This Linux- and macOS-only demo is written in plain C and provides numbered
Peer, Channel, and Pay menus. The executable links to and directly calls
`libfiber_ffi.so` (Linux) or `libfiber_ffi.dylib` (macOS). The C code depends
only on the system POSIX threads library, with no third-party libraries or
additional `dlopen` wrapper.

The startup sequence is:

```text
fiber_ckb_funding_address
  -> call fiber_ckb_discover_history_start_block on the first run
  -> fiber_prepare_ckb_with_history_start_block persists the wallet birthday
  -> wait for Light Client synchronization
  -> fiber_start
  -> numbered Peer / Channel / Pay menus
```

## Build

Linux requires a C compiler and the Rust build dependencies for this project.
macOS requires the Xcode Command Line Tools. Run this command from the
repository root:

```bash
make -C examples/c-cli
```

This first builds the dynamic library with the features required by the demo,
then builds the CLI with the system C compiler:

```text
target/c-cli/fiber-ffi-c-cli
target/release/libfiber_ffi.so       # Linux
target/release/libfiber_ffi.dylib    # macOS
```

The CLI has an rpath relative to its own location, so neither
`LD_LIBRARY_PATH` nor `DYLD_LIBRARY_PATH` is needed. Use the appropriate
command below to verify that the executable depends on the dynamic library:

```bash
ldd target/c-cli/fiber-ffi-c-cli           # Linux
otool -L target/c-cli/fiber-ffi-c-cli      # macOS
```

## Test wallet

The first `make` or `make setup` invocation uses OpenSSL to create the data
directory and generate a 32-byte CKB test private key automatically:

```text
examples/c-cli/data/ckb/key
```

The key file has `0600` permissions. If it already exists, the Makefile keeps
it and never overwrites it. Fiber still generates and manages the node identity
key (`sk` in the data directory) through the dynamic library.

When `FIBER_SECRET_KEY_PASSWORD` is not set, `make run` injects the fixed test
password `fiber-ffi-c-cli-demo-password`. On the first startup, Fiber migrates
the automatically generated plaintext key to the encrypted format. This
default password is only suitable for temporary testing. Never use this data
directory on mainnet or in production.

To use a custom password, set it explicitly before the first startup. The
environment variable takes precedence over the built-in value:

```bash
FIBER_SECRET_KEY_PASSWORD='replace-this' make -C examples/c-cli run
```

After the key is encrypted, every later run must use the same password that was
used on the first startup. The C program does not set a password automatically
when `target/c-cli/fiber-ffi-c-cli` is executed directly, so export the
environment variable first.

## Automatic wallet birthday discovery on first startup

When `wallet-birthday.json` does not exist on the first startup, the C CLI
performs three explicitly separated steps:

1. `fiber_ckb_funding_address` reads only the local configuration and Funding
   Key to obtain an address in the format of the configured chain.
2. `fiber_ckb_discover_history_start_block` receives the address and
   `--ckb-discovery-rpc`. It verifies that the Indexer is not beyond the allowed
   lag, then searches by Funding Lock in ascending order for the first basic CKB
   Cell with no Type Script and empty Data. If the wallet has a balance, it uses
   the earliest Cell's block height. Otherwise, it uses the Indexer tip. By
   default, it subtracts a 1,000-block safety window from that height.
3. The CLI passes only the resulting height to
   `fiber_prepare_ckb_with_history_start_block`. The prepare operation neither
   knows the external RPC URL nor accesses the external RPC. It atomically
   writes the result to `data/ckb/wallet-birthday.json`. That file includes the
   network, genesis hash, address, and lock args, preventing accidental reuse
   with another wallet or chain.

Later startups only read this file. They do not access the bootstrap RPC and
never advance the height automatically. If the legacy
`data/ckb/history_start_block` exists, it is considered only as an earlier safe
lower bound during the first migration: an older legacy value is preserved,
while a value later than the RPC discovery result cannot hide existing funds.

For advanced recovery, `ckb_light_client.history_start_block` can still be set
explicitly in YAML. The prepare operation preserves the earliest height among
the explicit configuration, the caller-provided value, the legacy value, and
the persisted value. The wallet birthday is part of the wallet backup. The demo
configuration specifies the pinned FundingLock, CommitmentLock, and RUSD Type
ID dependencies as explicit CellDep outpoints, so the basic CKB wallet birthday
is no longer constrained by the deployment heights of those contracts.

CellDeps explicitly included in the configuration are trusted release
configuration. The built-in Light Client still verifies the creating
transaction's commitment, output index, and contents, and performs local
transaction verification before sending a transaction. It does not scan from
each pinned CellDep's deployment block to the tip to prove again that the Cell
is unspent. Before publishing a new configuration, the release service should
use at least one trusted CKB RPC to check that every outpoint is still `live`.
If the configuration is served dynamically, the client must embed the release
public key and validate the configuration signature, network, and monotonically
increasing version while retaining built-in configuration as a fallback. If a
pinned item is a dep-group, the release check must also validate every member
outpoint referenced by the group data. The client gives those members the same
explicit trust to avoid triggering another history scan for each member.
Wallet funding inputs are not on this allowlist and always receive full Light
Client liveness verification. This behavior is enabled explicitly by
`ckb_light_client.trust_pinned_cell_deps: true` and is disabled by default.
The standard secp256k1 dep-group and its members do not depend on this setting:
they are committed by the already verified genesis block and can be pinned
directly, avoiding a scan back to genesis whenever a normal funding transaction
is verified.

With funding from both sides, an old Cell added by the remote peer often uses a
lock script that the local client has never subscribed to. A temporary scan
from the creation block to the tip cannot finish within Fiber's RPC timeout.
The demo configuration therefore sets
`ckb_light_client.peer_funding_liveness_rpc_url`. For that peer input, the
gateway asks a full node on the same chain only whether the input is currently
`live`; it does not use the Cell contents returned by the RPC. The Light Client
still obtains and verifies the creating transaction, output, and data. CellDeps
still use only the Light Client, and final confirmation of the funding
transaction is still tracked by the Light Client. Processing stops if the RPC
returns `dead` or `unknown`, the request fails, or the genesis hash does not
match. A forged `live` response can at most allow an invalid transaction to
reach local verification or P2P broadcast, where normal nodes still reject it;
the external RPC does not become a root of trust for funds.

This persistent runtime setting differs from `--ckb-discovery-rpc`: the latter
discovers the wallet birthday only on the first startup, while the former checks
the current liveness of a peer input only during dual-funded channel creation.
They may use the same service or separate services.

The Light Client persists the scan progress of required scripts. If the
configuration and wallet birthday remain unchanged, later startups resume from
the previous height instead of scanning again from the wallet birthday. The
first time a new version opens an old cache, it removes temporary scripts left
by ordinary transaction queries in earlier versions and safely rebuilds the
required-script progress from the original wallet birthday once. Later
startups can reuse that progress. To prevent a slow testnet bootnode from
blocking the CLI, startup continues after two usable peers are available. The
Light Client connects to up to eight ordinary peers in the background and also
maintains the configured regional preferred peer. Filter batches are selected
only from candidates that have proved the current tip. By default, 90% prefer
the regional node and 10% sample public nodes. A node is switched after six
seconds without a response and cooled down for 60 seconds after two consecutive
failures. The demo configuration keeps the strict
`startup_script_lag_tolerance: 0`, so the menu appears only after required
scripts reach the verified tip observed at startup. During operation it uses
`operational_lag_tolerance: 6`. The local RPC uses the height to which all
scripts have been fully scanned as a consistent operational tip for both the
Fiber chain and Indexer interfaces. A channel can be opened immediately when
this snapshot is at most six blocks behind the latest verified block header.
Newly arriving blocks do not move the completion condition during funding
selection or transaction verification, and Open Channel is not retried. When
the tolerance is exceeded, the Readiness API returns an estimated wait range.
Its first estimate is a low-confidence range based on the Light Client's
1,000-block filter batches, three-second polling interval, and 60-second peer
request timeout. After repeated attempts, it estimates using the measured
indexing speed. It reports a possible stall only if a complete 60-second peer
request window passes with no progress. The measured speed is retained when
`lag=0`, so the next new block does not immediately return the estimate to low
confidence.

## Run

Running through the Makefile from the repository root is recommended because it
performs setup, builds the project, and injects the test password:

```bash
make -C examples/c-cli run
```

Dynamic library logs and asynchronous events are appended to `fiber-ffi.log`
under the selected data directory by default (`examples/c-cli/data/fiber-ffi.log`
with the defaults). The terminal shows only menus and operation results so
scrolling logs do not interrupt the interactive interface. To select another
log file:

```bash
make -C examples/c-cli run CLI_ARGS='--log-file ./examples/c-cli/data/node-a.log'
```

To select another configuration, data directory, or log level:

```bash
target/c-cli/fiber-ffi-c-cli \
  --config examples/c-cli/config.testnet.yml \
  --data examples/c-cli/data \
  --log-level 'info,fiber_ffi=debug' \
  --log-file ./examples/c-cli/data/fiber-ffi.log
```

Initial discovery uses testnet `https://testnet.ckbapp.dev/` by default. You can
explicitly select an RPC with an enabled Indexer that matches `fiber.chain`:

```bash
target/c-cli/fiber-ffi-c-cli \
  --ckb-discovery-rpc https://your-testnet-rpc.example/
```

The default build features are `sqlite,ckb-light-client-portable`, because this
example requires `fiber_prepare_ckb` to start the built-in Light Client. Override
them when necessary:

```bash
make -C examples/c-cli FIBER_FEATURES='sqlite,ckb-light-client-portable'
```

After Fiber starts, the example calls `fiber_ckb_balance` to query the basic CKB
Cells currently indexed for the funding key through the built-in Light Client.
It displays the balance, Cell count, indexed height, and lag. The same output
also contains the chain-formatted `address` and hexadecimal `lock_args`. The
balance does not treat capacity occupied by UDT Cells as available CKB, but
capacity must still be reserved for a change Cell, concurrent transaction
inputs, and fees.

All amounts use the smallest unit, `shannons`. Leave the UDT type script blank
when opening a CKB channel. Select `Fibt` when creating a testnet CKB invoice.
Opening a channel and sending a real payment still require testnet funds, a
connected peer, and an available Fiber route.
