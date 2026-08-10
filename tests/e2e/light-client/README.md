# CKB Light Client E2E smoke harness

This temporary harness follows Fiber's existing `tests/deploy` and `tests/nodes`
E2E layout. It copies those fixtures into a temporary directory, initializes the
same CKB dev chain, starts four isolated CKB peers, discovers their real P2P peer
IDs, adds the custom-chain `ckb_light_client.bootnodes` entries, and exercises the
shared library through the public C ABI:

```text
fiber_start -> fiber_node_info -> fiber_stop
```

The Fiber checkout is never modified. Its fixtures are copied before the
initialization scripts run.

The harness builds the `ckb-light-client-portable` feature. The RC1 Light Client
dependency does not expose its RocksDB `portable` feature, so `fiber-ffi`
provides this feature-unification shim to keep E2E/CI binaries from depending on
the build host's AVX2/BMI instruction set.

## Prerequisites

- Rust 1.93.0, as selected by this repository's `rust-toolchain.toml`.
- `ckb`, `ckb-cli`, `curl`, `nc`, `pkill`, Python 3, and a C compiler.
- A Fiber checkout with `tests/deploy` and `tests/nodes`. By default the harness
  uses the sibling directory `../fiber`.
- TCP ports 8114, 8115, 8344, and 21714 must be available.

Check the prerequisites without starting anything:

```sh
make e2e-light-client-check
```

Run the smoke test:

```sh
make e2e-light-client
```

Use a different Fiber checkout or keep logs and generated state:

```sh
FIBER_SOURCE_DIR=/path/to/fiber KEEP_E2E_WORKDIR=1 make e2e-light-client
```

## Current boundary

The harness now asserts that the embedded Light Client obtains data from four
peers, scans every startup-required script to its verified tip, starts the local
loopback RPC gateway, and rewrites Fiber's in-memory CKB RPC URL before Fiber
starts. It also checks the complete public C ABI start/info/stop lifecycle.

This remains a startup smoke test. Follow-up E2E coverage is still needed for
per-method RPC result comparisons, reorgs, and channel funding/closing.
