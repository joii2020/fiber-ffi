# CKB Light Client E2E smoke harness

This temporary harness follows Fiber's existing `tests/deploy` and `tests/nodes`
E2E layout. It copies those fixtures into a temporary directory, initializes and
starts the same CKB dev chain, discovers the node's real P2P peer ID, adds a
custom-chain `ckb_light_client.bootnodes` entry, and exercises the shared library
through the public C ABI:

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

At the moment the embedded Light Client runtime and local RPC gateway have not
been wired into `start_node`. This harness therefore validates the feature build,
custom-chain configuration, real bootnode discovery, the Fiber dev-chain fixture,
and the FFI lifecycle. It does **not** yet prove that Fiber's CKB reads go through
the Light Client.

Once the runtime is connected, extend this harness to start four CKB peers and
add assertions for header readiness, script scanning, RPC routing, reorgs, and
zero Full Node HTTP access in `disable-ckb-rpc` mode.
