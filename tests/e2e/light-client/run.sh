#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
repo_dir="$(cd -- "$script_dir/../../.." >/dev/null 2>&1 && pwd)"
fiber_source_dir="${FIBER_SOURCE_DIR:-$repo_dir/../fiber}"
ckb_rpc_url="http://127.0.0.1:8114"
keep_workdir="${KEEP_E2E_WORKDIR:-}"
check_only=false

usage() {
    cat <<EOF
usage: ${BASH_SOURCE[0]} [--check]

Environment:
  FIBER_SOURCE_DIR    Fiber checkout containing tests/deploy and tests/nodes
  KEEP_E2E_WORKDIR    Keep the temporary fixture and logs when non-empty
  CARGO               Cargo executable (default: cargo)
  CC                  C compiler (default: cc)
EOF
}

for argument in "$@"; do
    case "$argument" in
        --check)
            check_only=true
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            usage >&2
            exit 2
            ;;
    esac
done

require_command() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "error: required command not found: $1" >&2
        return 1
    fi
}

require_file() {
    if ! [ -f "$1" ]; then
        echo "error: required Fiber E2E fixture not found: $1" >&2
        return 1
    fi
}

if ! fiber_source_dir="$(cd -- "$fiber_source_dir" >/dev/null 2>&1 && pwd)"; then
    echo "error: FIBER_SOURCE_DIR is not a directory: ${FIBER_SOURCE_DIR:-$repo_dir/../fiber}" >&2
    exit 1
fi

require_file "$fiber_source_dir/tests/deploy/init-dev-chain.sh"
require_file "$fiber_source_dir/tests/nodes/deployer/config.yml"
require_file "$fiber_source_dir/tests/nodes/deployer/dev.toml"

for command in "${CARGO:-cargo}" "${CC:-cc}" ckb ckb-cli curl nc pkill python3; do
    require_command "$command"
done

if "$check_only"; then
    echo "Fiber E2E fixtures and required commands are available."
    exit 0
fi

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/fiber-ffi-e2e.XXXXXX")"
fixture_dir="$work_dir/fiber-fixture"
ckb_log="$work_dir/ckb.log"
ckb_pid=""

cleanup() {
    exit_status=$?
    trap - EXIT INT TERM
    if [ -n "$ckb_pid" ] && kill -0 "$ckb_pid" >/dev/null 2>&1; then
        kill "$ckb_pid" >/dev/null 2>&1 || true
        wait "$ckb_pid" >/dev/null 2>&1 || true
    fi
    if [ -n "$keep_workdir" ]; then
        echo "E2E work directory kept at $work_dir"
    else
        case "$work_dir" in
            "${TMPDIR:-/tmp}"/fiber-ffi-e2e.*)
                rm -rf -- "$work_dir"
                ;;
            *)
                echo "warning: refusing to remove unexpected work directory: $work_dir" >&2
                ;;
        esac
    fi
    exit "$exit_status"
}
trap cleanup EXIT INT TERM

mkdir -p "$fixture_dir/tests"
cp -R "$fiber_source_dir/tests/deploy" "$fixture_dir/tests/deploy"
cp -R "$fiber_source_dir/tests/nodes" "$fixture_dir/tests/nodes"

echo "Initializing a temporary dev chain from Fiber's E2E fixtures ..."
"$fixture_dir/tests/deploy/init-dev-chain.sh"

echo "Starting CKB dev node ..."
ckb run -C "$fixture_dir/tests/deploy/node-data" --indexer >"$ckb_log" 2>&1 &
ckb_pid=$!

rpc_ready=false
for _ in {1..60}; do
    if ! kill -0 "$ckb_pid" >/dev/null 2>&1; then
        echo "error: CKB exited before RPC became ready" >&2
        tail -100 "$ckb_log" >&2 || true
        exit 1
    fi
    if curl --fail --silent --show-error \
        --header "Content-Type: application/json" \
        --data '{"id":1,"jsonrpc":"2.0","method":"get_tip_block_number","params":[]}' \
        "$ckb_rpc_url" >/dev/null 2>&1; then
        rpc_ready=true
        break
    fi
    sleep 1
done

if ! "$rpc_ready"; then
    echo "error: CKB RPC did not become ready at $ckb_rpc_url" >&2
    tail -100 "$ckb_log" >&2 || true
    exit 1
fi

bootnode="$("$script_dir/discover_bootnode.py" "$ckb_rpc_url")"
source_config="$fixture_dir/tests/nodes/1/config.yml"
light_client_config="$fixture_dir/tests/nodes/1/config-light-client.yml"
require_file "$source_config"
cp "$source_config" "$light_client_config"
cat >>"$light_client_config" <<EOF

# Added by fiber-ffi/tests/e2e/light-client/run.sh.
ckb_light_client:
  history_start_block: "0x0"
  bootnodes:
    - "$bootnode"
EOF

echo "Building fiber-ffi with the portable CKB Light Client feature ..."
(
    cd "$repo_dir"
    "${CARGO:-cargo}" build --locked --features ckb-light-client-portable
)

runner="$work_dir/ffi-smoke"
library_dir="$repo_dir/target/debug"
"${CC:-cc}" \
    -std=c11 \
    -I"$repo_dir/include" \
    "$script_dir/ffi_smoke.c" \
    -L"$library_dir" \
    -Wl,-rpath,"$library_dir" \
    -lfiber_ffi \
    -o "$runner"

echo "Starting fiber-ffi through its C ABI ..."
FIBER_SECRET_KEY_PASSWORD=password1 "$runner" "$light_client_config"
