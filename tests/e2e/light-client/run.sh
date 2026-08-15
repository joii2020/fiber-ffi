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
ckb_pids=()

cleanup() {
    exit_status=$?
    trap - EXIT INT TERM
    for ckb_pid in "${ckb_pids[@]}"; do
        if kill -0 "$ckb_pid" >/dev/null 2>&1; then
            kill "$ckb_pid" >/dev/null 2>&1 || true
            wait "$ckb_pid" >/dev/null 2>&1 || true
        fi
    done
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
CARGO_TARGET_DIR="$repo_dir/target" "$fixture_dir/tests/deploy/init-dev-chain.sh"

peer_rpc_urls=("$ckb_rpc_url")
peer_logs=("$ckb_log")
for peer_number in 1 2 3; do
    peer_dir="$fixture_dir/tests/deploy/ckb-peer-$peer_number"
    peer_rpc_port=$((8114 + peer_number * 100))
    peer_p2p_port=$((8115 + peer_number * 100))
    cp -R "$fixture_dir/tests/deploy/node-data" "$peer_dir"
    rm -rf -- "$peer_dir/data/network"
    sed -i.bak \
        -e "s/8114/$peer_rpc_port/g" \
        -e "s/8115/$peer_p2p_port/g" \
        "$peer_dir/ckb.toml"
    peer_rpc_urls+=("http://127.0.0.1:$peer_rpc_port")
    peer_logs+=("$work_dir/ckb-peer-$peer_number.log")
done

echo "Starting four CKB dev peers ..."
ckb run -C "$fixture_dir/tests/deploy/node-data" --indexer >"$ckb_log" 2>&1 &
ckb_pids+=("$!")
for peer_number in 1 2 3; do
    ckb run -C "$fixture_dir/tests/deploy/ckb-peer-$peer_number" \
        --skip-spec-check --indexer \
        >"${peer_logs[$peer_number]}" 2>&1 &
    ckb_pids+=("$!")
done

for peer_number in 0 1 2 3; do
    rpc_ready=false
    for _ in {1..60}; do
        if ! kill -0 "${ckb_pids[$peer_number]}" >/dev/null 2>&1; then
            echo "error: CKB peer $peer_number exited before RPC became ready" >&2
            tail -100 "${peer_logs[$peer_number]}" >&2 || true
            exit 1
        fi
        if curl --fail --silent --show-error \
            --header "Content-Type: application/json" \
            --data '{"id":1,"jsonrpc":"2.0","method":"get_tip_block_number","params":[]}' \
            "${peer_rpc_urls[$peer_number]}" >/dev/null 2>&1; then
            rpc_ready=true
            break
        fi
        sleep 1
    done

    if ! "$rpc_ready"; then
        echo "error: CKB RPC did not become ready at ${peer_rpc_urls[$peer_number]}" >&2
        tail -100 "${peer_logs[$peer_number]}" >&2 || true
        exit 1
    fi
done

bootnodes=()
for peer_rpc_url in "${peer_rpc_urls[@]}"; do
    bootnodes+=("$("$script_dir/discover_bootnode.py" "$peer_rpc_url")")
done
source_config="$fixture_dir/tests/nodes/1/config.yml"
light_client_config="$fixture_dir/tests/nodes/1/config-light-client.yml"
require_file "$source_config"
cp "$source_config" "$light_client_config"
# Prove that the feature never relies on the legacy full-node HTTP endpoint.
# Fiber still parses this field, but fiber-ffi must replace it in memory before
# any CKB RPC client is constructed.
sed -i.bak '/^ckb:$/a\  rpc_url: http://127.0.0.1:1' "$light_client_config"
{
    printf '\n# Added by fiber-ffi/tests/e2e/light-client/run.sh.\n'
    printf 'ckb_light_client:\n'
    printf '  history_start_block: "0x0"\n'
    printf '  peer_funding_liveness_rpc_url: "%s"\n' "$ckb_rpc_url"
    printf '  bootnodes:\n'
    for bootnode in "${bootnodes[@]}"; do
        printf '    - "%s"\n' "$bootnode"
    done
} >>"$light_client_config"

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

# The Light Client proves a state transition after its stored tip. Generate one
# block after its network has had time to connect; otherwise a fixture whose
# four peers all stop at exactly the initial tip cannot produce the next proof
# and script filtering remains at block zero. Do not keep mining, because the
# startup readiness check intentionally waits for scripts to catch a stable tip.
(
    sleep 15
    curl --fail --silent --show-error \
        --header "Content-Type: application/json" \
        --data '{"id":1,"jsonrpc":"2.0","method":"generate_block","params":[]}' \
        "$ckb_rpc_url" >/dev/null
) &
ckb_pids+=("$!")

echo "Starting fiber-ffi through its C ABI ..."
ffi_log="$work_dir/fiber-ffi.log"
FIBER_SECRET_KEY_PASSWORD=password1 \
    "$runner" "$light_client_config" "$ckb_rpc_url" 2>&1 | tee "$ffi_log"

grep -q "embedded CKB Light Client is ready" "$ffi_log"
grep -q "embedded CKB Light Client RPC gateway started" "$ffi_log"
grep -q '"status":"initializing"' "$ffi_log"
grep -Eq '"status":"(connecting|syncing_headers)"' "$ffi_log"
grep -q '"status":"syncing_scripts"' "$ffi_log"
grep -q 'fiber_ckb_discover_history_start_block: address=' "$ffi_log"
grep -q 'fiber_prepare_ckb: {"mode":"light_client","ready":true,"skipped":false,"status":"ready"}' "$ffi_log"
grep -q "reusing prepared embedded CKB Light Client gateway" "$ffi_log"
grep -q 'fiber_ckb_balance: .*"mode":"light_client".*"capacity_shannons":"[0-9][0-9]*".*"capacity_ckb":"[0-9][0-9]*\.[0-9][0-9]*"' "$ffi_log"
