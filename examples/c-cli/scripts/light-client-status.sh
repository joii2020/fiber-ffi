#!/bin/sh

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
log_file=${LIGHT_CLIENT_LOG:-"$script_dir/../data/fiber-ffi.log"}
wait_for_ready=false

usage() {
  cat <<EOF
Usage: $(basename "$0") [--wait] [--log FILE]

Show the embedded CKB Light Client synchronization status.

Options:
  --wait       Follow the log until synchronization is ready
  --log FILE   Read a different fiber-ffi log file
  -h, --help   Show this help

The LIGHT_CLIENT_LOG environment variable can also set the log path.
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --wait)
      wait_for_ready=true
      shift
      ;;
    --log)
      if [ "$#" -lt 2 ]; then
        echo "--log requires a file path" >&2
        exit 2
      fi
      log_file=$2
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [ ! -f "$log_file" ]; then
  echo "Light Client log not found: $log_file" >&2
  exit 1
fi

latest_status=$(grep -E \
  'waiting for embedded CKB Light Client required scripts|embedded CKB Light Client is ready' \
  "$log_file" | tail -n 1 || true)

print_status() {
  status_line=$1
  case "$status_line" in
    *'embedded CKB Light Client is ready'*)
      echo "READY: embedded CKB Light Client synchronization is complete"
      return 0
      ;;
    *'waiting for embedded CKB Light Client required scripts'*)
      target=$(printf '%s\n' "$status_line" |
        sed -n 's/.*target_tip=\([0-9][0-9]*\).*/\1/p')
      current=$(printf '%s\n' "$status_line" |
        sed -n 's/.*slowest_script=\([0-9][0-9]*\).*/\1/p')
      if [ -n "$target" ] && [ -n "$current" ]; then
        remaining=$((target - current))
        echo "SYNCING: current=$current target=$target remaining=$remaining"
      else
        echo "SYNCING: $status_line"
      fi
      return 1
      ;;
    *)
      echo "INITIALIZING: no script synchronization status found in $log_file"
      return 1
      ;;
  esac
}

if print_status "$latest_status"; then
  exit 0
fi

if [ "$wait_for_ready" = false ]; then
  exit 0
fi

echo "Waiting for embedded CKB Light Client synchronization..."
tail -n 0 -F "$log_file" | while IFS= read -r line; do
  case "$line" in
    *'waiting for embedded CKB Light Client required scripts'*)
      print_status "$line" || true
      ;;
    *'embedded CKB Light Client is ready'*)
      print_status "$line"
      break
      ;;
  esac
done
