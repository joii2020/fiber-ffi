#!/usr/bin/env python3
"""Print a dialable bootnode multiaddr from CKB's local_node_info RPC."""

from __future__ import annotations

import json
import sys
import urllib.request


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} CKB_RPC_URL", file=sys.stderr)
        return 2

    request = urllib.request.Request(
        sys.argv[1],
        data=json.dumps(
            {
                "id": 1,
                "jsonrpc": "2.0",
                "method": "local_node_info",
                "params": [],
            }
        ).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        payload = json.load(response)

    if "error" in payload:
        raise RuntimeError(f"local_node_info failed: {payload['error']}")

    result = payload["result"]
    node_id = result["node_id"]
    addresses = [entry["address"] for entry in result.get("addresses", [])]
    address = next((item for item in addresses if "/ip4/" in item and "/tcp/" in item), None)
    if address is None:
        raise RuntimeError(f"local_node_info returned no IPv4 TCP address: {addresses!r}")

    address = address.replace("/ip4/0.0.0.0/", "/ip4/127.0.0.1/", 1)
    if "/p2p/" not in address:
        address = f"{address}/p2p/{node_id}"
    print(address)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
