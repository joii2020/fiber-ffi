use std::{net::SocketAddr, path::PathBuf, time::Duration};

use serde::Deserialize;
use tentacle::multiaddr::Multiaddr;

use super::CkbDataMode;

pub(crate) const LOCAL_RPC_LISTEN_ADDRESS: &str = "127.0.0.1:0";
pub(crate) const MAX_OUTBOUND_PEERS: u32 = 8;
pub(crate) const HEADER_READY_TIMEOUT: Duration = Duration::from_secs(120);
pub(crate) const REMOTE_DATA_TIMEOUT: Duration = Duration::from_secs(8);

// Copied from the configuration shipped with the selected ckb-light-client tag:
// https://github.com/nervosnetwork/ckb-light-client/blob/v0.5.5-rc1/config/mainnet.toml
const MAINNET_BOOTNODES: &[&str] = &[
    "/ip4/16.163.82.218/tcp/8114/p2p/QmaZMemLXSsxKUrYNucjEbPxVX3rBKsGhWW2muWtWxUWyh",
    "/ip4/35.79.196.111/tcp/8114/p2p/QmYCRVonLfP18LSoz2WCHaXDorUYxuUMfhtcXK1TuZ1iwF",
    "/ip4/13.234.144.148/tcp/8114/p2p/QmbT7QimcrcD5k2znoJiWpxoESxang6z1Gy9wof1rT1LKR",
    "/ip4/34.64.120.143/tcp/8114/p2p/QmejEJEbDcGGMp4D6WtftMMVLkR1ZuBfMgyLFDMJymkDt6",
    "/ip4/3.218.170.86/tcp/8114/p2p/QmShw2vtVt49wJagc1zGQXGS6LkQTcHxnEV3xs6y8MAmQN",
    "/ip4/35.236.107.161/tcp/8114/p2p/QmSRj57aa9sR2AiTvMyrEea8n1sEM1cDTrfb2VHVJxnGuu",
    "/ip4/23.101.191.12/tcp/8114/p2p/QmexvXVDiRt2FBGptgK4gBJusWyyTEEaHeuCAa35EPNkZS",
    "/ip4/20.151.143.237/tcp/8114/p2p/QmNsGNQjYA6iP472bNnNE2GR31kCYBifhY1XcaUxRjZ1py",
    "/ip4/52.59.155.249/tcp/8114/p2p/QmRHqhSGMGm5FtnkW8D6T83X7YwaiMAZXCXJJaKzQEo3rb",
    "/ip4/3.10.216.39/tcp/8114/p2p/QmagxSv7GNwKXQE7mi1iDjFHghjUpbqjBgqSot7PmMJqHA",
    "/ip4/13.37.172.80/tcp/8114/p2p/QmXJg4iKbQzMpLhX75RyDn89Mv7N2H8vLePBR7kgZf6hYk",
    "/ip4/34.118.49.255/tcp/8114/p2p/QmeCzzVmSAU5LNYAeXhdJj8TCq335aJMqUxcvZXERBWdgS",
    "/ip4/40.115.75.216/tcp/8114/p2p/QmW3P1WYtuz9hitqctKnRZua2deHXhNePNjvtc9Qjnwp4q",
    "/ip4/34.176.239.95/tcp/8114/p2p/QmQoWrmuFauCn3zZ2mYYKAciG9opTbjzC2wVEfWveZNDt8",
    "/ip4/13.245.217.98/tcp/8114/p2p/Qmf4t1SzFhRWuGcFcgs7r4pXvkACsz3FcaBMcmMKQMMpn7",
];

// Source:
// https://github.com/nervosnetwork/ckb-light-client/blob/v0.5.5-rc1/config/testnet.toml
const TESTNET_BOOTNODES: &[&str] = &[
    "/ip4/18.217.146.65/tcp/8111/p2p/QmT6DFfm18wtbJz3y4aPNn3ac86N4d4p4xtfQRRPf73frC",
    "/ip4/18.136.60.221/tcp/8111/p2p/QmTt6HeNakL8Fpmevrhdna7J4NzEMf9pLchf1CXtmtSrwb",
    "/ip4/35.176.207.239/tcp/8111/p2p/QmSJTsMsMGBjzv1oBNwQU36VhQRxc2WQpFoRu1ZifYKrjZ",
    "/ip4/13.228.149.113/tcp/8111/p2p/QmQoTR39rBkpZVgLApDGDoFnJ2YDBS9hYeiib1Z6aoAdEf",
    "/ip4/157.241.73.87/tcp/8111/p2p/QmSPkAyXqsWpRiS7HpHLTProVdhQWLKFHCXbRjaLpJj7ZL",
    "/ip4/4.241.132.26/tcp/8111/p2p/QmX5D6aJiAQ5Fxn4BfVqSn6zrgyuQM1oXVC9yvmzLuHXnx",
    "/ip4/52.147.120.180/tcp/8111/p2p/QmPcJY2gZLUm66szYA9QaG1P3rzwseWCMgbj6AyNCyW4G2",
    "/ip4/18.167.196.121/tcp/8111/p2p/QmQMjFrNGaphzfHin3mbYybbJcFMDUihKAcknquYvm9J3W",
    "/ip4/34.216.103.183/tcp/8111/p2p/Qmd41MaByDprkC5gP1XBKgamZ9DTLNk37zbPgwtiWCzRV6",
    "/ip4/3.98.152.180/tcp/8111/p2p/QmWVuW5KquiWDSqgMJRFW1xRtVqkYJrWz6S9NNk6fFn3wh",
    "/ip4/18.192.147.65/tcp/8111/p2p/QmWcEhsMNRcfJit62EbKgzpgtAJZX1G3Ur4shXjcvLsYDb",
    "/ip4/13.236.13.195/tcp/8111/p2p/QmfUTZxsse7rFJTJfoUv8bbStoDLETxst5nJEpJozNuAnH",
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum LocalChain {
    Mainnet,
    Testnet,
    Custom(PathBuf),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LocalLightClientConfig {
    pub(crate) mode: CkbDataMode,
    pub(crate) chain: LocalChain,
    pub(crate) store_path: PathBuf,
    pub(crate) network_path: PathBuf,
    pub(crate) history_start_block: u64,
    pub(crate) bootnodes: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SerializedLightClientConfig {
    history_start_block: Option<String>,
    bootnodes: Option<Vec<String>>,
}

impl LocalLightClientConfig {
    pub(crate) fn build(
        base_dir: PathBuf,
        fiber_chain: &str,
        upstream_rpc_url: String,
        serialized: SerializedLightClientConfig,
    ) -> Result<Self, String> {
        let history_start_block = serialized
            .history_start_block
            .as_deref()
            .map(parse_hex_block_number)
            .transpose()?
            .unwrap_or_default();

        let (chain, bootnodes) = match fiber_chain {
            "mainnet" => {
                reject_builtin_bootnode_override(&serialized.bootnodes, "mainnet")?;
                (LocalChain::Mainnet, copy_bootnodes(MAINNET_BOOTNODES))
            }
            "testnet" => {
                reject_builtin_bootnode_override(&serialized.bootnodes, "testnet")?;
                (LocalChain::Testnet, copy_bootnodes(TESTNET_BOOTNODES))
            }
            path => {
                let bootnodes = serialized.bootnodes.ok_or_else(|| {
                    "ckb_light_client.bootnodes must be set for a custom fiber.chain".to_string()
                })?;
                validate_custom_bootnodes(&bootnodes)?;
                (LocalChain::Custom(base_dir.join(path)), bootnodes)
            }
        };

        let light_client_dir = base_dir.join("ckb-light-client");
        Ok(Self {
            mode: data_mode(upstream_rpc_url),
            chain,
            store_path: light_client_dir.join("store"),
            network_path: light_client_dir.join("network"),
            history_start_block,
            bootnodes,
        })
    }

    pub(crate) fn local_rpc_listen_address() -> SocketAddr {
        LOCAL_RPC_LISTEN_ADDRESS
            .parse()
            .expect("the fixed local RPC listen address is valid")
    }

    pub(crate) fn chain_label(&self) -> String {
        match &self.chain {
            LocalChain::Mainnet => "mainnet".to_string(),
            LocalChain::Testnet => "testnet".to_string(),
            LocalChain::Custom(path) => path.display().to_string(),
        }
    }
}

#[cfg(not(feature = "disable-ckb-rpc"))]
fn data_mode(upstream_rpc_url: String) -> CkbDataMode {
    CkbDataMode::Hybrid { upstream_rpc_url }
}

#[cfg(feature = "disable-ckb-rpc")]
fn data_mode(_upstream_rpc_url: String) -> CkbDataMode {
    CkbDataMode::LightClientOnly
}

fn parse_hex_block_number(value: &str) -> Result<u64, String> {
    let digits = value.strip_prefix("0x").ok_or_else(|| {
        "ckb_light_client.history_start_block must be a 0x-prefixed hexadecimal number".to_string()
    })?;
    if digits.is_empty() {
        return Err(
            "ckb_light_client.history_start_block must contain hexadecimal digits".to_string(),
        );
    }
    u64::from_str_radix(digits, 16)
        .map_err(|err| format!("invalid ckb_light_client.history_start_block {value:?}: {err}"))
}

fn reject_builtin_bootnode_override(
    bootnodes: &Option<Vec<String>>,
    chain: &str,
) -> Result<(), String> {
    if bootnodes.is_some() {
        return Err(format!(
            "ckb_light_client.bootnodes cannot override the bundled {chain} bootnodes"
        ));
    }
    Ok(())
}

fn validate_custom_bootnodes(bootnodes: &[String]) -> Result<(), String> {
    if bootnodes.is_empty() {
        return Err(
            "ckb_light_client.bootnodes must contain at least one address for a custom fiber.chain"
                .to_string(),
        );
    }
    for address in bootnodes {
        if address.trim().is_empty() {
            return Err("ckb_light_client.bootnodes cannot contain an empty address".to_string());
        }
        address.parse::<Multiaddr>().map_err(|err| {
            format!("invalid CKB Light Client bootnode address {address:?}: {err}")
        })?;
    }
    Ok(())
}

fn copy_bootnodes(bootnodes: &[&str]) -> Vec<String> {
    bootnodes
        .iter()
        .map(|address| (*address).to_string())
        .collect()
}
