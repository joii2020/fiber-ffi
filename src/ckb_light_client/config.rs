use std::{
    fs::File,
    io::BufReader,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use ckb_app_config::{NetworkConfig, SupportProtocol};
use serde::Deserialize;
use tentacle::multiaddr::Multiaddr;
use tracing::debug;

pub(crate) const LOCAL_RPC_LISTEN_ADDRESS: &str = "127.0.0.1:0";
pub(crate) const MAX_OUTBOUND_PEERS: u32 = 8;
pub(crate) const DEFAULT_REQUIRED_READY_PEERS: usize = 4;
pub(crate) const HEADER_READY_TIMEOUT: Duration = Duration::from_secs(120);
pub(crate) const REMOTE_DATA_TIMEOUT: Duration = Duration::from_secs(8);
pub(crate) const DEFAULT_FILTER_PREFERRED_PEER_CHANCE_PERCENT: u8 = 90;
pub(crate) const DEFAULT_FILTER_REQUEST_TIMEOUT_SECONDS: u64 = 6;
pub(crate) const DEFAULT_FILTER_PEER_FAILURE_THRESHOLD: u32 = 2;
pub(crate) const DEFAULT_FILTER_PEER_COOLDOWN_SECONDS: u64 = 60;
pub(crate) const WALLET_BIRTHDAY_FILE: &str = "wallet-birthday.json";
pub(crate) const LEGACY_HISTORY_START_BLOCK_FILE: &str = "history_start_block";

// Copied from the configuration shipped with the selected ckb-light-client tag:
// https://github.com/nervosnetwork/ckb-light-client/blob/12e29522ab7e078ada704d4ac04cbc0498009b7b/config/mainnet.toml
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
// https://github.com/nervosnetwork/ckb-light-client/blob/12e29522ab7e078ada704d4ac04cbc0498009b7b/config/testnet.toml
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

const HISTORY_START_BLOCK_ENV: &str = "FIBER_FFI_CKB_HISTORY_START_BLOCK";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum LocalChain {
    Mainnet,
    Testnet,
    Custom(PathBuf),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LocalLightClientConfig {
    pub(crate) chain: LocalChain,
    pub(crate) store_path: PathBuf,
    pub(crate) network_path: PathBuf,
    pub(crate) history_start_block: u64,
    pub(crate) history_start_block_is_explicit: bool,
    pub(crate) wallet_birthday_path: PathBuf,
    pub(crate) legacy_history_start_block_path: PathBuf,
    pub(crate) trust_pinned_cell_deps: bool,
    pub(crate) peer_funding_liveness_rpc_url: Option<String>,
    pub(crate) startup_min_peers: usize,
    pub(crate) startup_script_lag_tolerance: u64,
    pub(crate) operational_lag_tolerance: u64,
    pub(crate) bootnodes: Vec<String>,
    pub(crate) preferred_peers: Vec<String>,
    pub(crate) filter_preferred_peer_chance_percent: u8,
    pub(crate) filter_request_timeout_seconds: u64,
    pub(crate) filter_peer_failure_threshold: u32,
    pub(crate) filter_peer_cooldown_seconds: u64,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SerializedLightClientConfig {
    history_start_block: Option<String>,
    trust_pinned_cell_deps: Option<bool>,
    peer_funding_liveness_rpc_url: Option<String>,
    startup_min_peers: Option<usize>,
    startup_script_lag_tolerance: Option<u64>,
    operational_lag_tolerance: Option<u64>,
    bootnodes: Option<Vec<String>>,
    preferred_peers: Option<Vec<String>>,
    filter_preferred_peer_chance_percent: Option<u8>,
    filter_request_timeout_seconds: Option<u64>,
    filter_peer_failure_threshold: Option<u32>,
    filter_peer_cooldown_seconds: Option<u64>,
}

impl LocalLightClientConfig {
    pub(crate) fn build(
        base_dir: PathBuf,
        fiber_chain: &str,
        serialized: SerializedLightClientConfig,
    ) -> Result<Self, String> {
        let history_start_block_override = std::env::var(HISTORY_START_BLOCK_ENV)
            .map(Some)
            .or_else(|err| match err {
                std::env::VarError::NotPresent => Ok(None),
                std::env::VarError::NotUnicode(_) => Err(format!(
                    "{HISTORY_START_BLOCK_ENV} must contain valid UTF-8"
                )),
            })?;
        let configured_history_start_block = history_start_block_override
            .as_deref()
            .or(serialized.history_start_block.as_deref())
            .map(parse_hex_block_number)
            .transpose()?;
        let ckb_dir = base_dir.join("ckb");
        let wallet_birthday_path = ckb_dir.join(WALLET_BIRTHDAY_FILE);
        let legacy_history_start_block_path = ckb_dir.join(LEGACY_HISTORY_START_BLOCK_FILE);
        let persisted_history_start_block = if configured_history_start_block.is_none() {
            read_persisted_wallet_birthday_height(&wallet_birthday_path)?
        } else {
            None
        };
        let history_start_block = configured_history_start_block
            .or(persisted_history_start_block)
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
        let peer_funding_liveness_rpc_url = serialized
            .peer_funding_liveness_rpc_url
            .map(validate_peer_funding_liveness_rpc_url)
            .transpose()?;
        let startup_min_peers = serialized
            .startup_min_peers
            .unwrap_or(DEFAULT_REQUIRED_READY_PEERS);
        if !(1..=MAX_OUTBOUND_PEERS as usize).contains(&startup_min_peers) {
            return Err(format!(
                "ckb_light_client.startup_min_peers must be between 1 and {MAX_OUTBOUND_PEERS}"
            ));
        }
        let preferred_peers = serialized.preferred_peers.unwrap_or_default();
        validate_preferred_peers(&preferred_peers)?;
        if preferred_peers.len() > MAX_OUTBOUND_PEERS as usize {
            return Err(format!(
                "ckb_light_client.preferred_peers cannot contain more than {MAX_OUTBOUND_PEERS} addresses"
            ));
        }
        let filter_preferred_peer_chance_percent = serialized
            .filter_preferred_peer_chance_percent
            .unwrap_or(DEFAULT_FILTER_PREFERRED_PEER_CHANCE_PERCENT);
        if filter_preferred_peer_chance_percent > 100 {
            return Err(
                "ckb_light_client.filter_preferred_peer_chance_percent must be between 0 and 100"
                    .to_string(),
            );
        }
        let filter_request_timeout_seconds = serialized
            .filter_request_timeout_seconds
            .unwrap_or(DEFAULT_FILTER_REQUEST_TIMEOUT_SECONDS);
        if !(1..=60).contains(&filter_request_timeout_seconds) {
            return Err(
                "ckb_light_client.filter_request_timeout_seconds must be between 1 and 60"
                    .to_string(),
            );
        }
        let filter_peer_failure_threshold = serialized
            .filter_peer_failure_threshold
            .unwrap_or(DEFAULT_FILTER_PEER_FAILURE_THRESHOLD);
        if !(1..=10).contains(&filter_peer_failure_threshold) {
            return Err(
                "ckb_light_client.filter_peer_failure_threshold must be between 1 and 10"
                    .to_string(),
            );
        }
        let filter_peer_cooldown_seconds = serialized
            .filter_peer_cooldown_seconds
            .unwrap_or(DEFAULT_FILTER_PEER_COOLDOWN_SECONDS);
        if !(1..=3_600).contains(&filter_peer_cooldown_seconds) {
            return Err(
                "ckb_light_client.filter_peer_cooldown_seconds must be between 1 and 3600"
                    .to_string(),
            );
        }
        Ok(Self {
            chain,
            store_path: light_client_dir.join("store"),
            network_path: light_client_dir.join("network"),
            history_start_block,
            history_start_block_is_explicit: configured_history_start_block.is_some(),
            wallet_birthday_path,
            legacy_history_start_block_path,
            trust_pinned_cell_deps: serialized.trust_pinned_cell_deps.unwrap_or(false),
            peer_funding_liveness_rpc_url,
            startup_min_peers,
            startup_script_lag_tolerance: serialized
                .startup_script_lag_tolerance
                .unwrap_or_default(),
            operational_lag_tolerance: serialized.operational_lag_tolerance.unwrap_or_default(),
            bootnodes,
            preferred_peers,
            filter_preferred_peer_chance_percent,
            filter_request_timeout_seconds,
            filter_peer_failure_threshold,
            filter_peer_cooldown_seconds,
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

    pub(crate) fn log_summary(&self) {
        debug!(
            chain = %self.chain_label(),
            store_path = %self.store_path.display(),
            network_path = %self.network_path.display(),
            history_start_block = self.history_start_block,
            peer_funding_liveness_rpc_configured = self.peer_funding_liveness_rpc_url.is_some(),
            startup_min_peers = self.startup_min_peers,
            startup_script_lag_tolerance = self.startup_script_lag_tolerance,
            operational_lag_tolerance = self.operational_lag_tolerance,
            bootnodes = self.bootnodes.len(),
            preferred_peers = self.preferred_peers.len(),
            filter_preferred_peer_chance_percent = self.filter_preferred_peer_chance_percent,
            filter_request_timeout_seconds = self.filter_request_timeout_seconds,
            filter_peer_failure_threshold = self.filter_peer_failure_threshold,
            filter_peer_cooldown_seconds = self.filter_peer_cooldown_seconds,
            local_rpc_listen_address = %Self::local_rpc_listen_address(),
            max_outbound_peers = MAX_OUTBOUND_PEERS,
            header_ready_timeout_seconds = HEADER_READY_TIMEOUT.as_secs(),
            remote_data_timeout_seconds = REMOTE_DATA_TIMEOUT.as_secs(),
            "validated embedded CKB Light Client configuration"
        );
    }

    pub(crate) fn network_config(&self) -> Result<NetworkConfig, String> {
        let bootnodes = self
            .bootnodes
            .iter()
            .map(|address| {
                address.parse().map_err(|err| {
                    format!("invalid CKB Light Client bootnode address {address:?}: {err}")
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let whitelist_peers = self
            .preferred_peers
            .iter()
            .map(|address| {
                address.parse().map_err(|err| {
                    format!("invalid CKB Light Client preferred peer address {address:?}: {err}")
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(NetworkConfig {
            path: self.network_path.clone(),
            listen_addresses: Vec::new(),
            public_addresses: Vec::new(),
            bootnodes,
            // CKB's network service actively maintains connections to whitelist
            // peers even when whitelist_only is false. This makes them preferred
            // data peers while discovery can still find independent peers for
            // Light Client proof comparison.
            whitelist_only: false,
            whitelist_peers,
            max_peers: MAX_OUTBOUND_PEERS,
            max_outbound_peers: MAX_OUTBOUND_PEERS,
            ping_interval_secs: 120,
            ping_timeout_secs: 1_200,
            connect_outbound_interval_secs: 15,
            upnp: false,
            discovery_local_address: false,
            bootnode_mode: false,
            reuse_port_on_linux: false,
            reuse_tcp_with_ws: false,
            support_protocols: vec![
                SupportProtocol::Identify,
                SupportProtocol::Ping,
                SupportProtocol::Discovery,
                SupportProtocol::Feeler,
                SupportProtocol::DisconnectMessage,
            ],
            ..Default::default()
        })
    }
}

fn read_persisted_wallet_birthday_height(path: &Path) -> Result<Option<u64>, String> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(format!(
                "failed to read wallet birthday {}: {err}",
                path.display()
            ))
        }
    };
    let value = serde_json::from_reader::<_, serde_json::Value>(BufReader::new(file))
        .map_err(|err| format!("failed to parse wallet birthday {}: {err}", path.display()))?;
    value
        .get("history_start_block")
        .and_then(serde_json::Value::as_u64)
        .map(Some)
        .ok_or_else(|| {
            format!(
                "wallet birthday {} has no valid history_start_block",
                path.display()
            )
        })
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

fn validate_peer_funding_liveness_rpc_url(value: String) -> Result<String, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("ckb_light_client.peer_funding_liveness_rpc_url cannot be empty".to_string());
    }
    if !value.starts_with("http://") && !value.starts_with("https://") {
        return Err(
            "ckb_light_client.peer_funding_liveness_rpc_url must use http:// or https://"
                .to_string(),
        );
    }
    Ok(value.to_string())
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

fn validate_preferred_peers(peers: &[String]) -> Result<(), String> {
    let mut unique = std::collections::HashSet::with_capacity(peers.len());
    for address in peers {
        if address.trim().is_empty() {
            return Err("ckb_light_client.preferred_peers cannot contain an empty address".into());
        }
        address.parse::<Multiaddr>().map_err(|err| {
            format!("invalid CKB Light Client preferred peer address {address:?}: {err}")
        })?;
        if !address.contains("/p2p/") {
            return Err(format!(
                "CKB Light Client preferred peer address {address:?} must include /p2p/<peer-id>"
            ));
        }
        if !unique.insert(address) {
            return Err(format!(
                "ckb_light_client.preferred_peers contains duplicate address {address:?}"
            ));
        }
    }
    Ok(())
}

fn copy_bootnodes(bootnodes: &[&str]) -> Vec<String> {
    bootnodes
        .iter()
        .map(|address| (*address).to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deserialize(yaml: &str) -> Result<SerializedLightClientConfig, serde_yaml::Error> {
        serde_yaml::from_str(yaml)
    }

    #[test]
    fn mainnet_uses_fixed_defaults() {
        let config = LocalLightClientConfig::build(
            PathBuf::from("/tmp/fiber"),
            "mainnet",
            deserialize("{}").unwrap(),
        )
        .unwrap();

        assert_eq!(config.chain, LocalChain::Mainnet);
        assert_eq!(
            config.store_path,
            PathBuf::from("/tmp/fiber/ckb-light-client/store")
        );
        assert_eq!(
            config.network_path,
            PathBuf::from("/tmp/fiber/ckb-light-client/network")
        );
        assert_eq!(config.history_start_block, 0);
        assert!(!config.history_start_block_is_explicit);
        assert_eq!(
            config.wallet_birthday_path,
            PathBuf::from("/tmp/fiber/ckb/wallet-birthday.json")
        );
        assert!(!config.trust_pinned_cell_deps);
        assert!(config.peer_funding_liveness_rpc_url.is_none());
        assert_eq!(config.startup_min_peers, DEFAULT_REQUIRED_READY_PEERS);
        assert_eq!(config.startup_script_lag_tolerance, 0);
        assert_eq!(config.operational_lag_tolerance, 0);
        assert_eq!(config.bootnodes.len(), MAINNET_BOOTNODES.len());
        assert!(config.preferred_peers.is_empty());
        assert_eq!(
            config.filter_preferred_peer_chance_percent,
            DEFAULT_FILTER_PREFERRED_PEER_CHANCE_PERCENT
        );
        assert_eq!(
            config.filter_request_timeout_seconds,
            DEFAULT_FILTER_REQUEST_TIMEOUT_SECONDS
        );
        assert_eq!(
            config.filter_peer_failure_threshold,
            DEFAULT_FILTER_PEER_FAILURE_THRESHOLD
        );
        assert_eq!(
            config.filter_peer_cooldown_seconds,
            DEFAULT_FILTER_PEER_COOLDOWN_SECONDS
        );
        assert_eq!(
            LocalLightClientConfig::local_rpc_listen_address().to_string(),
            LOCAL_RPC_LISTEN_ADDRESS
        );
        assert_eq!(MAX_OUTBOUND_PEERS, 8);
        assert_eq!(DEFAULT_REQUIRED_READY_PEERS, 4);
        assert!(REMOTE_DATA_TIMEOUT < Duration::from_secs(10));
        assert_eq!(HEADER_READY_TIMEOUT, Duration::from_secs(120));
    }

    #[test]
    fn parses_history_start_block() {
        let config = LocalLightClientConfig::build(
            PathBuf::from("data"),
            "testnet",
            deserialize("history_start_block: '0x2a'").unwrap(),
        )
        .unwrap();

        assert_eq!(config.history_start_block, 42);
        assert!(config.history_start_block_is_explicit);
    }

    #[test]
    fn persisted_wallet_birthday_is_reused_but_an_explicit_height_wins() {
        let base_dir = std::env::temp_dir().join(format!(
            "fiber-ffi-light-client-config-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let ckb_dir = base_dir.join("ckb");
        std::fs::create_dir_all(&ckb_dir).unwrap();
        std::fs::write(
            ckb_dir.join(WALLET_BIRTHDAY_FILE),
            r#"{"history_start_block":4242}"#,
        )
        .unwrap();

        let persisted =
            LocalLightClientConfig::build(base_dir.clone(), "testnet", deserialize("{}").unwrap())
                .unwrap();
        assert_eq!(persisted.history_start_block, 4_242);
        assert!(!persisted.history_start_block_is_explicit);

        let explicit = LocalLightClientConfig::build(
            base_dir.clone(),
            "testnet",
            deserialize("history_start_block: '0x2a'").unwrap(),
        )
        .unwrap();
        assert_eq!(explicit.history_start_block, 42);
        assert!(explicit.history_start_block_is_explicit);

        std::fs::remove_dir_all(base_dir).unwrap();
    }

    #[test]
    fn pinned_cell_dep_trust_requires_explicit_opt_in() {
        let config = LocalLightClientConfig::build(
            PathBuf::from("data"),
            "testnet",
            deserialize("trust_pinned_cell_deps: true").unwrap(),
        )
        .unwrap();

        assert!(config.trust_pinned_cell_deps);
    }

    #[test]
    fn peer_funding_liveness_rpc_requires_an_explicit_http_url() {
        let config = LocalLightClientConfig::build(
            PathBuf::from("data"),
            "testnet",
            deserialize("peer_funding_liveness_rpc_url: ' https://testnet.ckbapp.dev/ '").unwrap(),
        )
        .unwrap();
        assert_eq!(
            config.peer_funding_liveness_rpc_url.as_deref(),
            Some("https://testnet.ckbapp.dev/")
        );

        for value in ["", "127.0.0.1:8114", "ftp://example.com"] {
            let error = LocalLightClientConfig::build(
                PathBuf::from("data"),
                "testnet",
                deserialize(&format!("peer_funding_liveness_rpc_url: '{value}'")).unwrap(),
            )
            .unwrap_err();
            assert!(error.contains("peer_funding_liveness_rpc_url"));
        }
    }

    #[test]
    fn startup_script_lag_tolerance_requires_explicit_opt_in() {
        let config = LocalLightClientConfig::build(
            PathBuf::from("data"),
            "testnet",
            deserialize("startup_script_lag_tolerance: 12").unwrap(),
        )
        .unwrap();

        assert_eq!(config.startup_script_lag_tolerance, 12);
    }

    #[test]
    fn operational_lag_tolerance_requires_explicit_opt_in() {
        let config = LocalLightClientConfig::build(
            PathBuf::from("data"),
            "testnet",
            deserialize("operational_lag_tolerance: 6").unwrap(),
        )
        .unwrap();

        assert_eq!(config.operational_lag_tolerance, 6);
    }

    #[test]
    fn filter_peer_selection_is_configurable_and_bounded() {
        let config = LocalLightClientConfig::build(
            PathBuf::from("data"),
            "testnet",
            deserialize(
                "filter_preferred_peer_chance_percent: 80\nfilter_request_timeout_seconds: 5\nfilter_peer_failure_threshold: 3\nfilter_peer_cooldown_seconds: 90",
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(config.filter_preferred_peer_chance_percent, 80);
        assert_eq!(config.filter_request_timeout_seconds, 5);
        assert_eq!(config.filter_peer_failure_threshold, 3);
        assert_eq!(config.filter_peer_cooldown_seconds, 90);

        for yaml in [
            "filter_preferred_peer_chance_percent: 101",
            "filter_request_timeout_seconds: 0",
            "filter_peer_failure_threshold: 0",
            "filter_peer_cooldown_seconds: 3601",
        ] {
            assert!(LocalLightClientConfig::build(
                PathBuf::from("data"),
                "testnet",
                deserialize(yaml).unwrap(),
            )
            .is_err());
        }
    }

    #[test]
    fn startup_min_peers_is_bounded_by_outbound_capacity() {
        let config = LocalLightClientConfig::build(
            PathBuf::from("data"),
            "testnet",
            deserialize("startup_min_peers: 2").unwrap(),
        )
        .unwrap();
        assert_eq!(config.startup_min_peers, 2);

        for value in [0, MAX_OUTBOUND_PEERS + 1] {
            let error = LocalLightClientConfig::build(
                PathBuf::from("data"),
                "testnet",
                deserialize(&format!("startup_min_peers: {value}")).unwrap(),
            )
            .unwrap_err();
            assert!(error.contains("startup_min_peers"));
        }
    }

    #[test]
    fn preferred_peers_are_kept_separate_from_discovery_bootnodes() {
        let preferred = MAINNET_BOOTNODES[0];
        let config = LocalLightClientConfig::build(
            PathBuf::from("data"),
            "testnet",
            deserialize(&format!("preferred_peers: ['{preferred}']")).unwrap(),
        )
        .unwrap();

        assert_eq!(config.preferred_peers, vec![preferred]);
        assert_eq!(config.network_config().unwrap().whitelist_peers.len(), 1);
        assert_eq!(config.bootnodes.len(), TESTNET_BOOTNODES.len());
    }

    #[test]
    fn preferred_peers_require_unique_peer_addresses() {
        let preferred = MAINNET_BOOTNODES[0];
        for yaml in [
            "preferred_peers: ['/ip4/127.0.0.1/tcp/8114']".to_string(),
            format!("preferred_peers: ['{preferred}', '{preferred}']"),
        ] {
            let error = LocalLightClientConfig::build(
                PathBuf::from("data"),
                "testnet",
                deserialize(&yaml).unwrap(),
            )
            .unwrap_err();
            assert!(error.contains("preferred peer") || error.contains("preferred_peers"));
        }
    }

    #[test]
    fn custom_chain_requires_bootnodes_and_resolves_chain_path() {
        let error = LocalLightClientConfig::build(
            PathBuf::from("/tmp/fiber"),
            "specs/dev.toml",
            deserialize("{}").unwrap(),
        )
        .unwrap_err();
        assert!(error.contains("must be set"));

        let config = LocalLightClientConfig::build(
            PathBuf::from("/tmp/fiber"),
            "specs/dev.toml",
            deserialize(&format!("bootnodes: ['{}']", MAINNET_BOOTNODES[0])).unwrap(),
        )
        .unwrap();
        assert_eq!(
            config.chain,
            LocalChain::Custom(PathBuf::from("/tmp/fiber/specs/dev.toml"))
        );
    }

    #[test]
    fn rejects_unsafe_or_unsupported_yaml_fields() {
        let error = deserialize("listen_address: '0.0.0.0:9000'").unwrap_err();
        assert!(error.to_string().contains("unknown field"));

        let error = LocalLightClientConfig::build(
            PathBuf::from("data"),
            "mainnet",
            deserialize("bootnodes: ['/ip4/127.0.0.1/tcp/8114']").unwrap(),
        )
        .unwrap_err();
        assert!(error.contains("cannot override"));
    }

    #[test]
    fn rejects_invalid_history_start_block() {
        for value in ["42", "0x", "0xnope"] {
            let serialized = deserialize(&format!("history_start_block: '{value}'")).unwrap();
            let error = LocalLightClientConfig::build(PathBuf::from("data"), "mainnet", serialized)
                .unwrap_err();
            assert!(error.contains("history_start_block"));
        }
    }
}
