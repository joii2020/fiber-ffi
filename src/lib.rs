use std::{
    cell::RefCell,
    ffi::{CStr, CString},
    fs::{File, OpenOptions},
    io::BufReader,
    os::raw::{c_char, c_void},
    panic::{catch_unwind, AssertUnwindSafe},
    path::{Path, PathBuf},
    ptr,
    sync::{mpsc as std_mpsc, Mutex, Once},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

#[cfg(feature = "disable-ckb-rpc")]
use std::io::Write;
#[cfg(feature = "disable-ckb-rpc")]
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(all(feature = "disable-ckb-rpc", unix))]
use std::os::unix::fs::PermissionsExt;

use ckb_chain_spec::ChainSpec;
use ckb_resource::Resource;
use ckb_sdk::rpc::ckb_indexer::{
    Order as CkbIndexerOrder, ScriptType as CkbIndexerScriptType, SearchKey as CkbIndexerSearchKey,
    SearchKeyFilter as CkbIndexerSearchKeyFilter, SearchMode as CkbIndexerSearchMode,
};
use ckb_sdk::{Address as CkbAddress, AddressPayload as CkbAddressPayload, NetworkType};
use clap_serde_derive::ClapSerde;
use fnn::ckb::client::CkbChainClient;
use fnn::{
    actors::RootActor,
    ckb::{
        client::CkbRpcClient,
        contracts::{try_init_contracts_context, TypeIDResolver},
        CkbChainActor, CkbConfig,
    },
    fiber::{graph::NetworkGraph, network::init_chain_hash},
    fiber::{NetworkActorCommand, NetworkActorMessage},
    start_network,
    store::actor::{StoreActor, StoreActorInitializationParameter},
    Config, FiberConfig, NetworkServiceEvent,
};
use ractor::{Actor, ActorRef};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tentacle::utils::TransportType;
use tokio::runtime::Handle as TokioHandle;
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, info, trace};
use tracing_subscriber::EnvFilter;

mod ffi_params;
mod ffi_types;

use ffi_params::{
    accept_channel_params_from_options, build_router_params_from_options, deserialize_object,
    deserialize_value, funding_lock_from_discovery_options, list_channels_params_from_options,
    list_payments_params_from_options, new_invoice_params_from_options,
    open_channel_params_from_options, open_channel_with_external_funding_params_from_options,
    optional_u64, parse_addr_type, parse_pubkey, required_hash_param,
    send_payment_params_from_options, send_payment_with_router_params_from_options,
    shutdown_channel_params_from_options, string_field,
    submit_signed_funding_tx_params_from_options, update_channel_params_from_options,
    validate_options_struct,
};

#[cfg(feature = "disable-ckb-rpc")]
mod ckb_light_client;

#[cfg(feature = "watchtower")]
use fnn::watchtower::{
    WatchtowerActor, WatchtowerMessage, DEFAULT_WATCHTOWER_CHECK_INTERVAL_SECONDS,
};

pub use ffi_types::*;

#[derive(Copy, Clone)]
struct EventCallback {
    callback: FiberEventCallback,
    user_data: usize,
}

// SAFETY: the wrapper stores only a function pointer and an opaque address. It
// never dereferences `user_data`; the foreign caller owns its synchronization.
unsafe impl Send for EventCallback {}
unsafe impl Sync for EventCallback {}

#[derive(Copy, Clone)]
struct CkbPrepareCallback {
    callback: FiberCkbPrepareCallback,
    user_data: usize,
}

// SAFETY: as with `EventCallback`, moving or sharing this value never accesses
// the pointee behind `user_data`; its lifetime is part of the callback contract.
unsafe impl Send for CkbPrepareCallback {}
unsafe impl Sync for CkbPrepareCallback {}

pub struct FiberHandle {
    stop_tx: Mutex<Option<oneshot::Sender<()>>>,
    thread: Mutex<Option<JoinHandle<()>>>,
    runtime_handle: TokioHandle,
    network_actor: ActorRef<NetworkActorMessage>,
    store: fnn::store::Store,
    fiber_config: FiberConfig,
    ckb_config: CkbConfig,
    #[cfg(feature = "disable-ckb-rpc")]
    ckb_monitor: Option<ckb_light_client::LocalCkbMonitor>,
    ckb_sync_estimator: Mutex<CkbSyncEstimator>,
}

enum StartupMessage {
    Started {
        runtime_handle: TokioHandle,
        network_actor: ActorRef<NetworkActorMessage>,
        store: fnn::store::Store,
        fiber_config: Box<FiberConfig>,
        ckb_config: Box<CkbConfig>,
        #[cfg(feature = "disable-ckb-rpc")]
        ckb_monitor: Option<ckb_light_client::LocalCkbMonitor>,
    },
    Failed(String),
}

const CKB_READINESS_MAX_TIP_AGE_MILLIS: u64 = 2 * 60 * 60 * 1_000;
// Prefix cell queries made by Fiber's funding collector need every required
// script to be scanned through the current Light Client tip. Even a one-block
// gap can therefore abort channel funding after the opening handshake.
const CKB_READINESS_MAX_INDEXER_LAG: u64 = 0;
const CKB_READINESS_RETRY_SECONDS: u64 = 3;
const CKB_FILTER_BATCH_SIZE: u64 = 1_000;
const CKB_PEER_REQUEST_TIMEOUT_SECONDS: u64 = 60;
const CKB_BALANCE_PAGE_SIZE: u32 = 1_000;
const DEFAULT_CKB_HISTORY_DISCOVERY_SAFETY_BLOCKS: u64 = 1_000;
const DEFAULT_CKB_HISTORY_DISCOVERY_MAX_INDEXER_LAG: u64 = 100;
#[cfg(feature = "disable-ckb-rpc")]
const CKB_WALLET_BIRTHDAY_VERSION: u32 = 1;
// Filter synchronization advances in bursts and one peer request is allowed to
// take this long. Treating the quiet time between batches as a stall makes the
// estimate oscillate between measured and stalled even while indexing normally.
const CKB_SYNC_STALL_THRESHOLD: Duration = Duration::from_secs(CKB_PEER_REQUEST_TIMEOUT_SECONDS);
const CKB_SYNC_SAMPLE_RESET_THRESHOLD: Duration =
    Duration::from_secs(2 * CKB_PEER_REQUEST_TIMEOUT_SECONDS);

#[derive(Clone, Debug, Serialize)]
struct CkbWaitEstimate {
    lower_seconds: u64,
    upper_seconds: u64,
    retry_after_seconds: u64,
    confidence: &'static str,
}

#[derive(Default)]
struct CkbSyncEstimator {
    last_sample: Option<(Instant, u64)>,
    lag_started_at: Option<Instant>,
    last_progress_at: Option<Instant>,
    smoothed_blocks_per_second: Option<f64>,
}

#[derive(Clone, Debug, Serialize)]
struct CkbReadiness {
    ready: bool,
    mode: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    tip_block_number: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    indexed_block_number: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lag: Option<u64>,
    max_acceptable_lag: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    wait_estimate: Option<CkbWaitEstimate>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

impl CkbReadiness {
    fn unavailable(tip_block_number: Option<u64>, reason: impl Into<String>) -> Self {
        Self {
            ready: false,
            mode: ckb_readiness_mode(),
            tip_block_number,
            indexed_block_number: None,
            lag: None,
            max_acceptable_lag: CKB_READINESS_MAX_INDEXER_LAG,
            wait_estimate: None,
            reason: Some(reason.into()),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct CkbBalance {
    ready: bool,
    mode: &'static str,
    address: String,
    lock_args: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    tip_block_number: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    indexed_block_number: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lag: Option<u64>,
    cell_count: u64,
    capacity_shannons: String,
    capacity_ckb: String,
    scope: &'static str,
}

#[cfg(feature = "disable-ckb-rpc")]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct WalletBirthdayMetadata {
    version: u32,
    network: String,
    genesis_hash: String,
    address: String,
    lock_args: String,
    history_start_block: u64,
    source: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct WalletHistoryDiscovery {
    indexer_tip: u64,
    earliest_base_ckb_cell_block: Option<u64>,
}

impl CkbSyncEstimator {
    fn observe(&mut self, readiness: &CkbReadiness, now: Instant) -> Option<CkbWaitEstimate> {
        if readiness.reason.as_deref() == Some("CKB chain tip is not current") {
            self.reset_unavailable();
            return None;
        }
        let (Some(indexed_block_number), Some(lag)) =
            (readiness.indexed_block_number, readiness.lag)
        else {
            self.reset_unavailable();
            return None;
        };

        if let Some((last_at, last_indexed)) = self.last_sample {
            let elapsed = now.saturating_duration_since(last_at);
            if indexed_block_number < last_indexed || elapsed > CKB_SYNC_SAMPLE_RESET_THRESHOLD {
                self.smoothed_blocks_per_second = None;
                self.last_progress_at = None;
                self.lag_started_at = Some(now);
            } else if indexed_block_number > last_indexed && elapsed >= Duration::from_millis(500) {
                let sample_rate =
                    (indexed_block_number - last_indexed) as f64 / elapsed.as_secs_f64();
                self.smoothed_blocks_per_second = Some(
                    self.smoothed_blocks_per_second
                        .map(|rate| rate * 0.5 + sample_rate * 0.5)
                        .unwrap_or(sample_rate),
                );
                self.last_progress_at = Some(now);
            }
        }
        self.last_sample = Some((now, indexed_block_number));

        if readiness.ready || lag == 0 {
            self.lag_started_at = None;
            return None;
        }

        self.lag_started_at.get_or_insert(now);

        if let Some(rate) = self.smoothed_blocks_per_second.filter(|rate| *rate > 0.0) {
            if self.last_progress_at.is_some_and(|last_progress| {
                now.saturating_duration_since(last_progress) < CKB_SYNC_STALL_THRESHOLD
            }) {
                let expected = ((lag as f64 / rate).ceil() as u64).max(1);
                return Some(CkbWaitEstimate {
                    lower_seconds: (expected / 2).max(1),
                    upper_seconds: expected
                        .saturating_mul(2)
                        .saturating_add(CKB_READINESS_RETRY_SECONDS),
                    retry_after_seconds: CKB_READINESS_RETRY_SECONDS,
                    confidence: "measured",
                });
            }
        }

        let batches = lag.div_ceil(CKB_FILTER_BATCH_SIZE).max(1);
        let lag_started_at = self.lag_started_at.unwrap_or(now);
        let stalled = now.saturating_duration_since(lag_started_at) >= CKB_SYNC_STALL_THRESHOLD;
        Some(CkbWaitEstimate {
            lower_seconds: batches.saturating_mul(CKB_READINESS_RETRY_SECONDS),
            upper_seconds: batches
                .saturating_mul(CKB_PEER_REQUEST_TIMEOUT_SECONDS)
                .clamp(
                    CKB_PEER_REQUEST_TIMEOUT_SECONDS,
                    30 * CKB_PEER_REQUEST_TIMEOUT_SECONDS,
                )
                .max(if stalled {
                    2 * CKB_PEER_REQUEST_TIMEOUT_SECONDS
                } else {
                    0
                }),
            retry_after_seconds: CKB_READINESS_RETRY_SECONDS,
            confidence: if stalled { "stalled" } else { "low" },
        })
    }

    fn reset_unavailable(&mut self) {
        self.last_sample = None;
        self.lag_started_at = None;
        self.last_progress_at = None;
        self.smoothed_blocks_per_second = None;
    }
}

fn ckb_readiness_mode() -> &'static str {
    if cfg!(feature = "disable-ckb-rpc") {
        "light_client"
    } else {
        "external_rpc"
    }
}

fn evaluate_ckb_readiness(
    tip_block_number: u64,
    tip_timestamp_millis: u64,
    indexed_block_number: u64,
    now_millis: u64,
) -> CkbReadiness {
    evaluate_ckb_readiness_with_lag_tolerance(
        tip_block_number,
        tip_timestamp_millis,
        indexed_block_number,
        now_millis,
        CKB_READINESS_MAX_INDEXER_LAG,
    )
}

fn evaluate_ckb_readiness_with_lag_tolerance(
    tip_block_number: u64,
    tip_timestamp_millis: u64,
    indexed_block_number: u64,
    now_millis: u64,
    max_acceptable_lag: u64,
) -> CkbReadiness {
    let lag = tip_block_number.saturating_sub(indexed_block_number);
    let tip_is_current = tip_block_number > 0
        && now_millis.abs_diff(tip_timestamp_millis) <= CKB_READINESS_MAX_TIP_AGE_MILLIS;
    let reason = if !tip_is_current {
        Some("CKB chain tip is not current".to_string())
    } else if lag > max_acceptable_lag {
        Some(format!(
            "CKB indexer is {lag} block(s) behind the chain tip; maximum acceptable lag is {max_acceptable_lag}"
        ))
    } else {
        None
    };

    CkbReadiness {
        ready: reason.is_none(),
        mode: ckb_readiness_mode(),
        tip_block_number: Some(tip_block_number),
        indexed_block_number: Some(indexed_block_number),
        lag: Some(lag),
        max_acceptable_lag,
        wait_estimate: None,
        reason,
    }
}

async fn query_ckb_readiness(ckb_config: &CkbConfig) -> CkbReadiness {
    let client = ckb_config.ckb_rpc_client();
    let tip = match client.get_tip_header().await {
        Ok(tip) => tip,
        Err(err) => {
            return CkbReadiness::unavailable(
                None,
                format!("failed to query CKB chain tip: {err}"),
            );
        }
    };
    let tip_block_number = tip.inner.number.value();
    let tip_timestamp_millis = tip.inner.timestamp.value();
    let indexed_tip = match client.get_indexer_tip().await {
        Ok(Some(indexed_tip)) => indexed_tip,
        Ok(None) => {
            return CkbReadiness::unavailable(
                Some(tip_block_number),
                "CKB indexer tip is not available",
            );
        }
        Err(err) => {
            return CkbReadiness::unavailable(
                Some(tip_block_number),
                format!("failed to query CKB indexer tip: {err}"),
            );
        }
    };

    evaluate_ckb_readiness(
        tip_block_number,
        tip_timestamp_millis,
        indexed_tip.block_number.value(),
        unix_time_millis(),
    )
}

fn unix_time_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn current_ckb_readiness(handle: &FiberHandle) -> CkbReadiness {
    #[cfg(feature = "disable-ckb-rpc")]
    let mut readiness = if let Some(local_ckb) = handle.ckb_monitor.as_ref() {
        let (tip_block_number, tip_timestamp_millis, indexed_block_number) =
            local_ckb.sync_snapshot();
        evaluate_ckb_readiness_with_lag_tolerance(
            tip_block_number,
            tip_timestamp_millis,
            indexed_block_number,
            unix_time_millis(),
            local_ckb.operational_lag_tolerance(),
        )
    } else {
        handle
            .runtime_handle
            .block_on(query_ckb_readiness(&handle.ckb_config))
    };
    #[cfg(not(feature = "disable-ckb-rpc"))]
    let mut readiness = handle
        .runtime_handle
        .block_on(query_ckb_readiness(&handle.ckb_config));
    if let Ok(mut estimator) = handle.ckb_sync_estimator.lock() {
        readiness.wait_estimate = estimator.observe(&readiness, Instant::now());
    }
    readiness
}

fn ensure_ckb_ready(handle: &FiberHandle) -> FfiCallResult<()> {
    let readiness = current_ckb_readiness(handle);
    if readiness.ready {
        return Ok(());
    }

    let detail = serde_json::to_string(&readiness)
        .unwrap_or_else(|_| "CKB readiness details are unavailable".to_string());
    Err(ffi_error(
        FiberFfiStatus::NotReady,
        format!("CKB backend is not ready: {detail}"),
    ))
}

fn format_ckb_capacity(shannons: u64) -> String {
    const SHANNONS_PER_CKB: u64 = 100_000_000;
    format!(
        "{}.{:08}",
        shannons / SHANNONS_PER_CKB,
        shannons % SHANNONS_PER_CKB
    )
}

fn ckb_address_network(chain: &str) -> NetworkType {
    match chain {
        "mainnet" | "ckb" => NetworkType::Mainnet,
        "testnet" | "ckb_testnet" => NetworkType::Testnet,
        "staging" | "ckb_staging" => NetworkType::Staging,
        "preview" | "ckb_preview" => NetworkType::Preview,
        "dev" | "ckb_dev" => NetworkType::Dev,
        _ => NetworkType::Dev,
    }
}

fn load_chain_genesis_block(
    chain: &str,
    base_dir: &Path,
) -> std::result::Result<ckb_types::core::BlockView, String> {
    let chain_spec = ChainSpec::load_from(&match chain {
        "mainnet" => Resource::bundled("specs/mainnet.toml".to_string()),
        "testnet" => Resource::bundled("specs/testnet.toml".to_string()),
        path => Resource::file_system(base_dir.join(path)),
    })
    .map_err(|err| format!("failed to load chain spec: {err}"))?;
    chain_spec
        .build_genesis()
        .map_err(|err| format!("failed to build ckb genesis block: {err}"))
}

fn funding_lock_from_genesis(
    ckb_config: &CkbConfig,
    genesis_block: &ckb_types::core::BlockView,
) -> std::result::Result<ckb_types::packed::Script, String> {
    use ckb_types::{core::ScriptHashType, prelude::*};

    let secp256k1_type_script = genesis_block
        .transaction(0)
        .and_then(|transaction| transaction.output(1))
        .and_then(|output| output.type_().to_opt())
        .ok_or_else(|| {
            "failed to derive the CKB funding lock script from the genesis block".to_string()
        })?;
    let secret_key = ckb_config
        .read_secret_key()
        .map_err(|err| format!("failed to read the CKB funding key: {err}"))?;
    let address_payload =
        CkbAddressPayload::from_pubkey(&secret_key.public_key(secp256k1::SECP256K1));
    Ok(ckb_types::packed::Script::new_builder()
        .code_hash(secp256k1_type_script.calc_script_hash())
        .hash_type(ScriptHashType::Type)
        .args(address_payload.args().pack())
        .build())
}

#[cfg(feature = "disable-ckb-rpc")]
fn read_wallet_birthday_metadata(
    path: &Path,
) -> std::result::Result<Option<WalletBirthdayMetadata>, String> {
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
    serde_json::from_reader(BufReader::new(file))
        .map(Some)
        .map_err(|err| format!("failed to parse wallet birthday {}: {err}", path.display()))
}

#[cfg(feature = "disable-ckb-rpc")]
fn write_wallet_birthday_metadata(
    path: &Path,
    metadata: &WalletBirthdayMetadata,
) -> std::result::Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| format!("wallet birthday path {} has no parent", path.display()))?;
    std::fs::create_dir_all(parent).map_err(|err| {
        format!(
            "failed to create wallet birthday directory {}: {err}",
            parent.display()
        )
    })?;
    #[cfg(unix)]
    std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700)).map_err(|err| {
        format!(
            "failed to secure wallet birthday directory {}: {err}",
            parent.display()
        )
    })?;

    let temp_id = NEXT_WALLET_BIRTHDAY_TEMP_ID.fetch_add(1, Ordering::Relaxed);
    let temp_path = parent.join(format!(
        ".{}.{}.{}.tmp",
        ckb_light_client::config::WALLET_BIRTHDAY_FILE,
        std::process::id(),
        temp_id
    ));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)
            .map_err(|err| {
                format!(
                    "failed to create temporary wallet birthday {}: {err}",
                    temp_path.display()
                )
            })?;
        #[cfg(unix)]
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .map_err(|err| {
                format!(
                    "failed to secure temporary wallet birthday {}: {err}",
                    temp_path.display()
                )
            })?;
        serde_json::to_writer_pretty(&mut file, metadata).map_err(|err| {
            format!(
                "failed to serialize wallet birthday {}: {err}",
                temp_path.display()
            )
        })?;
        file.write_all(b"\n").map_err(|err| {
            format!(
                "failed to finish wallet birthday {}: {err}",
                temp_path.display()
            )
        })?;
        file.sync_all().map_err(|err| {
            format!(
                "failed to flush wallet birthday {}: {err}",
                temp_path.display()
            )
        })?;
        std::fs::rename(&temp_path, path).map_err(|err| {
            format!(
                "failed to install wallet birthday {}: {err}",
                path.display()
            )
        })?;
        if let Ok(directory) = File::open(parent) {
            let _ = directory.sync_all();
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temp_path);
    }
    result
}

#[cfg(feature = "disable-ckb-rpc")]
fn read_legacy_history_start_block(path: &Path) -> std::result::Result<Option<u64>, String> {
    let value = match std::fs::read_to_string(path) {
        Ok(value) => value,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(format!(
                "failed to read legacy wallet birthday {}: {err}",
                path.display()
            ))
        }
    };
    let value = value.trim();
    let digits = value.strip_prefix("0x").ok_or_else(|| {
        format!(
            "legacy wallet birthday {} must be a 0x-prefixed hexadecimal height",
            path.display()
        )
    })?;
    if digits.is_empty() {
        return Err(format!(
            "legacy wallet birthday {} has an empty height",
            path.display()
        ));
    }
    u64::from_str_radix(digits, 16)
        .map(Some)
        .map_err(|err| format!("invalid legacy wallet birthday {}: {err}", path.display()))
}

fn select_wallet_history_start_block(
    discovery: &WalletHistoryDiscovery,
    safety_blocks: u64,
) -> u64 {
    discovery
        .earliest_base_ckb_cell_block
        .unwrap_or(discovery.indexer_tip)
        .saturating_sub(safety_blocks)
}

#[cfg(any(feature = "disable-ckb-rpc", test))]
fn select_earliest_history_start_block(
    configured: Option<u64>,
    persisted: Option<u64>,
    discovered: Option<u64>,
    legacy: Option<u64>,
) -> u64 {
    [configured, persisted, discovered, legacy]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or_default()
}

#[cfg(feature = "disable-ckb-rpc")]
fn validate_wallet_birthday_metadata(
    metadata: &WalletBirthdayMetadata,
    network: &str,
    genesis_hash: &str,
    address: &str,
    lock_args: &str,
    path: &Path,
) -> std::result::Result<(), String> {
    if metadata.version != CKB_WALLET_BIRTHDAY_VERSION {
        return Err(format!(
            "wallet birthday {} has unsupported version {}",
            path.display(),
            metadata.version
        ));
    }
    for (field, actual, expected) in [
        ("network", metadata.network.as_str(), network),
        ("genesis_hash", metadata.genesis_hash.as_str(), genesis_hash),
        ("address", metadata.address.as_str(), address),
        ("lock_args", metadata.lock_args.as_str(), lock_args),
    ] {
        if actual != expected {
            return Err(format!(
                "wallet birthday {} belongs to a different {field}: expected {expected}, found {actual}",
                path.display()
            ));
        }
    }
    Ok(())
}

async fn discover_wallet_history(
    rpc_url: &str,
    funding_lock: ckb_types::packed::Script,
    max_indexer_lag: u64,
) -> std::result::Result<WalletHistoryDiscovery, String> {
    let rpc = ckb_sdk::CkbRpcAsyncClient::new_with_timeout(rpc_url, Duration::from_secs(30))
        .map_err(|err| format!("failed to create wallet discovery RPC client: {err}"))?;
    let node_tip = rpc
        .get_tip_block_number()
        .await
        .map_err(|err| format!("wallet history discovery RPC get_tip_block_number failed: {err}"))?
        .value();
    let indexer_tip = rpc
        .get_indexer_tip()
        .await
        .map_err(|err| format!("wallet history discovery RPC get_indexer_tip failed: {err}"))?
        .ok_or_else(|| "wallet history discovery RPC has no CKB Indexer enabled".to_string())?
        .block_number
        .value();
    if indexer_tip > node_tip {
        return Err(format!(
            "wallet history discovery Indexer tip {indexer_tip} is above its node tip {node_tip}"
        ));
    }
    let indexer_lag = node_tip.saturating_sub(indexer_tip);
    if indexer_lag > max_indexer_lag {
        return Err(format!(
            "wallet history discovery Indexer is {indexer_lag} blocks behind its node; maximum acceptable lag is {max_indexer_lag}"
        ));
    }

    let search_key = CkbIndexerSearchKey {
        script: funding_lock.into(),
        script_type: CkbIndexerScriptType::Lock,
        script_search_mode: Some(CkbIndexerSearchMode::Exact),
        filter: Some(CkbIndexerSearchKeyFilter {
            script: None,
            script_len_range: Some([0u64.into(), 1u64.into()]),
            output_data: None,
            output_data_filter_mode: None,
            output_data_len_range: Some([0u64.into(), 1u64.into()]),
            output_capacity_range: None,
            block_range: None,
        }),
        with_data: Some(false),
        group_by_transaction: Some(false),
    };
    let earliest_base_ckb_cell_block = rpc
        .get_cells(search_key, CkbIndexerOrder::Asc, 1u32.into(), None)
        .await
        .map_err(|err| format!("wallet history discovery RPC get_cells failed: {err}"))?
        .objects
        .first()
        .map(|cell| cell.block_number.value());
    if earliest_base_ckb_cell_block.is_some_and(|height| height > indexer_tip) {
        return Err(
            "wallet history discovery Indexer returned a Cell above its own tip".to_string(),
        );
    }
    Ok(WalletHistoryDiscovery {
        indexer_tip,
        earliest_base_ckb_cell_block,
    })
}

async fn query_ckb_balance(
    ckb_config: &CkbConfig,
    chain: &str,
    readiness: CkbReadiness,
) -> std::result::Result<CkbBalance, String> {
    let funding_lock = ckb_config
        .get_default_funding_lock_script()
        .map_err(|err| format!("failed to derive the CKB funding lock script: {err}"))?;
    let address_payload = CkbAddressPayload::from(funding_lock.clone());
    let lock_args = format!("0x{}", hex::encode(address_payload.args()));
    let address = CkbAddress::new(ckb_address_network(chain), address_payload, true).to_string();
    let search_key = CkbIndexerSearchKey {
        script: funding_lock.into(),
        script_type: CkbIndexerScriptType::Lock,
        script_search_mode: Some(CkbIndexerSearchMode::Exact),
        filter: Some(CkbIndexerSearchKeyFilter {
            script: None,
            script_len_range: Some([0u64.into(), 1u64.into()]),
            output_data: None,
            output_data_filter_mode: None,
            output_data_len_range: Some([0u64.into(), 1u64.into()]),
            output_capacity_range: None,
            block_range: None,
        }),
        with_data: Some(false),
        group_by_transaction: Some(false),
    };
    let client = CkbRpcClient::new(ckb_config);
    let mut after = None;
    let mut cell_count = 0u64;
    let mut capacity_shannons = 0u64;

    loop {
        let page = client
            .get_cells(
                search_key.clone(),
                CkbIndexerOrder::Asc,
                CKB_BALANCE_PAGE_SIZE,
                after.clone(),
            )
            .await
            .map_err(|err| format!("failed to query CKB wallet cells: {err}"))?;
        let page_len = page.objects.len();

        for cell in page.objects {
            cell_count = cell_count
                .checked_add(1)
                .ok_or_else(|| "CKB wallet cell count overflowed u64".to_string())?;
            capacity_shannons = capacity_shannons
                .checked_add(cell.output.capacity.value())
                .ok_or_else(|| "CKB wallet capacity overflowed u64".to_string())?;
        }

        if page_len < CKB_BALANCE_PAGE_SIZE as usize {
            break;
        }
        if after.as_ref() == Some(&page.last_cursor) {
            return Err("CKB wallet cell pagination cursor did not advance".to_string());
        }
        after = Some(page.last_cursor);
    }

    Ok(CkbBalance {
        ready: readiness.ready,
        mode: readiness.mode,
        address,
        lock_args,
        tip_block_number: readiness.tip_block_number,
        indexed_block_number: readiness.indexed_block_number,
        lag: readiness.lag,
        cell_count,
        capacity_shannons: capacity_shannons.to_string(),
        capacity_ckb: format_ckb_capacity(capacity_shannons),
        scope: "base_ckb_only",
    })
}

#[cfg(feature = "disable-ckb-rpc")]
#[derive(Clone, Debug, Eq, PartialEq)]
struct CkbPreparationKey {
    config_path: String,
    database_prefix: Option<String>,
    config_contents_hash: String,
    funding_pubkey_hash: String,
    history_start_block: u64,
}

#[cfg(feature = "disable-ckb-rpc")]
struct PreparedCkbWorker {
    id: u64,
    key: CkbPreparationKey,
    start_tx: oneshot::Sender<PreparedCkbStartCommand>,
    thread: Option<JoinHandle<()>>,
}

#[cfg(feature = "disable-ckb-rpc")]
enum CkbPreparationState {
    Idle,
    Preparing(PreparedCkbWorker),
    Ready(PreparedCkbWorker),
    InUse(CkbPreparationKey),
}

#[cfg(feature = "disable-ckb-rpc")]
struct PreparedCkbStartCommand {
    callback: Option<EventCallback>,
    startup_tx: std_mpsc::Sender<StartupMessage>,
    stop_rx: oneshot::Receiver<()>,
}

#[derive(Default)]
struct PreparedCkbStart {
    #[cfg(feature = "disable-ckb-rpc")]
    local_ckb: Option<ckb_light_client::LocalCkbNodeHandle>,
}

static INIT_LOGGING: Once = Once::new();
static CHAIN_HASH_STATE: Mutex<Option<String>> = Mutex::new(None);
#[cfg(feature = "disable-ckb-rpc")]
static CKB_PREPARATION_STATE: Mutex<CkbPreparationState> = Mutex::new(CkbPreparationState::Idle);
#[cfg(feature = "disable-ckb-rpc")]
static NEXT_CKB_PREPARATION_ID: AtomicU64 = AtomicU64::new(1);
#[cfg(feature = "disable-ckb-rpc")]
static NEXT_WALLET_BIRTHDAY_TEMP_ID: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static LAST_ERROR: RefCell<Option<String>> = const { RefCell::new(None) };
}

#[no_mangle]
pub extern "C" fn fiber_version() -> *const c_char {
    concat!(env!("CARGO_PKG_VERSION"), "\0").as_ptr() as *const c_char
}

/// Derive the configured CKB funding address without starting Fiber or using RPC.
///
/// # Safety
///
/// `options` and its referenced strings must be valid for this call. `out_address`
/// must point to writable storage for one owned C string pointer.
#[no_mangle]
pub unsafe extern "C" fn fiber_ckb_funding_address(
    options: *const FiberStartOptions,
    out_address: *mut *mut c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let options = checked_options(options)?;
        prepare_out_string(out_address)?;
        let config_path = required_string(options.config_path, "config_path")?;
        let database_prefix = optional_string(options.database_prefix)?;
        let (parsed_config, genesis_block) =
            parse_config_with_genesis(&config_path, database_prefix)
                .map_err(|err| ffi_error(FiberFfiStatus::InvalidArgument, err))?;
        let fiber_config = parsed_config.fiber.fiber.as_ref().ok_or_else(|| {
            ffi_error(
                FiberFfiStatus::InvalidArgument,
                "fiber service must be enabled in config services",
            )
        })?;
        let ckb_config = parsed_config.fiber.ckb.as_ref().ok_or_else(|| {
            ffi_error(
                FiberFfiStatus::InvalidArgument,
                "service fiber requires service ckb to be enabled",
            )
        })?;
        let funding_lock = funding_lock_from_genesis(ckb_config, &genesis_block)
            .map_err(|err| ffi_error(FiberFfiStatus::InvalidArgument, err))?;
        let address = CkbAddress::new(
            ckb_address_network(&fiber_config.chain),
            CkbAddressPayload::from(funding_lock),
            true,
        )
        .to_string();
        write_string_out(out_address, &address)?;
        Ok(FiberFfiStatus::Ok)
    })
}

/// Query an explicitly supplied external CKB RPC/Indexer for a conservative
/// history start block. This function does not read or mutate Light Client state.
///
/// Exactly one of `lock_args`, `pubkey`, and `address` must be supplied.
///
/// # Safety
///
/// `options` and its referenced strings must be valid for this call. `out_height`
/// must point to writable `u64` storage.
#[no_mangle]
pub unsafe extern "C" fn fiber_ckb_discover_history_start_block(
    options: *const FiberCkbDiscoverHistoryStartBlockOptions,
    out_height: *mut u64,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let options = checked_options(options)?;
        if out_height.is_null() {
            return Err(ffi_error(
                FiberFfiStatus::NullPointer,
                "out_height must be non-null",
            ));
        }
        *out_height = 0;
        validate_options_struct::<FiberCkbDiscoverHistoryStartBlockOptions>(
            options.struct_size,
            options.flags,
            "FiberCkbDiscoverHistoryStartBlockOptions",
        )?;
        let rpc_url = required_string(options.rpc_url, "rpc_url")?;
        if !rpc_url.starts_with("https://") && !rpc_url.starts_with("http://") {
            return Err(ffi_error(
                FiberFfiStatus::InvalidArgument,
                "rpc_url must use http:// or https://",
            ));
        }
        let funding_lock = funding_lock_from_discovery_options(options)?;
        let safety_blocks = optional_u64(
            options.has_safety_blocks,
            options.safety_blocks,
            "has_safety_blocks",
        )?
        .unwrap_or(DEFAULT_CKB_HISTORY_DISCOVERY_SAFETY_BLOCKS);
        let max_indexer_lag = optional_u64(
            options.has_max_indexer_lag,
            options.max_indexer_lag,
            "has_max_indexer_lag",
        )?
        .unwrap_or(DEFAULT_CKB_HISTORY_DISCOVERY_MAX_INDEXER_LAG);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| {
                ffi_error(
                    FiberFfiStatus::StartupFailed,
                    format!("failed to create wallet discovery runtime: {err}"),
                )
            })?;
        let discovery = runtime
            .block_on(discover_wallet_history(
                &rpc_url,
                funding_lock,
                max_indexer_lag,
            ))
            .map_err(|err| ffi_error(FiberFfiStatus::StartupFailed, err))?;
        *out_height = select_wallet_history_start_block(&discovery, safety_blocks);
        Ok(FiberFfiStatus::Ok)
    })
}

/// Prepare the CKB backend used by the next `fiber_start` call.
///
/// The progress callback is never invoked inline. Its JSON string is borrowed
/// and is only valid for the duration of each callback.
///
/// # Safety
///
/// `options` and its referenced strings must remain valid for this call.
/// `completion_callback_user_data` must remain valid until the terminal
/// `ready` or `failed` callback returns, according to the ownership rules
/// chosen by the caller.
#[no_mangle]
pub unsafe extern "C" fn fiber_prepare_ckb(
    options: *const FiberStartOptions,
    completion_callback: Option<FiberCkbPrepareCallback>,
    completion_callback_user_data: *mut c_void,
) -> FiberFfiStatus {
    fiber_prepare_ckb_inner(
        options,
        None,
        completion_callback,
        completion_callback_user_data,
    )
}

/// Prepare the CKB backend using a caller-discovered history start block.
///
/// # Safety
///
/// The safety requirements are identical to `fiber_prepare_ckb`.
#[no_mangle]
pub unsafe extern "C" fn fiber_prepare_ckb_with_history_start_block(
    options: *const FiberStartOptions,
    history_start_block: u64,
    completion_callback: Option<FiberCkbPrepareCallback>,
    completion_callback_user_data: *mut c_void,
) -> FiberFfiStatus {
    fiber_prepare_ckb_inner(
        options,
        Some(history_start_block),
        completion_callback,
        completion_callback_user_data,
    )
}

unsafe fn fiber_prepare_ckb_inner(
    options: *const FiberStartOptions,
    discovered_history_start_block: Option<u64>,
    completion_callback: Option<FiberCkbPrepareCallback>,
    completion_callback_user_data: *mut c_void,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let options = checked_options(options)?;
        let completion_callback = completion_callback.ok_or_else(|| {
            ffi_error(
                FiberFfiStatus::NullPointer,
                "completion_callback must be non-null",
            )
        })?;
        let config_path = required_string(options.config_path, "config_path")?;
        let database_prefix = optional_string(options.database_prefix)?;
        let log_level = optional_string(options.log_level)?.unwrap_or_else(|| "info".to_string());
        let callback = CkbPrepareCallback {
            callback: completion_callback,
            user_data: completion_callback_user_data as usize,
        };

        init_logging(&log_level);

        #[cfg(not(feature = "disable-ckb-rpc"))]
        let _ = (
            &config_path,
            &database_prefix,
            discovered_history_start_block,
        );

        #[cfg(feature = "disable-ckb-rpc")]
        schedule_embedded_ckb_preparation(
            config_path,
            database_prefix,
            discovered_history_start_block,
            callback,
        )?;

        #[cfg(not(feature = "disable-ckb-rpc"))]
        thread::Builder::new()
            .name("fiber-ffi-prepare-ckb".to_string())
            .spawn(move || {
                emit_ckb_prepare_completion(
                    callback,
                    FiberFfiStatus::Ok,
                    json!({
                        "ready": true,
                        "mode": "external_rpc",
                        "skipped": true,
                        "status": "ready",
                    }),
                );
            })
            .map_err(|err| ffi_error(FiberFfiStatus::StartupFailed, err.to_string()))?;

        Ok(FiberFfiStatus::Ok)
    })
}

/// Starts a Fiber node and returns an owning handle.
///
/// # Safety
///
/// `options` and its strings must be valid for this call. `out_handle` must
/// be writable. On success, pass the returned handle to `fiber_stop` exactly once.
#[no_mangle]
pub unsafe extern "C" fn fiber_start(
    options: *const FiberStartOptions,
    out_handle: *mut *mut FiberHandle,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        if options.is_null() || out_handle.is_null() {
            return Err(ffi_error(
                FiberFfiStatus::NullPointer,
                "options and out_handle must be non-null",
            ));
        }

        *out_handle = ptr::null_mut();

        let options = &*options;
        let config_path = required_string(options.config_path, "config_path")?;
        let database_prefix = optional_string(options.database_prefix)?;
        let log_level = optional_string(options.log_level)?.unwrap_or_else(|| "info".to_string());
        let callback = options.event_callback.map(|callback| EventCallback {
            callback,
            user_data: options.event_callback_user_data as usize,
        });

        init_logging(&log_level);

        let (startup_tx, startup_rx) = std_mpsc::channel();
        let (stop_tx, stop_rx) = oneshot::channel();
        #[cfg(feature = "disable-ckb-rpc")]
        let thread = match take_prepared_ckb_worker(ckb_preparation_key(
            config_path.clone(),
            database_prefix.clone(),
        )?)? {
            Some(mut worker) => {
                let worker_key = worker.key.clone();
                if worker
                    .start_tx
                    .send(PreparedCkbStartCommand {
                        callback,
                        startup_tx: startup_tx.clone(),
                        stop_rx,
                    })
                    .is_err()
                {
                    clear_ckb_in_use(&worker_key);
                    return Err(ffi_error(
                        FiberFfiStatus::StartupFailed,
                        "prepared CKB runtime exited before Fiber could start",
                    ));
                }
                match worker.thread.take() {
                    Some(thread) => thread,
                    None => {
                        clear_ckb_in_use(&worker_key);
                        return Err(ffi_error(
                            FiberFfiStatus::Panic,
                            "prepared CKB runtime thread is missing",
                        ));
                    }
                }
            }
            None => {
                spawn_fiber_runtime(config_path, database_prefix, callback, startup_tx, stop_rx)?
            }
        };

        #[cfg(not(feature = "disable-ckb-rpc"))]
        let thread =
            spawn_fiber_runtime(config_path, database_prefix, callback, startup_tx, stop_rx)?;

        match startup_rx.recv() {
            Ok(StartupMessage::Started {
                runtime_handle,
                network_actor,
                store,
                fiber_config,
                ckb_config,
                #[cfg(feature = "disable-ckb-rpc")]
                ckb_monitor,
            }) => {
                let handle = Box::new(FiberHandle {
                    stop_tx: Mutex::new(Some(stop_tx)),
                    thread: Mutex::new(Some(thread)),
                    runtime_handle,
                    network_actor,
                    store,
                    fiber_config: *fiber_config,
                    ckb_config: *ckb_config,
                    #[cfg(feature = "disable-ckb-rpc")]
                    ckb_monitor,
                    ckb_sync_estimator: Mutex::new(CkbSyncEstimator::default()),
                });
                *out_handle = Box::into_raw(handle);
                Ok(FiberFfiStatus::Ok)
            }
            Ok(StartupMessage::Failed(err)) => {
                let _ = thread.join();
                Err(ffi_error(FiberFfiStatus::StartupFailed, err))
            }
            Err(err) => {
                let _ = thread.join();
                Err(ffi_error(
                    FiberFfiStatus::StartupFailed,
                    format!("runtime thread exited before reporting startup status: {err}"),
                ))
            }
        }
    })
}

fn spawn_fiber_runtime(
    config_path: String,
    database_prefix: Option<String>,
    callback: Option<EventCallback>,
    startup_tx: std_mpsc::Sender<StartupMessage>,
    stop_rx: oneshot::Receiver<()>,
) -> FfiCallResult<JoinHandle<()>> {
    thread::Builder::new()
        .name("fiber-ffi-runtime".to_string())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(err) => {
                    let _ = startup_tx.send(StartupMessage::Failed(format!(
                        "failed to create tokio runtime: {err}"
                    )));
                    return;
                }
            };

            let runtime_handle = runtime.handle().clone();
            let startup_tx_for_panic = startup_tx.clone();
            let startup_result = catch_unwind(AssertUnwindSafe(|| {
                runtime.block_on(run_fiber_node(
                    runtime_handle,
                    config_path,
                    database_prefix,
                    callback,
                    startup_tx,
                    stop_rx,
                    PreparedCkbStart::default(),
                ));
            }));
            if let Err(err) = startup_result {
                let _ = startup_tx_for_panic.send(StartupMessage::Failed(format!(
                    "runtime thread panicked during startup: {}",
                    panic_message(err)
                )));
            }
        })
        .map_err(|err| ffi_error(FiberFfiStatus::StartupFailed, err.to_string()))
}

async fn run_fiber_node(
    runtime_handle: TokioHandle,
    config_path: String,
    database_prefix: Option<String>,
    callback: Option<EventCallback>,
    startup_tx: std_mpsc::Sender<StartupMessage>,
    stop_rx: oneshot::Receiver<()>,
    prepared_ckb: PreparedCkbStart,
) {
    match start_node(config_path, database_prefix, callback, prepared_ckb).await {
        Ok(node) => {
            let network_actor = node.network_actor.clone();
            let store = node.store.clone();
            let fiber_config = node.fiber_config.clone();
            let ckb_config = node.ckb_config.clone();
            #[cfg(feature = "disable-ckb-rpc")]
            let ckb_monitor = node
                .local_ckb
                .as_ref()
                .map(ckb_light_client::LocalCkbNodeHandle::monitor);
            let _ = startup_tx.send(StartupMessage::Started {
                runtime_handle,
                network_actor,
                store,
                fiber_config: Box::new(fiber_config),
                ckb_config: Box::new(ckb_config),
                #[cfg(feature = "disable-ckb-rpc")]
                ckb_monitor,
            });
            stop_node_on_signal(node, stop_rx).await;
        }
        Err(err) => {
            let _ = startup_tx.send(StartupMessage::Failed(err));
        }
    }
}

fn panic_message(err: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = err.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = err.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic".to_string()
    }
}

#[cfg(feature = "disable-ckb-rpc")]
fn schedule_embedded_ckb_preparation(
    config_path: String,
    database_prefix: Option<String>,
    discovered_history_start_block: Option<u64>,
    callback: CkbPrepareCallback,
) -> FfiCallResult<()> {
    let key = ckb_preparation_key_with_history_floor(
        config_path.clone(),
        database_prefix.clone(),
        discovered_history_start_block,
    )?;
    let mut state = CKB_PREPARATION_STATE
        .lock()
        .map_err(|_| ffi_error(FiberFfiStatus::Panic, "CKB preparation mutex poisoned"))?;

    match &*state {
        CkbPreparationState::Preparing(worker) => {
            let detail = if worker.key == key {
                "CKB preparation is already in progress"
            } else {
                "CKB preparation is already in progress for a different configuration"
            };
            return Err(ffi_error(FiberFfiStatus::InvalidArgument, detail));
        }
        CkbPreparationState::Ready(worker) => {
            if worker.key != key {
                return Err(ffi_error(
                    FiberFfiStatus::InvalidArgument,
                    "CKB is already prepared for a different configuration",
                ));
            }
            spawn_embedded_ckb_ready(callback)
                .map_err(|err| ffi_error(FiberFfiStatus::StartupFailed, err))?;
            return Ok(());
        }
        CkbPreparationState::InUse(active_key) => {
            let detail = if active_key == &key {
                "prepared CKB is already in use by Fiber"
            } else {
                "CKB is already in use by Fiber with a different configuration"
            };
            return Err(ffi_error(FiberFfiStatus::InvalidArgument, detail));
        }
        CkbPreparationState::Idle => {}
    }

    let id = NEXT_CKB_PREPARATION_ID.fetch_add(1, Ordering::Relaxed);
    let (start_tx, start_rx) = oneshot::channel();
    let (begin_tx, begin_rx) = std_mpsc::sync_channel(0);
    let thread = thread::Builder::new()
        .name("fiber-ffi-prepare-ckb".to_string())
        .spawn(move || {
            if begin_rx.recv().is_err() {
                return;
            }
            run_embedded_ckb_preparation(
                id,
                config_path,
                database_prefix,
                discovered_history_start_block,
                callback,
                start_rx,
            );
        })
        .map_err(|err| ffi_error(FiberFfiStatus::StartupFailed, err.to_string()))?;

    *state = CkbPreparationState::Preparing(PreparedCkbWorker {
        id,
        key,
        start_tx,
        thread: Some(thread),
    });
    drop(state);
    begin_tx.send(()).map_err(|_| {
        ffi_error(
            FiberFfiStatus::StartupFailed,
            "failed to start CKB preparation runtime",
        )
    })?;
    Ok(())
}

#[cfg(feature = "disable-ckb-rpc")]
fn run_embedded_ckb_preparation(
    id: u64,
    config_path: String,
    database_prefix: Option<String>,
    discovered_history_start_block: Option<u64>,
    callback: CkbPrepareCallback,
    start_rx: oneshot::Receiver<PreparedCkbStartCommand>,
) {
    let completion_sent = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let completion_sent_in_runtime = completion_sent.clone();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(err) => {
            clear_ckb_preparation(id);
            emit_ckb_prepare_failure(callback, format!("failed to create tokio runtime: {err}"));
            return;
        }
    };
    let runtime_handle = runtime.handle().clone();
    let result = catch_unwind(AssertUnwindSafe(|| {
        runtime.block_on(async move {
            let report_status = |status| emit_embedded_ckb_progress(callback, status);
            match prepare_local_ckb(
                &config_path,
                database_prefix.clone(),
                discovered_history_start_block,
                &report_status,
            )
            .await
            {
                Ok(local_ckb) => {
                    let resolved_key =
                        match ckb_preparation_key(config_path.clone(), database_prefix.clone()) {
                            Ok(key) => key,
                            Err(err) => {
                                clear_ckb_preparation(id);
                                local_ckb.shutdown().await;
                                completion_sent_in_runtime.store(true, Ordering::Release);
                                emit_ckb_prepare_failure(callback, err.message);
                                return;
                            }
                        };
                    if !mark_ckb_preparation_ready(id, resolved_key.clone()) {
                        local_ckb.shutdown().await;
                        return;
                    }
                    completion_sent_in_runtime.store(true, Ordering::Release);
                    if let Err(err) = spawn_embedded_ckb_ready(callback) {
                        clear_ckb_preparation(id);
                        local_ckb.shutdown().await;
                        emit_ckb_prepare_failure(
                            callback,
                            format!("failed to dispatch CKB preparation callback: {err}"),
                        );
                        return;
                    }

                    match start_rx.await {
                        Ok(command) => {
                            run_fiber_node(
                                runtime_handle,
                                config_path,
                                database_prefix,
                                command.callback,
                                command.startup_tx,
                                command.stop_rx,
                                PreparedCkbStart {
                                    local_ckb: Some(local_ckb),
                                },
                            )
                            .await;
                            clear_ckb_in_use(&resolved_key);
                        }
                        Err(_) => {
                            local_ckb.shutdown().await;
                            clear_ckb_preparation(id);
                        }
                    }
                }
                Err(err) => {
                    clear_ckb_preparation(id);
                    completion_sent_in_runtime.store(true, Ordering::Release);
                    emit_ckb_prepare_failure(callback, err);
                }
            }
        });
    }));

    if let Err(err) = result {
        clear_ckb_preparation(id);
        if !completion_sent.swap(true, Ordering::AcqRel) {
            emit_ckb_prepare_failure(
                callback,
                format!("CKB preparation runtime panicked: {}", panic_message(err)),
            );
        }
    }
}

#[cfg(feature = "disable-ckb-rpc")]
fn mark_ckb_preparation_ready(id: u64, resolved_key: CkbPreparationKey) -> bool {
    let Ok(mut state) = CKB_PREPARATION_STATE.lock() else {
        return false;
    };
    let current = std::mem::replace(&mut *state, CkbPreparationState::Idle);
    match current {
        CkbPreparationState::Preparing(mut worker) if worker.id == id => {
            worker.key = resolved_key;
            *state = CkbPreparationState::Ready(worker);
            true
        }
        other => {
            *state = other;
            false
        }
    }
}

#[cfg(feature = "disable-ckb-rpc")]
fn clear_ckb_preparation(id: u64) {
    let Ok(mut state) = CKB_PREPARATION_STATE.lock() else {
        return;
    };
    let should_clear = matches!(
        &*state,
        CkbPreparationState::Preparing(worker) | CkbPreparationState::Ready(worker)
            if worker.id == id
    );
    if should_clear {
        *state = CkbPreparationState::Idle;
    }
}

#[cfg(feature = "disable-ckb-rpc")]
fn clear_ckb_in_use(key: &CkbPreparationKey) {
    let Ok(mut state) = CKB_PREPARATION_STATE.lock() else {
        return;
    };
    if matches!(&*state, CkbPreparationState::InUse(active_key) if active_key == key) {
        *state = CkbPreparationState::Idle;
    }
}

#[cfg(feature = "disable-ckb-rpc")]
fn take_prepared_ckb_worker(key: CkbPreparationKey) -> FfiCallResult<Option<PreparedCkbWorker>> {
    let mut state = CKB_PREPARATION_STATE
        .lock()
        .map_err(|_| ffi_error(FiberFfiStatus::Panic, "CKB preparation mutex poisoned"))?;
    let current = std::mem::replace(&mut *state, CkbPreparationState::Idle);
    match current {
        CkbPreparationState::Idle => Ok(None),
        CkbPreparationState::Preparing(worker) => {
            *state = CkbPreparationState::Preparing(worker);
            Err(ffi_error(
                FiberFfiStatus::StartupFailed,
                "CKB is still preparing; wait for the preparation callback before starting Fiber",
            ))
        }
        CkbPreparationState::Ready(worker) if worker.key == key => {
            *state = CkbPreparationState::InUse(key);
            Ok(Some(worker))
        }
        CkbPreparationState::Ready(worker) => {
            *state = CkbPreparationState::Ready(worker);
            Err(ffi_error(
                FiberFfiStatus::StartupFailed,
                "CKB was prepared with a different configuration, config_path, database_prefix, or funding key",
            ))
        }
        CkbPreparationState::InUse(active_key) => {
            *state = CkbPreparationState::InUse(active_key);
            Err(ffi_error(
                FiberFfiStatus::StartupFailed,
                "prepared CKB is already in use by Fiber",
            ))
        }
    }
}

#[cfg(feature = "disable-ckb-rpc")]
fn ckb_preparation_key(
    config_path: String,
    database_prefix: Option<String>,
) -> FfiCallResult<CkbPreparationKey> {
    ckb_preparation_key_with_history_floor(config_path, database_prefix, None)
}

#[cfg(feature = "disable-ckb-rpc")]
fn ckb_preparation_key_with_history_floor(
    config_path: String,
    database_prefix: Option<String>,
    history_start_block_floor: Option<u64>,
) -> FfiCallResult<CkbPreparationKey> {
    let config_contents = std::fs::read(&config_path).map_err(|err| {
        ffi_error(
            FiberFfiStatus::InvalidArgument,
            format!("failed to read config file {config_path}: {err}"),
        )
    })?;
    let parsed_config = parse_config_from_path(&config_path, database_prefix.clone())
        .map_err(|err| ffi_error(FiberFfiStatus::InvalidArgument, err))?;
    let ckb_config = parsed_config.fiber.ckb.as_ref().ok_or_else(|| {
        ffi_error(
            FiberFfiStatus::InvalidArgument,
            "service fiber requires service ckb to be enabled",
        )
    })?;
    let secret_key = ckb_config.read_secret_key().map_err(|err| {
        ffi_error(
            FiberFfiStatus::InvalidArgument,
            format!("failed to read the CKB funding key: {err}"),
        )
    })?;
    let funding_pubkey_hash = hex::encode(ckb_hash::blake2b_256(
        secret_key.public_key(secp256k1::SECP256K1).serialize(),
    ));
    let configured_height = parsed_config
        .light_client
        .history_start_block_is_explicit
        .then_some(parsed_config.light_client.history_start_block);
    let persisted_height =
        read_wallet_birthday_metadata(&parsed_config.light_client.wallet_birthday_path)
            .map_err(|err| ffi_error(FiberFfiStatus::InvalidArgument, err))?
            .map(|metadata| metadata.history_start_block);
    let legacy_height = read_legacy_history_start_block(
        &parsed_config.light_client.legacy_history_start_block_path,
    )
    .map_err(|err| ffi_error(FiberFfiStatus::InvalidArgument, err))?;
    let history_start_block = select_earliest_history_start_block(
        configured_height,
        persisted_height,
        history_start_block_floor,
        legacy_height,
    );

    Ok(CkbPreparationKey {
        config_path,
        database_prefix,
        config_contents_hash: hex::encode(ckb_hash::blake2b_256(&config_contents)),
        funding_pubkey_hash,
        history_start_block,
    })
}

#[cfg(feature = "disable-ckb-rpc")]
fn emit_embedded_ckb_ready(callback: CkbPrepareCallback) {
    emit_ckb_prepare_completion(
        callback,
        FiberFfiStatus::Ok,
        json!({
            "ready": true,
            "mode": "light_client",
            "skipped": false,
            "status": "ready",
        }),
    );
}

#[cfg(feature = "disable-ckb-rpc")]
fn spawn_embedded_ckb_ready(callback: CkbPrepareCallback) -> std::result::Result<(), String> {
    thread::Builder::new()
        .name("fiber-ffi-prepare-ckb-callback".to_string())
        .spawn(move || emit_embedded_ckb_ready(callback))
        .map(|_| ())
        .map_err(|err| err.to_string())
}

#[cfg(feature = "disable-ckb-rpc")]
fn emit_ckb_prepare_failure(callback: CkbPrepareCallback, error: impl Into<String>) {
    let error = sanitize_error_message(error.into());
    emit_ckb_prepare_completion(
        callback,
        FiberFfiStatus::StartupFailed,
        json!({
            "ready": false,
            "mode": "light_client",
            "status": "failed",
            "error": error,
        }),
    );
}

#[cfg(feature = "disable-ckb-rpc")]
fn emit_embedded_ckb_progress(
    callback: CkbPrepareCallback,
    status: ckb_light_client::CkbPrepareStatus,
) {
    let result = embedded_ckb_progress_result(status);
    emit_ckb_prepare_completion(callback, FiberFfiStatus::Ok, result);
}

#[cfg(feature = "disable-ckb-rpc")]
fn embedded_ckb_progress_result(status: ckb_light_client::CkbPrepareStatus) -> serde_json::Value {
    use ckb_light_client::CkbPrepareStatus;

    match status {
        CkbPrepareStatus::Initializing => json!({
            "ready": false,
            "mode": "light_client",
            "status": "initializing",
        }),
        CkbPrepareStatus::WalletBirthday {
            address,
            history_start_block,
            source,
        } => json!({
            "ready": false,
            "mode": "light_client",
            "status": "wallet_birthday",
            "address": address,
            "history_start_block": history_start_block,
            "source": source,
        }),
        CkbPrepareStatus::Connecting {
            connected_peers,
            required_peers,
            tip_block_number,
            tip_is_current,
        } => json!({
            "ready": false,
            "mode": "light_client",
            "status": "connecting",
            "connected_peers": connected_peers,
            "required_peers": required_peers,
            "tip_block_number": tip_block_number,
            "tip_is_current": tip_is_current,
        }),
        CkbPrepareStatus::SyncingHeaders {
            connected_peers,
            required_peers,
            tip_block_number,
            tip_is_current,
        } => json!({
            "ready": false,
            "mode": "light_client",
            "status": "syncing_headers",
            "connected_peers": connected_peers,
            "required_peers": required_peers,
            "tip_block_number": tip_block_number,
            "tip_is_current": tip_is_current,
        }),
        CkbPrepareStatus::SyncingScripts {
            tip_block_number,
            slowest_script_block_number,
            script_count,
        } => json!({
            "ready": false,
            "mode": "light_client",
            "status": "syncing_scripts",
            "tip_block_number": tip_block_number,
            "slowest_script_block_number": slowest_script_block_number,
            "script_count": script_count,
        }),
    }
}

fn emit_ckb_prepare_completion(
    callback: CkbPrepareCallback,
    status: FiberFfiStatus,
    result: serde_json::Value,
) {
    let result = serde_json::to_string(&result).unwrap_or_else(|_| {
        "{\"ready\":false,\"status\":\"failed\",\"error\":\"serialization failed\"}".to_string()
    });
    let result = CString::new(result)
        .expect("CKB preparation completion JSON cannot contain an interior NUL");
    unsafe {
        (callback.callback)(status, result.as_ptr(), callback.user_data as *mut c_void);
    }
}

/// Stops a Fiber node and releases its handle.
///
/// # Safety
///
/// `handle` must be a live pointer returned by `fiber_start`. Prevent concurrent
/// use of the handle, and do not use it after this call.
#[no_mangle]
pub unsafe extern "C" fn fiber_stop(handle: *mut FiberHandle) -> FiberFfiStatus {
    ffi_boundary(|| {
        if handle.is_null() {
            return Err(ffi_error(
                FiberFfiStatus::NullPointer,
                "handle must be non-null",
            ));
        }

        let handle = Box::from_raw(handle);
        let stop_tx = handle
            .stop_tx
            .lock()
            .map_err(|_| ffi_error(FiberFfiStatus::Panic, "stop mutex poisoned"))?
            .take();
        let thread = handle
            .thread
            .lock()
            .map_err(|_| ffi_error(FiberFfiStatus::Panic, "thread mutex poisoned"))?
            .take();

        let Some(stop_tx) = stop_tx else {
            return Err(ffi_error(
                FiberFfiStatus::AlreadyStopped,
                "fiber node is already stopped",
            ));
        };
        let _ = stop_tx.send(());

        if let Some(thread) = thread {
            thread
                .join()
                .map_err(|_| ffi_error(FiberFfiStatus::Panic, "runtime thread panicked"))?;
        }

        Ok(FiberFfiStatus::Ok)
    })
}

/// Returns information about the running Fiber node as JSON.
///
/// # Safety
///
/// `handle` must be live and `out_json` must be writable. Free the returned
/// string with `fiber_string_free`.
#[no_mangle]
pub unsafe extern "C" fn fiber_node_info(
    handle: *mut FiberHandle,
    out_json: *mut *mut c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        prepare_out_string(out_json)?;

        let response = handle
            .runtime_handle
            .block_on(call_node_info(handle.network_actor.clone()))
            .map_err(|err| ffi_error(FiberFfiStatus::InvalidArgument, err))?;
        write_json_out(out_json, node_info_to_json(response))?;

        Ok(FiberFfiStatus::Ok)
    })
}

/// Returns the current CKB synchronization readiness as JSON.
///
/// # Safety
///
/// `handle` must be live and `out_json` must be writable. Free the returned
/// string with `fiber_string_free`.
#[no_mangle]
pub unsafe extern "C" fn fiber_ckb_readiness(
    handle: *mut FiberHandle,
    out_json: *mut *mut c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        prepare_out_string(out_json)?;

        let readiness = current_ckb_readiness(handle);
        write_serializable_out(out_json, &readiness)?;

        Ok(FiberFfiStatus::Ok)
    })
}

/// Returns the indexed base-CKB capacity controlled by the configured funding
/// key. The result is a Light Client/indexer snapshot and can include cells
/// that an in-flight transaction has already reserved.
///
/// # Safety
///
/// `handle` must be live and `out_json` must be writable. Free the returned
/// string with `fiber_string_free`.
#[no_mangle]
pub unsafe extern "C" fn fiber_ckb_balance(
    handle: *mut FiberHandle,
    out_json: *mut *mut c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        prepare_out_string(out_json)?;

        let readiness = current_ckb_readiness(handle);
        let balance = handle
            .runtime_handle
            .block_on(query_ckb_balance(
                &handle.ckb_config,
                &handle.fiber_config.chain,
                readiness,
            ))
            .map_err(|err| ffi_error(FiberFfiStatus::InvalidArgument, err))?;
        write_serializable_out(out_json, &balance)?;

        Ok(FiberFfiStatus::Ok)
    })
}

/// Returns the connected peers as JSON.
///
/// # Safety
///
/// `handle` must be live and `out_json` must be writable. Free the returned
/// string with `fiber_string_free`.
#[no_mangle]
pub unsafe extern "C" fn fiber_list_peers(
    handle: *mut FiberHandle,
    out_json: *mut *mut c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        prepare_out_string(out_json)?;

        let response = handle
            .runtime_handle
            .block_on(call_list_peers(handle.network_actor.clone()))
            .map_err(|err| ffi_error(FiberFfiStatus::InvalidArgument, err))?;
        write_json_out(out_json, peers_to_json(response))?;

        Ok(FiberFfiStatus::Ok)
    })
}

/// Connects the running node to a peer.
///
/// # Safety
///
/// `handle` must be live. `options` and every non-null string it references
/// must remain valid for this call.
#[no_mangle]
pub unsafe extern "C" fn fiber_connect_peer(
    handle: *mut FiberHandle,
    options: *const FiberConnectPeerOptions,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        if options.is_null() {
            return Err(ffi_error(
                FiberFfiStatus::NullPointer,
                "options must be non-null",
            ));
        }

        let options = &*options;
        let address = optional_string(options.address)?;
        let pubkey = optional_string(options.pubkey)?;
        let addr_type = optional_string(options.addr_type)?;
        if address.as_deref().is_some_and(str::is_empty) {
            return Err(ffi_error(
                FiberFfiStatus::InvalidArgument,
                "address must not be empty",
            ));
        }
        if pubkey.as_deref().is_some_and(str::is_empty) {
            return Err(ffi_error(
                FiberFfiStatus::InvalidArgument,
                "pubkey must not be empty",
            ));
        }
        if address.is_some() == pubkey.is_some() {
            return Err(ffi_error(
                FiberFfiStatus::InvalidArgument,
                "exactly one of address or pubkey must be set",
            ));
        }

        let command = if let Some(address) = address {
            let address = address
                .parse::<fnn::fiber_types::Multiaddr>()
                .map_err(|err| ffi_error(FiberFfiStatus::InvalidArgument, err.to_string()))?;
            ConnectPeerCommand::Address {
                address,
                save: options.save != 0,
            }
        } else {
            ConnectPeerCommand::Pubkey {
                pubkey: parse_pubkey(pubkey.as_deref().expect("checked above"))?,
                addr_type: parse_addr_type(addr_type.as_deref())?,
            }
        };

        handle
            .runtime_handle
            .block_on(call_connect_peer(handle.network_actor.clone(), command))
            .map_err(|err| ffi_error(FiberFfiStatus::InvalidArgument, err))?;

        Ok(FiberFfiStatus::Ok)
    })
}

/// Disconnects a peer by public key.
///
/// # Safety
///
/// `handle` must be live. `pubkey` must be a valid, NUL-terminated C string
/// for the duration of this call.
#[no_mangle]
pub unsafe extern "C" fn fiber_disconnect_peer(
    handle: *mut FiberHandle,
    pubkey: *const c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        let pubkey = parse_pubkey(&required_string(pubkey, "pubkey")?)?;

        handle
            .runtime_handle
            .block_on(call_disconnect_peer(handle.network_actor.clone(), pubkey))
            .map_err(|err| ffi_error(FiberFfiStatus::InvalidArgument, err))?;

        Ok(FiberFfiStatus::Ok)
    })
}

/// Opens a channel and returns its temporary channel ID.
///
/// # Safety
///
/// `handle` must be live. `options` and its strings must remain valid for this
/// call. The output must be writable; free its string with `fiber_string_free`.
#[no_mangle]
pub unsafe extern "C" fn fiber_open_channel(
    handle: *mut FiberHandle,
    options: *const FiberOpenChannelOptions,
    out_temporary_channel_id: *mut *mut c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        let options = checked_options(options)?;
        prepare_out_string(out_temporary_channel_id)?;

        let params = open_channel_params_from_options(options)?;
        ensure_ckb_ready(handle)?;
        let response = handle
            .runtime_handle
            .block_on(call_open_channel(handle, params))?;
        write_serializable_field_out(out_temporary_channel_id, &response, "temporary_channel_id")?;
        Ok(FiberFfiStatus::Ok)
    })
}

/// Accepts a pending channel and returns its channel ID.
///
/// # Safety
///
/// `handle` must be live. `options` and its strings must remain valid for this
/// call. The output must be writable; free its string with `fiber_string_free`.
#[no_mangle]
pub unsafe extern "C" fn fiber_accept_channel(
    handle: *mut FiberHandle,
    options: *const FiberAcceptChannelOptions,
    out_channel_id: *mut *mut c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        let options = checked_options(options)?;
        prepare_out_string(out_channel_id)?;

        let params = accept_channel_params_from_options(options)?;
        let response = handle
            .runtime_handle
            .block_on(call_accept_channel(handle, params))?;
        write_serializable_field_out(out_channel_id, &response, "channel_id")?;
        Ok(FiberFfiStatus::Ok)
    })
}

/// Opens a channel whose funding transaction is signed externally.
///
/// # Safety
///
/// `handle` must be live. `options` and its strings must remain valid for this
/// call. `out_json` must be writable; free it with `fiber_string_free`.
#[no_mangle]
pub unsafe extern "C" fn fiber_open_channel_with_external_funding(
    handle: *mut FiberHandle,
    options: *const FiberOpenChannelWithExternalFundingOptions,
    out_json: *mut *mut c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        let options = checked_options(options)?;
        prepare_out_string(out_json)?;

        let params = open_channel_with_external_funding_params_from_options(options)?;
        let response = handle
            .runtime_handle
            .block_on(call_open_channel_with_external_funding(handle, params))?;
        write_serializable_out(out_json, &response)?;
        Ok(FiberFfiStatus::Ok)
    })
}

/// Submits an externally signed funding transaction.
///
/// # Safety
///
/// `handle` must be live. `options` and its strings must remain valid for this
/// call. `out_json` must be writable; free it with `fiber_string_free`.
#[no_mangle]
pub unsafe extern "C" fn fiber_submit_signed_funding_tx(
    handle: *mut FiberHandle,
    options: *const FiberSubmitSignedFundingTxOptions,
    out_json: *mut *mut c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        let options = checked_options(options)?;
        prepare_out_string(out_json)?;

        let params = submit_signed_funding_tx_params_from_options(options)?;
        let response = handle
            .runtime_handle
            .block_on(call_submit_signed_funding_tx(handle, params))?;
        write_serializable_out(out_json, &response)?;
        Ok(FiberFfiStatus::Ok)
    })
}

/// Abandons a channel.
///
/// # Safety
///
/// `handle` must be live. `channel_id` must be a valid, NUL-terminated C
/// string for the duration of this call.
#[no_mangle]
pub unsafe extern "C" fn fiber_abandon_channel(
    handle: *mut FiberHandle,
    channel_id: *const c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        let params = required_hash_param("channel_id", channel_id)?;
        let params = deserialize_value::<fnn::rpc::channel::AbandonChannelParams>(params)?;
        handle
            .runtime_handle
            .block_on(call_abandon_channel(handle, params))?;
        Ok(FiberFfiStatus::Ok)
    })
}

/// Returns channels matching the supplied filters as JSON.
///
/// # Safety
///
/// `handle` must be live. `options` and its strings must remain valid for this
/// call. `out_json` must be writable; free it with `fiber_string_free`.
#[no_mangle]
pub unsafe extern "C" fn fiber_list_channels(
    handle: *mut FiberHandle,
    options: *const FiberListChannelsOptions,
    out_json: *mut *mut c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        let options = checked_options(options)?;
        prepare_out_string(out_json)?;

        let params = list_channels_params_from_options(options)?;
        let response = handle
            .runtime_handle
            .block_on(call_list_channels(handle, params))?;
        write_serializable_out(out_json, &response)?;
        Ok(FiberFfiStatus::Ok)
    })
}

/// Shuts down a channel.
///
/// # Safety
///
/// `handle` must be live. `options` and its strings must remain valid for
/// the duration of this call.
#[no_mangle]
pub unsafe extern "C" fn fiber_shutdown_channel(
    handle: *mut FiberHandle,
    options: *const FiberShutdownChannelOptions,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        let options = checked_options(options)?;
        let params = shutdown_channel_params_from_options(options)?;
        handle
            .runtime_handle
            .block_on(call_shutdown_channel(handle, params))?;
        Ok(FiberFfiStatus::Ok)
    })
}

/// Updates channel settings.
///
/// # Safety
///
/// `handle` must be live. `options` and its strings must remain valid for
/// the duration of this call.
#[no_mangle]
pub unsafe extern "C" fn fiber_update_channel(
    handle: *mut FiberHandle,
    options: *const FiberUpdateChannelOptions,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        let options = checked_options(options)?;
        let params = update_channel_params_from_options(options)?;
        handle
            .runtime_handle
            .block_on(call_update_channel(handle, params))?;
        Ok(FiberFfiStatus::Ok)
    })
}

/// Sends a payment and returns its state as JSON.
///
/// # Safety
///
/// `handle` must be live. `options` and its strings must remain valid for this
/// call. `out_json` must be writable; free it with `fiber_string_free`.
#[no_mangle]
pub unsafe extern "C" fn fiber_send_payment(
    handle: *mut FiberHandle,
    options: *const FiberSendPaymentOptions,
    out_json: *mut *mut c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        let options = checked_options(options)?;
        prepare_out_string(out_json)?;

        let params = send_payment_params_from_options(options)?;
        let response = handle
            .runtime_handle
            .block_on(call_send_payment(handle, params))?;
        write_serializable_out(out_json, &response)?;
        Ok(FiberFfiStatus::Ok)
    })
}

/// Builds a payment route and returns it as JSON.
///
/// # Safety
///
/// `handle` must be live. `options` and its strings must remain valid for this
/// call. `out_json` must be writable; free it with `fiber_string_free`.
#[no_mangle]
pub unsafe extern "C" fn fiber_build_router(
    handle: *mut FiberHandle,
    options: *const FiberBuildRouterOptions,
    out_json: *mut *mut c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        let options = checked_options(options)?;
        prepare_out_string(out_json)?;

        let params = build_router_params_from_options(options)?;
        let response = handle
            .runtime_handle
            .block_on(call_build_router(handle, params))?;
        write_serializable_out(out_json, &response)?;
        Ok(FiberFfiStatus::Ok)
    })
}

/// Sends a payment through a caller-provided route.
///
/// # Safety
///
/// `handle` must be live. `options` and its strings must remain valid for this
/// call. `out_json` must be writable; free it with `fiber_string_free`.
#[no_mangle]
pub unsafe extern "C" fn fiber_send_payment_with_router(
    handle: *mut FiberHandle,
    options: *const FiberSendPaymentWithRouterOptions,
    out_json: *mut *mut c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        let options = checked_options(options)?;
        prepare_out_string(out_json)?;

        let params = send_payment_with_router_params_from_options(options)?;
        let response = handle
            .runtime_handle
            .block_on(call_send_payment_with_router(handle, params))?;
        write_serializable_out(out_json, &response)?;
        Ok(FiberFfiStatus::Ok)
    })
}

/// Returns a payment by hash as JSON.
///
/// # Safety
///
/// `handle` must be live and `payment_hash` must be a valid C string.
/// `out_json` must be writable; free it with `fiber_string_free`.
#[no_mangle]
pub unsafe extern "C" fn fiber_get_payment(
    handle: *mut FiberHandle,
    payment_hash: *const c_char,
    out_json: *mut *mut c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        prepare_out_string(out_json)?;

        let params = required_hash_param("payment_hash", payment_hash)?;
        let params = deserialize_value::<fnn::rpc::payment::GetPaymentCommandParams>(params)?;
        let response = handle
            .runtime_handle
            .block_on(call_get_payment(handle, params))?;
        write_serializable_out(out_json, &response)?;
        Ok(FiberFfiStatus::Ok)
    })
}

/// Returns payments matching the supplied filters as JSON.
///
/// # Safety
///
/// `handle` must be live. `options` and its strings must remain valid for this
/// call. `out_json` must be writable; free it with `fiber_string_free`.
#[no_mangle]
pub unsafe extern "C" fn fiber_list_payments(
    handle: *mut FiberHandle,
    options: *const FiberListPaymentsOptions,
    out_json: *mut *mut c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        let options = checked_options(options)?;
        prepare_out_string(out_json)?;

        let params = list_payments_params_from_options(options)?;
        let response = handle
            .runtime_handle
            .block_on(call_list_payments(handle, params))?;
        write_serializable_out(out_json, &response)?;
        Ok(FiberFfiStatus::Ok)
    })
}

/// Creates an invoice and returns its encoded address.
///
/// # Safety
///
/// `handle` must be live. `options` and its strings must remain valid for this
/// call. The output must be writable; free it with `fiber_string_free`.
#[no_mangle]
pub unsafe extern "C" fn fiber_new_invoice(
    handle: *mut FiberHandle,
    options: *const FiberNewInvoiceOptions,
    out_invoice_address: *mut *mut c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        let options = checked_options(options)?;
        prepare_out_string(out_invoice_address)?;

        let params = new_invoice_params_from_options(options)?;
        let response = handle
            .runtime_handle
            .block_on(call_new_invoice(handle, params))?;
        write_serializable_field_out(out_invoice_address, &response, "invoice_address")?;
        Ok(FiberFfiStatus::Ok)
    })
}

/// Parses an encoded invoice and returns it as JSON.
///
/// # Safety
///
/// `handle` must be live and `invoice` must be a valid C string. `out_json`
/// must be writable; free it with `fiber_string_free`.
#[no_mangle]
pub unsafe extern "C" fn fiber_parse_invoice(
    handle: *mut FiberHandle,
    invoice: *const c_char,
    out_json: *mut *mut c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        prepare_out_string(out_json)?;

        let mut value = serde_json::Map::new();
        value.insert("invoice".to_string(), string_field("invoice", invoice)?);
        let params = deserialize_object::<fnn::rpc::invoice::ParseInvoiceParams>(value)?;
        let response = handle
            .runtime_handle
            .block_on(call_parse_invoice(handle, params))?;
        write_serializable_out(out_json, &response)?;
        Ok(FiberFfiStatus::Ok)
    })
}

/// Returns an invoice by payment hash as JSON.
///
/// # Safety
///
/// `handle` must be live and `payment_hash` must be a valid C string.
/// `out_json` must be writable; free it with `fiber_string_free`.
#[no_mangle]
pub unsafe extern "C" fn fiber_get_invoice(
    handle: *mut FiberHandle,
    payment_hash: *const c_char,
    out_json: *mut *mut c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        prepare_out_string(out_json)?;

        let params = required_hash_param("payment_hash", payment_hash)?;
        let params = deserialize_value::<fnn::rpc::invoice::InvoiceParams>(params)?;
        let response = handle
            .runtime_handle
            .block_on(call_get_invoice(handle, params))?;
        write_serializable_out(out_json, &response)?;
        Ok(FiberFfiStatus::Ok)
    })
}

/// Cancels an invoice and returns its updated state as JSON.
///
/// # Safety
///
/// `handle` must be live and `payment_hash` must be a valid C string.
/// `out_json` must be writable; free it with `fiber_string_free`.
#[no_mangle]
pub unsafe extern "C" fn fiber_cancel_invoice(
    handle: *mut FiberHandle,
    payment_hash: *const c_char,
    out_json: *mut *mut c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        prepare_out_string(out_json)?;

        let params = required_hash_param("payment_hash", payment_hash)?;
        let params = deserialize_value::<fnn::rpc::invoice::InvoiceParams>(params)?;
        let response = handle
            .runtime_handle
            .block_on(call_cancel_invoice(handle, params))?;
        write_serializable_out(out_json, &response)?;
        Ok(FiberFfiStatus::Ok)
    })
}

/// Settles an invoice.
///
/// # Safety
///
/// `handle` must be live. `payment_hash` and `payment_preimage` must be
/// valid, NUL-terminated C strings for the duration of this call.
#[no_mangle]
pub unsafe extern "C" fn fiber_settle_invoice(
    handle: *mut FiberHandle,
    payment_hash: *const c_char,
    payment_preimage: *const c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;

        let mut value = serde_json::Map::new();
        value.insert(
            "payment_hash".to_string(),
            string_field("payment_hash", payment_hash)?,
        );
        value.insert(
            "payment_preimage".to_string(),
            string_field("payment_preimage", payment_preimage)?,
        );
        let params = deserialize_object::<fnn::rpc::invoice::SettleInvoiceParams>(value)?;
        handle
            .runtime_handle
            .block_on(call_settle_invoice(handle, params))?;
        Ok(FiberFfiStatus::Ok)
    })
}

/// Releases a C string returned by this library.
///
/// # Safety
///
/// `string` must be null or returned by a Fiber FFI function. A non-null
/// pointer must be released exactly once and not used afterward.
#[no_mangle]
pub unsafe extern "C" fn fiber_string_free(string: *mut c_char) {
    if !string.is_null() {
        let _ = CString::from_raw(string);
    }
}

/// Copies the calling thread's last FFI error message into `buffer`.
///
/// # Safety
///
/// When non-null and `buffer_len` is non-zero, `buffer` must be valid for
/// writes of `buffer_len` bytes. A null buffer queries the required byte count.
#[no_mangle]
pub unsafe extern "C" fn fiber_last_error_message(buffer: *mut c_char, buffer_len: usize) -> usize {
    LAST_ERROR.with(|last_error| {
        let last_error = last_error.borrow();
        let Some(message) = last_error.as_deref() else {
            if !buffer.is_null() && buffer_len > 0 {
                *buffer = 0;
            }
            return 0;
        };

        let bytes = message.as_bytes();
        if buffer.is_null() || buffer_len == 0 {
            return bytes.len();
        }

        let copy_len = bytes.len().min(buffer_len.saturating_sub(1));
        ptr::copy_nonoverlapping(bytes.as_ptr(), buffer.cast::<u8>(), copy_len);
        *buffer.add(copy_len) = 0;
        bytes.len()
    })
}

struct RunningNode {
    root_actor: ActorRef<String>,
    root_token: CancellationToken,
    root_tracker: TaskTracker,
    network_actor: ActorRef<NetworkActorMessage>,
    store: fnn::store::Store,
    fiber_config: FiberConfig,
    ckb_config: CkbConfig,
    #[cfg(feature = "disable-ckb-rpc")]
    local_ckb: Option<ckb_light_client::LocalCkbNodeHandle>,
}

struct ParsedFfiConfig {
    fiber: Config,
    #[cfg(feature = "disable-ckb-rpc")]
    light_client: ckb_light_client::config::LocalLightClientConfig,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
enum FfiService {
    #[serde(alias = "fiber", alias = "FIBER")]
    Fiber,
    #[serde(alias = "ckb", alias = "CKB")]
    CkbChain,
    #[serde(alias = "rpc", alias = "RPC")]
    Rpc,
    #[serde(alias = "cch", alias = "CCH")]
    Cch,
}

#[derive(Deserialize)]
struct FfiSerializedConfig {
    services: Option<Vec<FfiService>>,
    fiber: Option<<FiberConfig as ClapSerde>::Opt>,
    ckb: Option<<CkbConfig as ClapSerde>::Opt>,
    #[cfg(feature = "disable-ckb-rpc")]
    ckb_light_client: Option<ckb_light_client::config::SerializedLightClientConfig>,
}

fn parse_config_from_path(
    config_path: &str,
    database_prefix: Option<String>,
) -> std::result::Result<ParsedFfiConfig, String> {
    let config_path = PathBuf::from(config_path);
    let base_dir = database_prefix
        .map(PathBuf::from)
        .or_else(|| config_path.parent().map(Path::to_path_buf))
        .unwrap_or_else(|| PathBuf::from("."));

    let file = File::open(&config_path).map_err(|err| {
        format!(
            "failed to read config file {}: {err}",
            config_path.display()
        )
    })?;
    let reader = BufReader::new(file);
    let mut value = serde_yaml::from_reader::<_, serde_yaml::Value>(reader).map_err(|err| {
        format!(
            "failed to parse config file {}: {err}",
            config_path.display()
        )
    })?;
    set_base_dir(&mut value, "fiber", &base_dir.join("fiber"));
    set_base_dir(
        &mut value,
        "ckb",
        &base_dir.join(fnn::ckb::DEFAULT_CKB_BASE_DIR_NAME),
    );
    let config_from_file = serde_yaml::from_value::<FfiSerializedConfig>(value).map_err(|err| {
        format!(
            "failed to parse config file {}: {err}",
            config_path.display()
        )
    })?;

    let services = config_from_file.services.unwrap_or_default();
    if services.is_empty() {
        return Err("must run at least one service in config services".to_string());
    }

    let fiber_config = config_from_file
        .fiber
        .map(FiberConfig::from)
        .ok_or_else(|| "fiber config must be set".to_string())?;
    #[cfg(feature = "disable-ckb-rpc")]
    let fiber_chain = fiber_config.chain.clone();
    let (fiber, disabled_fiber) = if services.contains(&FfiService::Fiber) {
        (Some(fiber_config), None)
    } else {
        (None, Some(fiber_config))
    };
    let ckb = if services.contains(&FfiService::CkbChain) {
        Some(CkbConfig::from(config_from_file.ckb.unwrap_or_default()))
    } else {
        None
    };

    let fiber = Config {
        fiber,
        disabled_fiber,
        cch: None,
        rpc: None,
        ckb,
        base_dir,
        check_validate: false,
        restore: None,
    };

    #[cfg(feature = "disable-ckb-rpc")]
    let light_client = ckb_light_client::config::LocalLightClientConfig::build(
        fiber.base_dir.clone(),
        &fiber_chain,
        config_from_file.ckb_light_client.unwrap_or_default(),
    )?;

    Ok(ParsedFfiConfig {
        fiber,
        #[cfg(feature = "disable-ckb-rpc")]
        light_client,
    })
}

fn parse_config_with_genesis(
    config_path: &str,
    database_prefix: Option<String>,
) -> std::result::Result<(ParsedFfiConfig, ckb_types::core::BlockView), String> {
    let parsed_config = parse_config_from_path(config_path, database_prefix)?;
    let fiber_config = parsed_config
        .fiber
        .fiber
        .as_ref()
        .ok_or_else(|| "fiber service must be enabled in config services".to_string())?;
    parsed_config
        .fiber
        .ckb
        .as_ref()
        .ok_or_else(|| "service fiber requires service ckb to be enabled".to_string())?;
    let genesis_block = load_chain_genesis_block(
        &fiber_config.chain,
        Path::new(&parsed_config.fiber.base_dir),
    )?;
    Ok((parsed_config, genesis_block))
}

fn set_base_dir(value: &mut serde_yaml::Value, section: &str, base_dir: &Path) {
    let Some(root) = value.as_mapping_mut() else {
        return;
    };
    let Some(section) = root.get_mut(serde_yaml::Value::String(section.to_string())) else {
        return;
    };
    let Some(section) = section.as_mapping_mut() else {
        return;
    };
    section.insert(
        serde_yaml::Value::String("base_dir".to_string()),
        serde_yaml::Value::String(base_dir.display().to_string()),
    );
}

#[cfg(feature = "disable-ckb-rpc")]
fn resolve_wallet_birthday(
    parsed_config: &mut ParsedFfiConfig,
    genesis_block: &ckb_types::core::BlockView,
    discovered_history_start_block: Option<u64>,
    status_reporter: ckb_light_client::CkbPrepareStatusReporter<'_>,
) -> std::result::Result<(), String> {
    use ckb_types::prelude::Unpack;

    let fiber_config = parsed_config
        .fiber
        .fiber
        .as_ref()
        .ok_or_else(|| "fiber service must be enabled in config services".to_string())?;
    let ckb_config = parsed_config
        .fiber
        .ckb
        .as_ref()
        .ok_or_else(|| "service fiber requires service ckb to be enabled".to_string())?;
    let funding_lock = funding_lock_from_genesis(ckb_config, genesis_block)?;
    let address_payload = CkbAddressPayload::from(funding_lock.clone());
    let lock_args = format!("0x{}", hex::encode(address_payload.args()));
    let address = CkbAddress::new(
        ckb_address_network(&fiber_config.chain),
        address_payload,
        true,
    )
    .to_string();
    let expected_genesis_hash: ckb_types::H256 = genesis_block.hash().unpack();
    let genesis_hash = format!("{expected_genesis_hash:#x}");

    let birthday_path = parsed_config.light_client.wallet_birthday_path.clone();
    let legacy_start = read_legacy_history_start_block(
        &parsed_config.light_client.legacy_history_start_block_path,
    )?;

    let persisted = read_wallet_birthday_metadata(&birthday_path)?;
    if let Some(metadata) = persisted.as_ref() {
        validate_wallet_birthday_metadata(
            metadata,
            &fiber_config.chain,
            &genesis_hash,
            &address,
            &lock_args,
            &birthday_path,
        )?;
    }

    let persisted_height = persisted
        .as_ref()
        .map(|metadata| metadata.history_start_block);
    let configured_height = parsed_config
        .light_client
        .history_start_block_is_explicit
        .then_some(parsed_config.light_client.history_start_block);
    let has_history_start_block = [
        configured_height,
        persisted_height,
        discovered_history_start_block,
        legacy_start,
    ]
    .into_iter()
    .any(|height| height.is_some());
    if !has_history_start_block {
        parsed_config.light_client.history_start_block = 0;
        status_reporter(ckb_light_client::CkbPrepareStatus::WalletBirthday {
            address,
            history_start_block: 0,
            source: "default_genesis".to_string(),
        });
        return Ok(());
    }
    let history_start_block = select_earliest_history_start_block(
        configured_height,
        persisted_height,
        discovered_history_start_block,
        legacy_start,
    );

    let source = if persisted_height == Some(history_start_block) {
        "persisted"
    } else if legacy_start == Some(history_start_block)
        && persisted_height != Some(history_start_block)
        && discovered_history_start_block != Some(history_start_block)
    {
        "legacy_floor"
    } else if discovered_history_start_block == Some(history_start_block)
        && persisted_height != Some(history_start_block)
    {
        "external_discovery"
    } else if configured_height == Some(history_start_block) {
        "configured"
    } else {
        "earliest_safe_height"
    };
    if persisted_height != Some(history_start_block) {
        let metadata = WalletBirthdayMetadata {
            version: CKB_WALLET_BIRTHDAY_VERSION,
            network: fiber_config.chain.clone(),
            genesis_hash,
            address: address.clone(),
            lock_args,
            history_start_block,
            source: source.to_string(),
        };
        write_wallet_birthday_metadata(&birthday_path, &metadata)?;
    }
    parsed_config.light_client.history_start_block = history_start_block;
    status_reporter(ckb_light_client::CkbPrepareStatus::WalletBirthday {
        address,
        history_start_block,
        source: source.to_string(),
    });
    Ok(())
}

#[cfg(feature = "disable-ckb-rpc")]
async fn prepare_local_ckb(
    config_path: &str,
    database_prefix: Option<String>,
    discovered_history_start_block: Option<u64>,
    status_reporter: ckb_light_client::CkbPrepareStatusReporter<'_>,
) -> std::result::Result<ckb_light_client::LocalCkbNodeHandle, String> {
    status_reporter(ckb_light_client::CkbPrepareStatus::Initializing);
    let (mut parsed_config, genesis_block) =
        parse_config_with_genesis(config_path, database_prefix)?;
    resolve_wallet_birthday(
        &mut parsed_config,
        &genesis_block,
        discovered_history_start_block,
        status_reporter,
    )?;
    parsed_config.light_client.log_summary();

    let light_client_config = parsed_config.light_client;
    let config = parsed_config.fiber;
    let fiber_config = config
        .fiber
        .as_ref()
        .ok_or_else(|| "fiber service must be enabled in config services".to_string())?;
    let ckb_config = config
        .ckb
        .as_ref()
        .ok_or_else(|| "service fiber requires service ckb to be enabled".to_string())?;
    start_embedded_ckb(
        light_client_config,
        fiber_config,
        ckb_config,
        &genesis_block,
        Some(status_reporter),
    )
    .await
}

#[cfg(feature = "disable-ckb-rpc")]
async fn start_embedded_ckb(
    config: ckb_light_client::config::LocalLightClientConfig,
    fiber_config: &FiberConfig,
    ckb_config: &CkbConfig,
    genesis_block: &ckb_types::core::BlockView,
    status_reporter: Option<ckb_light_client::CkbPrepareStatusReporter<'_>>,
) -> std::result::Result<ckb_light_client::LocalCkbNodeHandle, String> {
    let (required_scripts, pinned_cell_deps) = required_light_client_dependencies(
        fiber_config,
        ckb_config,
        genesis_block,
        config.history_start_block,
        config.trust_pinned_cell_deps,
    )?;
    ckb_light_client::LocalCkbNodeHandle::start(
        config,
        required_scripts,
        pinned_cell_deps,
        status_reporter,
    )
    .await
}

async fn start_node(
    config_path: String,
    database_prefix: Option<String>,
    callback: Option<EventCallback>,
    prepared_ckb: PreparedCkbStart,
) -> std::result::Result<RunningNode, String> {
    info!(
        "Starting node with git version {} ({})",
        fnn::get_git_version(),
        fnn::get_git_commit_info()
    );

    let (parsed_config, genesis_block) = parse_config_with_genesis(&config_path, database_prefix)?;
    #[cfg(feature = "disable-ckb-rpc")]
    let mut parsed_config = parsed_config;
    #[cfg(feature = "disable-ckb-rpc")]
    resolve_wallet_birthday(&mut parsed_config, &genesis_block, None, &|_| {})?;
    #[cfg(feature = "disable-ckb-rpc")]
    parsed_config.light_client.log_summary();
    #[cfg(feature = "disable-ckb-rpc")]
    let light_client_config = parsed_config.light_client.clone();
    let config = parsed_config.fiber;
    let fiber_config = config
        .fiber
        .clone()
        .ok_or_else(|| "fiber service must be enabled in config services".to_string())?;
    let ckb_config = config
        .ckb
        .clone()
        .ok_or_else(|| "service fiber requires service ckb to be enabled".to_string())?;

    #[cfg(feature = "disable-ckb-rpc")]
    let (ckb_config, local_ckb) = if let Some(local_ckb) = prepared_ckb.local_ckb {
        let mut ckb_config = ckb_config;
        ckb_config.rpc_url = local_ckb.rpc_url().to_string();
        info!(
            rpc_url = %ckb_config.rpc_url,
            "reusing prepared embedded CKB Light Client gateway"
        );
        (ckb_config, Some(local_ckb))
    } else {
        let mut ckb_config = ckb_config;
        let local_ckb = start_embedded_ckb(
            light_client_config,
            &fiber_config,
            &ckb_config,
            &genesis_block,
            None,
        )
        .await?;
        ckb_config.rpc_url = local_ckb.rpc_url().to_string();
        info!(
            rpc_url = %ckb_config.rpc_url,
            "redirected Fiber CKB RPC to the embedded Light Client gateway"
        );
        (ckb_config, Some(local_ckb))
    };

    #[cfg(not(feature = "disable-ckb-rpc"))]
    let _ = prepared_ckb;

    let store = fnn::store::open_store_with_migration(
        fiber_config.store_path(),
        Box::new(ffi_confirm),
        Box::new(ffi_progress),
    )
    .map_err(|err| err.to_string())?;

    let root_tracker = TaskTracker::new();
    let root_token = CancellationToken::new();
    let root_actor = RootActor::start(root_tracker.clone(), root_token.clone()).await;

    let store_actor = Actor::spawn_linked(
        Some("store_actor".to_string()),
        StoreActor::new(),
        StoreActorInitializationParameter {
            store: store.clone(),
            backup_path: fiber_config.base_dir().join("backups"),
            ckb_key_path: ckb_config.base_dir().join("key"),
            fiber_key_path: fiber_config.base_dir().join("sk"),
            backup_interval_hours: 24,
        },
        root_actor.get_cell(),
    )
    .await
    .map_err(|err| format!("failed to start store actor: {err}"))?
    .0;

    let chain_hash = genesis_block.hash().into();
    let chain_hash_label = format!("{chain_hash:?}");
    {
        let mut initialized_chain_hash = CHAIN_HASH_STATE
            .lock()
            .map_err(|_| "chain hash state mutex poisoned".to_string())?;
        match initialized_chain_hash.as_ref() {
            Some(existing_chain_hash) if existing_chain_hash != &chain_hash_label => {
                return Err(
                    "cannot restart fiber with a different chain hash in the same process"
                        .to_string(),
                );
            }
            Some(_) => {}
            None => {
                init_chain_hash(chain_hash);
                *initialized_chain_hash = Some(chain_hash_label);
            }
        }
    }
    let type_id_resolver = TypeIDResolver::new(ckb_config.rpc_url.clone());
    try_init_contracts_context(
        genesis_block,
        fiber_config.scripts.clone(),
        ckb_config.udt_whitelist.clone().unwrap_or_default(),
        Some(type_id_resolver),
    )
    .await
    .map_err(|err| format!("failed to init contracts context: {err}"))?;

    let ckb_chain_actor = Actor::spawn_linked(
        Some("ckb".to_string()),
        CkbChainActor {},
        ckb_config.clone(),
        root_actor.get_cell(),
    )
    .await
    .map_err(|err| format!("failed to start ckb actor: {err}"))?
    .0;

    const CHANNEL_SIZE: usize = 4000;
    let (event_sender, mut event_receiver) = mpsc::channel(CHANNEL_SIZE);
    let node_public_key = fiber_config.public_key();
    let network_graph = std::sync::Arc::new(RwLock::new(NetworkGraph::new(
        store.clone(),
        fnn::fiber::types::pubkey_from_tentacle(node_public_key),
        fiber_config.announce_private_addr(),
    )));
    let default_shutdown_script = ckb_config
        .get_default_funding_lock_script()
        .map_err(|err| format!("failed to get default funding lock script: {err}"))?;

    let chain_client = CkbRpcClient::new(&ckb_config);
    let network_actor = start_network(
        fiber_config.clone(),
        chain_client,
        ckb_chain_actor,
        event_sender,
        root_tracker.clone(),
        root_actor.get_cell(),
        store.clone(),
        Some(store_actor),
        network_graph,
        default_shutdown_script,
    )
    .await;
    let ckb_config_for_handle = ckb_config.clone();

    #[cfg(feature = "watchtower")]
    let watchtower_actor = if fiber_config.disable_built_in_watchtower.unwrap_or_default() {
        None
    } else {
        let actor = Actor::spawn_linked(
            Some("watchtower".to_string()),
            WatchtowerActor::new(store.clone()),
            ckb_config,
            root_actor.get_cell(),
        )
        .await
        .map_err(|err| format!("failed to start watchtower actor: {err}"))?
        .0;
        actor.send_interval(
            Duration::from_secs(
                fiber_config
                    .watchtower_check_interval_seconds
                    .unwrap_or(DEFAULT_WATCHTOWER_CHECK_INTERVAL_SECONDS),
            ),
            || WatchtowerMessage::PeriodicCheck,
        );
        Some(actor)
    };

    root_tracker.spawn(async move {
        while let Some(event) = event_receiver.recv().await {
            emit_event(callback, &event);
            #[cfg(feature = "watchtower")]
            if let Some(watchtower_actor) = watchtower_actor.as_ref() {
                forward_event_to_actor(event, watchtower_actor);
            }
        }
        trace!("fiber ffi event loop stopped");
    });

    Ok(RunningNode {
        root_actor,
        root_token,
        root_tracker,
        network_actor,
        store,
        fiber_config,
        ckb_config: ckb_config_for_handle,
        #[cfg(feature = "disable-ckb-rpc")]
        local_ckb,
    })
}

async fn stop_node_on_signal(node: RunningNode, stop_rx: oneshot::Receiver<()>) {
    let _ = stop_rx.await;
    node.root_token.cancel();
    node.root_actor
        .stop(Some("fiber_stop requested".to_string()));
    node.root_tracker.close();
    node.root_tracker.wait().await;
    #[cfg(feature = "disable-ckb-rpc")]
    if let Some(local_ckb) = node.local_ckb {
        local_ckb.shutdown().await;
    }
    debug!("fiber ffi runtime stopped");
}

#[cfg(feature = "disable-ckb-rpc")]
fn required_light_client_dependencies(
    fiber_config: &FiberConfig,
    ckb_config: &CkbConfig,
    genesis_block: &ckb_types::core::BlockView,
    history_start_block: u64,
    trust_configured_cell_deps: bool,
) -> std::result::Result<
    (
        Vec<ckb_light_client::runtime::RequiredScript>,
        std::collections::HashSet<ckb_types::packed::OutPoint>,
    ),
    String,
> {
    use ckb_light_client::runtime::RequiredScript;
    use ckb_types::{packed, prelude::*};

    let mut scripts = Vec::new();
    let funding_lock = funding_lock_from_genesis(ckb_config, genesis_block)?;
    scripts.push(RequiredScript::lock(
        funding_lock.into(),
        history_start_block,
    ));

    for type_id in fiber_config
        .scripts
        .iter()
        .flat_map(|script| script.cell_deps.iter())
        .filter_map(|cell_dep| cell_dep.type_id.clone())
    {
        scripts.push(RequiredScript::type_(type_id, history_start_block));
    }

    for type_id in ckb_config
        .udt_whitelist
        .as_ref()
        .into_iter()
        .flat_map(|whitelist| whitelist.0.iter())
        .flat_map(|udt| udt.cell_deps.iter())
        .filter_map(|cell_dep| cell_dep.type_id.clone())
    {
        scripts.push(RequiredScript::type_(type_id, history_start_block));
    }

    // The secp256k1 dep-group is committed by the selected chain's genesis
    // block, which the Light Client consensus already trusts. Treating it and
    // its members as pinned avoids a pointless scan back to genesis whenever a
    // normal funding transaction is verified.
    let secp256k1_dep_group_tx_hash = genesis_block
        .transaction(1)
        .ok_or_else(|| "CKB genesis block has no secp256k1 dep-group transaction".to_string())?
        .hash();
    let mut pinned_cell_deps = std::collections::HashSet::from([packed::OutPoint::new_builder()
        .tx_hash(secp256k1_dep_group_tx_hash)
        .index(0u32)
        .build()]);

    if trust_configured_cell_deps {
        pinned_cell_deps.extend(
            fiber_config
                .scripts
                .iter()
                .flat_map(|script| script.cell_deps.iter())
                .filter_map(|dependency| dependency.cell_dep.clone())
                .map(|cell_dep| {
                    let cell_dep: packed::CellDep = cell_dep.into();
                    cell_dep.out_point()
                })
                .chain(
                    ckb_config
                        .udt_whitelist
                        .as_ref()
                        .into_iter()
                        .flat_map(|whitelist| whitelist.0.iter())
                        .flat_map(|udt| udt.cell_deps.iter())
                        .filter_map(|dependency| dependency.cell_dep.as_ref())
                        .map(|cell_dep| cell_dep.out_point.clone().into()),
                ),
        );
    }

    Ok((scripts, pinned_cell_deps))
}

enum ConnectPeerCommand {
    Address {
        address: fnn::fiber_types::Multiaddr,
        save: bool,
    },
    Pubkey {
        pubkey: fnn::fiber_types::Pubkey,
        addr_type: Option<TransportType>,
    },
}

async fn call_node_info(
    actor: ActorRef<NetworkActorMessage>,
) -> std::result::Result<fnn::fiber::network::NodeInfoResponse, String> {
    let message = |reply| NetworkActorMessage::Command(NetworkActorCommand::NodeInfo((), reply));
    match ractor::call!(actor, message) {
        Ok(result) => result,
        Err(err) => Err(err.to_string()),
    }
}

async fn call_list_peers(
    actor: ActorRef<NetworkActorMessage>,
) -> std::result::Result<Vec<fnn::fiber::network::PeerInfo>, String> {
    let message = |reply| NetworkActorMessage::Command(NetworkActorCommand::ListPeers((), reply));
    match ractor::call!(actor, message) {
        Ok(result) => result,
        Err(err) => Err(err.to_string()),
    }
}

async fn call_connect_peer(
    actor: ActorRef<NetworkActorMessage>,
    command: ConnectPeerCommand,
) -> std::result::Result<(), String> {
    match command {
        ConnectPeerCommand::Address { address, save } => {
            let message = |reply| {
                NetworkActorMessage::Command(NetworkActorCommand::ConnectPeer(
                    address,
                    save,
                    fnn::fiber::network::PeerConnectSource::Manual,
                    Some(reply),
                ))
            };
            match ractor::call!(actor, message) {
                Ok(result) => result,
                Err(err) => Err(err.to_string()),
            }
        }
        ConnectPeerCommand::Pubkey { pubkey, addr_type } => {
            let message = |reply| {
                NetworkActorMessage::Command(NetworkActorCommand::ConnectPeerWithPubkey(
                    pubkey,
                    addr_type,
                    fnn::fiber::network::PeerConnectSource::Manual,
                    reply,
                ))
            };
            match ractor::call!(actor, message) {
                Ok(result) => result,
                Err(err) => Err(err.to_string()),
            }
        }
    }
}

async fn call_disconnect_peer(
    actor: ActorRef<NetworkActorMessage>,
    pubkey: fnn::fiber_types::Pubkey,
) -> std::result::Result<(), String> {
    let message = |reply| {
        NetworkActorMessage::Command(NetworkActorCommand::DisconnectPeer(
            pubkey,
            fnn::fiber::network::PeerDisconnectReason::Requested,
            Some(reply),
        ))
    };
    match ractor::call!(actor, message) {
        Ok(result) => result,
        Err(err) => Err(err.to_string()),
    }
}

async fn call_open_channel(
    handle: &FiberHandle,
    params: fnn::rpc::channel::OpenChannelParams,
) -> FfiCallResult<fnn::rpc::channel::OpenChannelResult> {
    channel_rpc(handle)
        .open_channel(params)
        .await
        .map_err(rpc_ffi_error)
}

async fn call_accept_channel(
    handle: &FiberHandle,
    params: fnn::rpc::channel::AcceptChannelParams,
) -> FfiCallResult<fnn::rpc::channel::AcceptChannelResult> {
    channel_rpc(handle)
        .accept_channel(params)
        .await
        .map_err(rpc_ffi_error)
}

async fn call_abandon_channel(
    handle: &FiberHandle,
    params: fnn::rpc::channel::AbandonChannelParams,
) -> FfiCallResult<()> {
    channel_rpc(handle)
        .abandon_channel(params)
        .await
        .map_err(rpc_ffi_error)
}

async fn call_list_channels(
    handle: &FiberHandle,
    params: fnn::rpc::channel::ListChannelsParams,
) -> FfiCallResult<fnn::rpc::channel::ListChannelsResult> {
    channel_rpc(handle)
        .list_channels(params)
        .await
        .map_err(rpc_ffi_error)
}

async fn call_shutdown_channel(
    handle: &FiberHandle,
    params: fnn::rpc::channel::ShutdownChannelParams,
) -> FfiCallResult<()> {
    channel_rpc(handle)
        .shutdown_channel(params)
        .await
        .map_err(rpc_ffi_error)
}

async fn call_update_channel(
    handle: &FiberHandle,
    params: fnn::rpc::channel::UpdateChannelParams,
) -> FfiCallResult<()> {
    channel_rpc(handle)
        .update_channel(params)
        .await
        .map_err(rpc_ffi_error)
}

async fn call_open_channel_with_external_funding(
    handle: &FiberHandle,
    params: fnn::rpc::channel::OpenChannelWithExternalFundingParams,
) -> FfiCallResult<fnn::rpc::channel::OpenChannelWithExternalFundingResult> {
    channel_rpc(handle)
        .open_channel_with_external_funding(params)
        .await
        .map_err(rpc_ffi_error)
}

async fn call_submit_signed_funding_tx(
    handle: &FiberHandle,
    params: fnn::rpc::channel::SubmitSignedFundingTxParams,
) -> FfiCallResult<fnn::rpc::channel::SubmitSignedFundingTxResult> {
    channel_rpc(handle)
        .submit_signed_funding_tx(params)
        .await
        .map_err(rpc_ffi_error)
}

async fn call_send_payment(
    handle: &FiberHandle,
    params: fnn::rpc::payment::SendPaymentCommandParams,
) -> FfiCallResult<fnn::rpc::payment::GetPaymentCommandResult> {
    payment_rpc(handle)
        .send_payment(params)
        .await
        .map_err(rpc_ffi_error)
}

async fn call_get_payment(
    handle: &FiberHandle,
    params: fnn::rpc::payment::GetPaymentCommandParams,
) -> FfiCallResult<fnn::rpc::payment::GetPaymentCommandResult> {
    payment_rpc(handle)
        .get_payment(params)
        .await
        .map_err(rpc_ffi_error)
}

async fn call_list_payments(
    handle: &FiberHandle,
    params: fnn::rpc::payment::ListPaymentsParams,
) -> FfiCallResult<fnn::rpc::payment::ListPaymentsResult> {
    payment_rpc(handle)
        .list_payments(params)
        .await
        .map_err(rpc_ffi_error)
}

async fn call_build_router(
    handle: &FiberHandle,
    params: fnn::rpc::payment::BuildRouterParams,
) -> FfiCallResult<fnn::rpc::payment::BuildPaymentRouterResult> {
    payment_rpc(handle)
        .build_router(params)
        .await
        .map_err(rpc_ffi_error)
}

async fn call_send_payment_with_router(
    handle: &FiberHandle,
    params: fnn::rpc::payment::SendPaymentWithRouterParams,
) -> FfiCallResult<fnn::rpc::payment::GetPaymentCommandResult> {
    payment_rpc(handle)
        .send_payment_with_router(params)
        .await
        .map_err(rpc_ffi_error)
}

async fn call_new_invoice(
    handle: &FiberHandle,
    params: fnn::rpc::invoice::NewInvoiceParams,
) -> FfiCallResult<fnn::rpc::invoice::InvoiceResult> {
    invoice_rpc(handle)
        .new_invoice(params)
        .await
        .map_err(rpc_ffi_error)
}

async fn call_parse_invoice(
    handle: &FiberHandle,
    params: fnn::rpc::invoice::ParseInvoiceParams,
) -> FfiCallResult<fnn::rpc::invoice::ParseInvoiceResult> {
    invoice_rpc(handle)
        .parse_invoice(params)
        .await
        .map_err(rpc_ffi_error)
}

async fn call_get_invoice(
    handle: &FiberHandle,
    params: fnn::rpc::invoice::InvoiceParams,
) -> FfiCallResult<fnn::rpc::invoice::GetInvoiceResult> {
    invoice_rpc(handle)
        .get_invoice(params)
        .await
        .map_err(rpc_ffi_error)
}

async fn call_cancel_invoice(
    handle: &FiberHandle,
    params: fnn::rpc::invoice::InvoiceParams,
) -> FfiCallResult<fnn::rpc::invoice::GetInvoiceResult> {
    invoice_rpc(handle)
        .cancel_invoice(params)
        .await
        .map_err(rpc_ffi_error)
}

async fn call_settle_invoice(
    handle: &FiberHandle,
    params: fnn::rpc::invoice::SettleInvoiceParams,
) -> FfiCallResult<fnn::rpc::invoice::SettleInvoiceResult> {
    invoice_rpc(handle)
        .settle_invoice(params)
        .await
        .map_err(rpc_ffi_error)
}

fn channel_rpc(handle: &FiberHandle) -> fnn::rpc::channel::ChannelRpcServerImpl<fnn::store::Store> {
    fnn::rpc::channel::ChannelRpcServerImpl::new(handle.network_actor.clone(), handle.store.clone())
}

fn payment_rpc(handle: &FiberHandle) -> fnn::rpc::payment::PaymentRpcServerImpl<fnn::store::Store> {
    fnn::rpc::payment::PaymentRpcServerImpl::new(handle.network_actor.clone(), handle.store.clone())
}

fn invoice_rpc(handle: &FiberHandle) -> fnn::rpc::invoice::InvoiceRpcServerImpl<fnn::store::Store> {
    fnn::rpc::invoice::InvoiceRpcServerImpl::new(
        handle.store.clone(),
        Some(handle.network_actor.clone()),
        Some(handle.fiber_config.clone()),
    )
}

fn rpc_ffi_error(err: impl std::fmt::Display) -> FfiError {
    ffi_error(FiberFfiStatus::InvalidArgument, err.to_string())
}

fn node_info_to_json(response: fnn::fiber::network::NodeInfoResponse) -> serde_json::Value {
    json!({
        "version": env!("CARGO_PKG_VERSION"),
        "commit_hash": fnn::get_git_commit_info(),
        "pubkey": pubkey_to_hex(&response.node_id),
        "features": response.features.enabled_features_names(),
        "node_name": response.node_name.map(|name| name.to_string()),
        "addresses": response.addresses.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "chain_hash": hash_to_hex(&response.chain_hash),
        "open_channel_auto_accept_min_ckb_funding_amount": response.open_channel_auto_accept_min_ckb_funding_amount,
        "auto_accept_channel_ckb_funding_amount": response.auto_accept_channel_ckb_funding_amount,
        "tlc_expiry_delta": response.tlc_expiry_delta,
        "tlc_min_value": response.tlc_min_value,
        "tlc_fee_proportional_millionths": response.tlc_fee_proportional_millionths,
        "channel_count": response.channel_count,
        "pending_channel_count": response.pending_channel_count,
        "peers_count": response.peers_count,
        "udt_cfg_infos": response.udt_cfg_infos,
    })
}

fn peers_to_json(peers: Vec<fnn::fiber::network::PeerInfo>) -> serde_json::Value {
    json!({
        "peers": peers
            .into_iter()
            .map(|peer| json!({
                "pubkey": pubkey_to_hex(&peer.pubkey),
                "address": peer.address.to_string(),
            }))
            .collect::<Vec<_>>(),
    })
}

fn ffi_confirm(plan: fnn::store::MigrationPlan) -> bool {
    tracing::info!("{}", plan.message);
    true
}

fn ffi_progress(progress: fnn::store::MigrationProgress) {
    tracing::info!(
        "[{}/{}] {}",
        progress.current_step,
        progress.total_steps,
        progress.message
    );
}

fn emit_event(callback: Option<EventCallback>, event: &NetworkServiceEvent) {
    let Some(callback) = callback else {
        return;
    };
    let Ok(event_json) = CString::new(event_to_json(event).to_string()) else {
        tracing::warn!("failed to convert event json to C string");
        return;
    };
    unsafe {
        (callback.callback)(event_json.as_ptr(), callback.user_data as *mut c_void);
    }
}

fn event_to_json(event: &NetworkServiceEvent) -> serde_json::Value {
    match event {
        NetworkServiceEvent::NetworkStarted(pubkey, listening_addrs, announced_addrs) => json!({
            "kind": "NetworkStarted",
            "pubkey": pubkey_to_hex(pubkey),
            "listening_addrs": listening_addrs.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "announced_addrs": announced_addrs.iter().map(ToString::to_string).collect::<Vec<_>>(),
        }),
        NetworkServiceEvent::NetworkStopped(pubkey) => json!({
            "kind": "NetworkStopped",
            "pubkey": pubkey_to_hex(pubkey),
        }),
        NetworkServiceEvent::PeerConnected(pubkey, addr) => json!({
            "kind": "PeerConnected",
            "pubkey": pubkey_to_hex(pubkey),
            "addr": addr.to_string(),
        }),
        NetworkServiceEvent::PeerDisConnected(pubkey, addr) => json!({
            "kind": "PeerDisConnected",
            "pubkey": pubkey_to_hex(pubkey),
            "addr": addr.to_string(),
        }),
        NetworkServiceEvent::ChannelCreated(pubkey, channel_id) => json!({
            "kind": "ChannelCreated",
            "pubkey": pubkey_to_hex(pubkey),
            "channel_id": hash_to_hex(channel_id),
        }),
        NetworkServiceEvent::ChannelPendingToBeAccepted(pubkey, channel_id) => json!({
            "kind": "ChannelPendingToBeAccepted",
            "pubkey": pubkey_to_hex(pubkey),
            "channel_id": hash_to_hex(channel_id),
        }),
        NetworkServiceEvent::RemoteTxComplete(pubkey, channel_id, ..) => json!({
            "kind": "RemoteTxComplete",
            "pubkey": pubkey_to_hex(pubkey),
            "channel_id": hash_to_hex(channel_id),
            "redacted": true,
        }),
        NetworkServiceEvent::ChannelReady(pubkey, channel_id, out_point) => json!({
            "kind": "ChannelReady",
            "pubkey": pubkey_to_hex(pubkey),
            "channel_id": hash_to_hex(channel_id),
            "out_point": format!("{out_point:?}"),
        }),
        NetworkServiceEvent::ChannelOnline(pubkey, channel_id, out_point) => json!({
            "kind": "ChannelOnline",
            "pubkey": pubkey_to_hex(pubkey),
            "channel_id": hash_to_hex(channel_id),
            "out_point": format!("{out_point:?}"),
        }),
        NetworkServiceEvent::ChannelOffline(pubkey, channel_id, out_point) => json!({
            "kind": "ChannelOffline",
            "pubkey": pubkey_to_hex(pubkey),
            "channel_id": hash_to_hex(channel_id),
            "out_point": format!("{out_point:?}"),
        }),
        NetworkServiceEvent::ChannelClosed(pubkey, channel_id, tx_hash) => json!({
            "kind": "ChannelClosed",
            "pubkey": pubkey_to_hex(pubkey),
            "channel_id": hash_to_hex(channel_id),
            "tx_hash": format!("{tx_hash:?}"),
        }),
        NetworkServiceEvent::ChannelAbandon(channel_id) => json!({
            "kind": "ChannelAbandon",
            "channel_id": hash_to_hex(channel_id),
        }),
        NetworkServiceEvent::ChannelFundingAborted(channel_id) => json!({
            "kind": "ChannelFundingAborted",
            "channel_id": hash_to_hex(channel_id),
        }),
        NetworkServiceEvent::RevokeAndAckReceived(pubkey, channel_id, ..) => json!({
            "kind": "RevokeAndAckReceived",
            "pubkey": pubkey_to_hex(pubkey),
            "channel_id": hash_to_hex(channel_id),
            "redacted": true,
        }),
        NetworkServiceEvent::RemoteCommitmentSigned(pubkey, channel_id, ..) => json!({
            "kind": "RemoteCommitmentSigned",
            "pubkey": pubkey_to_hex(pubkey),
            "channel_id": hash_to_hex(channel_id),
            "redacted": true,
        }),
        NetworkServiceEvent::LocalCommitmentSigned(channel_id, _) => json!({
            "kind": "LocalCommitmentSigned",
            "channel_id": hash_to_hex(channel_id),
            "redacted": true,
        }),
        NetworkServiceEvent::PreimageCreated(payment_hash, _) => json!({
            "kind": "PreimageCreated",
            "payment_hash": hash_to_hex(payment_hash),
            "redacted": true,
        }),
        NetworkServiceEvent::PreimageRemoved(payment_hash) => json!({
            "kind": "PreimageRemoved",
            "payment_hash": hash_to_hex(payment_hash),
        }),
        #[cfg(debug_assertions)]
        NetworkServiceEvent::DebugEvent(debug_event) => json!({
            "kind": "DebugEvent",
            "message": format!("{debug_event:?}"),
        }),
    }
}

fn pubkey_to_hex(pubkey: &fnn::fiber_types::Pubkey) -> String {
    hex::encode(pubkey.serialize())
}

fn hash_to_hex(hash: &fnn::fiber_types::Hash256) -> String {
    format!("{hash:#x}")
}

#[cfg(feature = "watchtower")]
fn forward_event_to_actor(
    event: NetworkServiceEvent,
    watchtower_actor: &ActorRef<WatchtowerMessage>,
) {
    match event {
        NetworkServiceEvent::RemoteTxComplete(
            _peer_id,
            channel_id,
            funding_udt_type_script,
            local_settlement_key,
            remote_settlement_key,
            local_funding_pubkey,
            remote_funding_pubkey,
            remote_settlement_data,
        ) => {
            let _ = watchtower_actor.send_message(WatchtowerMessage::CreateChannel(
                channel_id,
                funding_udt_type_script,
                local_settlement_key,
                remote_settlement_key,
                local_funding_pubkey,
                remote_funding_pubkey,
                remote_settlement_data,
            ));
        }
        NetworkServiceEvent::ChannelClosed(_, channel_id, _)
        | NetworkServiceEvent::ChannelAbandon(channel_id) => {
            let _ = watchtower_actor.send_message(WatchtowerMessage::RemoveChannel(channel_id));
        }
        NetworkServiceEvent::RevokeAndAckReceived(
            _peer_id,
            channel_id,
            revocation_data,
            settlement_data,
        ) => {
            let _ = watchtower_actor.send_message(WatchtowerMessage::UpdateRevocation(
                channel_id,
                revocation_data,
                settlement_data,
            ));
        }
        NetworkServiceEvent::RemoteCommitmentSigned(
            _peer_id,
            channel_id,
            _commitment_tx,
            settlement_data,
        ) => {
            let _ = watchtower_actor.send_message(WatchtowerMessage::UpdateLocalSettlement(
                channel_id,
                settlement_data,
            ));
        }
        NetworkServiceEvent::LocalCommitmentSigned(channel_id, settlement_data) => {
            let _ = watchtower_actor.send_message(
                WatchtowerMessage::UpdatePendingRemoteSettlement(channel_id, settlement_data),
            );
        }
        NetworkServiceEvent::PreimageCreated(payment_hash, preimage) => {
            let _ = watchtower_actor
                .send_message(WatchtowerMessage::CreatePreimage(payment_hash, preimage));
        }
        NetworkServiceEvent::PreimageRemoved(payment_hash) => {
            let _ = watchtower_actor.send_message(WatchtowerMessage::RemovePreimage(payment_hash));
        }
        _ => {}
    }
}

fn required_string(ptr: *const c_char, name: &str) -> FfiCallResult<String> {
    if ptr.is_null() {
        return Err(ffi_error(
            FiberFfiStatus::NullPointer,
            format!("{name} must be non-null"),
        ));
    }
    unsafe {
        CStr::from_ptr(ptr)
            .to_str()
            .map(str::to_owned)
            .map_err(|err| ffi_error(FiberFfiStatus::InvalidArgument, err.to_string()))
    }
}

fn optional_string(ptr: *const c_char) -> FfiCallResult<Option<String>> {
    if ptr.is_null() {
        return Ok(None);
    }
    unsafe {
        CStr::from_ptr(ptr)
            .to_str()
            .map(str::to_owned)
            .map(Some)
            .map_err(|err| ffi_error(FiberFfiStatus::InvalidArgument, err.to_string()))
    }
}

unsafe fn checked_handle<'a>(handle: *mut FiberHandle) -> FfiCallResult<&'a FiberHandle> {
    if handle.is_null() {
        return Err(ffi_error(
            FiberFfiStatus::NullPointer,
            "handle must be non-null",
        ));
    }
    Ok(&*handle)
}

unsafe fn checked_options<'a, T>(options: *const T) -> FfiCallResult<&'a T> {
    if options.is_null() {
        return Err(ffi_error(
            FiberFfiStatus::NullPointer,
            "options must be non-null",
        ));
    }
    Ok(&*options)
}

fn prepare_out_string(out_string: *mut *mut c_char) -> FfiCallResult<()> {
    if out_string.is_null() {
        return Err(ffi_error(
            FiberFfiStatus::NullPointer,
            "out_json must be non-null",
        ));
    }
    unsafe {
        *out_string = ptr::null_mut();
    }
    Ok(())
}

fn write_string_out(out_string: *mut *mut c_char, value: &str) -> FfiCallResult<()> {
    let string = CString::new(value).map_err(|err| {
        ffi_error(
            FiberFfiStatus::InvalidArgument,
            format!("failed to allocate string: {err}"),
        )
    })?;
    unsafe {
        *out_string = string.into_raw();
    }
    Ok(())
}

fn write_json_out(out_json: *mut *mut c_char, value: serde_json::Value) -> FfiCallResult<()> {
    let json = serde_json::to_string(&value).map_err(|err| {
        ffi_error(
            FiberFfiStatus::InvalidArgument,
            format!("failed to serialize json: {err}"),
        )
    })?;
    let json = CString::new(json).map_err(|err| {
        ffi_error(
            FiberFfiStatus::InvalidArgument,
            format!("failed to allocate json string: {err}"),
        )
    })?;
    unsafe {
        *out_json = json.into_raw();
    }
    Ok(())
}

fn write_serializable_out<T: Serialize>(
    out_json: *mut *mut c_char,
    value: &T,
) -> FfiCallResult<()> {
    let json = serde_json::to_value(value).map_err(|err| {
        ffi_error(
            FiberFfiStatus::InvalidArgument,
            format!("failed to serialize json: {err}"),
        )
    })?;
    write_json_out(out_json, json)
}

fn write_serializable_field_out<T: Serialize>(
    out_string: *mut *mut c_char,
    value: &T,
    field: &str,
) -> FfiCallResult<()> {
    let json = serde_json::to_value(value).map_err(|err| {
        ffi_error(
            FiberFfiStatus::InvalidArgument,
            format!("failed to serialize json: {err}"),
        )
    })?;
    let field_value = json.get(field).ok_or_else(|| {
        ffi_error(
            FiberFfiStatus::InvalidArgument,
            format!("serialized response is missing {field}"),
        )
    })?;
    let Some(field_value) = field_value.as_str() else {
        return Err(ffi_error(
            FiberFfiStatus::InvalidArgument,
            format!("serialized response field {field} is not a string"),
        ));
    };
    let string = CString::new(field_value).map_err(|err| {
        ffi_error(
            FiberFfiStatus::InvalidArgument,
            format!("failed to allocate string: {err}"),
        )
    })?;
    unsafe {
        *out_string = string.into_raw();
    }
    Ok(())
}

fn init_logging(log_level: &str) {
    let filter = EnvFilter::try_new(log_level).unwrap_or_else(|_| EnvFilter::new("info"));
    INIT_LOGGING.call_once(|| {
        let builder = tracing_subscriber::fmt().with_env_filter(filter);
        if let Some(path) = std::env::var_os("FIBER_FFI_LOG_FILE") {
            match OpenOptions::new().create(true).append(true).open(path) {
                Ok(file) => {
                    let _ = builder
                        .with_ansi(false)
                        .with_writer(Mutex::new(file))
                        .try_init();
                }
                Err(_) => {
                    let _ = builder.try_init();
                }
            }
        } else {
            let _ = builder.try_init();
        }
        debug!("fiber ffi logging initialized");
    });
}

struct FfiError {
    status: FiberFfiStatus,
    message: String,
}

type FfiCallResult<T> = Result<T, FfiError>;

fn ffi_boundary(f: impl FnOnce() -> FfiCallResult<FiberFfiStatus>) -> FiberFfiStatus {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(status)) => {
            clear_last_error();
            status
        }
        Ok(Err(err)) => {
            set_last_error(err.message);
            err.status
        }
        Err(_) => {
            set_last_error("fiber ffi call panicked");
            FiberFfiStatus::Panic
        }
    }
}

fn ffi_error(status: FiberFfiStatus, message: impl Into<String>) -> FfiError {
    FfiError {
        status,
        message: sanitize_error_message(message.into()),
    }
}

fn set_last_error(message: impl Into<String>) {
    let message = sanitize_error_message(message.into());
    LAST_ERROR.with(|last_error| {
        *last_error.borrow_mut() = Some(message);
    });
}

fn clear_last_error() {
    LAST_ERROR.with(|last_error| {
        *last_error.borrow_mut() = None;
    });
}

fn sanitize_error_message(message: String) -> String {
    message.replace('\0', "\\0")
}

#[cfg(test)]
mod ckb_readiness_tests {
    use super::*;

    fn discovery_options() -> FiberCkbDiscoverHistoryStartBlockOptions {
        FiberCkbDiscoverHistoryStartBlockOptions {
            struct_size: std::mem::size_of::<FiberCkbDiscoverHistoryStartBlockOptions>() as u32,
            flags: 0,
            rpc_url: ptr::null(),
            lock_args: ptr::null(),
            pubkey: ptr::null(),
            address: ptr::null(),
            safety_blocks: 0,
            has_safety_blocks: 0,
            max_indexer_lag: 0,
            has_max_indexer_lag: 0,
        }
    }

    #[test]
    fn discovery_wallet_identity_accepts_lock_args_pubkey_or_address() {
        let secret_key = secp256k1::SecretKey::from_slice(&[1u8; 32]).unwrap();
        let pubkey = secret_key.public_key(secp256k1::SECP256K1);
        let payload = CkbAddressPayload::from_pubkey(&pubkey);
        let lock_args = CString::new(format!("0x{}", hex::encode(payload.args()))).unwrap();
        let pubkey = CString::new(hex::encode(pubkey.serialize())).unwrap();
        let address =
            CString::new(CkbAddress::new(NetworkType::Testnet, payload, true).to_string()).unwrap();

        let mut by_lock_args = discovery_options();
        by_lock_args.lock_args = lock_args.as_ptr();
        let mut by_pubkey = discovery_options();
        by_pubkey.pubkey = pubkey.as_ptr();
        let mut by_address = discovery_options();
        by_address.address = address.as_ptr();

        let expected = funding_lock_from_discovery_options(&by_lock_args)
            .unwrap_or_else(|err| panic!("{}", err.message));
        assert_eq!(
            funding_lock_from_discovery_options(&by_pubkey)
                .unwrap_or_else(|err| panic!("{}", err.message)),
            expected
        );
        assert_eq!(
            funding_lock_from_discovery_options(&by_address)
                .unwrap_or_else(|err| panic!("{}", err.message)),
            expected
        );
    }

    #[test]
    fn discovery_wallet_identity_requires_exactly_one_input() {
        let mut options = discovery_options();
        assert!(funding_lock_from_discovery_options(&options).is_err());

        let lock_args = CString::new("0x0000000000000000000000000000000000000000").unwrap();
        let address = CString::new("ckt1qyqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqgp8n6a").unwrap();
        options.lock_args = lock_args.as_ptr();
        options.address = address.as_ptr();
        assert!(funding_lock_from_discovery_options(&options).is_err());
    }

    #[test]
    fn preparation_never_advances_any_known_history_height() {
        assert_eq!(
            select_earliest_history_start_block(Some(80), Some(70), Some(50), Some(60)),
            50
        );
        assert_eq!(
            select_earliest_history_start_block(None, Some(40), Some(90), None),
            40
        );
        assert_eq!(
            select_earliest_history_start_block(None, None, None, None),
            0
        );
    }

    #[test]
    fn ckb_capacity_uses_eight_decimal_places() {
        assert_eq!(format_ckb_capacity(0), "0.00000000");
        assert_eq!(format_ckb_capacity(50_000_000_274), "500.00000274");
    }

    #[test]
    fn ckb_address_network_uses_the_chain_prefix() {
        assert_eq!(ckb_address_network("mainnet"), NetworkType::Mainnet);
        assert_eq!(ckb_address_network("testnet"), NetworkType::Testnet);
        assert_eq!(ckb_address_network("ckb_preview"), NetworkType::Preview);
        assert_eq!(
            ckb_address_network("/tmp/custom-dev.toml"),
            NetworkType::Dev
        );
    }

    #[test]
    fn readiness_rejects_indexer_lag_above_the_operational_limit() {
        let readiness = evaluate_ckb_readiness(120, 10_000, 110, 10_000);

        assert!(!readiness.ready);
        assert_eq!(readiness.tip_block_number, Some(120));
        assert_eq!(readiness.indexed_block_number, Some(110));
        assert_eq!(readiness.lag, Some(10));
        assert_eq!(
            readiness.reason.as_deref(),
            Some("CKB indexer is 10 block(s) behind the chain tip; maximum acceptable lag is 0")
        );
    }

    #[test]
    fn readiness_rejects_one_block_script_index_drift() {
        let readiness = evaluate_ckb_readiness(120, 10_000, 119, 10_000);

        assert!(!readiness.ready);
        assert_eq!(readiness.lag, Some(1));
        assert_eq!(readiness.max_acceptable_lag, 0);
        assert_eq!(
            readiness.reason.as_deref(),
            Some("CKB indexer is 1 block(s) behind the chain tip; maximum acceptable lag is 0")
        );
    }

    #[test]
    fn operational_snapshot_accepts_lag_within_its_explicit_limit() {
        let readiness = evaluate_ckb_readiness_with_lag_tolerance(120, 10_000, 114, 10_000, 6);

        assert!(readiness.ready);
        assert_eq!(readiness.lag, Some(6));
        assert_eq!(readiness.max_acceptable_lag, 6);
        assert_eq!(readiness.reason, None);
    }

    #[test]
    fn readiness_rejects_a_stale_chain_tip() {
        let now = CKB_READINESS_MAX_TIP_AGE_MILLIS + 20_000;
        let readiness = evaluate_ckb_readiness(120, 10_000, 120, now);

        assert!(!readiness.ready);
        assert_eq!(
            readiness.reason.as_deref(),
            Some("CKB chain tip is not current")
        );
        assert!(CkbSyncEstimator::default()
            .observe(&readiness, Instant::now())
            .is_none());
    }

    #[test]
    fn readiness_accepts_a_current_fully_indexed_tip() {
        let readiness = evaluate_ckb_readiness(120, 10_000, 120, 10_000);

        assert!(readiness.ready);
        assert_eq!(readiness.lag, Some(0));
        assert!(readiness.wait_estimate.is_none());
        assert_eq!(readiness.reason, None);
    }

    #[test]
    fn first_lag_sample_returns_a_conservative_protocol_based_range() {
        let readiness = evaluate_ckb_readiness(120, 10_000, 114, 10_000);
        let mut estimator = CkbSyncEstimator::default();
        let estimate = estimator.observe(&readiness, Instant::now()).unwrap();

        assert_eq!(estimate.lower_seconds, 3);
        assert_eq!(estimate.upper_seconds, 60);
        assert_eq!(estimate.retry_after_seconds, 3);
        assert_eq!(estimate.confidence, "low");
    }

    #[test]
    fn repeated_samples_use_observed_indexing_speed() {
        let started = Instant::now();
        let mut estimator = CkbSyncEstimator::default();
        let first = evaluate_ckb_readiness(120, 10_000, 110, 10_000);
        let second = evaluate_ckb_readiness(120, 10_000, 113, 10_000);

        estimator.observe(&first, started).unwrap();
        let estimate = estimator
            .observe(&second, started + Duration::from_secs(3))
            .unwrap();

        assert_eq!(estimate.lower_seconds, 3);
        assert_eq!(estimate.upper_seconds, 17);
        assert_eq!(estimate.confidence, "measured");
    }

    #[test]
    fn catch_up_to_zero_preserves_the_measured_rate_for_the_next_block() {
        let started = Instant::now();
        let mut estimator = CkbSyncEstimator::default();
        let behind = evaluate_ckb_readiness(120, 10_000, 110, 10_000);
        let caught_up = evaluate_ckb_readiness(120, 10_000, 120, 10_000);
        let next_block = evaluate_ckb_readiness(121, 10_000, 120, 10_000);

        estimator.observe(&behind, started).unwrap();
        assert!(estimator
            .observe(&caught_up, started + Duration::from_secs(10))
            .is_none());
        let estimate = estimator
            .observe(&next_block, started + Duration::from_secs(13))
            .unwrap();

        assert_eq!(estimate.confidence, "measured");
        assert_eq!(estimate.lower_seconds, 1);
        assert_eq!(estimate.upper_seconds, 5);
    }

    #[test]
    fn normal_filter_batch_pause_keeps_the_measured_rate() {
        let started = Instant::now();
        let mut estimator = CkbSyncEstimator::default();
        let first = evaluate_ckb_readiness(120, 10_000, 110, 10_000);
        let progressed = evaluate_ckb_readiness(120, 10_000, 115, 10_000);
        let waiting_for_batch = evaluate_ckb_readiness(121, 10_000, 115, 10_000);

        estimator.observe(&first, started).unwrap();
        estimator
            .observe(&progressed, started + Duration::from_secs(5))
            .unwrap();
        let estimate = estimator
            .observe(&waiting_for_batch, started + Duration::from_secs(45))
            .unwrap();

        assert_eq!(estimate.confidence, "measured");
    }

    #[test]
    fn unchanged_progress_is_reported_as_stalled_after_threshold() {
        let started = Instant::now();
        let mut estimator = CkbSyncEstimator::default();
        let readiness = evaluate_ckb_readiness(120, 10_000, 114, 10_000);

        estimator.observe(&readiness, started).unwrap();
        let estimate = estimator
            .observe(
                &readiness,
                started + CKB_SYNC_STALL_THRESHOLD + Duration::from_secs(1),
            )
            .unwrap();

        assert_eq!(estimate.upper_seconds, 120);
        assert_eq!(estimate.confidence, "stalled");
    }
}

#[cfg(all(test, not(feature = "disable-ckb-rpc")))]
mod tests {
    use super::*;
    use std::time::Duration;

    unsafe extern "C" fn prepare_callback(
        status: FiberFfiStatus,
        result_json: *const c_char,
        user_data: *mut c_void,
    ) {
        let sender =
            &*(user_data as *const std_mpsc::Sender<(FiberFfiStatus, String, thread::ThreadId)>);
        let result_json = CStr::from_ptr(result_json).to_string_lossy().into_owned();
        sender
            .send((status, result_json, thread::current().id()))
            .expect("test receiver must remain alive");
    }

    #[test]
    fn prepare_ckb_without_embedded_client_completes_asynchronously() {
        let config_path = CString::new("unused-for-external-rpc-mode").unwrap();
        let options = FiberStartOptions {
            config_path: config_path.as_ptr(),
            database_prefix: ptr::null(),
            log_level: ptr::null(),
            event_callback: None,
            event_callback_user_data: ptr::null_mut(),
        };
        let caller_thread = thread::current().id();
        let (sender, receiver): (
            std_mpsc::Sender<(FiberFfiStatus, String, thread::ThreadId)>,
            std_mpsc::Receiver<(FiberFfiStatus, String, thread::ThreadId)>,
        ) = std_mpsc::channel();
        let sender_ptr = (&sender as *const std_mpsc::Sender<_>) as *mut c_void;

        let status = unsafe { fiber_prepare_ckb(&options, Some(prepare_callback), sender_ptr) };
        assert_eq!(status, FiberFfiStatus::Ok);

        let (callback_status, result_json, callback_thread) = receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("preparation callback must run");
        assert_eq!(callback_status, FiberFfiStatus::Ok);
        assert_ne!(callback_thread, caller_thread);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&result_json).unwrap(),
            json!({
                "ready": true,
                "mode": "external_rpc",
                "skipped": true,
                "status": "ready",
            })
        );
    }
}

#[cfg(all(test, feature = "disable-ckb-rpc"))]
mod embedded_prepare_tests {
    use super::*;
    use ckb_light_client::CkbPrepareStatus;

    #[test]
    fn prepare_progress_exposes_distinct_light_client_statuses() {
        let initializing = embedded_ckb_progress_result(CkbPrepareStatus::Initializing);
        assert_eq!(initializing["status"], "initializing");
        assert_eq!(initializing["ready"], false);

        let birthday = embedded_ckb_progress_result(CkbPrepareStatus::WalletBirthday {
            address: "ckt1example".to_string(),
            history_start_block: 900,
            source: "external_discovery".to_string(),
        });
        assert_eq!(birthday["status"], "wallet_birthday");
        assert_eq!(birthday["history_start_block"], 900);
        assert_eq!(birthday["source"], "external_discovery");

        let connecting = embedded_ckb_progress_result(CkbPrepareStatus::Connecting {
            connected_peers: 1,
            required_peers: 2,
            tip_block_number: 42,
            tip_is_current: false,
        });
        assert_eq!(connecting["status"], "connecting");
        assert_eq!(connecting["connected_peers"], 1);
        assert_eq!(connecting["required_peers"], 2);

        let syncing_headers = embedded_ckb_progress_result(CkbPrepareStatus::SyncingHeaders {
            connected_peers: 2,
            required_peers: 2,
            tip_block_number: 43,
            tip_is_current: false,
        });
        assert_eq!(syncing_headers["status"], "syncing_headers");
        assert_eq!(syncing_headers["tip_block_number"], 43);

        let syncing_scripts = embedded_ckb_progress_result(CkbPrepareStatus::SyncingScripts {
            tip_block_number: 50,
            slowest_script_block_number: 47,
            script_count: 3,
        });
        assert_eq!(syncing_scripts["status"], "syncing_scripts");
        assert_eq!(syncing_scripts["slowest_script_block_number"], 47);
        assert_eq!(syncing_scripts["script_count"], 3);
    }

    #[test]
    fn wallet_history_selection_uses_earliest_cell() {
        let funded = WalletHistoryDiscovery {
            indexer_tip: 9_998,
            earliest_base_ckb_cell_block: Some(7_000),
        };
        assert_eq!(select_wallet_history_start_block(&funded, 1_000), 6_000);

        let empty = WalletHistoryDiscovery {
            indexer_tip: 9_998,
            earliest_base_ckb_cell_block: None,
        };
        assert_eq!(select_wallet_history_start_block(&empty, 1_000), 8_998);
    }

    #[test]
    fn wallet_birthday_metadata_round_trips_and_rejects_another_wallet() {
        let directory = std::env::temp_dir().join(format!(
            "fiber-ffi-wallet-birthday-test-{}-{}",
            std::process::id(),
            NEXT_WALLET_BIRTHDAY_TEMP_ID.fetch_add(1, Ordering::Relaxed)
        ));
        let path = directory.join("wallet-birthday.json");
        let metadata = WalletBirthdayMetadata {
            version: CKB_WALLET_BIRTHDAY_VERSION,
            network: "testnet".to_string(),
            genesis_hash: "0xgenesis".to_string(),
            address: "ckt1wallet".to_string(),
            lock_args: "0x1234".to_string(),
            history_start_block: 42,
            source: "external_discovery".to_string(),
        };

        write_wallet_birthday_metadata(&path, &metadata).unwrap();
        assert_eq!(
            read_wallet_birthday_metadata(&path).unwrap(),
            Some(metadata.clone())
        );
        validate_wallet_birthday_metadata(
            &metadata,
            "testnet",
            "0xgenesis",
            "ckt1wallet",
            "0x1234",
            &path,
        )
        .unwrap();
        let error = validate_wallet_birthday_metadata(
            &metadata,
            "testnet",
            "0xgenesis",
            "ckt1another",
            "0x1234",
            &path,
        )
        .unwrap_err();
        assert!(error.contains("different address"));

        std::fs::remove_dir_all(directory).unwrap();
    }
}
