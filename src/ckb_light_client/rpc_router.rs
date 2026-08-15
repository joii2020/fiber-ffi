use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

use ckb_chain_spec::consensus::Consensus;
use ckb_jsonrpc_types::{
    BlockNumber, CellData, CellInfo, CellWithStatus, EpochNumber, EpochView, HeaderView, JsonBytes,
    OutPoint as JsonOutPoint, Script, Transaction, TransactionView, Uint32, Uint64,
};
use ckb_light_client_lib::{
    service::{
        CellType, FetchStatus, LightClientChainService, LightClientService, Pagination,
        ScriptStatus, ScriptType, SearchKey, SetScriptsCommand, Status, TransactionWithStatus, Tx,
    },
    storage::{
        IteratorDirection, IteratorStart, Key, KeyPrefix, LightClientStorage, Storage,
        StorageBackend, StorageWithChainData,
    },
};
use ckb_sdk::CkbRpcClient;
use ckb_types::{
    bytes::Bytes,
    core::{DepType, EpochNumberWithFraction, TransactionView as CoreTransactionView},
    packed,
    prelude::{Entity, IntoTransactionView, Pack, Unpack},
    H256,
};
use jsonrpc_core::{Error, ErrorCode, Params, Result};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use tracing::{debug, warn};

use super::config::REMOTE_DATA_TIMEOUT;

const NOT_READY_ERROR: i64 = -32010;
const UNSUPPORTED_ERROR: i64 = -32011;
const TRANSACTION_FAILED_TO_RESOLVE: i64 = -301;
const TRANSACTION_FAILED_TO_VERIFY: i64 = -302;
const POOL_REJECTED_TRANSACTION_BY_MIN_FEE_RATE: i64 = -1104;
const MAX_QUERY_LIMIT: u32 = 1_000;
// Bump the marker when changing subscription semantics. V2 deliberately drops
// scripts discovered by the old, unrestricted get_transaction path once, then
// preserves scripts added by the controlled tracked-transaction expansion.
const REQUIRED_SCRIPTS_MARKER_KEY: &[u8] = b"FIBER_FFI_REQUIRED_SCRIPTS_V2";
const SCRIPT_COVERAGE_KEY_PREFIX: &[u8] = b"FIBER_FFI_SCRIPT_COVERAGE_V2";

const LOCAL_METHODS: &[&str] = &[
    "get_cells",
    "get_transactions",
    "get_tip_header",
    "get_tip_block_number",
    "get_indexer_tip",
    "get_consensus",
    "get_epoch_by_number",
    "get_block_by_number",
    "get_header",
    "get_header_by_number",
    "get_block_median_time",
    "get_transaction",
    "get_live_cell",
    "send_transaction",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Route {
    Local,
    Unsupported,
}

#[derive(Clone)]
pub(crate) struct RpcRouter {
    storage: Arc<Storage>,
    chain_service: LightClientChainService,
    consensus: Arc<Consensus>,
    pending_inputs: Arc<Mutex<PendingInputReservations>>,
    indexed_script_starts: Arc<Mutex<HashMap<(packed::Script, IndexedScriptType), u64>>>,
    pinned_cell_deps: Arc<HashSet<packed::OutPoint>>,
    peer_funding_liveness_rpc: Option<PeerFundingLivenessRpc>,
}

#[derive(Clone)]
struct PeerFundingLivenessRpc {
    url: String,
    expected_genesis_hash: H256,
    chain_verified: Arc<AtomicBool>,
}

impl PeerFundingLivenessRpc {
    fn new(url: String, expected_genesis_hash: H256) -> std::result::Result<Self, String> {
        CkbRpcClient::new_with_timeout(&url, REMOTE_DATA_TIMEOUT)
            .map_err(|_| "invalid ckb_light_client.peer_funding_liveness_rpc_url".to_string())?;
        Ok(Self {
            url,
            expected_genesis_hash,
            chain_verified: Arc::new(AtomicBool::new(false)),
        })
    }

    fn cell_is_live(
        &self,
        out_point: &packed::OutPoint,
        deadline: Instant,
    ) -> std::result::Result<bool, String> {
        self.verify_chain(deadline)?;
        let client = self.client(deadline)?;
        let response = client
            .get_live_cell(out_point.clone().into(), false)
            .map_err(|_| "peer funding liveness RPC get_live_cell failed".to_string())?;
        let is_live = interpret_external_liveness_status(&response.status)?;
        debug!(
            ?out_point,
            status = response.status,
            "used configured external RPC as peer funding input liveness reference"
        );
        Ok(is_live)
    }

    fn verify_chain(&self, deadline: Instant) -> std::result::Result<(), String> {
        if self.chain_verified.load(Ordering::Acquire) {
            return Ok(());
        }
        let client = self.client(deadline)?;
        let observed = client
            .get_block_hash(BlockNumber::from(0u64))
            .map_err(|_| "peer funding liveness RPC genesis query failed".to_string())?
            .ok_or_else(|| {
                "peer funding liveness RPC returned no genesis block hash".to_string()
            })?;
        if observed != self.expected_genesis_hash {
            return Err(format!(
                "peer funding liveness RPC is on the wrong chain: expected genesis {:#x}, got {observed:#x}",
                self.expected_genesis_hash
            ));
        }
        self.chain_verified.store(true, Ordering::Release);
        Ok(())
    }

    fn client(&self, deadline: Instant) -> std::result::Result<CkbRpcClient, String> {
        let timeout = deadline
            .checked_duration_since(Instant::now())
            .ok_or_else(|| {
                "peer funding liveness RPC query exceeded the Light Client data deadline"
                    .to_string()
            })?;
        if timeout.is_zero() {
            return Err(
                "peer funding liveness RPC query exceeded the Light Client data deadline"
                    .to_string(),
            );
        }
        CkbRpcClient::new_with_timeout(&self.url, timeout)
            .map_err(|_| "failed to initialize peer funding liveness RPC".to_string())
    }
}

fn interpret_external_liveness_status(status: &str) -> std::result::Result<bool, String> {
    match status {
        "live" => Ok(true),
        // Modern CKB nodes may report a spent cell as unknown instead of dead.
        // The producing transaction was already verified locally, so neither
        // response is sufficient to let the funding transaction proceed.
        "dead" | "unknown" => Ok(false),
        other => Err(format!(
            "peer funding liveness RPC returned unsupported cell status {other:?}"
        )),
    }
}

#[derive(Clone, Copy)]
enum LivenessPolicy {
    LightClientOnly,
    AllowPeerFundingRpc,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum IndexedScriptType {
    Lock,
    Type,
}

impl IndexedScriptType {
    fn service_type(self) -> ScriptType {
        match self {
            Self::Lock => ScriptType::Lock,
            Self::Type => ScriptType::Type,
        }
    }
}

#[derive(Default)]
struct PendingInputReservations {
    owners: HashMap<packed::OutPoint, packed::Byte32>,
}

impl PendingInputReservations {
    fn retain(&mut self, mut keep: impl FnMut(&packed::Byte32) -> bool) {
        self.owners.retain(|_, owner| keep(owner));
    }

    fn conflicting_owner(
        &self,
        inputs: &[packed::OutPoint],
        tx_hash: &packed::Byte32,
    ) -> Option<(packed::OutPoint, packed::Byte32)> {
        inputs.iter().find_map(|input| {
            self.owners
                .get(input)
                .and_then(|owner| (owner != tx_hash).then(|| (input.clone(), owner.clone())))
        })
    }

    fn reserve(&mut self, inputs: &[packed::OutPoint], tx_hash: &packed::Byte32) {
        for input in inputs {
            self.owners.insert(input.clone(), tx_hash.clone());
        }
    }
}

impl RpcRouter {
    pub(crate) fn new(
        storage: Storage,
        chain_data: StorageWithChainData,
        consensus: Arc<Consensus>,
        pinned_cell_deps: HashSet<packed::OutPoint>,
        peer_funding_liveness_rpc_url: Option<String>,
    ) -> std::result::Result<Self, String> {
        let chain_service =
            LightClientChainService::new(chain_data.clone(), Arc::clone(&consensus));
        let peer_funding_liveness_rpc = peer_funding_liveness_rpc_url
            .map(|url| PeerFundingLivenessRpc::new(url, consensus.genesis_hash().unpack()))
            .transpose()?;

        Ok(Self {
            storage: Arc::new(storage),
            chain_service,
            consensus,
            pending_inputs: Arc::new(Mutex::new(PendingInputReservations::default())),
            indexed_script_starts: Arc::new(Mutex::new(HashMap::new())),
            pinned_cell_deps: Arc::new(pinned_cell_deps),
            peer_funding_liveness_rpc,
        })
    }

    pub(crate) fn methods() -> impl Iterator<Item = &'static str> {
        LOCAL_METHODS.iter().copied()
    }

    pub(crate) fn tip_header(&self) -> HeaderView {
        self.chain_service.get_tip_header()
    }

    pub(crate) fn script_statuses(&self) -> Vec<ScriptStatus> {
        self.chain_service.get_scripts()
    }

    pub(crate) fn indexed_tip_number(&self) -> u64 {
        indexed_tip_number(
            self.chain_service.get_tip_header().inner.number.value(),
            &self.chain_service.get_scripts(),
        )
    }

    fn operational_tip_header(&self) -> Result<HeaderView> {
        let block_number = self.indexed_tip_number();
        let network_tip = self.chain_service.get_tip_header();
        if network_tip.inner.number.value() == block_number {
            return Ok(network_tip);
        }

        let block_hash = self.block_hash_by_number(block_number).ok_or_else(|| {
            not_ready(format!(
                "verified header at operational block {block_number} is not ready"
            ))
        })?;
        let block_hash: H256 = block_hash.unpack();
        self.chain_service.get_header(&block_hash).ok_or_else(|| {
            not_ready(format!(
                "verified header at operational block {block_number} is not stored"
            ))
        })
    }

    pub(crate) fn register_scripts(
        &self,
        scripts: impl IntoIterator<Item = ScriptStatus>,
    ) -> std::result::Result<(), String> {
        let requested = scripts
            .into_iter()
            .map(|status| {
                let script_type = match status.script_type {
                    ScriptType::Lock => IndexedScriptType::Lock,
                    ScriptType::Type => IndexedScriptType::Type,
                };
                let status_block = status.block_number.value();
                let first_indexed_block = if status_block == 0 {
                    0
                } else {
                    status_block.saturating_add(1)
                };
                (status.script, script_type, first_indexed_block)
            })
            .collect::<Vec<_>>();
        let marker = required_scripts_marker(&requested);
        let marker_matches = self
            .storage
            .get(REQUIRED_SCRIPTS_MARKER_KEY)
            .map_err(|err| format!("failed to read required script marker: {err}"))?
            .is_some_and(|stored| stored == marker);
        let existing = self.chain_service.get_scripts();
        let (required, set_changed) =
            reconcile_required_scripts(existing, requested.iter(), marker_matches);
        let registered_scripts = required
            .iter()
            .map(|status| {
                let script_type = match status.script_type {
                    ScriptType::Lock => IndexedScriptType::Lock,
                    ScriptType::Type => IndexedScriptType::Type,
                };
                (status.script.clone(), script_type)
            })
            .collect::<Vec<_>>();

        // Script progress is persistent. Re-registering a required script with
        // its original wallet-birthday height would otherwise rewind it on every
        // process start. A marker records the base script set and original start
        // heights. A matching cache also retains scripts added by the controlled
        // tracked-transaction expansion, so channel monitoring survives restart.
        // A mismatched/old cache gets one safe rescan that removes scripts added
        // by the former unrestricted get_transaction path.
        if !marker_matches || set_changed {
            self.chain_service
                .set_scripts(required, Some(SetScriptsCommand::All));
            self.storage
                .put(REQUIRED_SCRIPTS_MARKER_KEY.to_vec(), marker)
                .map_err(|err| format!("failed to write required script marker: {err}"))?;
        }

        let mut starts = self
            .indexed_script_starts
            .lock()
            .map_err(|_| "indexed script coverage lock is poisoned".to_string())?;
        starts.clear();
        for (script, script_type) in registered_scripts {
            if let Some(first_indexed_block) = self.read_script_coverage(&script, script_type)? {
                starts.insert((script.into(), script_type), first_indexed_block);
            }
        }
        for (script, script_type, first_indexed_block) in requested {
            self.write_script_coverage(&script, script_type, first_indexed_block)?;
            let key = (script.into(), script_type);
            starts
                .entry(key)
                .and_modify(|known_start| *known_start = (*known_start).min(first_indexed_block))
                .or_insert(first_indexed_block);
        }
        Ok(())
    }

    pub(crate) fn handle(&self, method: &'static str, params: Params) -> Result<Value> {
        let started = Instant::now();
        let route = route_method(method);
        let result = match route {
            Route::Local => self.handle_local(method, params),
            Route::Unsupported => Err(unsupported(format!(
                "{method} is not supported by the embedded CKB RPC gateway"
            ))),
        };
        debug!(
            method,
            ?route,
            elapsed_ms = started.elapsed().as_millis(),
            success = result.is_ok(),
            "handled embedded CKB RPC request"
        );
        result
    }

    fn handle_local(&self, method: &str, params: Params) -> Result<Value> {
        match method {
            "get_cells" => self.get_cells(params),
            "get_transactions" => self.get_transactions(params),
            "get_tip_header" => to_value(self.operational_tip_header()?),
            "get_tip_block_number" => to_value(BlockNumber::from(self.indexed_tip_number())),
            "get_indexer_tip" => self.get_indexer_tip(),
            "get_consensus" => to_value(ckb_jsonrpc_types::Consensus::from(
                (*self.consensus).clone(),
            )),
            "get_epoch_by_number" => self.get_epoch_by_number(params),
            "get_block_by_number" => self.get_block_by_number(params),
            "get_header" => self.get_header(params),
            "get_header_by_number" => self.get_header_by_number(params),
            "get_block_median_time" => self.get_block_median_time(params),
            "get_transaction" => self.get_transaction(params),
            "get_live_cell" => self.get_live_cell(params),
            "send_transaction" => self.send_transaction(params),
            _ => Err(Error::method_not_found()),
        }
    }

    fn get_cells(&self, params: Params) -> Result<Value> {
        let params = positional(params)?;
        let search_value = required_param::<Value>(&params, 0, "search_key")?;
        let order_value = required_param::<Value>(&params, 1, "order")?;
        let limit = required_param::<Uint32>(&params, 2, "limit")?.value();
        let after = optional_param::<JsonBytes>(&params, 3)?;
        validate_limit(limit)?;

        let search_mode = search_mode(&search_value);
        let search_key = decode::<SearchKey>(search_value.clone(), "search_key")?;
        match search_mode {
            SearchMode::Exact => {
                self.require_script_registered(&search_key)?;
                self.wait_script_ready(&search_key.script, &search_key.script_type)?;
            }
            SearchMode::Prefix => self.wait_all_scripts_ready()?,
            SearchMode::Partial => {
                return Err(unsupported(
                    "partial indexer search is not supported by the embedded Light Client",
                ));
            }
        }

        let service = LightClientService::new(Arc::clone(&self.storage));
        if search_mode == SearchMode::Exact {
            let script = search_key.script.clone();
            let script_type = copy_script_type(&search_key.script_type);
            let page = collect_exact_page(
                limit,
                after,
                |page_limit, cursor| {
                    service
                        .get_cells(
                            decode(search_value.clone(), "search_key")?,
                            decode(order_value.clone(), "order")?,
                            page_limit,
                            cursor,
                        )
                        .map_err(|err| invalid_params(err.to_string()))
                },
                |cell| cell_matches_script(&cell.output, &script, &script_type).then_some(cell),
            )?;
            return to_value(page);
        }

        let page = service
            .get_cells(
                search_key,
                decode(order_value, "order")?,
                Uint32::from(limit),
                after,
            )
            .map_err(|err| invalid_params(err.to_string()))?;
        to_value(page)
    }

    fn get_transactions(&self, params: Params) -> Result<Value> {
        let params = positional(params)?;
        let search_value = required_param::<Value>(&params, 0, "search_key")?;
        let order_value = required_param::<Value>(&params, 1, "order")?;
        let limit = required_param::<Uint32>(&params, 2, "limit")?.value();
        let after = optional_param::<JsonBytes>(&params, 3)?;
        validate_limit(limit)?;

        let mode = search_mode(&search_value);
        let search_key = decode::<SearchKey>(search_value.clone(), "search_key")?;
        let exact_match = (mode == SearchMode::Exact).then(|| {
            (
                search_key.script.clone(),
                copy_script_type(&search_key.script_type),
            )
        });
        match mode {
            SearchMode::Exact => {
                self.require_script_registered(&search_key)?;
                self.wait_script_ready(&search_key.script, &search_key.script_type)?;
            }
            SearchMode::Prefix => self.wait_all_scripts_ready()?,
            SearchMode::Partial => {
                return Err(unsupported(
                    "partial indexer search is not supported by the embedded Light Client",
                ));
            }
        }

        let service = LightClientService::new(Arc::clone(&self.storage));
        let page = if let Some((script, script_type)) = exact_match {
            collect_exact_page(
                limit,
                after,
                |page_limit, cursor| {
                    service
                        .get_transactions(
                            decode(search_value.clone(), "search_key")?,
                            decode(order_value.clone(), "order")?,
                            page_limit,
                            cursor,
                        )
                        .map_err(|err| invalid_params(err.to_string()))
                },
                |transaction| {
                    filter_exact_transaction(transaction, &script, &script_type, &self.storage)
                },
            )?
        } else {
            service
                .get_transactions(
                    search_key,
                    decode(order_value, "order")?,
                    Uint32::from(limit),
                    after,
                )
                .map_err(|err| invalid_params(err.to_string()))?
        };
        indexer_transactions_to_value(page)
    }

    fn get_indexer_tip(&self) -> Result<Value> {
        let block_number = self.indexed_tip_number();
        let block_hash = self.block_hash_by_number(block_number).ok_or_else(|| {
            not_ready(format!("block hash at height {block_number} is not ready"))
        })?;
        to_value(json!({
            "block_hash": H256::from(block_hash),
            "block_number": BlockNumber::from(block_number),
        }))
    }

    /// Resolve both matched-block mappings and the verified rolling header
    /// window maintained by the Light Client protocol. Near the chain tip most
    /// blocks do not match a watched script, so they intentionally have no
    /// `BlockNumber` storage entry even though their hashes are already proved.
    fn block_hash_by_number(&self, block_number: u64) -> Option<packed::Byte32> {
        if let Some(hash) = self.storage.get_block_hash(block_number) {
            return Some(hash);
        }

        let tip = self.chain_service.get_tip_header();
        if tip.inner.number.value() == block_number {
            return Some(tip.hash.pack());
        }

        find_recent_block_hash(block_number, self.storage.get_last_n_headers())
    }

    fn get_epoch_by_number(&self, params: Params) -> Result<Value> {
        let params = positional(params)?;
        if params.len() != 1 {
            return Err(invalid_params(
                "get_epoch_by_number accepts exactly one epoch number",
            ));
        }
        let requested = required_param::<EpochNumber>(&params, 0, "epoch_number")?.value();
        let tip = self.chain_service.get_tip_header();
        let mut headers = self.stored_headers()?;
        headers.push(tip.clone());
        headers.push(self.consensus.genesis_block().header().into());

        let epoch = resolve_epoch_view_for_fiber(
            requested,
            &tip,
            self.consensus.cellbase_maturity(),
            &headers,
        )
        .map_err(not_ready)?;
        to_value(epoch)
    }

    /// Return every proved header addressable by block number in the Light
    /// Client store. A block matching a registered script always has such a
    /// mapping, which is the property the Fiber cell collector needs here.
    fn stored_headers(&self) -> Result<Vec<HeaderView>> {
        let prefix = vec![KeyPrefix::BlockNumber as u8];
        let take_prefix = prefix.clone();
        let entries = self.storage.collect_iterator(
            IteratorStart::From(prefix),
            IteratorDirection::Forward,
            Box::new(move |key| key.starts_with(&take_prefix)),
            Box::new(|_key, value| Some(value.to_vec())),
            usize::MAX,
        );

        entries
            .into_iter()
            .map(|entry| {
                let hash = packed::Byte32::from_slice(&entry.value).map_err(|err| {
                    internal_error(format!(
                        "invalid stored block hash while resolving epoch: {err}"
                    ))
                })?;
                self.storage.get_header(&hash).ok_or_else(|| {
                    internal_error(format!(
                        "stored block number mapping has no header for {hash:?}"
                    ))
                })
            })
            .map(|result| result.map(Into::into))
            .collect()
    }

    fn get_block_by_number(&self, params: Params) -> Result<Value> {
        let params = positional(params)?;
        let number = required_param::<BlockNumber>(&params, 0, "block_number")?.value();
        let verbosity = optional_param::<Uint32>(&params, 1)?
            .map(|value| value.value())
            .unwrap_or(2);
        if number != 0 {
            return Err(unsupported(
                "the embedded Light Client only stores the complete genesis block",
            ));
        }

        let block = self.chain_service.get_genesis_block();
        match verbosity {
            0 => {
                let block: ckb_types::core::BlockView = block.into();
                to_value(JsonBytes::from_bytes(block.data().as_bytes()))
            }
            1 | 2 => to_value(block),
            _ => Err(invalid_params("verbosity must be 0, 1, or 2")),
        }
    }

    fn get_header(&self, params: Params) -> Result<Value> {
        let params = positional(params)?;
        let hash = required_param::<H256>(&params, 0, "block_hash")?;
        let verbosity = optional_param::<Uint32>(&params, 1)?
            .map(|value| value.value())
            .unwrap_or(1);
        let header = self.fetch_header(&hash)?;
        match (header, verbosity) {
            (None, _) => Ok(Value::Null),
            (Some(header), 0) => {
                let header: ckb_types::core::HeaderView = header.into();
                to_value(JsonBytes::from_bytes(header.data().as_bytes()))
            }
            (Some(header), 1 | 2) => to_value(header),
            (_, _) => Err(invalid_params("verbosity must be 0, 1, or 2")),
        }
    }

    fn get_header_by_number(&self, params: Params) -> Result<Value> {
        let params = positional(params)?;
        let number = required_param::<BlockNumber>(&params, 0, "block_number")?.value();
        let verbosity = optional_param::<Uint32>(&params, 1)?
            .map(|value| value.value())
            .unwrap_or(1);
        let Some(hash) = self.block_hash_by_number(number) else {
            return Err(not_ready(format!(
                "header at block number {number} is not available locally"
            )));
        };
        let Some(header) = self.chain_service.get_header(&hash.unpack()) else {
            return Err(not_ready(format!(
                "header at block number {number} is not ready"
            )));
        };

        match verbosity {
            0 => {
                let header: ckb_types::core::HeaderView = header.into();
                to_value(JsonBytes::from_bytes(header.data().as_bytes()))
            }
            1 | 2 => to_value(header),
            _ => Err(invalid_params("verbosity must be 0, 1, or 2")),
        }
    }

    fn get_block_median_time(&self, params: Params) -> Result<Value> {
        let params = positional(params)?;
        let mut hash = required_param::<H256>(&params, 0, "block_hash")?;
        let mut timestamps = Vec::with_capacity(self.consensus.median_time_block_count());

        for _ in 0..self.consensus.median_time_block_count() {
            let header = self
                .chain_service
                .get_header(&hash)
                .ok_or_else(|| not_ready(format!("header {hash:#x} is not ready")))?;
            timestamps.push(header.inner.timestamp.value());
            if header.inner.number.value() == 0 {
                break;
            }
            hash = header.inner.parent_hash;
        }
        timestamps.sort_unstable();
        to_value(Uint64::from(timestamps[timestamps.len() >> 1]))
    }

    fn get_transaction(&self, params: Params) -> Result<Value> {
        let params = positional(params)?;
        let hash = required_param::<H256>(&params, 0, "tx_hash")?;
        let verbosity = optional_param::<Uint32>(&params, 1)?
            .map(|value| value.value())
            .unwrap_or(2);
        let only_committed = optional_param::<bool>(&params, 2)?.unwrap_or(false);
        if verbosity > 2 {
            return Err(invalid_params("verbosity must be 0, 1, or 2"));
        }

        let transaction = self.fetch_transaction(&hash)?;
        if matches!(transaction.tx_status.status, Status::Committed) {
            if let Some(transaction_view) = transaction.transaction.as_ref() {
                // Follow only descendants of scripts we already track. The old
                // behavior subscribed to every output of every transaction read
                // through this RPC (including ancient CellDeps), repeatedly
                // rewinding the one global block-filter cursor.
                if self.transaction_extends_tracked_chain(&transaction_view.inner) {
                    let packed_hash =
                        packed::Byte32::from_slice(hash.as_bytes()).map_err(|err| {
                            internal_error(format!(
                                "invalid transaction hash from Light Client: {err}"
                            ))
                        })?;
                    if let Some((block_number, _, _)) = self.storage.get_transaction(&packed_hash) {
                        self.register_transaction_output_scripts(
                            &transaction_view.inner,
                            block_number,
                        )?;
                    }
                }
            }
        }
        transaction_to_full_node_value(
            transaction,
            &hash,
            verbosity,
            only_committed,
            &self.storage,
            &self.chain_service,
        )
    }

    fn get_live_cell(&self, params: Params) -> Result<Value> {
        let params = positional(params)?;
        if !(2..=3).contains(&params.len()) {
            return Err(invalid_params(
                "get_live_cell accepts out_point, with_data, and optional include_tx_pool",
            ));
        }
        let out_point = required_param::<JsonOutPoint>(&params, 0, "out_point")?;
        let with_data = required_param::<bool>(&params, 1, "with_data")?;
        let include_tx_pool = optional_param::<bool>(&params, 2)?.unwrap_or(false);
        if include_tx_pool {
            return Err(unsupported(
                "get_live_cell include_tx_pool=true is not supported by the embedded Light Client",
            ));
        }

        let validation_tip = self.indexed_tip_number();
        let out_point: packed::OutPoint = out_point.into();
        let tx_hash: H256 = out_point.tx_hash().unpack();
        let deadline = Instant::now() + REMOTE_DATA_TIMEOUT;
        let transaction = self.fetch_transaction_until(&tx_hash, deadline)?;
        if !matches!(transaction.tx_status.status, Status::Committed) {
            return to_value(cell_with_status("unknown", None, None));
        }
        let block_hash = transaction.tx_status.block_hash.clone();
        let transaction = transaction.transaction.ok_or_else(|| {
            not_ready(format!(
                "committed producing transaction for {out_point:?} is not ready"
            ))
        })?;
        let transaction: packed::Transaction = transaction.inner.into();
        let output_index: u32 = out_point.index().unpack();
        let Some(output) = transaction.raw().outputs().get(output_index as usize) else {
            return to_value(cell_with_status("unknown", None, None));
        };
        let Some(data) = transaction
            .raw()
            .outputs_data()
            .get(output_index as usize)
            .map(|data| data.raw_data())
        else {
            return Err(internal_error(format!(
                "producing transaction for {out_point:?} has no matching output data"
            )));
        };

        if !self.pinned_cell_deps.contains(&out_point)
            && !self.committed_cell_is_live(
                &out_point,
                &output,
                validation_tip,
                deadline,
                LivenessPolicy::AllowPeerFundingRpc,
            )?
        {
            return to_value(cell_with_status("dead", None, None));
        }

        let data = with_data.then(|| CellData {
            content: JsonBytes::from_bytes(data.clone()),
            hash: packed::CellOutput::calc_data_hash(&data).into(),
        });
        to_value(cell_with_status(
            "live",
            Some(CellInfo {
                output: output.into(),
                data,
            }),
            block_hash,
        ))
    }

    fn send_transaction(&self, params: Params) -> Result<Value> {
        let params = positional(params)?;
        if params.len() > 2 {
            return Err(invalid_params(
                "send_transaction accepts transaction and optional outputs_validator",
            ));
        }
        let transaction = required_param::<Transaction>(&params, 0, "transaction")?;
        if let Some(outputs_validator) = optional_param::<String>(&params, 1)? {
            if outputs_validator != "passthrough" {
                return Err(unsupported(format!(
                    "outputs_validator {outputs_validator:?} is not supported by the embedded Light Client"
                )));
            }
        }

        let packed_transaction: packed::Transaction = transaction.clone().into();
        let transaction_view = packed_transaction.into_view();
        let tx_hash = transaction_view.hash();
        let inputs = transaction_view.input_pts_iter().collect::<Vec<_>>();
        let mut unique_inputs = HashSet::with_capacity(inputs.len());
        if let Some(duplicated) = inputs
            .iter()
            .find(|input| !unique_inputs.insert((*input).clone()))
        {
            return Err(transaction_failed_to_resolve(format!(
                "Resolve failed Dead({duplicated:?}): the transaction contains a duplicated input"
            )));
        }

        // Fetch and prove all referenced chain data before running the Light Client's
        // contextual verifier. In particular, this compensates for its storage
        // CellProvider treating every stored historical output as live.
        self.prepare_transaction(&transaction_view, Instant::now() + REMOTE_DATA_TIMEOUT)?;

        // Keep conflict detection and insertion into PendingTxs in one critical
        // section. This makes concurrent RPC submissions atomic from the local
        // gateway's point of view.
        let mut pending_inputs = self
            .pending_inputs
            .lock()
            .map_err(|_| internal_error("pending input reservation lock is poisoned"))?;
        pending_inputs.retain(|owner| {
            let owner: H256 = owner.unpack();
            matches!(
                self.chain_service.get_transaction(&owner).tx_status.status,
                Status::Pending
            )
        });
        if let Some((input, owner)) = pending_inputs.conflicting_owner(&inputs, &tx_hash) {
            return Err(transaction_failed_to_resolve(format!(
                "Resolve failed Dead({input:?}): input is reserved by pending transaction {owner:?}"
            )));
        }

        let returned_hash = self
            .chain_service
            .send_transaction(transaction.clone())
            .map_err(map_send_transaction_error)?;
        let returned_hash: packed::Byte32 = returned_hash.pack();
        if returned_hash != tx_hash {
            return Err(internal_error(format!(
                "Light Client returned transaction hash {returned_hash:?}, expected {tx_hash:?}"
            )));
        }
        pending_inputs.reserve(&inputs, &tx_hash);
        drop(pending_inputs);

        let first_required_block = self
            .chain_service
            .get_tip_header()
            .inner
            .number
            .value()
            .saturating_add(1);
        self.register_transaction_output_scripts(&transaction, first_required_block)?;

        to_value(H256::from(tx_hash))
    }

    fn prepare_transaction(
        &self,
        transaction: &CoreTransactionView,
        deadline: Instant,
    ) -> Result<()> {
        // Freeze one already indexed and verified height for the whole
        // validation. Newly verified headers must not keep moving the finish
        // line while transaction scripts are being prepared.
        let validation_tip = self.indexed_tip_number();
        for header_hash in transaction.header_deps().into_iter() {
            let header_hash: H256 = header_hash.unpack();
            if self.fetch_header_until(&header_hash, deadline)?.is_none() {
                return Err(transaction_failed_to_resolve(format!(
                    "Resolve failed InvalidHeader({header_hash:#x})"
                )));
            }
        }

        // Without an external liveness reference, inputs and unpinned deps need
        // complete Light Client script coverage from their creation blocks. If
        // the explicit peer-funding RPC is configured, only unknown transaction
        // inputs may use it; CellDeps always retain Light Client-only coverage.
        let unpinned_deps = || {
            transaction
                .cell_deps()
                .into_iter()
                .map(|cell_dep| cell_dep.out_point())
                .filter(|out_point| !self.pinned_cell_deps.contains(out_point))
        };
        if self.peer_funding_liveness_rpc.is_some() {
            self.ensure_out_points_liveness_coverage(unpinned_deps(), deadline)?;
        } else {
            self.ensure_out_points_liveness_coverage(
                transaction.input_pts_iter().chain(unpinned_deps()),
                deadline,
            )?;
        }

        let mut prepared = HashMap::<packed::OutPoint, Bytes>::new();
        for input in transaction.input_pts_iter() {
            self.prepare_out_point(
                &input,
                &mut prepared,
                validation_tip,
                deadline,
                true,
                LivenessPolicy::AllowPeerFundingRpc,
            )?;
        }
        for cell_dep in transaction.cell_deps().into_iter() {
            let out_point = cell_dep.out_point();
            let verify_liveness = !self.pinned_cell_deps.contains(&out_point);
            let data = self.prepare_out_point(
                &out_point,
                &mut prepared,
                validation_tip,
                deadline,
                verify_liveness,
                LivenessPolicy::LightClientOnly,
            )?;
            if cell_dep.dep_type() == DepType::DepGroup.into() {
                let dep_group = packed::OutPointVec::from_slice(data.as_ref()).map_err(|err| {
                    transaction_failed_to_resolve(format!(
                        "Resolve failed InvalidDepGroup({out_point:?}): {err}"
                    ))
                })?;
                if dep_group.is_empty() {
                    return Err(transaction_failed_to_resolve(format!(
                        "Resolve failed InvalidDepGroup({out_point:?}): dep group is empty"
                    )));
                }
                if verify_liveness {
                    self.ensure_out_points_liveness_coverage(dep_group.clone(), deadline)?;
                }
                for dep_out_point in dep_group.into_iter() {
                    // A trusted pinned dep-group commits to its member list, so
                    // the same explicit trust decision applies transitively.
                    self.prepare_out_point(
                        &dep_out_point,
                        &mut prepared,
                        validation_tip,
                        deadline,
                        verify_liveness,
                        LivenessPolicy::LightClientOnly,
                    )?;
                }
            }
        }
        Ok(())
    }

    fn ensure_out_points_liveness_coverage(
        &self,
        out_points: impl IntoIterator<Item = packed::OutPoint>,
        deadline: Instant,
    ) -> Result<()> {
        let mut registered = self
            .chain_service
            .get_scripts()
            .into_iter()
            .map(|status| {
                let script_type = match status.script_type {
                    ScriptType::Lock => IndexedScriptType::Lock,
                    ScriptType::Type => IndexedScriptType::Type,
                };
                (packed::Script::from(status.script), script_type)
            })
            .collect::<HashSet<_>>();
        let mut additions = Vec::new();

        for out_point in out_points {
            let tx_hash: H256 = out_point.tx_hash().unpack();
            let fetched = self.fetch_transaction_until(&tx_hash, deadline)?;
            if !matches!(fetched.tx_status.status, Status::Committed) {
                continue;
            }
            let Some((block_number, _, transaction)) =
                self.storage.get_transaction(&out_point.tx_hash())
            else {
                continue;
            };
            let output_index: u32 = out_point.index().unpack();
            let Some(output) = transaction.raw().outputs().get(output_index as usize) else {
                continue;
            };

            if output_matches_registered_scripts(&output, &registered) {
                continue;
            }
            let lock = output.lock();
            if registered.insert((lock.clone(), IndexedScriptType::Lock)) {
                additions.push((lock.into(), IndexedScriptType::Lock, block_number));
            }
        }

        if !additions.is_empty() {
            debug!(
                script_count = additions.len(),
                "batched Light Client subscriptions needed for transaction liveness proofs"
            );
            self.ensure_script_coverage(additions)
                .map_err(internal_error)?;
        }
        Ok(())
    }

    fn prepare_out_point(
        &self,
        out_point: &packed::OutPoint,
        prepared: &mut HashMap<packed::OutPoint, Bytes>,
        validation_tip: u64,
        deadline: Instant,
        verify_liveness: bool,
        liveness_policy: LivenessPolicy,
    ) -> Result<Bytes> {
        if let Some(data) = prepared.get(out_point) {
            return Ok(data.clone());
        }

        let tx_hash: H256 = out_point.tx_hash().unpack();
        let transaction = self.fetch_transaction_until(&tx_hash, deadline)?;
        let status = transaction.tx_status.status;
        let transaction = transaction.transaction.ok_or_else(|| {
            transaction_failed_to_resolve(format!(
                "Resolve failed Unknown({out_point:?}): producing transaction is not committed or locally pending"
            ))
        })?;
        let transaction: packed::Transaction = transaction.inner.into();
        let output_index: u32 = out_point.index().unpack();
        let output = transaction
            .raw()
            .outputs()
            .get(output_index as usize)
            .ok_or_else(|| {
                transaction_failed_to_resolve(format!(
                    "Resolve failed Unknown({out_point:?}): output index is out of bounds"
                ))
            })?;
        let data = transaction
            .raw()
            .outputs_data()
            .get(output_index as usize)
            .ok_or_else(|| {
                transaction_failed_to_resolve(format!(
                    "Resolve failed Unknown({out_point:?}): output data is missing"
                ))
            })?
            .raw_data();

        match status {
            Status::Pending => {}
            Status::Committed if verify_liveness => self.ensure_committed_cell_live(
                out_point,
                &output,
                validation_tip,
                deadline,
                liveness_policy,
            )?,
            Status::Committed => {
                debug!(?out_point, "using pinned CellDep liveness assertion");
            }
            Status::Unknown => {
                return Err(transaction_failed_to_resolve(format!(
                    "Resolve failed Unknown({out_point:?})"
                )));
            }
        }
        prepared.insert(out_point.clone(), data.clone());
        Ok(data)
    }

    fn ensure_committed_cell_live(
        &self,
        out_point: &packed::OutPoint,
        output: &packed::CellOutput,
        validation_tip: u64,
        deadline: Instant,
        liveness_policy: LivenessPolicy,
    ) -> Result<()> {
        if self.committed_cell_is_live(
            out_point,
            output,
            validation_tip,
            deadline,
            liveness_policy,
        )? {
            Ok(())
        } else {
            Err(transaction_failed_to_resolve(format!(
                "Resolve failed Dead({out_point:?})"
            )))
        }
    }

    fn committed_cell_is_live(
        &self,
        out_point: &packed::OutPoint,
        output: &packed::CellOutput,
        validation_tip: u64,
        deadline: Instant,
        liveness_policy: LivenessPolicy,
    ) -> Result<bool> {
        let Some((block_number, _, _)) = self.storage.get_transaction(&out_point.tx_hash()) else {
            return Err(not_ready(format!(
                "producing transaction for {out_point:?} is not stored yet"
            )));
        };
        let output_index: u32 = out_point.index().unpack();
        let lock_script = output.lock();
        let type_script = output.type_().to_opt();
        let statuses = self.chain_service.get_scripts();
        let lock_script_json: Script = lock_script.clone().into();
        let lock_registered = statuses.iter().any(|status| {
            status.script == lock_script_json && matches!(status.script_type, ScriptType::Lock)
        });
        let type_registered = type_script.as_ref().is_some_and(|type_script| {
            let type_script: Script = type_script.clone().into();
            statuses.iter().any(|status| {
                status.script == type_script && matches!(status.script_type, ScriptType::Type)
            })
        });
        let (indexed_script, indexed_script_type) = if !lock_registered && type_registered {
            (
                type_script.expect("registered type script should exist"),
                IndexedScriptType::Type,
            )
        } else if lock_registered {
            (lock_script, IndexedScriptType::Lock)
        } else {
            if matches!(liveness_policy, LivenessPolicy::AllowPeerFundingRpc) {
                if let Some(rpc) = &self.peer_funding_liveness_rpc {
                    // Only the boolean result crosses this boundary. `output`
                    // was extracted above from the Light Client-verified
                    // producing transaction and is never replaced by RPC data.
                    return rpc.cell_is_live(out_point, deadline).map_err(not_ready);
                }
            }
            return Err(not_ready(format!(
                "cannot prove liveness for {out_point:?}: neither output script is in the configured or tracked Light Client subscription set"
            )));
        };
        let script: Script = indexed_script.clone().into();
        let service_script_type = indexed_script_type.service_type();
        let index_covers_creation =
            self.script_index_covers_creation(&indexed_script, indexed_script_type, block_number)?;
        if !index_covers_creation {
            return Err(not_ready(format!(
                "cannot prove liveness for {out_point:?}: its creation at block {block_number} predates the tracked script coverage; lower ckb_light_client.history_start_block and resynchronize"
            )));
        }

        self.wait_script_ready_until(&script, &service_script_type, validation_tip, deadline)?;
        self.cell_index_contains_out_point(
            &indexed_script,
            indexed_script_type,
            block_number,
            output_index,
            out_point,
        )
    }

    fn script_index_covers_creation(
        &self,
        script: &packed::Script,
        script_type: IndexedScriptType,
        block_number: u64,
    ) -> Result<bool> {
        let key = (script.clone(), script_type);
        let mut starts = self
            .indexed_script_starts
            .lock()
            .map_err(|_| internal_error("indexed script coverage lock is poisoned"))?;
        if starts
            .get(&key)
            .is_some_and(|first_indexed_block| *first_indexed_block <= block_number)
        {
            return Ok(true);
        }

        let script_json: Script = script.clone().into();
        let status = self.chain_service.get_scripts().into_iter().find(|status| {
            status.script == script_json
                && same_script_type(&status.script_type, &script_type.service_type())
        });
        let Some(status) = status else {
            return Ok(false);
        };
        let status_block = status.block_number.value();
        if status_block >= block_number {
            return Ok(false);
        }

        // Seeing an in-progress script below the producing block proves that its
        // contiguous scan starts no later than the next block. Record that
        // conservative boundary while script updates are excluded by the lock.
        let first_indexed_block = if status_block == 0 {
            0
        } else {
            status_block.saturating_add(1)
        };
        starts.insert(key, first_indexed_block);
        Ok(true)
    }

    /// Register scripts while holding the coverage lock across `set_scripts`.
    ///
    /// `SetScriptsCommand::Partial` overwrites a script's stored progress. Keeping
    /// the comparison, write, and in-memory update in one critical section makes
    /// the earliest requested start authoritative even when requests race.
    fn ensure_script_coverage(
        &self,
        scripts: impl IntoIterator<Item = (Script, IndexedScriptType, u64)>,
    ) -> std::result::Result<(), String> {
        let mut requested = HashMap::<(packed::Script, IndexedScriptType), (Script, u64)>::new();
        for (script, script_type, first_indexed_block) in scripts {
            let key = (script.clone().into(), script_type);
            requested
                .entry(key)
                .and_modify(|(_, start)| *start = (*start).min(first_indexed_block))
                .or_insert((script, first_indexed_block));
        }

        let mut starts = self
            .indexed_script_starts
            .lock()
            .map_err(|_| "indexed script coverage lock is poisoned".to_string())?;
        let updates = requested
            .into_iter()
            .filter_map(|(key, (script, requested_start))| {
                let needs_update = starts
                    .get(&key)
                    .is_none_or(|known_start| requested_start < *known_start);
                needs_update.then_some((key, script, requested_start))
            })
            .collect::<Vec<_>>();
        if updates.is_empty() {
            return Ok(());
        }

        self.chain_service.set_scripts(
            updates
                .iter()
                .map(|(key, script, first_indexed_block)| ScriptStatus {
                    script: script.clone(),
                    script_type: match key.1 {
                        IndexedScriptType::Lock => ScriptType::Lock,
                        IndexedScriptType::Type => ScriptType::Type,
                    },
                    block_number: BlockNumber::from(first_indexed_block.saturating_sub(1)),
                })
                .collect(),
            Some(SetScriptsCommand::Partial),
        );
        for (key, script, requested_start) in updates {
            self.write_script_coverage(&script, key.1, requested_start)?;
            starts.insert(key, requested_start);
        }
        Ok(())
    }

    fn read_script_coverage(
        &self,
        script: &Script,
        script_type: IndexedScriptType,
    ) -> std::result::Result<Option<u64>, String> {
        let Some(value) = self
            .storage
            .get(script_coverage_key(script, script_type))
            .map_err(|err| format!("failed to read script coverage: {err}"))?
        else {
            return Ok(None);
        };
        let bytes: [u8; 8] = value
            .as_slice()
            .try_into()
            .map_err(|_| "stored script coverage has an invalid length".to_string())?;
        Ok(Some(u64::from_be_bytes(bytes)))
    }

    fn write_script_coverage(
        &self,
        script: &Script,
        script_type: IndexedScriptType,
        first_indexed_block: u64,
    ) -> std::result::Result<(), String> {
        self.storage
            .put(
                script_coverage_key(script, script_type),
                first_indexed_block.to_be_bytes().to_vec(),
            )
            .map_err(|err| format!("failed to persist script coverage: {err}"))
    }

    fn cell_index_contains(
        &self,
        script: &packed::Script,
        script_type: IndexedScriptType,
        block_number: u64,
        tx_index: u32,
        output_index: u32,
        expected_tx_hash: &packed::Byte32,
    ) -> Result<bool> {
        let key = match script_type {
            IndexedScriptType::Lock => {
                Key::CellLockScript(script, block_number, tx_index, output_index)
            }
            IndexedScriptType::Type => {
                Key::CellTypeScript(script, block_number, tx_index, output_index)
            }
        }
        .into_vec();
        let value = self.storage.get(key).map_err(|err| {
            internal_error(format!("failed to read Light Client UTXO index: {err}"))
        })?;
        Ok(value.as_deref() == Some(expected_tx_hash.as_slice()))
    }

    fn cell_index_contains_out_point(
        &self,
        script: &packed::Script,
        script_type: IndexedScriptType,
        block_number: u64,
        output_index: u32,
        out_point: &packed::OutPoint,
    ) -> Result<bool> {
        // A transaction fetched through a transaction proof is initially stored
        // with an unknown tx index. Filtering its complete block replaces that
        // placeholder with the canonical index, so reload it after the script
        // scan rather than retaining the pre-scan value.
        let Some((stored_block_number, tx_index, _)) =
            self.storage.get_transaction(&out_point.tx_hash())
        else {
            return Err(not_ready(format!(
                "producing transaction for {out_point:?} disappeared during script scan"
            )));
        };
        if stored_block_number != block_number {
            return Err(internal_error(format!(
                "producing transaction for {out_point:?} moved from block {block_number} to {stored_block_number}"
            )));
        }
        self.cell_index_contains(
            script,
            script_type,
            block_number,
            tx_index,
            output_index,
            &out_point.tx_hash(),
        )
    }

    fn fetch_header(&self, hash: &H256) -> Result<Option<HeaderView>> {
        self.fetch_header_until(hash, Instant::now() + REMOTE_DATA_TIMEOUT)
    }

    fn fetch_header_until(&self, hash: &H256, deadline: Instant) -> Result<Option<HeaderView>> {
        if let Some(header) = self.chain_service.get_header(hash) {
            return Ok(Some(header));
        }
        loop {
            match self.chain_service.fetch_header(hash) {
                FetchStatus::Fetched { data } => return Ok(Some(data)),
                FetchStatus::NotFound => return Ok(None),
                FetchStatus::Added { .. } | FetchStatus::Fetching { .. } => {}
            }
            if Instant::now() >= deadline {
                return Err(not_ready(format!("header {hash:#x} is not ready")));
            }
            thread::sleep(Duration::from_millis(100));
        }
    }

    fn fetch_transaction(&self, hash: &H256) -> Result<TransactionWithStatus> {
        self.fetch_transaction_until(hash, Instant::now() + REMOTE_DATA_TIMEOUT)
    }

    fn fetch_transaction_until(
        &self,
        hash: &H256,
        deadline: Instant,
    ) -> Result<TransactionWithStatus> {
        let transaction = self.chain_service.get_transaction(hash);
        if transaction.transaction.is_some() {
            return Ok(transaction);
        }
        loop {
            match self.chain_service.fetch_transaction(hash) {
                FetchStatus::Fetched { data } => return Ok(data),
                FetchStatus::NotFound => return Ok(self.chain_service.get_transaction(hash)),
                FetchStatus::Added { .. } | FetchStatus::Fetching { .. } => {}
            }
            if Instant::now() >= deadline {
                return Err(not_ready(format!("transaction {hash:#x} is not ready")));
            }
            thread::sleep(Duration::from_millis(100));
        }
    }

    fn require_script_registered(&self, search_key: &SearchKey) -> Result<()> {
        let exists = self.chain_service.get_scripts().into_iter().any(|status| {
            status.script == search_key.script
                && same_script_type(&status.script_type, &search_key.script_type)
        });
        if exists {
            return Ok(());
        }
        Err(not_ready(format!(
            "script 0x{} is not in the Light Client subscription set; register it during startup instead of an indexer query",
            hex::encode(
                packed::Script::from(search_key.script.clone())
                    .calc_script_hash()
                    .as_slice()
            )
        )))
    }

    fn wait_script_ready(&self, script: &Script, script_type: &ScriptType) -> Result<()> {
        self.wait_script_ready_until(
            script,
            script_type,
            self.indexed_tip_number(),
            Instant::now() + REMOTE_DATA_TIMEOUT,
        )
    }

    fn wait_script_ready_until(
        &self,
        script: &Script,
        script_type: &ScriptType,
        target_tip: u64,
        deadline: Instant,
    ) -> Result<()> {
        loop {
            let ready = self.chain_service.get_scripts().into_iter().any(|status| {
                status.script == *script
                    && same_script_type(&status.script_type, script_type)
                    && status.block_number.value() >= target_tip
            });
            if ready {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(not_ready(format!(
                    "script 0x{} has not been scanned to operational block {target_tip}",
                    hex::encode(
                        packed::Script::from(script.clone())
                            .calc_script_hash()
                            .as_slice()
                    )
                )));
            }
            thread::sleep(Duration::from_millis(100));
        }
    }

    fn wait_all_scripts_ready(&self) -> Result<()> {
        let deadline = Instant::now() + REMOTE_DATA_TIMEOUT;
        let target_tip = self.indexed_tip_number();
        loop {
            let scripts = self.chain_service.get_scripts();
            if scripts.is_empty() {
                return Err(not_ready(
                    "prefix queries require previously registered complete scripts",
                ));
            }
            let slowest_script = scripts
                .iter()
                .map(|script| script.block_number.value())
                .min()
                .unwrap_or_default();
            if slowest_script >= target_tip {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(not_ready(format!(
                    "a required script is only scanned to block {slowest_script}, operational block is {target_tip}"
                )));
            }
            thread::sleep(Duration::from_millis(100));
        }
    }

    fn register_transaction_output_scripts(
        &self,
        transaction: &Transaction,
        first_required_block: u64,
    ) -> Result<()> {
        let existing = self.chain_service.get_scripts();
        let mut added = Vec::new();

        for output in &transaction.outputs {
            let scripts = std::iter::once((&output.lock, ScriptType::Lock)).chain(
                output
                    .type_
                    .as_ref()
                    .map(|script| (script, ScriptType::Type)),
            );
            for (script, script_type) in scripts {
                let indexed_script_type = match script_type {
                    ScriptType::Lock => IndexedScriptType::Lock,
                    ScriptType::Type => IndexedScriptType::Type,
                };
                let already_registered = existing.iter().any(|status| {
                    status.script == *script && same_script_type(&status.script_type, &script_type)
                }) || added.iter().any(
                    |(added_script, added_type, _): &(Script, IndexedScriptType, u64)| {
                        added_script == script && *added_type == indexed_script_type
                    },
                );
                if !already_registered {
                    added.push((script.clone(), indexed_script_type, first_required_block));
                }
            }
        }

        if !added.is_empty() {
            debug!(
                script_count = added.len(),
                first_required_block,
                "extended Light Client subscriptions along a tracked transaction chain"
            );
            self.ensure_script_coverage(added).map_err(internal_error)?;
        }
        Ok(())
    }

    fn transaction_extends_tracked_chain(&self, transaction: &Transaction) -> bool {
        let registered = self
            .chain_service
            .get_scripts()
            .into_iter()
            .map(|status| {
                let script_type = match status.script_type {
                    ScriptType::Lock => IndexedScriptType::Lock,
                    ScriptType::Type => IndexedScriptType::Type,
                };
                (packed::Script::from(status.script), script_type)
            })
            .collect::<HashSet<_>>();
        let transaction: packed::Transaction = transaction.clone().into();

        transaction.raw().inputs().into_iter().any(|input| {
            let previous = input.previous_output();
            let output_index: u32 = previous.index().unpack();
            self.storage
                .get_transaction(&previous.tx_hash())
                .and_then(|(_, _, transaction)| {
                    transaction.raw().outputs().get(output_index as usize)
                })
                .is_some_and(|output| output_matches_registered_scripts(&output, &registered))
        })
    }
}

fn script_coverage_key(script: &Script, script_type: IndexedScriptType) -> Vec<u8> {
    let packed: packed::Script = script.clone().into();
    let mut key = Vec::with_capacity(SCRIPT_COVERAGE_KEY_PREFIX.len() + 33);
    key.extend_from_slice(SCRIPT_COVERAGE_KEY_PREFIX);
    key.push(match script_type {
        IndexedScriptType::Lock => 0,
        IndexedScriptType::Type => 1,
    });
    key.extend_from_slice(packed.calc_script_hash().as_slice());
    key
}

fn output_matches_registered_scripts(
    output: &packed::CellOutput,
    registered: &HashSet<(packed::Script, IndexedScriptType)>,
) -> bool {
    registered.contains(&(output.lock(), IndexedScriptType::Lock))
        || output
            .type_()
            .to_opt()
            .is_some_and(|script| registered.contains(&(script, IndexedScriptType::Type)))
}

fn reconcile_required_scripts<'a>(
    existing: Vec<ScriptStatus>,
    requested: impl IntoIterator<Item = &'a (Script, IndexedScriptType, u64)>,
    preserve_progress: bool,
) -> (Vec<ScriptStatus>, bool) {
    let mut required: HashMap<(packed::Script, IndexedScriptType), ScriptStatus> =
        if preserve_progress {
            existing
                .iter()
                .map(|status| {
                    let script_type = match status.script_type {
                        ScriptType::Lock => IndexedScriptType::Lock,
                        ScriptType::Type => IndexedScriptType::Type,
                    };
                    let copied = ScriptStatus {
                        script: status.script.clone(),
                        script_type: script_type.service_type(),
                        block_number: BlockNumber::from(status.block_number.value()),
                    };
                    ((status.script.clone().into(), script_type), copied)
                })
                .collect::<HashMap<_, _>>()
        } else {
            HashMap::new()
        };
    let mut set_changed = false;

    for (script, script_type, first_indexed_block) in requested {
        let key = (script.clone().into(), *script_type);
        required.entry(key).or_insert_with(|| {
            set_changed = true;
            ScriptStatus {
                script: script.clone(),
                script_type: script_type.service_type(),
                block_number: BlockNumber::from(first_indexed_block.saturating_sub(1)),
            }
        });
    }

    if !preserve_progress {
        set_changed = existing.len() != required.len()
            || existing.iter().any(|status| {
                let script_type = match status.script_type {
                    ScriptType::Lock => IndexedScriptType::Lock,
                    ScriptType::Type => IndexedScriptType::Type,
                };
                !required.contains_key(&(status.script.clone().into(), script_type))
            });
    }
    (required.into_values().collect(), set_changed)
}

fn required_scripts_marker(requested: &[(Script, IndexedScriptType, u64)]) -> Vec<u8> {
    let mut entries = requested
        .iter()
        .map(|(script, script_type, first_indexed_block)| {
            let packed: packed::Script = script.clone().into();
            let mut entry = Vec::with_capacity(packed.as_slice().len() + 13);
            entry.push(match script_type {
                IndexedScriptType::Lock => 0,
                IndexedScriptType::Type => 1,
            });
            entry.extend_from_slice(&first_indexed_block.to_be_bytes());
            entry.extend_from_slice(&(packed.as_slice().len() as u32).to_be_bytes());
            entry.extend_from_slice(packed.as_slice());
            entry
        })
        .collect::<Vec<_>>();
    entries.sort_unstable();
    entries.dedup();

    let mut marker = Vec::new();
    for entry in entries {
        marker.extend_from_slice(&(entry.len() as u32).to_be_bytes());
        marker.extend_from_slice(&entry);
    }
    marker
}

/// Resolve the epoch query used by ckb-sdk's `DefaultCellCollector`.
///
/// If any proved local header belongs to the requested epoch, it contains the
/// exact epoch start and length. Otherwise, the only supported historical
/// query is the maturity target derived from the current tip. For that query a
/// one-block synthetic epoch makes ckb-sdk calculate the greatest block number
/// of a locally indexed mature header. This is selection-equivalent for Fiber:
/// every candidate returned by the Light Client index has its creating block
/// header stored, while an as-yet unindexed candidate is conservatively skipped
/// until the next collection attempt.
fn resolve_epoch_view_for_fiber(
    requested: u64,
    tip: &HeaderView,
    cellbase_maturity: EpochNumberWithFraction,
    stored_headers: &[HeaderView],
) -> std::result::Result<Option<EpochView>, String> {
    let tip_epoch = EpochNumberWithFraction::from_full_value(tip.inner.epoch.value());
    if requested > tip_epoch.number() {
        return Ok(None);
    }

    if let Some(header) = stored_headers.iter().find(|header| {
        EpochNumberWithFraction::from_full_value(header.inner.epoch.value()).number() == requested
    }) {
        return Ok(Some(epoch_view_from_header(header)));
    }

    let tip_epoch_rational = tip_epoch.to_rational();
    let maturity_rational = cellbase_maturity.to_rational();
    if tip_epoch_rational < maturity_rational {
        return Err(format!(
            "epoch {requested} is not represented by a proved local header"
        ));
    }
    let mature_epoch = tip_epoch_rational - maturity_rational;
    let mature_epoch_number = u64::from_le_bytes(
        mature_epoch.clone().into_u256().to_le_bytes()[..8]
            .try_into()
            .expect("epoch number fits into u64"),
    );
    if requested != mature_epoch_number {
        return Err(format!(
            "epoch {requested} is not represented by a proved local header"
        ));
    }

    let mature_header = stored_headers
        .iter()
        .filter(|header| {
            EpochNumberWithFraction::from_full_value(header.inner.epoch.value()).to_rational()
                <= mature_epoch
        })
        .max_by_key(|header| header.inner.number.value())
        .ok_or_else(|| {
            format!("no proved local header can establish maturity for epoch {requested}")
        })?;

    Ok(Some(EpochView {
        number: EpochNumber::from(requested),
        start_number: mature_header.inner.number,
        length: BlockNumber::from(1u64),
        compact_target: mature_header.inner.compact_target,
    }))
}

fn epoch_view_from_header(header: &HeaderView) -> EpochView {
    let epoch = EpochNumberWithFraction::from_full_value(header.inner.epoch.value());
    EpochView {
        number: EpochNumber::from(epoch.number()),
        start_number: BlockNumber::from(header.inner.number.value().saturating_sub(epoch.index())),
        length: BlockNumber::from(epoch.length()),
        compact_target: header.inner.compact_target,
    }
}

fn cell_with_status(
    status: &str,
    cell: Option<CellInfo>,
    block_hash: Option<H256>,
) -> CellWithStatus {
    CellWithStatus {
        cell,
        status: status.to_string(),
        block_hash,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SearchMode {
    Prefix,
    Exact,
    Partial,
}

fn search_mode(search_key: &Value) -> SearchMode {
    match search_key
        .get("script_search_mode")
        .and_then(Value::as_str)
        .unwrap_or("prefix")
    {
        "exact" => SearchMode::Exact,
        "partial" => SearchMode::Partial,
        _ => SearchMode::Prefix,
    }
}

fn route_method(method: &str) -> Route {
    if LOCAL_METHODS.contains(&method) {
        Route::Local
    } else {
        Route::Unsupported
    }
}

fn positional(params: Params) -> Result<Vec<Value>> {
    match params {
        Params::Array(values) => Ok(values),
        Params::None => Ok(Vec::new()),
        Params::Map(_) => Err(invalid_params(
            "only positional CKB RPC parameters are supported",
        )),
    }
}

fn required_param<T: DeserializeOwned>(params: &[Value], index: usize, name: &str) -> Result<T> {
    let value = params
        .get(index)
        .cloned()
        .ok_or_else(|| invalid_params(format!("missing {name}")))?;
    decode(value, name)
}

fn optional_param<T: DeserializeOwned>(params: &[Value], index: usize) -> Result<Option<T>> {
    match params.get(index) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => serde_json::from_value(value.clone())
            .map(Some)
            .map_err(|err| invalid_params(err.to_string())),
    }
}

fn decode<T: DeserializeOwned>(value: Value, name: &str) -> Result<T> {
    serde_json::from_value(value).map_err(|err| invalid_params(format!("invalid {name}: {err}")))
}

fn to_value(value: impl serde::Serialize) -> Result<Value> {
    serde_json::to_value(value).map_err(|err| internal_error(err.to_string()))
}

fn validate_limit(limit: u32) -> Result<()> {
    if limit == 0 || limit > MAX_QUERY_LIMIT {
        return Err(invalid_params(format!(
            "limit must be between 1 and {MAX_QUERY_LIMIT}"
        )));
    }
    Ok(())
}

/// Fill one exact-match page from a source whose primary-script lookup is only
/// prefix-aware. Each source request is capped at the number of exact results
/// still needed, so the returned cursor never skips an unreturned exact match.
fn collect_exact_page<T>(
    limit: u32,
    after: Option<JsonBytes>,
    mut fetch: impl FnMut(Uint32, Option<JsonBytes>) -> Result<Pagination<T>>,
    mut exact_match: impl FnMut(T) -> Option<T>,
) -> Result<Pagination<T>> {
    let mut objects = Vec::with_capacity(limit as usize);
    let mut cursor = after;
    let mut last_cursor = cursor
        .clone()
        .unwrap_or_else(|| JsonBytes::from_vec(Vec::new()));

    while objects.len() < limit as usize {
        let remaining = limit - objects.len() as u32;
        let page = fetch(Uint32::from(remaining), cursor.clone())?;
        let source_is_exhausted = page.objects.is_empty();
        let next_cursor = page.last_cursor;
        let cursor_advanced = cursor.as_ref() != Some(&next_cursor);

        if !source_is_exhausted {
            last_cursor = next_cursor.clone();
        }
        objects.extend(page.objects.into_iter().filter_map(&mut exact_match));

        if source_is_exhausted {
            break;
        }
        if !cursor_advanced {
            return Err(internal_error(
                "Light Client prefix pagination cursor did not advance",
            ));
        }
        cursor = Some(next_cursor);
    }

    Ok(Pagination {
        objects,
        last_cursor,
    })
}

fn copy_script_type(script_type: &ScriptType) -> ScriptType {
    match script_type {
        ScriptType::Lock => ScriptType::Lock,
        ScriptType::Type => ScriptType::Type,
    }
}

fn same_script_type(left: &ScriptType, right: &ScriptType) -> bool {
    matches!(
        (left, right),
        (ScriptType::Lock, ScriptType::Lock) | (ScriptType::Type, ScriptType::Type)
    )
}

fn cell_matches_script(
    output: &ckb_jsonrpc_types::CellOutput,
    script: &Script,
    script_type: &ScriptType,
) -> bool {
    match script_type {
        ScriptType::Lock => output.lock == *script,
        ScriptType::Type => output.type_.as_ref() == Some(script),
    }
}

fn filter_exact_transaction(
    transaction: Tx,
    script: &Script,
    script_type: &ScriptType,
    storage: &Storage,
) -> Option<Tx> {
    match transaction {
        Tx::Ungrouped(transaction) => transaction_cell_matches(
            &transaction.transaction,
            &transaction.io_type,
            transaction.io_index.value(),
            script,
            script_type,
            storage,
        )
        .then_some(Tx::Ungrouped(transaction)),
        Tx::Grouped(mut transaction) => {
            transaction.cells.retain(|(io_type, io_index)| {
                transaction_cell_matches(
                    &transaction.transaction,
                    io_type,
                    io_index.value(),
                    script,
                    script_type,
                    storage,
                )
            });
            (!transaction.cells.is_empty()).then_some(Tx::Grouped(transaction))
        }
    }
}

fn transaction_cell_matches(
    transaction: &TransactionView,
    io_type: &CellType,
    io_index: u32,
    script: &Script,
    script_type: &ScriptType,
    storage: &Storage,
) -> bool {
    let output = match io_type {
        CellType::Output => transaction.inner.outputs.get(io_index as usize).cloned(),
        CellType::Input => transaction
            .inner
            .inputs
            .get(io_index as usize)
            .and_then(|input| {
                let previous_output = &input.previous_output;
                packed::Byte32::from_slice(previous_output.tx_hash.as_bytes())
                    .ok()
                    .and_then(|tx_hash| storage.get_transaction(&tx_hash))
                    .and_then(|(_, _, transaction)| {
                        Transaction::from(transaction)
                            .outputs
                            .get(previous_output.index.value() as usize)
                            .cloned()
                    })
            }),
    };
    output
        .as_ref()
        .is_some_and(|output| cell_matches_script(output, script, script_type))
}

fn indexer_transactions_to_value(page: Pagination<Tx>) -> Result<Value> {
    let objects = page
        .objects
        .into_iter()
        .map(|transaction| {
            let mut value =
                serde_json::to_value(transaction).map_err(|err| internal_error(err.to_string()))?;
            let object = value
                .as_object_mut()
                .ok_or_else(|| internal_error("invalid Light Client transaction response"))?;
            let transaction = object
                .remove("transaction")
                .ok_or_else(|| internal_error("Light Client transaction has no transaction"))?;
            let tx_hash = transaction
                .get("hash")
                .cloned()
                .ok_or_else(|| internal_error("Light Client transaction has no hash"))?;
            object.insert("tx_hash".to_string(), tx_hash);
            Ok(value)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(json!({
        "objects": objects,
        "last_cursor": page.last_cursor,
    }))
}

fn transaction_to_full_node_value(
    transaction: TransactionWithStatus,
    tx_hash: &H256,
    verbosity: u32,
    only_committed: bool,
    storage: &Storage,
    chain_service: &LightClientChainService,
) -> Result<Value> {
    let committed = matches!(transaction.tx_status.status, Status::Committed);
    let transaction_json = if only_committed && !committed {
        None
    } else {
        transaction.transaction
    };
    let block_hash = if only_committed && !committed {
        None
    } else {
        transaction.tx_status.block_hash
    };
    let status = if only_committed && !committed {
        "unknown"
    } else {
        match transaction.tx_status.status {
            Status::Pending => "pending",
            Status::Committed => "committed",
            Status::Unknown => "unknown",
        }
    };

    let (block_number, tx_index) = if committed {
        storage
            .get_transaction(
                &packed::Byte32::from_slice(tx_hash.as_bytes()).map_err(|err| {
                    internal_error(format!("invalid transaction hash from Light Client: {err}"))
                })?,
            )
            .map(|(number, index, _)| (Some(BlockNumber::from(number)), Some(Uint32::from(index))))
            .unwrap_or((None, None))
    } else {
        (None, None)
    };

    let transaction_value = match (verbosity, transaction_json) {
        (_, None) | (1, _) => Value::Null,
        (0, Some(transaction)) => {
            let packed: packed::Transaction = transaction.inner.into();
            to_value(JsonBytes::from_bytes(packed.as_bytes()))?
        }
        (2, Some(transaction)) => to_value(transaction)?,
        _ => Value::Null,
    };

    if let (Some(block_hash), None) = (&block_hash, &block_number) {
        warn!(
            tx_hash = format_args!("{tx_hash:#x}"),
            block_hash = format_args!("{block_hash:#x}"),
            "committed Light Client transaction is missing its local block number"
        );
        let _ = chain_service.get_header(block_hash);
    }

    Ok(json!({
        "transaction": transaction_value,
        "cycles": transaction.cycles,
        "time_added_to_pool": null,
        "tx_status": {
            "status": status,
            "block_number": block_number,
            "block_hash": block_hash,
            "tx_index": tx_index,
            "reason": null,
        },
        "fee": null,
        "min_replace_fee": null,
    }))
}

fn invalid_params(message: impl Into<String>) -> Error {
    Error::invalid_params(message.into())
}

fn find_recent_block_hash(
    block_number: u64,
    headers: impl IntoIterator<Item = (u64, packed::Byte32)>,
) -> Option<packed::Byte32> {
    headers
        .into_iter()
        .find_map(|(number, hash)| (number == block_number).then_some(hash))
}

fn not_ready(message: impl Into<String>) -> Error {
    Error {
        code: ErrorCode::ServerError(NOT_READY_ERROR),
        message: message.into(),
        data: None,
    }
}

fn unsupported(message: impl Into<String>) -> Error {
    Error {
        code: ErrorCode::ServerError(UNSUPPORTED_ERROR),
        message: message.into(),
        data: None,
    }
}

fn transaction_failed_to_resolve(message: impl Into<String>) -> Error {
    transaction_error(
        TRANSACTION_FAILED_TO_RESOLVE,
        "TransactionFailedToResolve",
        message,
    )
}

fn transaction_failed_to_verify(message: impl Into<String>) -> Error {
    transaction_error(
        TRANSACTION_FAILED_TO_VERIFY,
        "TransactionFailedToVerify",
        message,
    )
}

fn map_send_transaction_error(err: ckb_light_client_lib::error::Error) -> Error {
    let message = err.to_string();
    if message.contains("OutPoint(") {
        transaction_failed_to_resolve(message)
    } else if message.to_ascii_lowercase().contains("low fee rate") {
        transaction_error(
            POOL_REJECTED_TRANSACTION_BY_MIN_FEE_RATE,
            "PoolRejectedTransactionByMinFeeRate",
            message,
        )
    } else {
        transaction_failed_to_verify(message)
    }
}

fn indexed_tip_number(network_tip: u64, scripts: &[ScriptStatus]) -> u64 {
    scripts
        .iter()
        .map(|script| script.block_number.value())
        .min()
        .unwrap_or(network_tip)
        .min(network_tip)
}

fn transaction_error(code: i64, name: &'static str, message: impl Into<String>) -> Error {
    let message = message.into();
    Error {
        code: ErrorCode::ServerError(code),
        message: format!("{name}: {message}"),
        data: Some(Value::String(message)),
    }
}

fn internal_error(message: impl Into<String>) -> Error {
    Error {
        code: ErrorCode::InternalError,
        message: message.into(),
        data: None,
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, net::SocketAddr, time::Duration};

    use ckb_jsonrpc_types::{BlockNumber, JsonBytes, Script};
    use ckb_light_client_lib::service::{Pagination, ScriptStatus, ScriptType};
    use ckb_types::{
        core::{EpochNumberWithFraction, HeaderBuilder},
        packed,
        prelude::{Builder, Entity, Pack},
    };
    use jsonrpc_core::{ErrorCode, IoHandler};
    use jsonrpc_http_server::ServerBuilder;

    use super::{
        collect_exact_page, find_recent_block_hash, indexed_tip_number,
        interpret_external_liveness_status, map_send_transaction_error,
        output_matches_registered_scripts, reconcile_required_scripts,
        resolve_epoch_view_for_fiber, route_method, IndexedScriptType, PeerFundingLivenessRpc,
        PendingInputReservations, Route, POOL_REJECTED_TRANSACTION_BY_MIN_FEE_RATE,
        TRANSACTION_FAILED_TO_RESOLVE, TRANSACTION_FAILED_TO_VERIFY,
    };

    fn script(tag: u8) -> Script {
        packed::Script::new_builder()
            .code_hash(packed::Byte32::new([tag; 32]))
            .build()
            .into()
    }

    fn status(script: Script, block_number: u64) -> ScriptStatus {
        ScriptStatus {
            script,
            script_type: ScriptType::Lock,
            block_number: BlockNumber::from(block_number),
        }
    }

    fn header(
        number: u64,
        epoch_number: u64,
        epoch_index: u64,
        epoch_length: u64,
    ) -> ckb_jsonrpc_types::HeaderView {
        HeaderBuilder::default()
            .number(number)
            .epoch(
                EpochNumberWithFraction::new(epoch_number, epoch_index, epoch_length).full_value(),
            )
            .compact_target(0x1e08_3126u32)
            .build()
            .into()
    }

    #[test]
    fn routing_is_fixed_before_execution() {
        assert_eq!(route_method("get_cells"), Route::Local);
        assert_eq!(route_method("get_epoch_by_number"), Route::Local);
        assert_eq!(route_method("get_live_cell"), Route::Local);
        assert_eq!(route_method("send_transaction"), Route::Local);
        assert_eq!(route_method("set_scripts"), Route::Unsupported);
    }

    #[test]
    fn external_liveness_reference_only_accepts_live() {
        assert_eq!(interpret_external_liveness_status("live"), Ok(true));
        assert_eq!(interpret_external_liveness_status("dead"), Ok(false));
        assert_eq!(interpret_external_liveness_status("unknown"), Ok(false));
        assert!(interpret_external_liveness_status("pending").is_err());
    }

    #[test]
    fn peer_funding_liveness_rpc_checks_chain_and_only_uses_status() {
        let genesis_hash = ckb_types::H256::from([7u8; 32]);
        let mut handler = IoHandler::new();
        let returned_genesis_hash = genesis_hash.clone();
        handler.add_sync_method("get_block_hash", move |_| {
            Ok(serde_json::json!(returned_genesis_hash))
        });
        handler.add_sync_method("get_live_cell", |_| {
            // A live status without Cell contents is deliberately enough: the
            // production path obtains those contents from the Light Client.
            Ok(serde_json::json!({
                "cell": null,
                "status": "live",
                "block_hash": null
            }))
        });
        let server = ServerBuilder::new(handler)
            .threads(1)
            .start_http(&"127.0.0.1:0".parse::<SocketAddr>().unwrap())
            .unwrap();
        let rpc = PeerFundingLivenessRpc::new(format!("http://{}", server.address()), genesis_hash)
            .unwrap();
        let out_point = packed::OutPoint::new_builder()
            .tx_hash(packed::Byte32::new([9u8; 32]))
            .index(0u32)
            .build();

        assert!(rpc
            .cell_is_live(
                &out_point,
                std::time::Instant::now() + Duration::from_secs(2)
            )
            .unwrap());

        let wrong_chain_rpc = PeerFundingLivenessRpc::new(
            format!("http://{}", server.address()),
            ckb_types::H256::from([8u8; 32]),
        )
        .unwrap();
        assert!(wrong_chain_rpc
            .cell_is_live(
                &out_point,
                std::time::Instant::now() + Duration::from_secs(2)
            )
            .unwrap_err()
            .contains("wrong chain"));
        server.close();
    }

    #[test]
    fn recent_verified_headers_resolve_unmatched_block_numbers() {
        let expected = packed::Byte32::new([2; 32]);
        let headers = vec![
            (100, packed::Byte32::new([1; 32])),
            (101, expected.clone()),
            (102, packed::Byte32::new([3; 32])),
        ];

        assert_eq!(find_recent_block_hash(101, headers.clone()), Some(expected));
        assert_eq!(find_recent_block_hash(99, headers), None);
    }

    #[test]
    fn operational_tip_is_the_slowest_complete_script_height() {
        let scripts = vec![status(script(1), 120), status(script(2), 114)];

        assert_eq!(indexed_tip_number(121, &scripts), 114);
        assert_eq!(indexed_tip_number(121, &[]), 121);
        assert_eq!(indexed_tip_number(121, &[status(script(3), 130)]), 121);
    }

    #[test]
    fn required_script_reconciliation_keeps_persisted_progress() {
        let required = script(1);
        let requested = [(required.clone(), IndexedScriptType::Lock, 1_000)];
        let (reconciled, replace) =
            reconcile_required_scripts(vec![status(required, 2_000)], requested.iter(), true);

        assert!(!replace);
        assert_eq!(reconciled.len(), 1);
        assert_eq!(reconciled[0].block_number.value(), 2_000);
    }

    #[test]
    fn matching_required_marker_preserves_controlled_discovered_scripts() {
        let required = script(1);
        let discovered = script(2);
        let requested = [(required.clone(), IndexedScriptType::Lock, 1_000)];
        let (reconciled, replace) = reconcile_required_scripts(
            vec![
                status(required.clone(), 2_000),
                status(discovered.clone(), 500),
            ],
            requested.iter(),
            true,
        );

        assert!(!replace);
        assert_eq!(reconciled.len(), 2);
        assert!(reconciled.iter().any(|status| status.script == required));
        assert!(reconciled.iter().any(|status| status.script == discovered));
    }

    #[test]
    fn mismatched_required_marker_discards_old_discovered_scripts() {
        let required = script(1);
        let stale = script(2);
        let requested = [(required.clone(), IndexedScriptType::Lock, 1_000)];
        let (reconciled, replace) = reconcile_required_scripts(
            vec![status(required.clone(), 2_000), status(stale, 500)],
            requested.iter(),
            false,
        );

        assert!(replace);
        assert_eq!(reconciled.len(), 1);
        assert_eq!(reconciled[0].script, required);
        assert_eq!(reconciled[0].block_number.value(), 999);
    }

    #[test]
    fn required_script_reconciliation_adds_missing_scripts_at_requested_start() {
        let required = script(1);
        let requested = [(required.clone(), IndexedScriptType::Lock, 1_000)];
        let (reconciled, replace) = reconcile_required_scripts(Vec::new(), requested.iter(), false);

        assert!(replace);
        assert_eq!(reconciled.len(), 1);
        assert_eq!(reconciled[0].script, required);
        assert_eq!(reconciled[0].block_number.value(), 999);
    }

    #[test]
    fn required_script_reconciliation_rescans_an_unmarked_cache() {
        let required = script(1);
        let requested = [(required.clone(), IndexedScriptType::Lock, 1_000)];
        let (reconciled, replace) =
            reconcile_required_scripts(vec![status(required, 2_000)], requested.iter(), false);

        assert!(!replace);
        assert_eq!(reconciled[0].block_number.value(), 999);
    }

    #[test]
    fn tracked_output_match_accepts_registered_lock_or_type_only() {
        let lock = packed::Script::from(script(1));
        let type_ = packed::Script::from(script(2));
        let output = packed::CellOutput::new_builder()
            .lock(lock.clone())
            .type_(Some(type_.clone()).pack())
            .build();

        assert!(output_matches_registered_scripts(
            &output,
            &HashSet::from([(lock, IndexedScriptType::Lock)])
        ));
        assert!(output_matches_registered_scripts(
            &output,
            &HashSet::from([(type_, IndexedScriptType::Type)])
        ));
        assert!(!output_matches_registered_scripts(&output, &HashSet::new()));
    }

    #[test]
    fn epoch_query_returns_exact_view_from_a_proved_header() {
        let tip = header(105, 10, 5, 10);
        let proved = header(63, 6, 3, 10);

        let epoch =
            resolve_epoch_view_for_fiber(6, &tip, EpochNumberWithFraction::new(4, 0, 1), &[proved])
                .unwrap()
                .unwrap();

        assert_eq!(epoch.number.value(), 6);
        assert_eq!(epoch.start_number.value(), 60);
        assert_eq!(epoch.length.value(), 10);
        assert_eq!(epoch.compact_target.value(), 0x1e08_3126);
    }

    #[test]
    fn maturity_epoch_without_a_header_uses_the_latest_proved_mature_block() {
        let tip = header(105, 10, 5, 10);
        let latest_proved_mature = header(59, 5, 9, 10);
        let proved_but_immature = header(70, 7, 0, 10);

        let epoch = resolve_epoch_view_for_fiber(
            6,
            &tip,
            EpochNumberWithFraction::new(4, 0, 1),
            &[latest_proved_mature, proved_but_immature],
        )
        .unwrap()
        .unwrap();

        // ckb-sdk computes start + floor(fraction * length). A length of one
        // therefore gives exactly the latest proved mature block, 59.
        assert_eq!(epoch.number.value(), 6);
        assert_eq!(epoch.start_number.value(), 59);
        assert_eq!(epoch.length.value(), 1);
    }

    #[test]
    fn epoch_query_does_not_invent_unrelated_history_or_future_epochs() {
        let tip = header(105, 10, 5, 10);
        let proved = header(59, 5, 9, 10);
        let maturity = EpochNumberWithFraction::new(4, 0, 1);

        assert!(
            resolve_epoch_view_for_fiber(4, &tip, maturity, std::slice::from_ref(&proved)).is_err()
        );
        assert_eq!(
            resolve_epoch_view_for_fiber(11, &tip, maturity, &[proved]).unwrap(),
            None
        );
    }

    #[test]
    fn pending_input_reservations_are_idempotent_and_detect_conflicts() {
        let input = packed::OutPoint::new(packed::Byte32::new([1; 32]), 0);
        let first = packed::Byte32::new([2; 32]);
        let second = packed::Byte32::new([3; 32]);
        let mut reservations = PendingInputReservations::default();

        reservations.reserve(std::slice::from_ref(&input), &first);
        assert!(reservations
            .conflicting_owner(std::slice::from_ref(&input), &first)
            .is_none());
        assert_eq!(
            reservations
                .conflicting_owner(std::slice::from_ref(&input), &second)
                .map(|(_, owner)| owner),
            Some(first.clone())
        );

        reservations.retain(|owner| owner != &first);
        assert!(reservations
            .conflicting_owner(std::slice::from_ref(&input), &second)
            .is_none());
    }

    #[test]
    fn exact_pagination_continues_past_prefix_only_results() {
        let source = [1u8, 2, 3, 4];
        let mut requested_limits = Vec::new();
        let page = collect_exact_page(
            2,
            None,
            |limit, after| {
                requested_limits.push(limit.value());
                let start = after
                    .as_ref()
                    .and_then(|cursor| cursor.as_bytes().first())
                    .copied()
                    .unwrap_or_default() as usize;
                let end = (start + limit.value() as usize).min(source.len());
                Ok(Pagination {
                    objects: source[start..end].to_vec(),
                    last_cursor: JsonBytes::from_vec(vec![end as u8]),
                })
            },
            |value| (value % 2 == 0).then_some(value),
        )
        .unwrap();

        assert_eq!(page.objects, vec![2, 4]);
        assert_eq!(page.last_cursor.as_bytes(), &[4]);
        assert_eq!(requested_limits, vec![2, 1, 1]);
    }

    #[test]
    fn light_client_verification_errors_use_ckb_rpc_codes() {
        let resolve = map_send_transaction_error(ckb_light_client_lib::error::Error::runtime(
            "invalid transaction: OutPoint(Unknown(...))",
        ));
        assert_eq!(
            resolve.code,
            ErrorCode::ServerError(TRANSACTION_FAILED_TO_RESOLVE)
        );

        let low_fee = map_send_transaction_error(ckb_light_client_lib::error::Error::runtime(
            "Transaction rejected by low fee rate",
        ));
        assert_eq!(
            low_fee.code,
            ErrorCode::ServerError(POOL_REJECTED_TRANSACTION_BY_MIN_FEE_RATE)
        );

        let verify = map_send_transaction_error(ckb_light_client_lib::error::Error::runtime(
            "invalid transaction: Script(Error {...})",
        ));
        assert_eq!(
            verify.code,
            ErrorCode::ServerError(TRANSACTION_FAILED_TO_VERIFY)
        );
    }
}
