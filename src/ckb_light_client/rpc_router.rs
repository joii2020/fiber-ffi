use std::{
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use ckb_chain_spec::consensus::Consensus;
use ckb_jsonrpc_types::{
    BlockNumber, HeaderView, JsonBytes, Script, Transaction, TransactionView, Uint32, Uint64,
};
use ckb_light_client_lib::{
    service::{
        CellType, FetchStatus, LightClientChainService, LightClientService, Pagination,
        ScriptStatus, ScriptType, SearchKey, SetScriptsCommand, Status, TransactionWithStatus, Tx,
    },
    storage::{LightClientStorage, Storage, StorageWithChainData},
};
use ckb_types::{
    packed,
    prelude::{Entity, Unpack},
    H256,
};
use jsonrpc_core::{Error, ErrorCode, Params, Result};
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use tracing::{debug, warn};

use super::config::REMOTE_DATA_TIMEOUT;

const NOT_READY_ERROR: i64 = -32010;
const UNSUPPORTED_ERROR: i64 = -32011;
const UPSTREAM_ERROR: i64 = -32012;
const MAX_QUERY_LIMIT: u32 = 1_000;

const LOCAL_METHODS: &[&str] = &[
    "get_cells",
    "get_transactions",
    "get_tip_header",
    "get_tip_block_number",
    "get_indexer_tip",
    "get_consensus",
    "get_block_by_number",
    "get_header",
    "get_header_by_number",
    "get_block_median_time",
    "get_transaction",
];

const UPSTREAM_METHODS: &[&str] = &["get_epoch_by_number", "get_live_cell", "send_transaction"];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Route {
    Local,
    Upstream,
    Unsupported,
}

#[derive(Clone)]
pub(crate) struct RpcRouter {
    upstream_rpc_url: String,
    storage: Arc<Storage>,
    chain_service: LightClientChainService,
    consensus: Arc<Consensus>,
    history_start_block: u64,
    upstream_client: Client,
    runtime_handle: tokio::runtime::Handle,
}

impl RpcRouter {
    pub(crate) fn new(
        upstream_rpc_url: String,
        storage: Storage,
        chain_data: StorageWithChainData,
        consensus: Arc<Consensus>,
        history_start_block: u64,
    ) -> std::result::Result<Self, String> {
        let upstream_client = Client::builder()
            .timeout(REMOTE_DATA_TIMEOUT)
            .no_proxy()
            .build()
            .map_err(|err| format!("failed to create CKB RPC upstream client: {err}"))?;
        let chain_service =
            LightClientChainService::new(chain_data.clone(), Arc::clone(&consensus));

        Ok(Self {
            upstream_rpc_url,
            storage: Arc::new(storage),
            chain_service,
            consensus,
            history_start_block,
            upstream_client,
            runtime_handle: tokio::runtime::Handle::current(),
        })
    }

    pub(crate) fn methods() -> impl Iterator<Item = &'static str> {
        LOCAL_METHODS.iter().chain(UPSTREAM_METHODS.iter()).copied()
    }

    pub(crate) fn tip_header(&self) -> HeaderView {
        self.chain_service.get_tip_header()
    }

    pub(crate) fn script_statuses(&self) -> Vec<ScriptStatus> {
        self.chain_service.get_scripts()
    }

    pub(crate) fn register_scripts(
        &self,
        scripts: impl IntoIterator<Item = ScriptStatus>,
    ) -> std::result::Result<(), String> {
        let scripts = scripts.into_iter().collect::<Vec<_>>();
        if !scripts.is_empty() {
            self.chain_service
                .set_scripts(scripts, Some(SetScriptsCommand::Partial));
        }
        Ok(())
    }

    pub(crate) fn handle(&self, method: &'static str, params: Params) -> Result<Value> {
        let started = Instant::now();
        let route = route_method(method);
        let result = match route {
            Route::Local => self.handle_local(method, params),
            Route::Upstream => self.forward_upstream(method, params),
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
            "get_tip_header" => to_value(self.chain_service.get_tip_header()),
            "get_tip_block_number" => to_value(self.chain_service.get_tip_header().inner.number),
            "get_indexer_tip" => self.get_indexer_tip(),
            "get_consensus" => to_value(ckb_jsonrpc_types::Consensus::from(
                (*self.consensus).clone(),
            )),
            "get_block_by_number" => self.get_block_by_number(params),
            "get_header" => self.get_header(params),
            "get_header_by_number" => self.get_header_by_number(params),
            "get_block_median_time" => self.get_block_median_time(params),
            "get_transaction" => self.get_transaction(params),
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
                self.ensure_script_registered(&search_key, self.history_start_block)?;
                self.wait_script_ready(&search_key.script, &search_key.script_type)?;
            }
            SearchMode::Prefix => self.ensure_all_scripts_ready()?,
            SearchMode::Partial => {
                return Err(unsupported(
                    "partial indexer search is not supported by the embedded Light Client",
                ));
            }
        }

        let service = LightClientService::new(Arc::clone(&self.storage));
        let page = service
            .get_cells(
                search_key,
                decode(order_value, "order")?,
                Uint32::from(limit),
                after,
            )
            .map_err(|err| invalid_params(err.to_string()))?;

        if search_mode == SearchMode::Exact {
            let script = decode::<SearchKey>(search_value, "search_key")?.script;
            let script_type = decode::<SearchKey>(
                required_param::<Value>(&params, 0, "search_key")?,
                "search_key",
            )?
            .script_type;
            let objects = page
                .objects
                .into_iter()
                .filter(|cell| cell_matches_script(&cell.output, &script, &script_type))
                .collect();
            return to_value(Pagination {
                objects,
                last_cursor: page.last_cursor,
            });
        }

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
        let search_key = decode::<SearchKey>(search_value, "search_key")?;
        let exact_match = (mode == SearchMode::Exact).then(|| {
            (
                search_key.script.clone(),
                copy_script_type(&search_key.script_type),
            )
        });
        match mode {
            SearchMode::Exact => {
                self.ensure_script_registered(&search_key, self.history_start_block)?;
                self.wait_script_ready(&search_key.script, &search_key.script_type)?;
            }
            SearchMode::Prefix => self.ensure_all_scripts_ready()?,
            SearchMode::Partial => {
                return Err(unsupported(
                    "partial indexer search is not supported by the embedded Light Client",
                ));
            }
        }

        let mut page = LightClientService::new(Arc::clone(&self.storage))
            .get_transactions(
                search_key,
                decode(order_value, "order")?,
                Uint32::from(limit),
                after,
            )
            .map_err(|err| invalid_params(err.to_string()))?;
        if let Some((script, script_type)) = exact_match {
            page.objects = page
                .objects
                .into_iter()
                .filter_map(|transaction| {
                    filter_exact_transaction(transaction, &script, &script_type, &self.storage)
                })
                .collect();
        }
        indexer_transactions_to_value(page)
    }

    fn get_indexer_tip(&self) -> Result<Value> {
        let scripts = self.chain_service.get_scripts();
        let block_number = scripts
            .iter()
            .map(|script| script.block_number.value())
            .min()
            .unwrap_or_else(|| self.chain_service.get_tip_header().inner.number.value());
        let block_hash = self.storage.get_block_hash(block_number).ok_or_else(|| {
            not_ready(format!("block hash at height {block_number} is not ready"))
        })?;
        to_value(json!({
            "block_hash": H256::from(block_hash),
            "block_number": BlockNumber::from(block_number),
        }))
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
        let Some(hash) = self.storage.get_block_hash(number) else {
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
                let packed_hash = packed::Byte32::from_slice(hash.as_bytes()).map_err(|err| {
                    internal_error(format!("invalid transaction hash from Light Client: {err}"))
                })?;
                if let Some((block_number, _, _)) = self.storage.get_transaction(&packed_hash) {
                    self.register_transaction_output_scripts(&transaction_view.inner, block_number);
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

    fn fetch_header(&self, hash: &H256) -> Result<Option<HeaderView>> {
        if let Some(header) = self.chain_service.get_header(hash) {
            return Ok(Some(header));
        }
        let deadline = Instant::now() + REMOTE_DATA_TIMEOUT;
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
        let transaction = self.chain_service.get_transaction(hash);
        if transaction.transaction.is_some() {
            return Ok(transaction);
        }

        let deadline = Instant::now() + REMOTE_DATA_TIMEOUT;
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

    fn ensure_script_registered(&self, search_key: &SearchKey, start_block: u64) -> Result<()> {
        let exists = self.chain_service.get_scripts().into_iter().any(|status| {
            status.script == search_key.script
                && same_script_type(&status.script_type, &search_key.script_type)
        });
        if exists {
            return Ok(());
        }

        self.chain_service.set_scripts(
            vec![ScriptStatus {
                script: search_key.script.clone(),
                script_type: copy_script_type(&search_key.script_type),
                block_number: BlockNumber::from(start_block.saturating_sub(1)),
            }],
            Some(SetScriptsCommand::Partial),
        );
        Ok(())
    }

    fn wait_script_ready(&self, script: &Script, script_type: &ScriptType) -> Result<()> {
        let deadline = Instant::now() + REMOTE_DATA_TIMEOUT;
        loop {
            let tip = self.chain_service.get_tip_header().inner.number.value();
            let ready = self.chain_service.get_scripts().into_iter().any(|status| {
                status.script == *script
                    && same_script_type(&status.script_type, script_type)
                    && status.block_number.value() >= tip
            });
            if ready {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(not_ready(format!(
                    "script 0x{} has not been scanned to the Light Client tip",
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

    fn ensure_all_scripts_ready(&self) -> Result<()> {
        let scripts = self.chain_service.get_scripts();
        if scripts.is_empty() {
            return Err(not_ready(
                "prefix queries require previously registered complete scripts",
            ));
        }
        let tip = self.chain_service.get_tip_header().inner.number.value();
        if let Some(script) = scripts
            .into_iter()
            .find(|script| script.block_number.value() < tip)
        {
            return Err(not_ready(format!(
                "a required script is only scanned to block {}, Light Client tip is {tip}",
                script.block_number.value()
            )));
        }
        Ok(())
    }

    fn register_transaction_output_scripts(
        &self,
        transaction: &Transaction,
        first_required_block: u64,
    ) {
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
                let already_registered =
                    existing
                        .iter()
                        .chain(added.iter())
                        .any(|status: &ScriptStatus| {
                            status.script == *script
                                && same_script_type(&status.script_type, &script_type)
                        });
                if !already_registered {
                    added.push(ScriptStatus {
                        script: script.clone(),
                        script_type,
                        block_number: BlockNumber::from(first_required_block.saturating_sub(1)),
                    });
                }
            }
        }

        if !added.is_empty() {
            debug!(
                script_count = added.len(),
                first_required_block, "registered scripts discovered in a transaction"
            );
            self.chain_service
                .set_scripts(added, Some(SetScriptsCommand::Partial));
        }
    }

    fn forward_upstream(&self, method: &str, params: Params) -> Result<Value> {
        let submitted_transaction = if method == "send_transaction" {
            match &params {
                Params::Array(values) => values
                    .first()
                    .and_then(|value| serde_json::from_value::<Transaction>(value.clone()).ok()),
                Params::Map(_) | Params::None => None,
            }
        } else {
            None
        };
        let params = match params {
            Params::Array(values) => Value::Array(values),
            Params::Map(values) => Value::Object(values),
            Params::None => Value::Array(Vec::new()),
        };
        let response = self
            .runtime_handle
            .block_on(
                self.upstream_client
                    .post(&self.upstream_rpc_url)
                    .json(&json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": method,
                        "params": params,
                    }))
                    .send(),
            )
            .and_then(|response| response.error_for_status())
            .map_err(|err| upstream_error(format!("upstream {method} request failed: {err}")))?;
        let response = self
            .runtime_handle
            .block_on(response.json::<Value>())
            .map_err(|err| upstream_error(format!("invalid upstream {method} response: {err}")))?;

        if let Some(error) = response.get("error") {
            let code = error
                .get("code")
                .and_then(Value::as_i64)
                .unwrap_or(UPSTREAM_ERROR);
            return Err(Error {
                code: ErrorCode::ServerError(code),
                message: error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("upstream CKB RPC error")
                    .to_string(),
                data: error.get("data").cloned(),
            });
        }
        let result = response
            .get("result")
            .cloned()
            .ok_or_else(|| upstream_error(format!("upstream {method} response has no result")))?;
        if let Some(transaction) = submitted_transaction {
            let first_required_block = self
                .chain_service
                .get_tip_header()
                .inner
                .number
                .value()
                .saturating_add(1);
            self.register_transaction_output_scripts(&transaction, first_required_block);
        }
        Ok(result)
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
    } else if UPSTREAM_METHODS.contains(&method) {
        Route::Upstream
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

fn upstream_error(message: impl Into<String>) -> Error {
    Error {
        code: ErrorCode::ServerError(UPSTREAM_ERROR),
        message: message.into(),
        data: None,
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
    use super::{route_method, Route};

    #[test]
    fn routing_is_fixed_before_execution() {
        assert_eq!(route_method("get_cells"), Route::Local);
        assert_eq!(route_method("get_epoch_by_number"), Route::Upstream);
        assert_eq!(route_method("send_transaction"), Route::Upstream);
        assert_eq!(route_method("set_scripts"), Route::Unsupported);
    }
}
