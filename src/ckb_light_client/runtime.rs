use std::{
    collections::HashSet,
    fs,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use ckb_async_runtime::Handle as CkbRuntimeHandle;
use ckb_chain_spec::{consensus::Consensus, ChainSpec};
use ckb_jsonrpc_types::{BlockNumber, Script};
use ckb_light_client_lib::{
    protocols::{
        FilterProtocol, LightClientProtocol, Peers, PendingTxs, RelayProtocol, SyncProtocol,
        BAD_MESSAGE_ALLOWED_EACH_HOUR, CHECK_POINT_INTERVAL,
    },
    service::{ScriptStatus, ScriptType},
    storage::{LightClientStorage, Storage, StorageWithChainData},
};
use ckb_network::{
    network::TransportType, CKBProtocol, CKBProtocolHandler, Flags, NetworkController,
    NetworkService, NetworkState, SupportProtocols,
};
use ckb_resource::Resource;
use ckb_stop_handler::{broadcast_exit_signals, has_received_stop_signal};
use ckb_types::packed;
use jsonrpc_http_server::Server;
use tokio::sync::mpsc;
use tracing::{debug, info};

use super::{
    config::{LocalChain, LocalLightClientConfig, HEADER_READY_TIMEOUT},
    rpc_router::RpcRouter,
    rpc_server,
};

static LOCAL_LIGHT_CLIENT_ACTIVE: AtomicBool = AtomicBool::new(false);
const MAX_TIP_AGE: Duration = Duration::from_secs(2 * 60 * 60);
const READINESS_POLL_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CkbPrepareStatus {
    Initializing,
    WalletBirthday {
        address: String,
        history_start_block: u64,
        source: String,
    },
    Connecting {
        connected_peers: usize,
        required_peers: usize,
        tip_block_number: u64,
        tip_is_current: bool,
    },
    SyncingHeaders {
        connected_peers: usize,
        required_peers: usize,
        tip_block_number: u64,
        tip_is_current: bool,
    },
    SyncingScripts {
        tip_block_number: u64,
        slowest_script_block_number: u64,
        script_count: usize,
    },
}

pub(crate) type CkbPrepareStatusReporter<'a> = &'a (dyn Fn(CkbPrepareStatus) + Send + Sync);

#[derive(Debug)]
pub(crate) struct RequiredScript {
    pub(crate) script: Script,
    pub(crate) script_type: ScriptType,
    pub(crate) start_block: u64,
}

impl RequiredScript {
    pub(crate) fn lock(script: Script, start_block: u64) -> Self {
        Self {
            script,
            script_type: ScriptType::Lock,
            start_block,
        }
    }

    pub(crate) fn type_(script: Script, start_block: u64) -> Self {
        Self {
            script,
            script_type: ScriptType::Type,
            start_block,
        }
    }

    fn into_status(self) -> ScriptStatus {
        ScriptStatus {
            script: self.script,
            script_type: self.script_type,
            block_number: BlockNumber::from(registration_height(self.start_block)),
        }
    }
}

pub(crate) struct LocalCkbNodeHandle {
    rpc_url: String,
    rpc_server: Option<Server>,
    monitor: LocalCkbMonitor,
    runtime_handle: CkbRuntimeHandle,
    runtime_stop_rx: mpsc::Receiver<()>,
    _network_controller: NetworkController,
}

#[derive(Clone)]
pub(crate) struct LocalCkbMonitor {
    router: RpcRouter,
    operational_lag_tolerance: u64,
}

impl LocalCkbMonitor {
    pub(crate) fn sync_snapshot(&self) -> (u64, u64, u64) {
        let tip = self.router.tip_header();
        (
            tip.inner.number.value(),
            tip.inner.timestamp.value(),
            self.router.indexed_tip_number(),
        )
    }

    pub(crate) fn operational_lag_tolerance(&self) -> u64 {
        self.operational_lag_tolerance
    }
}

impl LocalCkbNodeHandle {
    pub(crate) async fn start(
        config: LocalLightClientConfig,
        required_scripts: Vec<RequiredScript>,
        pinned_cell_deps: HashSet<packed::OutPoint>,
        status_reporter: Option<CkbPrepareStatusReporter<'_>>,
    ) -> Result<Self, String> {
        if has_received_stop_signal() {
            return Err(
                "the embedded CKB Light Client cannot be restarted after it has stopped"
                    .to_string(),
            );
        }
        if LOCAL_LIGHT_CLIENT_ACTIVE
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err("only one embedded CKB Light Client can run in this process".to_string());
        }

        match Self::start_inner(config, required_scripts, pinned_cell_deps, status_reporter).await {
            Ok(handle) => Ok(handle),
            Err(err) => {
                LOCAL_LIGHT_CLIENT_ACTIVE.store(false, Ordering::SeqCst);
                Err(err)
            }
        }
    }

    async fn start_inner(
        config: LocalLightClientConfig,
        required_scripts: Vec<RequiredScript>,
        pinned_cell_deps: HashSet<packed::OutPoint>,
        status_reporter: Option<CkbPrepareStatusReporter<'_>>,
    ) -> Result<Self, String> {
        fs::create_dir_all(&config.store_path).map_err(|err| {
            format!(
                "failed to create CKB Light Client store directory {}: {err}",
                config.store_path.display()
            )
        })?;
        fs::create_dir_all(&config.network_path).map_err(|err| {
            format!(
                "failed to create CKB Light Client network directory {}: {err}",
                config.network_path.display()
            )
        })?;

        let storage = Storage::new(&config.store_path);
        let consensus = Arc::new(load_consensus(&config.chain)?);
        storage.init_genesis_block(consensus.genesis_block().data());
        storage.cleanup_invalid_matched_blocks();

        let pending_txs = Arc::new(RwLock::new(PendingTxs::default()));
        let peers = Arc::new(Peers::new(
            config.network_config()?.max_outbound_peers,
            CHECK_POINT_INTERVAL,
            storage.get_last_check_point(),
            BAD_MESSAGE_ALLOWED_EACH_HOUR,
        ));
        let network_state = NetworkState::from_config(config.network_config()?)
            .map(|state| {
                Arc::new(state.required_flags(
                    Flags::DISCOVERY
                        | Flags::SYNC
                        | Flags::RELAY
                        | Flags::LIGHT_CLIENT
                        | Flags::BLOCK_FILTER,
                ))
            })
            .map_err(|err| format!("failed to initialize CKB Light Client network: {err}"))?;

        let protocols = build_protocols(
            Arc::clone(&network_state),
            storage.clone(),
            Arc::clone(&peers),
            Arc::clone(&pending_txs),
            Arc::clone(&consensus),
        );
        let required_protocol_ids = vec![
            SupportProtocols::Sync.protocol_id(),
            SupportProtocols::LightClient.protocol_id(),
            SupportProtocols::Filter.protocol_id(),
        ];

        let chain_data = StorageWithChainData::new(
            storage.clone(),
            Arc::clone(&peers),
            Arc::clone(&pending_txs),
        );
        let router = RpcRouter::new(
            storage,
            chain_data,
            Arc::clone(&consensus),
            pinned_cell_deps,
            config.peer_funding_liveness_rpc_url.clone(),
        )?;
        let startup_script_lag_tolerance = config.startup_script_lag_tolerance;
        let operational_lag_tolerance = config.operational_lag_tolerance;
        let startup_min_peers = config.startup_min_peers;
        router.register_scripts(
            required_scripts
                .into_iter()
                .map(RequiredScript::into_status),
        )?;

        let rpc_server = rpc_server::start(
            LocalLightClientConfig::local_rpc_listen_address(),
            router.clone(),
        )?;

        let (runtime_guard_tx, mut runtime_stop_rx) = mpsc::channel(1);
        let mut runtime_handle =
            CkbRuntimeHandle::new(tokio::runtime::Handle::current(), Some(runtime_guard_tx));
        let network_controller = match NetworkService::new(
            Arc::clone(&network_state),
            protocols,
            required_protocol_ids,
            (
                consensus.identify_name(),
                env!("CARGO_PKG_VERSION").to_string(),
                Flags::DISCOVERY,
            ),
            TransportType::Tcp,
        )
        .start(&runtime_handle)
        {
            Ok(controller) => controller,
            Err(err) => {
                close_rpc_server(rpc_server);
                return Err(format!("failed to start CKB Light Client network: {err}"));
            }
        };

        if let Err(err) = wait_until_ready(
            &router,
            &network_controller,
            startup_min_peers,
            startup_script_lag_tolerance,
            status_reporter,
        )
        .await
        {
            close_rpc_server(rpc_server);
            broadcast_exit_signals();
            runtime_handle.drop_guard();
            let _ = tokio::time::timeout(Duration::from_secs(10), runtime_stop_rx.recv()).await;
            return Err(err);
        }

        let rpc_url = format!("http://{}", rpc_server.address());
        info!(rpc_url, "embedded CKB Light Client RPC gateway started");

        Ok(Self {
            rpc_url,
            rpc_server: Some(rpc_server),
            monitor: LocalCkbMonitor {
                router,
                operational_lag_tolerance,
            },
            runtime_handle,
            runtime_stop_rx,
            _network_controller: network_controller,
        })
    }

    pub(crate) fn rpc_url(&self) -> &str {
        &self.rpc_url
    }

    pub(crate) fn monitor(&self) -> LocalCkbMonitor {
        self.monitor.clone()
    }

    pub(crate) async fn shutdown(mut self) {
        if let Some(server) = self.rpc_server.take() {
            close_rpc_server(server);
        }

        broadcast_exit_signals();
        self.runtime_handle.drop_guard();
        match tokio::time::timeout(Duration::from_secs(10), self.runtime_stop_rx.recv()).await {
            Ok(_) => debug!("embedded CKB Light Client tasks stopped"),
            Err(_) => debug!("timed out waiting for embedded CKB Light Client tasks to stop"),
        }
        LOCAL_LIGHT_CLIENT_ACTIVE.store(false, Ordering::SeqCst);
    }
}

async fn wait_until_ready(
    router: &RpcRouter,
    network_controller: &NetworkController,
    startup_min_peers: usize,
    startup_script_lag_tolerance: u64,
    status_reporter: Option<CkbPrepareStatusReporter<'_>>,
) -> Result<(), String> {
    let target_tip = wait_chain_ready(
        router,
        network_controller,
        startup_min_peers,
        status_reporter,
    )
    .await?;
    wait_required_scripts(
        router,
        target_tip,
        startup_script_lag_tolerance,
        status_reporter,
    )
    .await;
    Ok(())
}

async fn wait_chain_ready(
    router: &RpcRouter,
    network_controller: &NetworkController,
    required_peers: usize,
    status_reporter: Option<CkbPrepareStatusReporter<'_>>,
) -> Result<u64, String> {
    let deadline = tokio::time::Instant::now() + HEADER_READY_TIMEOUT;
    let mut last_progress_log = tokio::time::Instant::now() - Duration::from_secs(5);

    loop {
        let connected_peers = network_controller.connected_peers().len();
        let tip = router.tip_header();
        let tip_number = tip.inner.number.value();
        let tip_is_current = tip_number > 0 && header_is_current(tip.inner.timestamp.value());

        if connected_peers >= required_peers && tip_is_current {
            info!(
                connected_peers,
                tip_number, "embedded CKB Light Client chain is ready"
            );
            return Ok(tip_number);
        }

        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "embedded CKB Light Client chain was not ready within {}s: connected peers {connected_peers}/{required_peers}, tip {tip_number}, current tip {tip_is_current}",
                HEADER_READY_TIMEOUT.as_secs()
            ));
        }

        if last_progress_log.elapsed() >= Duration::from_secs(5) {
            if let Some(report_status) = status_reporter {
                let status = if connected_peers < required_peers {
                    CkbPrepareStatus::Connecting {
                        connected_peers,
                        required_peers,
                        tip_block_number: tip_number,
                        tip_is_current,
                    }
                } else {
                    CkbPrepareStatus::SyncingHeaders {
                        connected_peers,
                        required_peers,
                        tip_block_number: tip_number,
                        tip_is_current,
                    }
                };
                report_status(status);
            }
            info!(
                connected_peers,
                required_peers,
                tip_number,
                tip_is_current,
                "waiting for embedded CKB Light Client chain readiness"
            );
            last_progress_log = tokio::time::Instant::now();
        }
        tokio::time::sleep(READINESS_POLL_INTERVAL).await;
    }
}

async fn wait_required_scripts(
    router: &RpcRouter,
    target_tip: u64,
    startup_script_lag_tolerance: u64,
    status_reporter: Option<CkbPrepareStatusReporter<'_>>,
) {
    let mut last_progress_log = tokio::time::Instant::now() - Duration::from_secs(5);

    loop {
        let scripts = router.script_statuses();
        let slowest_script = scripts
            .iter()
            .map(|status| status.block_number.value())
            .min()
            .unwrap_or_default();
        let required_height =
            startup_required_script_height(target_tip, startup_script_lag_tolerance);
        let scripts_are_ready = !scripts.is_empty() && slowest_script >= required_height;

        if scripts_are_ready {
            info!(
                target_tip,
                slowest_script,
                startup_script_lag_tolerance,
                script_count = scripts.len(),
                "embedded CKB Light Client is ready"
            );
            return;
        }

        if last_progress_log.elapsed() >= Duration::from_secs(5) {
            if let Some(report_status) = status_reporter {
                report_status(CkbPrepareStatus::SyncingScripts {
                    tip_block_number: target_tip,
                    slowest_script_block_number: slowest_script,
                    script_count: scripts.len(),
                });
            }
            info!(
                target_tip,
                slowest_script,
                script_count = scripts.len(),
                "waiting for embedded CKB Light Client required scripts"
            );
            last_progress_log = tokio::time::Instant::now();
        }
        tokio::time::sleep(READINESS_POLL_INTERVAL).await;
    }
}

fn startup_required_script_height(target_tip: u64, lag_tolerance: u64) -> u64 {
    target_tip.saturating_sub(lag_tolerance)
}

fn header_is_current(timestamp_millis: u64) -> bool {
    let now_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    now_millis.abs_diff(timestamp_millis) <= MAX_TIP_AGE.as_millis() as u64
}

impl Drop for LocalCkbNodeHandle {
    fn drop(&mut self) {
        if let Some(server) = self.rpc_server.take() {
            close_rpc_server(server);
        }
        broadcast_exit_signals();
        self.runtime_handle.drop_guard();
        LOCAL_LIGHT_CLIENT_ACTIVE.store(false, Ordering::SeqCst);
    }
}

fn close_rpc_server(server: Server) {
    let close_thread = std::thread::Builder::new()
        .name("ckb-light-client-rpc-close".to_string())
        .spawn(move || server.close())
        .expect("the embedded CKB RPC close thread can be created");
    if close_thread.join().is_err() {
        debug!("embedded CKB RPC close thread panicked");
    }
}

fn registration_height(first_required_block: u64) -> u64 {
    first_required_block.saturating_sub(1)
}

fn load_consensus(chain: &LocalChain) -> Result<Consensus, String> {
    let resource = match chain {
        LocalChain::Mainnet => Resource::bundled("specs/mainnet.toml".to_string()),
        LocalChain::Testnet => Resource::bundled("specs/testnet.toml".to_string()),
        LocalChain::Custom(path) => Resource::file_system(path.clone()),
    };
    ChainSpec::load_from(&resource)
        .map_err(|err| format!("failed to load CKB Light Client chain spec: {err}"))?
        .build_consensus()
        .map_err(|err| format!("failed to build CKB Light Client consensus: {err}"))
}

fn build_protocols(
    network_state: Arc<NetworkState>,
    storage: Storage,
    peers: Arc<Peers>,
    pending_txs: Arc<RwLock<PendingTxs>>,
    consensus: Arc<Consensus>,
) -> Vec<CKBProtocol> {
    let sync = SyncProtocol::new(storage.clone(), Arc::clone(&peers));
    let relay = RelayProtocol::new(pending_txs, Arc::clone(&peers), storage.clone());
    let light_client: Box<dyn CKBProtocolHandler> = Box::new(LightClientProtocol::new(
        storage.clone(),
        Arc::clone(&peers),
        (*consensus).clone(),
    ));
    let filter = FilterProtocol::new(storage, peers);

    vec![
        CKBProtocol::new_with_support_protocol(
            SupportProtocols::Sync,
            Box::new(sync),
            Arc::clone(&network_state),
        ),
        CKBProtocol::new_with_support_protocol(
            SupportProtocols::RelayV3,
            Box::new(relay),
            Arc::clone(&network_state),
        ),
        CKBProtocol::new_with_support_protocol(
            SupportProtocols::LightClient,
            light_client,
            Arc::clone(&network_state),
        ),
        CKBProtocol::new_with_support_protocol(
            SupportProtocols::Filter,
            Box::new(filter),
            network_state,
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::{registration_height, startup_required_script_height};

    #[test]
    fn registration_includes_first_required_block() {
        assert_eq!(registration_height(0), 0);
        assert_eq!(registration_height(1), 0);
        assert_eq!(registration_height(42), 41);
    }

    #[test]
    fn startup_script_lag_tolerance_has_an_inclusive_saturating_boundary() {
        assert_eq!(startup_required_script_height(1_000, 100), 900);
        assert_eq!(startup_required_script_height(52, 100), 0);
        assert_eq!(startup_required_script_height(0, 100), 0);
    }
}
