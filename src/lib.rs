use std::{
    ffi::{CStr, CString},
    fs::File,
    io::BufReader,
    os::raw::{c_char, c_void},
    panic::{catch_unwind, AssertUnwindSafe},
    path::{Path, PathBuf},
    ptr,
    sync::{mpsc as std_mpsc, Mutex, Once},
    thread::{self, JoinHandle},
};

use ckb_chain_spec::ChainSpec;
use ckb_resource::Resource;
use clap_serde_derive::ClapSerde;
use fnn::{
    actors::RootActor,
    ckb::{
        client::CkbRpcClient,
        contracts::{try_init_contracts_context, TypeIDResolver},
        CkbChainActor, CkbConfig,
    },
    fiber::{graph::NetworkGraph, network::init_chain_hash},
    start_network, Config, FiberConfig, NetworkServiceEvent,
};
use ractor::{Actor, ActorRef};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, info, trace};
use tracing_subscriber::EnvFilter;

#[cfg(feature = "watchtower")]
use fnn::watchtower::{
    WatchtowerActor, WatchtowerMessage, DEFAULT_WATCHTOWER_CHECK_INTERVAL_SECONDS,
};
#[cfg(feature = "watchtower")]
use std::time::Duration;

#[repr(C)]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum FiberFfiStatus {
    Ok = 0,
    NullPointer = 1,
    InvalidArgument = 2,
    StartupFailed = 3,
    AlreadyStopped = 4,
    Panic = 5,
}

#[repr(C)]
pub struct FiberFfiResult {
    pub status: FiberFfiStatus,
    pub error_message: *mut c_char,
}

pub type FiberEventCallback = unsafe extern "C" fn(*const c_char, *mut c_void);

#[repr(C)]
pub struct FiberStartOptions {
    pub config_path: *const c_char,
    pub database_prefix: *const c_char,
    pub log_level: *const c_char,
    pub event_callback: Option<FiberEventCallback>,
    pub event_callback_user_data: *mut c_void,
}

#[derive(Copy, Clone)]
struct EventCallback {
    callback: FiberEventCallback,
    user_data: usize,
}

unsafe impl Send for EventCallback {}
unsafe impl Sync for EventCallback {}

pub struct FiberHandle {
    stop_tx: Mutex<Option<oneshot::Sender<()>>>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

enum StartupMessage {
    Started,
    Failed(String),
}

static INIT_LOGGING: Once = Once::new();

#[no_mangle]
pub extern "C" fn fiber_version() -> *const c_char {
    concat!(env!("CARGO_PKG_VERSION"), "\0").as_ptr() as *const c_char
}

#[no_mangle]
pub unsafe extern "C" fn fiber_start(
    options: *const FiberStartOptions,
    out_handle: *mut *mut FiberHandle,
) -> FiberFfiResult {
    ffi_boundary(|| {
        if options.is_null() || out_handle.is_null() {
            return Ok(result(
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
        let thread = thread::Builder::new()
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

                runtime.block_on(async move {
                    match start_node(config_path, database_prefix, callback).await {
                        Ok(node) => {
                            let _ = startup_tx.send(StartupMessage::Started);
                            stop_node_on_signal(node, stop_rx).await;
                        }
                        Err(err) => {
                            let _ = startup_tx.send(StartupMessage::Failed(err));
                        }
                    }
                });
            })
            .map_err(|err| ffi_error(FiberFfiStatus::StartupFailed, err.to_string()))?;

        match startup_rx.recv() {
            Ok(StartupMessage::Started) => {
                let handle = Box::new(FiberHandle {
                    stop_tx: Mutex::new(Some(stop_tx)),
                    thread: Mutex::new(Some(thread)),
                });
                *out_handle = Box::into_raw(handle);
                Ok(ok())
            }
            Ok(StartupMessage::Failed(err)) => {
                let _ = thread.join();
                Ok(result(FiberFfiStatus::StartupFailed, err))
            }
            Err(err) => {
                let _ = thread.join();
                Ok(result(
                    FiberFfiStatus::StartupFailed,
                    format!("runtime thread exited before reporting startup status: {err}"),
                ))
            }
        }
    })
}

#[no_mangle]
pub unsafe extern "C" fn fiber_stop(handle: *mut FiberHandle) -> FiberFfiResult {
    ffi_boundary(|| {
        if handle.is_null() {
            return Ok(result(
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
            return Ok(result(
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

        Ok(ok())
    })
}

#[no_mangle]
pub unsafe extern "C" fn fiber_free_string(value: *mut c_char) {
    if !value.is_null() {
        let _ = CString::from_raw(value);
    }
}

struct RunningNode {
    root_actor: ActorRef<String>,
    root_token: CancellationToken,
    root_tracker: TaskTracker,
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
}

fn parse_config_from_path(
    config_path: &str,
    database_prefix: Option<String>,
) -> std::result::Result<Config, String> {
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

    Ok(Config {
        fiber,
        disabled_fiber,
        cch: None,
        rpc: None,
        ckb,
        base_dir,
        check_validate: false,
    })
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

async fn start_node(
    config_path: String,
    database_prefix: Option<String>,
    callback: Option<EventCallback>,
) -> std::result::Result<RunningNode, String> {
    info!(
        "Starting node with git version {} ({})",
        fnn::get_git_version(),
        fnn::get_git_commit_info()
    );

    let config = parse_config_from_path(&config_path, database_prefix)?;
    let fiber_config = config
        .fiber
        .clone()
        .ok_or_else(|| "fiber service must be enabled in config services".to_string())?;
    let ckb_config = config
        .ckb
        .clone()
        .ok_or_else(|| "service fiber requires service ckb to be enabled".to_string())?;

    let store = fnn::store::open_store_with_migration(
        fiber_config.store_path(),
        Box::new(ffi_confirm),
        Box::new(ffi_progress),
    )
    .map_err(|err| err.to_string())?;

    let root_tracker = TaskTracker::new();
    let root_token = CancellationToken::new();
    let root_actor = RootActor::start(root_tracker.clone(), root_token.clone()).await;

    let chain_spec = ChainSpec::load_from(&match fiber_config.chain.as_str() {
        "mainnet" => Resource::bundled("specs/mainnet.toml".to_string()),
        "testnet" => Resource::bundled("specs/testnet.toml".to_string()),
        path => Resource::file_system(Path::new(&config.base_dir).join(path)),
    })
    .map_err(|err| format!("failed to load chain spec: {err}"))?;
    let genesis_block = chain_spec
        .build_genesis()
        .map_err(|err| format!("failed to build ckb genesis block: {err}"))?;

    init_chain_hash(genesis_block.hash().into());
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
    let _network_actor = start_network(
        fiber_config.clone(),
        chain_client,
        ckb_chain_actor,
        event_sender,
        root_tracker.clone(),
        root_actor.get_cell(),
        store.clone(),
        network_graph,
        default_shutdown_script,
    )
    .await;

    #[cfg(feature = "watchtower")]
    let watchtower_actor = if fiber_config.disable_built_in_watchtower.unwrap_or_default() {
        None
    } else {
        let actor = Actor::spawn_linked(
            Some("watchtower".to_string()),
            WatchtowerActor::new(store),
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
    })
}

async fn stop_node_on_signal(node: RunningNode, stop_rx: oneshot::Receiver<()>) {
    let _ = stop_rx.await;
    node.root_token.cancel();
    node.root_actor
        .stop(Some("fiber_stop requested".to_string()));
    node.root_tracker.close();
    node.root_tracker.wait().await;
    debug!("fiber ffi runtime stopped");
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

fn required_string(ptr: *const c_char, name: &str) -> Result<String, FiberFfiResult> {
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

fn optional_string(ptr: *const c_char) -> Result<Option<String>, FiberFfiResult> {
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

fn init_logging(log_level: &str) {
    let filter = EnvFilter::try_new(log_level).unwrap_or_else(|_| EnvFilter::new("info"));
    INIT_LOGGING.call_once(|| {
        let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
    });
}

fn ffi_boundary(f: impl FnOnce() -> Result<FiberFfiResult, FiberFfiResult>) -> FiberFfiResult {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(result)) => result,
        Ok(Err(result)) => result,
        Err(_) => result(FiberFfiStatus::Panic, "fiber ffi call panicked"),
    }
}

fn ok() -> FiberFfiResult {
    FiberFfiResult {
        status: FiberFfiStatus::Ok,
        error_message: ptr::null_mut(),
    }
}

fn result(status: FiberFfiStatus, message: impl Into<String>) -> FiberFfiResult {
    FiberFfiResult {
        status,
        error_message: string_to_c(message.into()),
    }
}

fn ffi_error(status: FiberFfiStatus, message: impl Into<String>) -> FiberFfiResult {
    result(status, message)
}

fn string_to_c(message: String) -> *mut c_char {
    let sanitized = message.replace('\0', "\\0");
    CString::new(sanitized)
        .expect("sanitized string contains no interior nul")
        .into_raw()
}
