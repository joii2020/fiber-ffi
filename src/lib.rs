use std::{
    cell::RefCell,
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
    fiber::{NetworkActorCommand, NetworkActorMessage},
    start_network, Config, FiberConfig, NetworkServiceEvent,
};
use ractor::{Actor, ActorRef};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::json;
use tentacle::utils::TransportType;
use tokio::runtime::Handle as TokioHandle;
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

pub type FiberEventCallback = unsafe extern "C" fn(*const c_char, *mut c_void);

#[repr(C)]
pub struct FiberStartOptions {
    pub config_path: *const c_char,
    pub database_prefix: *const c_char,
    pub log_level: *const c_char,
    pub event_callback: Option<FiberEventCallback>,
    pub event_callback_user_data: *mut c_void,
}

#[repr(C)]
pub struct FiberConnectPeerOptions {
    pub address: *const c_char,
    pub pubkey: *const c_char,
    pub addr_type: *const c_char,
    pub save: i32,
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
    runtime_handle: TokioHandle,
    network_actor: ActorRef<NetworkActorMessage>,
    store: fnn::store::Store,
    fiber_config: FiberConfig,
}

enum StartupMessage {
    Started {
        runtime_handle: TokioHandle,
        network_actor: ActorRef<NetworkActorMessage>,
        store: fnn::store::Store,
        fiber_config: FiberConfig,
    },
    Failed(String),
}

static INIT_LOGGING: Once = Once::new();

thread_local! {
    static LAST_ERROR: RefCell<Option<String>> = const { RefCell::new(None) };
}

#[no_mangle]
pub extern "C" fn fiber_version() -> *const c_char {
    concat!(env!("CARGO_PKG_VERSION"), "\0").as_ptr() as *const c_char
}

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

                let runtime_handle = runtime.handle().clone();
                runtime.block_on(async move {
                    match start_node(config_path, database_prefix, callback).await {
                        Ok(node) => {
                            let network_actor = node.network_actor.clone();
                            let store = node.store.clone();
                            let fiber_config = node.fiber_config.clone();
                            let _ = startup_tx.send(StartupMessage::Started {
                                runtime_handle,
                                network_actor,
                                store,
                                fiber_config,
                            });
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
            Ok(StartupMessage::Started {
                runtime_handle,
                network_actor,
                store,
                fiber_config,
            }) => {
                let handle = Box::new(FiberHandle {
                    stop_tx: Mutex::new(Some(stop_tx)),
                    thread: Mutex::new(Some(thread)),
                    runtime_handle,
                    network_actor,
                    store,
                    fiber_config,
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

#[no_mangle]
pub unsafe extern "C" fn fiber_open_channel(
    handle: *mut FiberHandle,
    params_json: *const c_char,
    out_json: *mut *mut c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        prepare_out_string(out_json)?;
        let params = parse_json_params::<fnn::rpc::channel::OpenChannelParams>(params_json)?;
        let response = handle
            .runtime_handle
            .block_on(call_open_channel(handle, params))?;
        write_serializable_out(out_json, &response)?;
        Ok(FiberFfiStatus::Ok)
    })
}

#[no_mangle]
pub unsafe extern "C" fn fiber_accept_channel(
    handle: *mut FiberHandle,
    params_json: *const c_char,
    out_json: *mut *mut c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        prepare_out_string(out_json)?;
        let params = parse_json_params::<fnn::rpc::channel::AcceptChannelParams>(params_json)?;
        let response = handle
            .runtime_handle
            .block_on(call_accept_channel(handle, params))?;
        write_serializable_out(out_json, &response)?;
        Ok(FiberFfiStatus::Ok)
    })
}

#[no_mangle]
pub unsafe extern "C" fn fiber_abandon_channel(
    handle: *mut FiberHandle,
    params_json: *const c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        let params = parse_json_params::<fnn::rpc::channel::AbandonChannelParams>(params_json)?;
        handle
            .runtime_handle
            .block_on(call_abandon_channel(handle, params))?;
        Ok(FiberFfiStatus::Ok)
    })
}

#[no_mangle]
pub unsafe extern "C" fn fiber_list_channels(
    handle: *mut FiberHandle,
    params_json: *const c_char,
    out_json: *mut *mut c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        prepare_out_string(out_json)?;
        let params = parse_json_params::<fnn::rpc::channel::ListChannelsParams>(params_json)?;
        let response = handle
            .runtime_handle
            .block_on(call_list_channels(handle, params))?;
        write_serializable_out(out_json, &response)?;
        Ok(FiberFfiStatus::Ok)
    })
}

#[no_mangle]
pub unsafe extern "C" fn fiber_shutdown_channel(
    handle: *mut FiberHandle,
    params_json: *const c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        let params = parse_json_params::<fnn::rpc::channel::ShutdownChannelParams>(params_json)?;
        handle
            .runtime_handle
            .block_on(call_shutdown_channel(handle, params))?;
        Ok(FiberFfiStatus::Ok)
    })
}

#[no_mangle]
pub unsafe extern "C" fn fiber_update_channel(
    handle: *mut FiberHandle,
    params_json: *const c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        let params = parse_json_params::<fnn::rpc::channel::UpdateChannelParams>(params_json)?;
        handle
            .runtime_handle
            .block_on(call_update_channel(handle, params))?;
        Ok(FiberFfiStatus::Ok)
    })
}

#[no_mangle]
pub unsafe extern "C" fn fiber_open_channel_with_external_funding(
    handle: *mut FiberHandle,
    params_json: *const c_char,
    out_json: *mut *mut c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        prepare_out_string(out_json)?;
        let params = parse_json_params::<fnn::rpc::channel::OpenChannelWithExternalFundingParams>(
            params_json,
        )?;
        let response = handle
            .runtime_handle
            .block_on(call_open_channel_with_external_funding(handle, params))?;
        write_serializable_out(out_json, &response)?;
        Ok(FiberFfiStatus::Ok)
    })
}

#[no_mangle]
pub unsafe extern "C" fn fiber_submit_signed_funding_tx(
    handle: *mut FiberHandle,
    params_json: *const c_char,
    out_json: *mut *mut c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        prepare_out_string(out_json)?;
        let params =
            parse_json_params::<fnn::rpc::channel::SubmitSignedFundingTxParams>(params_json)?;
        let response = handle
            .runtime_handle
            .block_on(call_submit_signed_funding_tx(handle, params))?;
        write_serializable_out(out_json, &response)?;
        Ok(FiberFfiStatus::Ok)
    })
}

#[no_mangle]
pub unsafe extern "C" fn fiber_send_payment(
    handle: *mut FiberHandle,
    params_json: *const c_char,
    out_json: *mut *mut c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        prepare_out_string(out_json)?;
        let params = parse_json_params::<fnn::rpc::payment::SendPaymentCommandParams>(params_json)?;
        let response = handle
            .runtime_handle
            .block_on(call_send_payment(handle, params))?;
        write_serializable_out(out_json, &response)?;
        Ok(FiberFfiStatus::Ok)
    })
}

#[no_mangle]
pub unsafe extern "C" fn fiber_get_payment(
    handle: *mut FiberHandle,
    params_json: *const c_char,
    out_json: *mut *mut c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        prepare_out_string(out_json)?;
        let params = parse_json_params::<fnn::rpc::payment::GetPaymentCommandParams>(params_json)?;
        let response = handle
            .runtime_handle
            .block_on(call_get_payment(handle, params))?;
        write_serializable_out(out_json, &response)?;
        Ok(FiberFfiStatus::Ok)
    })
}

#[no_mangle]
pub unsafe extern "C" fn fiber_list_payments(
    handle: *mut FiberHandle,
    params_json: *const c_char,
    out_json: *mut *mut c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        prepare_out_string(out_json)?;
        let params = parse_json_params::<fnn::rpc::payment::ListPaymentsParams>(params_json)?;
        let response = handle
            .runtime_handle
            .block_on(call_list_payments(handle, params))?;
        write_serializable_out(out_json, &response)?;
        Ok(FiberFfiStatus::Ok)
    })
}

#[no_mangle]
pub unsafe extern "C" fn fiber_build_router(
    handle: *mut FiberHandle,
    params_json: *const c_char,
    out_json: *mut *mut c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        prepare_out_string(out_json)?;
        let params = parse_json_params::<fnn::rpc::payment::BuildRouterParams>(params_json)?;
        let response = handle
            .runtime_handle
            .block_on(call_build_router(handle, params))?;
        write_serializable_out(out_json, &response)?;
        Ok(FiberFfiStatus::Ok)
    })
}

#[no_mangle]
pub unsafe extern "C" fn fiber_send_payment_with_router(
    handle: *mut FiberHandle,
    params_json: *const c_char,
    out_json: *mut *mut c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        prepare_out_string(out_json)?;
        let params =
            parse_json_params::<fnn::rpc::payment::SendPaymentWithRouterParams>(params_json)?;
        let response = handle
            .runtime_handle
            .block_on(call_send_payment_with_router(handle, params))?;
        write_serializable_out(out_json, &response)?;
        Ok(FiberFfiStatus::Ok)
    })
}

#[no_mangle]
pub unsafe extern "C" fn fiber_new_invoice(
    handle: *mut FiberHandle,
    params_json: *const c_char,
    out_json: *mut *mut c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        prepare_out_string(out_json)?;
        let params = parse_json_params::<fnn::rpc::invoice::NewInvoiceParams>(params_json)?;
        let response = handle
            .runtime_handle
            .block_on(call_new_invoice(handle, params))?;
        write_serializable_out(out_json, &response)?;
        Ok(FiberFfiStatus::Ok)
    })
}

#[no_mangle]
pub unsafe extern "C" fn fiber_parse_invoice(
    handle: *mut FiberHandle,
    params_json: *const c_char,
    out_json: *mut *mut c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        prepare_out_string(out_json)?;
        let params = parse_json_params::<fnn::rpc::invoice::ParseInvoiceParams>(params_json)?;
        let response = handle
            .runtime_handle
            .block_on(call_parse_invoice(handle, params))?;
        write_serializable_out(out_json, &response)?;
        Ok(FiberFfiStatus::Ok)
    })
}

#[no_mangle]
pub unsafe extern "C" fn fiber_get_invoice(
    handle: *mut FiberHandle,
    params_json: *const c_char,
    out_json: *mut *mut c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        prepare_out_string(out_json)?;
        let params = parse_json_params::<fnn::rpc::invoice::InvoiceParams>(params_json)?;
        let response = handle
            .runtime_handle
            .block_on(call_get_invoice(handle, params))?;
        write_serializable_out(out_json, &response)?;
        Ok(FiberFfiStatus::Ok)
    })
}

#[no_mangle]
pub unsafe extern "C" fn fiber_cancel_invoice(
    handle: *mut FiberHandle,
    params_json: *const c_char,
    out_json: *mut *mut c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        prepare_out_string(out_json)?;
        let params = parse_json_params::<fnn::rpc::invoice::InvoiceParams>(params_json)?;
        let response = handle
            .runtime_handle
            .block_on(call_cancel_invoice(handle, params))?;
        write_serializable_out(out_json, &response)?;
        Ok(FiberFfiStatus::Ok)
    })
}

#[no_mangle]
pub unsafe extern "C" fn fiber_settle_invoice(
    handle: *mut FiberHandle,
    params_json: *const c_char,
    out_json: *mut *mut c_char,
) -> FiberFfiStatus {
    ffi_boundary(|| {
        let handle = checked_handle(handle)?;
        prepare_out_string(out_json)?;
        let params = parse_json_params::<fnn::rpc::invoice::SettleInvoiceParams>(params_json)?;
        let response = handle
            .runtime_handle
            .block_on(call_settle_invoice(handle, params))?;
        write_serializable_out(out_json, &response)?;
        Ok(FiberFfiStatus::Ok)
    })
}

#[no_mangle]
pub unsafe extern "C" fn fiber_string_free(string: *mut c_char) {
    if !string.is_null() {
        let _ = CString::from_raw(string);
    }
}

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
    let network_actor = start_network(
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

fn parse_json_params<T: DeserializeOwned>(ptr: *const c_char) -> FfiCallResult<T> {
    let json = required_string(ptr, "params_json")?;
    serde_json::from_str(&json).map_err(|err| {
        ffi_error(
            FiberFfiStatus::InvalidArgument,
            format!("invalid params_json: {err}"),
        )
    })
}

fn parse_pubkey(value: &str) -> FfiCallResult<fnn::fiber_types::Pubkey> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    let bytes = hex::decode(value).map_err(|err| {
        ffi_error(
            FiberFfiStatus::InvalidArgument,
            format!("invalid pubkey hex: {err}"),
        )
    })?;
    if bytes.len() != fnn::fiber_types::Pubkey::serialization_len() {
        return Err(ffi_error(
            FiberFfiStatus::InvalidArgument,
            format!(
                "pubkey must be {} bytes compressed secp256k1 key",
                fnn::fiber_types::Pubkey::serialization_len()
            ),
        ));
    }
    fnn::fiber_types::Pubkey::from_slice(&bytes).map_err(|err| {
        ffi_error(
            FiberFfiStatus::InvalidArgument,
            format!("invalid pubkey: {err}"),
        )
    })
}

fn parse_addr_type(value: Option<&str>) -> FfiCallResult<Option<TransportType>> {
    let Some(value) = value else {
        return Ok(None);
    };
    match value.to_ascii_lowercase().as_str() {
        "" => Ok(None),
        "tcp" => Ok(Some(TransportType::Tcp)),
        "ws" => Ok(Some(TransportType::Ws)),
        "wss" => Ok(Some(TransportType::Wss)),
        _ => Err(ffi_error(
            FiberFfiStatus::InvalidArgument,
            "addr_type must be tcp, ws, or wss",
        )),
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

fn init_logging(log_level: &str) {
    let filter = EnvFilter::try_new(log_level).unwrap_or_else(|_| EnvFilter::new("info"));
    INIT_LOGGING.call_once(|| {
        let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
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
