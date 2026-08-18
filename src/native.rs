//! Safe Rust API for embedding a Fiber node.
//!
//! The C ABI in the crate root is an adapter over this module. Rust callers use
//! the typed Fiber RPC parameters and results directly and never need to deal
//! with raw pointers, C strings, status codes, or JSON output buffers.

use std::{fmt, sync::Arc};

use super::{
    call_abandon_channel, call_accept_channel, call_build_router, call_cancel_invoice,
    call_connect_peer, call_disconnect_peer, call_get_invoice, call_get_payment,
    call_list_channels, call_list_payments, call_list_peers, call_new_invoice, call_node_info,
    call_open_channel, call_open_channel_with_external_funding, call_parse_invoice,
    call_send_payment, call_send_payment_with_router, call_settle_invoice, call_shutdown_channel,
    call_submit_signed_funding_tx, call_update_channel, current_ckb_readiness, ensure_ckb_ready,
    init_logging, query_ckb_balance, ConnectPeerCommand, EventCallback, FiberHandle,
    StartupMessage,
};

pub use super::{CkbBalance, CkbReadiness, CkbWaitEstimate};
pub use fnn as fiber;

/// Native Fiber and RPC types used by [`FiberNode`].
///
/// Re-exporting these types keeps downstream users from needing to discover the
/// internal `fnn` module paths used by this crate's public API.
pub mod types {
    pub use ckb_types::packed::Script as CkbScript;
    pub use fnn::fiber::network::{NetworkServiceEvent, NodeInfoResponse, PeerInfo};
    pub use fnn::fiber_types::{Hash256, Multiaddr, Pubkey};
    pub use fnn::rpc::channel::{
        AbandonChannelParams, AcceptChannelParams, AcceptChannelResult, ListChannelsParams,
        ListChannelsResult, OpenChannelParams, OpenChannelResult,
        OpenChannelWithExternalFundingParams, OpenChannelWithExternalFundingResult,
        ShutdownChannelParams, SubmitSignedFundingTxParams, SubmitSignedFundingTxResult,
        UpdateChannelParams,
    };
    pub use fnn::rpc::invoice::{
        GetInvoiceResult, InvoiceParams, InvoiceResult, NewInvoiceParams, ParseInvoiceParams,
        ParseInvoiceResult, SettleInvoiceParams, SettleInvoiceResult,
    };
    pub use fnn::rpc::payment::{
        BuildPaymentRouterResult, BuildRouterParams, GetPaymentCommandParams,
        GetPaymentCommandResult, ListPaymentsParams, ListPaymentsResult, SendPaymentCommandParams,
        SendPaymentWithRouterParams,
    };
    pub use tentacle::utils::TransportType;
}

/// The broad category of an error returned by the native API.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    InvalidArgument,
    StartupFailed,
    AlreadyStopped,
    Panic,
    NotReady,
}

/// Error returned by the safe Rust API.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Error {
    kind: ErrorKind,
    message: String,
}

impl Error {
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub(super) fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;
pub type CkbPreparationResult = Result<serde_json::Value>;

/// Called on Fiber's event task. The callback must return quickly and must not
/// block the node runtime.
pub type EventHandler = Arc<dyn Fn(&types::NetworkServiceEvent) + Send + Sync + 'static>;

/// Receives CKB preparation progress and its terminal result. A successful
/// value contains the same stable progress object serialized by the C ABI.
pub type CkbPreparationHandler = Arc<dyn Fn(CkbPreparationResult) + Send + Sync + 'static>;

/// Options for starting a native Fiber node.
#[derive(Clone)]
pub struct StartOptions {
    pub config_path: String,
    pub database_prefix: Option<String>,
    pub log_level: String,
    pub event_handler: Option<EventHandler>,
}

impl StartOptions {
    pub fn new(config_path: impl Into<String>) -> Self {
        Self {
            config_path: config_path.into(),
            database_prefix: None,
            log_level: "info".to_string(),
            event_handler: None,
        }
    }
}

impl fmt::Debug for StartOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StartOptions")
            .field("config_path", &self.config_path)
            .field("database_prefix", &self.database_prefix)
            .field("log_level", &self.log_level)
            .field(
                "event_handler",
                &self.event_handler.as_ref().map(|_| "<callback>"),
            )
            .finish()
    }
}

/// Selects how a native peer connection is established.
#[derive(Clone, Debug)]
pub enum ConnectPeerOptions {
    Address {
        address: types::Multiaddr,
        save: bool,
    },
    Pubkey {
        pubkey: types::Pubkey,
        address_type: Option<types::TransportType>,
    },
}

/// Options for discovering the earliest CKB block a wallet needs to scan.
#[derive(Clone, Debug)]
pub struct CkbHistoryDiscoveryOptions {
    pub rpc_url: String,
    pub funding_lock: types::CkbScript,
    pub safety_blocks: u64,
    pub max_indexer_lag: u64,
}

/// Return the crate version used by both native and C callers.
pub const fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Convert node information to the stable JSON representation used by the C
/// ABI and JSON-RPC-facing demos.
pub fn node_info_to_json(response: types::NodeInfoResponse) -> serde_json::Value {
    super::node_info_to_json(response)
}

/// Convert a node event to the stable JSON representation used by the C ABI.
///
/// This is useful for native embedders that want structured event logs without
/// depending on Fiber's internal event enum layout.
pub fn event_to_json(event: &types::NetworkServiceEvent) -> serde_json::Value {
    super::event_to_json(event)
}

/// Derive the configured CKB funding address without starting Fiber.
pub fn ckb_funding_address(config_path: &str, database_prefix: Option<String>) -> Result<String> {
    let (parsed_config, genesis_block) =
        super::parse_config_with_genesis(config_path, database_prefix)
            .map_err(|error| Error::new(ErrorKind::InvalidArgument, error))?;
    let fiber_config = parsed_config.fiber.fiber.as_ref().ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidArgument,
            "fiber service must be enabled in config services",
        )
    })?;
    let ckb_config = parsed_config.fiber.ckb.as_ref().ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidArgument,
            "service fiber requires service ckb to be enabled",
        )
    })?;
    let funding_lock = super::funding_lock_from_genesis(ckb_config, &genesis_block)
        .map_err(|error| Error::new(ErrorKind::InvalidArgument, error))?;
    Ok(ckb_sdk::Address::new(
        super::ckb_address_network(&fiber_config.chain),
        ckb_sdk::AddressPayload::from(funding_lock),
        true,
    )
    .to_string())
}

/// Discover a conservative wallet-history start block through an explicit CKB
/// RPC/Indexer endpoint. This does not read or mutate Light Client state.
pub async fn discover_ckb_history_start_block(options: CkbHistoryDiscoveryOptions) -> Result<u64> {
    if !options.rpc_url.starts_with("https://") && !options.rpc_url.starts_with("http://") {
        return Err(Error::new(
            ErrorKind::InvalidArgument,
            "rpc_url must use http:// or https://",
        ));
    }
    let discovery = super::discover_wallet_history(
        &options.rpc_url,
        options.funding_lock,
        options.max_indexer_lag,
    )
    .await
    .map_err(|error| Error::new(ErrorKind::StartupFailed, error))?;
    Ok(super::select_wallet_history_start_block(
        &discovery,
        options.safety_blocks,
    ))
}

/// Prepare the CKB backend for the next [`FiberNode::start`] call.
///
/// The handler is always invoked asynchronously. With the embedded Light
/// Client it receives progress until `ready` or an error; with external RPC it
/// receives one successful `ready` update.
pub fn prepare_ckb(
    options: &StartOptions,
    discovered_history_start_block: Option<u64>,
    handler: CkbPreparationHandler,
) -> Result<()> {
    init_logging(&options.log_level);

    #[cfg(feature = "disable-ckb-rpc")]
    super::schedule_embedded_ckb_preparation(
        options.config_path.clone(),
        options.database_prefix.clone(),
        discovered_history_start_block,
        handler,
    )?;

    #[cfg(not(feature = "disable-ckb-rpc"))]
    {
        let _ = discovered_history_start_block;
        std::thread::Builder::new()
            .name("fiber-native-prepare-ckb".to_string())
            .spawn(move || {
                handler(Ok(serde_json::json!({
                    "ready": true,
                    "mode": "external_rpc",
                    "skipped": true,
                    "status": "ready",
                })));
            })
            .map_err(|error| Error::new(ErrorKind::StartupFailed, error.to_string()))?;
    }

    Ok(())
}

/// A running Fiber node.
///
/// The node owns a dedicated Tokio runtime thread. RPC methods are async and
/// can be awaited from any Rust executor. [`FiberNode::stop`] is synchronous
/// because it waits for that dedicated thread to terminate.
pub type FiberNode = FiberHandle;

impl FiberHandle {
    /// Start a Fiber node on its dedicated runtime thread.
    pub fn start(options: StartOptions) -> Result<Self> {
        init_logging(&options.log_level);

        let callback: Option<EventCallback> = options.event_handler;
        let config_path = options.config_path;
        let database_prefix = options.database_prefix;
        let (startup_tx, startup_rx) = std::sync::mpsc::channel();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();

        #[cfg(feature = "disable-ckb-rpc")]
        let thread = match super::take_prepared_ckb_worker(super::ckb_preparation_key(
            config_path.clone(),
            database_prefix.clone(),
        )?)? {
            Some(mut worker) => {
                let worker_key = worker.key.clone();
                if worker
                    .start_tx
                    .send(super::PreparedCkbStartCommand {
                        callback,
                        startup_tx: startup_tx.clone(),
                        stop_rx,
                    })
                    .is_err()
                {
                    super::clear_ckb_in_use(&worker_key);
                    return Err(Error::new(
                        ErrorKind::StartupFailed,
                        "prepared CKB runtime exited before Fiber could start",
                    ));
                }
                match worker.thread.take() {
                    Some(thread) => thread,
                    None => {
                        super::clear_ckb_in_use(&worker_key);
                        return Err(Error::new(
                            ErrorKind::Panic,
                            "prepared CKB runtime thread is missing",
                        ));
                    }
                }
            }
            None => super::spawn_fiber_runtime(
                config_path,
                database_prefix,
                callback,
                startup_tx,
                stop_rx,
            )?,
        };

        #[cfg(not(feature = "disable-ckb-rpc"))]
        let thread = super::spawn_fiber_runtime(
            config_path,
            database_prefix,
            callback,
            startup_tx,
            stop_rx,
        )?;

        match startup_rx.recv() {
            Ok(StartupMessage::Started {
                runtime_handle,
                network_actor,
                store,
                fiber_config,
                ckb_config,
                #[cfg(feature = "disable-ckb-rpc")]
                ckb_monitor,
            }) => Ok(Self {
                stop_tx: std::sync::Mutex::new(Some(stop_tx)),
                thread: std::sync::Mutex::new(Some(thread)),
                runtime_handle,
                network_actor,
                store,
                fiber_config: *fiber_config,
                ckb_config: *ckb_config,
                #[cfg(feature = "disable-ckb-rpc")]
                ckb_monitor,
                ckb_sync_estimator: std::sync::Mutex::new(super::CkbSyncEstimator::default()),
            }),
            Ok(StartupMessage::Failed(error)) => {
                let _ = thread.join();
                Err(Error::new(ErrorKind::StartupFailed, error))
            }
            Err(error) => {
                let _ = thread.join();
                Err(Error::new(
                    ErrorKind::StartupFailed,
                    format!("runtime thread exited before reporting startup status: {error}"),
                ))
            }
        }
    }

    /// Stop the node and wait for its runtime thread to terminate.
    pub fn stop(&self) -> Result<()> {
        let stop_tx = self
            .stop_tx
            .lock()
            .map_err(|_| Error::new(ErrorKind::Panic, "stop mutex poisoned"))?
            .take();
        let thread = self
            .thread
            .lock()
            .map_err(|_| Error::new(ErrorKind::Panic, "thread mutex poisoned"))?
            .take();

        let Some(stop_tx) = stop_tx else {
            return Err(Error::new(
                ErrorKind::AlreadyStopped,
                "fiber node is already stopped",
            ));
        };
        let _ = stop_tx.send(());
        if let Some(thread) = thread {
            thread
                .join()
                .map_err(|_| Error::new(ErrorKind::Panic, "runtime thread panicked"))?;
        }
        Ok(())
    }

    pub async fn node_info(&self) -> Result<types::NodeInfoResponse> {
        call_node_info(self.network_actor.clone())
            .await
            .map_err(|error| Error::new(ErrorKind::InvalidArgument, error))
    }

    pub fn ckb_readiness(&self) -> super::CkbReadiness {
        current_ckb_readiness(self)
    }

    pub async fn ckb_balance(&self) -> Result<super::CkbBalance> {
        let readiness = current_ckb_readiness(self);
        query_ckb_balance(&self.ckb_config, &self.fiber_config.chain, readiness)
            .await
            .map_err(|error| Error::new(ErrorKind::InvalidArgument, error))
    }

    pub async fn list_peers(&self) -> Result<Vec<types::PeerInfo>> {
        call_list_peers(self.network_actor.clone())
            .await
            .map_err(|error| Error::new(ErrorKind::InvalidArgument, error))
    }

    pub async fn connect_peer(&self, options: ConnectPeerOptions) -> Result<()> {
        let command = match options {
            ConnectPeerOptions::Address { address, save } => {
                ConnectPeerCommand::Address { address, save }
            }
            ConnectPeerOptions::Pubkey {
                pubkey,
                address_type,
            } => ConnectPeerCommand::Pubkey {
                pubkey,
                addr_type: address_type,
            },
        };
        call_connect_peer(self.network_actor.clone(), command)
            .await
            .map_err(|error| Error::new(ErrorKind::InvalidArgument, error))
    }

    pub async fn disconnect_peer(&self, pubkey: types::Pubkey) -> Result<()> {
        call_disconnect_peer(self.network_actor.clone(), pubkey)
            .await
            .map_err(|error| Error::new(ErrorKind::InvalidArgument, error))
    }

    pub async fn open_channel(
        &self,
        params: types::OpenChannelParams,
    ) -> Result<types::OpenChannelResult> {
        ensure_ckb_ready(self)?;
        call_open_channel(self, params).await
    }

    pub async fn accept_channel(
        &self,
        params: types::AcceptChannelParams,
    ) -> Result<types::AcceptChannelResult> {
        call_accept_channel(self, params).await
    }

    pub async fn open_channel_with_external_funding(
        &self,
        params: types::OpenChannelWithExternalFundingParams,
    ) -> Result<types::OpenChannelWithExternalFundingResult> {
        call_open_channel_with_external_funding(self, params).await
    }

    pub async fn submit_signed_funding_tx(
        &self,
        params: types::SubmitSignedFundingTxParams,
    ) -> Result<types::SubmitSignedFundingTxResult> {
        call_submit_signed_funding_tx(self, params).await
    }

    pub async fn abandon_channel(&self, params: types::AbandonChannelParams) -> Result<()> {
        call_abandon_channel(self, params).await
    }

    pub async fn list_channels(
        &self,
        params: types::ListChannelsParams,
    ) -> Result<types::ListChannelsResult> {
        call_list_channels(self, params).await
    }

    pub async fn shutdown_channel(&self, params: types::ShutdownChannelParams) -> Result<()> {
        call_shutdown_channel(self, params).await
    }

    pub async fn update_channel(&self, params: types::UpdateChannelParams) -> Result<()> {
        call_update_channel(self, params).await
    }

    pub async fn send_payment(
        &self,
        params: types::SendPaymentCommandParams,
    ) -> Result<types::GetPaymentCommandResult> {
        call_send_payment(self, params).await
    }

    pub async fn get_payment(
        &self,
        params: types::GetPaymentCommandParams,
    ) -> Result<types::GetPaymentCommandResult> {
        call_get_payment(self, params).await
    }

    pub async fn list_payments(
        &self,
        params: types::ListPaymentsParams,
    ) -> Result<types::ListPaymentsResult> {
        call_list_payments(self, params).await
    }

    pub async fn build_router(
        &self,
        params: types::BuildRouterParams,
    ) -> Result<types::BuildPaymentRouterResult> {
        call_build_router(self, params).await
    }

    pub async fn send_payment_with_router(
        &self,
        params: types::SendPaymentWithRouterParams,
    ) -> Result<types::GetPaymentCommandResult> {
        call_send_payment_with_router(self, params).await
    }

    pub async fn new_invoice(
        &self,
        params: types::NewInvoiceParams,
    ) -> Result<types::InvoiceResult> {
        call_new_invoice(self, params).await
    }

    pub async fn parse_invoice(
        &self,
        params: types::ParseInvoiceParams,
    ) -> Result<types::ParseInvoiceResult> {
        call_parse_invoice(self, params).await
    }

    pub async fn get_invoice(
        &self,
        params: types::InvoiceParams,
    ) -> Result<types::GetInvoiceResult> {
        call_get_invoice(self, params).await
    }

    pub async fn cancel_invoice(
        &self,
        params: types::InvoiceParams,
    ) -> Result<types::GetInvoiceResult> {
        call_cancel_invoice(self, params).await
    }

    pub async fn settle_invoice(
        &self,
        params: types::SettleInvoiceParams,
    ) -> Result<types::SettleInvoiceResult> {
        call_settle_invoice(self, params).await
    }
}

impl Drop for FiberHandle {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}
