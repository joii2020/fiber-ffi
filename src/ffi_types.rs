//! C-compatible types exposed by the Fiber FFI.
//!
//! Keeping the ABI declarations separate from runtime behavior makes it easier
//! to compare these layouts with `include/fiber_ffi.h`.

use std::os::raw::{c_char, c_void};

#[repr(C)]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum FiberFfiStatus {
    Ok = 0,
    NullPointer = 1,
    InvalidArgument = 2,
    StartupFailed = 3,
    AlreadyStopped = 4,
    Panic = 5,
    NotReady = 6,
}

pub type FiberEventCallback = unsafe extern "C" fn(*const c_char, *mut c_void);
pub type FiberCkbPrepareCallback = unsafe extern "C" fn(FiberFfiStatus, *const c_char, *mut c_void);

#[repr(C)]
pub struct FiberStartOptions {
    pub config_path: *const c_char,
    pub database_prefix: *const c_char,
    pub log_level: *const c_char,
    pub event_callback: Option<FiberEventCallback>,
    pub event_callback_user_data: *mut c_void,
}

#[repr(C)]
pub struct FiberCkbDiscoverHistoryStartBlockOptions {
    pub struct_size: u32,
    pub flags: u32,
    pub rpc_url: *const c_char,
    pub lock_args: *const c_char,
    pub pubkey: *const c_char,
    pub address: *const c_char,
    pub safety_blocks: u64,
    pub has_safety_blocks: i32,
    pub max_indexer_lag: u64,
    pub has_max_indexer_lag: i32,
}

#[repr(C)]
pub struct FiberConnectPeerOptions {
    pub address: *const c_char,
    pub pubkey: *const c_char,
    pub addr_type: *const c_char,
    pub save: i32,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct FiberU128 {
    pub low: u64,
    pub high: u64,
}

pub type FiberInvoiceCurrency = i32;
pub(super) const FIBER_INVOICE_CURRENCY_DEFAULT: FiberInvoiceCurrency = 0;
pub(super) const FIBER_INVOICE_CURRENCY_FIBB: FiberInvoiceCurrency = 1;
pub(super) const FIBER_INVOICE_CURRENCY_FIBT: FiberInvoiceCurrency = 2;
pub(super) const FIBER_INVOICE_CURRENCY_FIBD: FiberInvoiceCurrency = 3;

pub type FiberHashAlgorithm = i32;
pub(super) const FIBER_HASH_ALGORITHM_DEFAULT: FiberHashAlgorithm = 0;
pub(super) const FIBER_HASH_ALGORITHM_CKB_HASH: FiberHashAlgorithm = 1;
pub(super) const FIBER_HASH_ALGORITHM_SHA256: FiberHashAlgorithm = 2;

pub type FiberPaymentStatusFilter = i32;
pub(super) const FIBER_PAYMENT_STATUS_FILTER_ALL: FiberPaymentStatusFilter = 0;
pub(super) const FIBER_PAYMENT_STATUS_FILTER_CREATED: FiberPaymentStatusFilter = 1;
pub(super) const FIBER_PAYMENT_STATUS_FILTER_INFLIGHT: FiberPaymentStatusFilter = 2;
pub(super) const FIBER_PAYMENT_STATUS_FILTER_SUCCESS: FiberPaymentStatusFilter = 3;
pub(super) const FIBER_PAYMENT_STATUS_FILTER_FAILED: FiberPaymentStatusFilter = 4;

#[repr(C)]
pub struct FiberOpenChannelOptions {
    pub struct_size: u32,
    pub flags: u32,
    pub pubkey: *const c_char,
    pub funding_amount: FiberU128,
    pub has_public: i32,
    pub public_: i32,
    pub has_one_way: i32,
    pub one_way: i32,
    pub funding_udt_type_script_json: *const c_char,
    pub shutdown_script_json: *const c_char,
    pub commitment_delay_epoch: u64,
    pub has_commitment_delay_epoch: i32,
    pub commitment_fee_rate: u64,
    pub has_commitment_fee_rate: i32,
    pub funding_fee_rate: u64,
    pub has_funding_fee_rate: i32,
    pub tlc_expiry_delta: u64,
    pub has_tlc_expiry_delta: i32,
    pub tlc_min_value: FiberU128,
    pub has_tlc_min_value: i32,
    pub tlc_fee_proportional_millionths: FiberU128,
    pub has_tlc_fee_proportional_millionths: i32,
    pub max_tlc_value_in_flight: FiberU128,
    pub has_max_tlc_value_in_flight: i32,
    pub max_tlc_number_in_flight: u64,
    pub has_max_tlc_number_in_flight: i32,
}

#[repr(C)]
pub struct FiberAcceptChannelOptions {
    pub struct_size: u32,
    pub flags: u32,
    pub temporary_channel_id: *const c_char,
    pub funding_amount: FiberU128,
    pub shutdown_script_json: *const c_char,
    pub max_tlc_value_in_flight: FiberU128,
    pub has_max_tlc_value_in_flight: i32,
    pub max_tlc_number_in_flight: u64,
    pub has_max_tlc_number_in_flight: i32,
    pub tlc_min_value: FiberU128,
    pub has_tlc_min_value: i32,
    pub tlc_fee_proportional_millionths: FiberU128,
    pub has_tlc_fee_proportional_millionths: i32,
    pub tlc_expiry_delta: u64,
    pub has_tlc_expiry_delta: i32,
}

#[repr(C)]
pub struct FiberOpenChannelWithExternalFundingOptions {
    pub struct_size: u32,
    pub flags: u32,
    pub pubkey: *const c_char,
    pub funding_amount: FiberU128,
    pub has_public: i32,
    pub public_: i32,
    pub funding_udt_type_script_json: *const c_char,
    pub shutdown_script_json: *const c_char,
    pub funding_lock_script_json: *const c_char,
    pub funding_lock_script_cell_deps_json: *const c_char,
    pub commitment_delay_epoch: u64,
    pub has_commitment_delay_epoch: i32,
    pub commitment_fee_rate: u64,
    pub has_commitment_fee_rate: i32,
    pub funding_fee_rate: u64,
    pub has_funding_fee_rate: i32,
    pub tlc_expiry_delta: u64,
    pub has_tlc_expiry_delta: i32,
    pub tlc_min_value: FiberU128,
    pub has_tlc_min_value: i32,
    pub tlc_fee_proportional_millionths: FiberU128,
    pub has_tlc_fee_proportional_millionths: i32,
    pub max_tlc_value_in_flight: FiberU128,
    pub has_max_tlc_value_in_flight: i32,
    pub max_tlc_number_in_flight: u64,
    pub has_max_tlc_number_in_flight: i32,
}

#[repr(C)]
pub struct FiberSubmitSignedFundingTxOptions {
    pub struct_size: u32,
    pub flags: u32,
    pub channel_id: *const c_char,
    pub signed_funding_tx_json: *const c_char,
}

#[repr(C)]
pub struct FiberListChannelsOptions {
    pub struct_size: u32,
    pub flags: u32,
    pub pubkey: *const c_char,
    pub has_include_closed: i32,
    pub include_closed: i32,
    pub has_only_pending: i32,
    pub only_pending: i32,
}

#[repr(C)]
pub struct FiberShutdownChannelOptions {
    pub struct_size: u32,
    pub flags: u32,
    pub channel_id: *const c_char,
    pub close_script_json: *const c_char,
    pub fee_rate: u64,
    pub has_fee_rate: i32,
    pub has_force: i32,
    pub force: i32,
}

#[repr(C)]
pub struct FiberUpdateChannelOptions {
    pub struct_size: u32,
    pub flags: u32,
    pub channel_id: *const c_char,
    pub has_enabled: i32,
    pub enabled: i32,
    pub tlc_expiry_delta: u64,
    pub has_tlc_expiry_delta: i32,
    pub tlc_minimum_value: FiberU128,
    pub has_tlc_minimum_value: i32,
    pub tlc_fee_proportional_millionths: FiberU128,
    pub has_tlc_fee_proportional_millionths: i32,
}

#[repr(C)]
pub struct FiberSendPaymentOptions {
    pub struct_size: u32,
    pub flags: u32,
    pub target_pubkey: *const c_char,
    pub amount: FiberU128,
    pub has_amount: i32,
    pub payment_hash: *const c_char,
    pub final_tlc_expiry_delta: u64,
    pub has_final_tlc_expiry_delta: i32,
    pub tlc_expiry_limit: u64,
    pub has_tlc_expiry_limit: i32,
    pub invoice: *const c_char,
    pub timeout: u64,
    pub has_timeout: i32,
    pub max_fee_amount: FiberU128,
    pub has_max_fee_amount: i32,
    pub max_fee_rate: u64,
    pub has_max_fee_rate: i32,
    pub max_parts: u64,
    pub has_max_parts: i32,
    pub trampoline_hops_json: *const c_char,
    pub has_keysend: i32,
    pub keysend: i32,
    pub udt_type_script_json: *const c_char,
    pub has_allow_self_payment: i32,
    pub allow_self_payment: i32,
    pub custom_records_json: *const c_char,
    pub hop_hints_json: *const c_char,
    pub has_dry_run: i32,
    pub dry_run: i32,
}

#[repr(C)]
pub struct FiberBuildRouterOptions {
    pub struct_size: u32,
    pub flags: u32,
    pub amount: FiberU128,
    pub has_amount: i32,
    pub udt_type_script_json: *const c_char,
    pub hops_info_json: *const c_char,
    pub final_tlc_expiry_delta: u64,
    pub has_final_tlc_expiry_delta: i32,
}

#[repr(C)]
pub struct FiberSendPaymentWithRouterOptions {
    pub struct_size: u32,
    pub flags: u32,
    pub payment_hash: *const c_char,
    pub router_json: *const c_char,
    pub invoice: *const c_char,
    pub custom_records_json: *const c_char,
    pub has_keysend: i32,
    pub keysend: i32,
    pub udt_type_script_json: *const c_char,
    pub has_dry_run: i32,
    pub dry_run: i32,
}

#[repr(C)]
pub struct FiberListPaymentsOptions {
    pub struct_size: u32,
    pub flags: u32,
    pub status: FiberPaymentStatusFilter,
    pub limit: u64,
    pub has_limit: i32,
    pub after: *const c_char,
}

#[repr(C)]
pub struct FiberNewInvoiceOptions {
    pub struct_size: u32,
    pub flags: u32,
    pub amount: FiberU128,
    pub description: *const c_char,
    pub currency: FiberInvoiceCurrency,
    pub payment_preimage: *const c_char,
    pub payment_hash: *const c_char,
    pub expiry: u64,
    pub has_expiry: i32,
    pub fallback_address: *const c_char,
    pub final_expiry_delta: u64,
    pub has_final_expiry_delta: i32,
    pub udt_type_script_json: *const c_char,
    pub hash_algorithm: FiberHashAlgorithm,
    pub has_allow_mpp: i32,
    pub allow_mpp: i32,
    pub has_allow_trampoline_routing: i32,
    pub allow_trampoline_routing: i32,
}
