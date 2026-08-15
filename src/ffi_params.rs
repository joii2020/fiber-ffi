//! Conversion from C-compatible option structs to Fiber RPC parameters.

use std::os::raw::c_char;

use serde::de::DeserializeOwned;
use tentacle::utils::TransportType;

use super::ffi_types::*;
use super::{
    ffi_error, optional_string, required_string, CkbAddress, CkbAddressPayload, FfiCallResult,
};

pub(super) fn open_channel_params_from_options(
    options: &FiberOpenChannelOptions,
) -> FfiCallResult<fnn::rpc::channel::OpenChannelParams> {
    validate_options_struct::<FiberOpenChannelOptions>(
        options.struct_size,
        options.flags,
        "FiberOpenChannelOptions",
    )?;
    let mut value = serde_json::Map::new();
    value.insert(
        "pubkey".to_string(),
        string_field("pubkey", options.pubkey)?,
    );
    value.insert(
        "funding_amount".to_string(),
        u128_hex_field(options.funding_amount),
    );
    insert_optional_bool(&mut value, "public", options.has_public, options.public_);
    insert_optional_bool(&mut value, "one_way", options.has_one_way, options.one_way);
    insert_optional_json(
        &mut value,
        "funding_udt_type_script",
        options.funding_udt_type_script_json,
    )?;
    insert_optional_json(&mut value, "shutdown_script", options.shutdown_script_json)?;
    insert_optional_u64_number(
        &mut value,
        "commitment_delay_epoch",
        options.has_commitment_delay_epoch,
        options.commitment_delay_epoch,
    );
    insert_optional_u64_hex(
        &mut value,
        "commitment_fee_rate",
        options.has_commitment_fee_rate,
        options.commitment_fee_rate,
    );
    insert_optional_u64_hex(
        &mut value,
        "funding_fee_rate",
        options.has_funding_fee_rate,
        options.funding_fee_rate,
    );
    insert_optional_u64_hex(
        &mut value,
        "tlc_expiry_delta",
        options.has_tlc_expiry_delta,
        options.tlc_expiry_delta,
    );
    insert_optional_u128_hex(
        &mut value,
        "tlc_min_value",
        options.has_tlc_min_value,
        options.tlc_min_value,
    );
    insert_optional_u128_hex(
        &mut value,
        "tlc_fee_proportional_millionths",
        options.has_tlc_fee_proportional_millionths,
        options.tlc_fee_proportional_millionths,
    );
    insert_optional_u128_hex(
        &mut value,
        "max_tlc_value_in_flight",
        options.has_max_tlc_value_in_flight,
        options.max_tlc_value_in_flight,
    );
    insert_optional_u64_hex(
        &mut value,
        "max_tlc_number_in_flight",
        options.has_max_tlc_number_in_flight,
        options.max_tlc_number_in_flight,
    );
    deserialize_object(value)
}

pub(super) fn accept_channel_params_from_options(
    options: &FiberAcceptChannelOptions,
) -> FfiCallResult<fnn::rpc::channel::AcceptChannelParams> {
    validate_options_struct::<FiberAcceptChannelOptions>(
        options.struct_size,
        options.flags,
        "FiberAcceptChannelOptions",
    )?;
    let mut value = serde_json::Map::new();
    value.insert(
        "temporary_channel_id".to_string(),
        string_field("temporary_channel_id", options.temporary_channel_id)?,
    );
    value.insert(
        "funding_amount".to_string(),
        u128_hex_field(options.funding_amount),
    );
    insert_optional_json(&mut value, "shutdown_script", options.shutdown_script_json)?;
    insert_optional_u128_hex(
        &mut value,
        "max_tlc_value_in_flight",
        options.has_max_tlc_value_in_flight,
        options.max_tlc_value_in_flight,
    );
    insert_optional_u64_hex(
        &mut value,
        "max_tlc_number_in_flight",
        options.has_max_tlc_number_in_flight,
        options.max_tlc_number_in_flight,
    );
    insert_optional_u128_hex(
        &mut value,
        "tlc_min_value",
        options.has_tlc_min_value,
        options.tlc_min_value,
    );
    insert_optional_u128_hex(
        &mut value,
        "tlc_fee_proportional_millionths",
        options.has_tlc_fee_proportional_millionths,
        options.tlc_fee_proportional_millionths,
    );
    insert_optional_u64_hex(
        &mut value,
        "tlc_expiry_delta",
        options.has_tlc_expiry_delta,
        options.tlc_expiry_delta,
    );
    deserialize_object(value)
}

pub(super) fn open_channel_with_external_funding_params_from_options(
    options: &FiberOpenChannelWithExternalFundingOptions,
) -> FfiCallResult<fnn::rpc::channel::OpenChannelWithExternalFundingParams> {
    validate_options_struct::<FiberOpenChannelWithExternalFundingOptions>(
        options.struct_size,
        options.flags,
        "FiberOpenChannelWithExternalFundingOptions",
    )?;
    let mut value = serde_json::Map::new();
    value.insert(
        "pubkey".to_string(),
        string_field("pubkey", options.pubkey)?,
    );
    value.insert(
        "funding_amount".to_string(),
        u128_hex_field(options.funding_amount),
    );
    insert_optional_bool(&mut value, "public", options.has_public, options.public_);
    insert_optional_json(
        &mut value,
        "funding_udt_type_script",
        options.funding_udt_type_script_json,
    )?;
    value.insert(
        "shutdown_script".to_string(),
        json_field("shutdown_script_json", options.shutdown_script_json)?,
    );
    value.insert(
        "funding_lock_script".to_string(),
        json_field("funding_lock_script_json", options.funding_lock_script_json)?,
    );
    insert_optional_json(
        &mut value,
        "funding_lock_script_cell_deps",
        options.funding_lock_script_cell_deps_json,
    )?;
    insert_optional_u64_number(
        &mut value,
        "commitment_delay_epoch",
        options.has_commitment_delay_epoch,
        options.commitment_delay_epoch,
    );
    insert_optional_u64_hex(
        &mut value,
        "commitment_fee_rate",
        options.has_commitment_fee_rate,
        options.commitment_fee_rate,
    );
    insert_optional_u64_hex(
        &mut value,
        "funding_fee_rate",
        options.has_funding_fee_rate,
        options.funding_fee_rate,
    );
    insert_optional_u64_hex(
        &mut value,
        "tlc_expiry_delta",
        options.has_tlc_expiry_delta,
        options.tlc_expiry_delta,
    );
    insert_optional_u128_hex(
        &mut value,
        "tlc_min_value",
        options.has_tlc_min_value,
        options.tlc_min_value,
    );
    insert_optional_u128_hex(
        &mut value,
        "tlc_fee_proportional_millionths",
        options.has_tlc_fee_proportional_millionths,
        options.tlc_fee_proportional_millionths,
    );
    insert_optional_u128_hex(
        &mut value,
        "max_tlc_value_in_flight",
        options.has_max_tlc_value_in_flight,
        options.max_tlc_value_in_flight,
    );
    insert_optional_u64_hex(
        &mut value,
        "max_tlc_number_in_flight",
        options.has_max_tlc_number_in_flight,
        options.max_tlc_number_in_flight,
    );
    deserialize_object(value)
}

pub(super) fn submit_signed_funding_tx_params_from_options(
    options: &FiberSubmitSignedFundingTxOptions,
) -> FfiCallResult<fnn::rpc::channel::SubmitSignedFundingTxParams> {
    validate_options_struct::<FiberSubmitSignedFundingTxOptions>(
        options.struct_size,
        options.flags,
        "FiberSubmitSignedFundingTxOptions",
    )?;
    let mut value = serde_json::Map::new();
    value.insert(
        "channel_id".to_string(),
        string_field("channel_id", options.channel_id)?,
    );
    value.insert(
        "signed_funding_tx".to_string(),
        json_field("signed_funding_tx_json", options.signed_funding_tx_json)?,
    );
    deserialize_object(value)
}

pub(super) fn list_channels_params_from_options(
    options: &FiberListChannelsOptions,
) -> FfiCallResult<fnn::rpc::channel::ListChannelsParams> {
    validate_options_struct::<FiberListChannelsOptions>(
        options.struct_size,
        options.flags,
        "FiberListChannelsOptions",
    )?;
    let mut value = serde_json::Map::new();
    insert_optional_string(&mut value, "pubkey", options.pubkey)?;
    insert_optional_bool(
        &mut value,
        "include_closed",
        options.has_include_closed,
        options.include_closed,
    );
    insert_optional_bool(
        &mut value,
        "only_pending",
        options.has_only_pending,
        options.only_pending,
    );
    deserialize_object(value)
}

pub(super) fn shutdown_channel_params_from_options(
    options: &FiberShutdownChannelOptions,
) -> FfiCallResult<fnn::rpc::channel::ShutdownChannelParams> {
    validate_options_struct::<FiberShutdownChannelOptions>(
        options.struct_size,
        options.flags,
        "FiberShutdownChannelOptions",
    )?;
    let mut value = serde_json::Map::new();
    value.insert(
        "channel_id".to_string(),
        string_field("channel_id", options.channel_id)?,
    );
    insert_optional_json(&mut value, "close_script", options.close_script_json)?;
    insert_optional_u64_hex(
        &mut value,
        "fee_rate",
        options.has_fee_rate,
        options.fee_rate,
    );
    insert_optional_bool(&mut value, "force", options.has_force, options.force);
    deserialize_object(value)
}

pub(super) fn update_channel_params_from_options(
    options: &FiberUpdateChannelOptions,
) -> FfiCallResult<fnn::rpc::channel::UpdateChannelParams> {
    validate_options_struct::<FiberUpdateChannelOptions>(
        options.struct_size,
        options.flags,
        "FiberUpdateChannelOptions",
    )?;
    let mut value = serde_json::Map::new();
    value.insert(
        "channel_id".to_string(),
        string_field("channel_id", options.channel_id)?,
    );
    insert_optional_bool(&mut value, "enabled", options.has_enabled, options.enabled);
    insert_optional_u64_hex(
        &mut value,
        "tlc_expiry_delta",
        options.has_tlc_expiry_delta,
        options.tlc_expiry_delta,
    );
    insert_optional_u128_hex(
        &mut value,
        "tlc_minimum_value",
        options.has_tlc_minimum_value,
        options.tlc_minimum_value,
    );
    insert_optional_u128_hex(
        &mut value,
        "tlc_fee_proportional_millionths",
        options.has_tlc_fee_proportional_millionths,
        options.tlc_fee_proportional_millionths,
    );
    deserialize_object(value)
}

pub(super) fn send_payment_params_from_options(
    options: &FiberSendPaymentOptions,
) -> FfiCallResult<fnn::rpc::payment::SendPaymentCommandParams> {
    validate_options_struct::<FiberSendPaymentOptions>(
        options.struct_size,
        options.flags,
        "FiberSendPaymentOptions",
    )?;
    let mut value = serde_json::Map::new();
    insert_optional_string(&mut value, "target_pubkey", options.target_pubkey)?;
    insert_optional_u128_hex(&mut value, "amount", options.has_amount, options.amount);
    insert_optional_string(&mut value, "payment_hash", options.payment_hash)?;
    insert_optional_u64_hex(
        &mut value,
        "final_tlc_expiry_delta",
        options.has_final_tlc_expiry_delta,
        options.final_tlc_expiry_delta,
    );
    insert_optional_u64_hex(
        &mut value,
        "tlc_expiry_limit",
        options.has_tlc_expiry_limit,
        options.tlc_expiry_limit,
    );
    insert_optional_string(&mut value, "invoice", options.invoice)?;
    insert_optional_u64_hex(&mut value, "timeout", options.has_timeout, options.timeout);
    insert_optional_u128_hex(
        &mut value,
        "max_fee_amount",
        options.has_max_fee_amount,
        options.max_fee_amount,
    );
    insert_optional_u64_hex(
        &mut value,
        "max_fee_rate",
        options.has_max_fee_rate,
        options.max_fee_rate,
    );
    insert_optional_u64_hex(
        &mut value,
        "max_parts",
        options.has_max_parts,
        options.max_parts,
    );
    insert_optional_json(&mut value, "trampoline_hops", options.trampoline_hops_json)?;
    insert_optional_bool(&mut value, "keysend", options.has_keysend, options.keysend);
    insert_optional_json(&mut value, "udt_type_script", options.udt_type_script_json)?;
    insert_optional_bool(
        &mut value,
        "allow_self_payment",
        options.has_allow_self_payment,
        options.allow_self_payment,
    );
    insert_optional_json(&mut value, "custom_records", options.custom_records_json)?;
    insert_optional_json(&mut value, "hop_hints", options.hop_hints_json)?;
    insert_optional_bool(&mut value, "dry_run", options.has_dry_run, options.dry_run);
    deserialize_object(value)
}

pub(super) fn build_router_params_from_options(
    options: &FiberBuildRouterOptions,
) -> FfiCallResult<fnn::rpc::payment::BuildRouterParams> {
    validate_options_struct::<FiberBuildRouterOptions>(
        options.struct_size,
        options.flags,
        "FiberBuildRouterOptions",
    )?;
    let mut value = serde_json::Map::new();
    insert_optional_u128_hex(&mut value, "amount", options.has_amount, options.amount);
    insert_optional_json(&mut value, "udt_type_script", options.udt_type_script_json)?;
    value.insert(
        "hops_info".to_string(),
        json_field("hops_info_json", options.hops_info_json)?,
    );
    insert_optional_u64_hex(
        &mut value,
        "final_tlc_expiry_delta",
        options.has_final_tlc_expiry_delta,
        options.final_tlc_expiry_delta,
    );
    deserialize_object(value)
}

pub(super) fn send_payment_with_router_params_from_options(
    options: &FiberSendPaymentWithRouterOptions,
) -> FfiCallResult<fnn::rpc::payment::SendPaymentWithRouterParams> {
    validate_options_struct::<FiberSendPaymentWithRouterOptions>(
        options.struct_size,
        options.flags,
        "FiberSendPaymentWithRouterOptions",
    )?;
    let mut value = serde_json::Map::new();
    insert_optional_string(&mut value, "payment_hash", options.payment_hash)?;
    value.insert(
        "router".to_string(),
        json_field("router_json", options.router_json)?,
    );
    insert_optional_string(&mut value, "invoice", options.invoice)?;
    insert_optional_json(&mut value, "custom_records", options.custom_records_json)?;
    insert_optional_bool(&mut value, "keysend", options.has_keysend, options.keysend);
    insert_optional_json(&mut value, "udt_type_script", options.udt_type_script_json)?;
    insert_optional_bool(&mut value, "dry_run", options.has_dry_run, options.dry_run);
    deserialize_object(value)
}

pub(super) fn list_payments_params_from_options(
    options: &FiberListPaymentsOptions,
) -> FfiCallResult<fnn::rpc::payment::ListPaymentsParams> {
    validate_options_struct::<FiberListPaymentsOptions>(
        options.struct_size,
        options.flags,
        "FiberListPaymentsOptions",
    )?;
    let mut value = serde_json::Map::new();
    match options.status {
        FIBER_PAYMENT_STATUS_FILTER_ALL => {}
        FIBER_PAYMENT_STATUS_FILTER_CREATED => {
            value.insert(
                "status".to_string(),
                serde_json::Value::String("Created".to_string()),
            );
        }
        FIBER_PAYMENT_STATUS_FILTER_INFLIGHT => {
            value.insert(
                "status".to_string(),
                serde_json::Value::String("Inflight".to_string()),
            );
        }
        FIBER_PAYMENT_STATUS_FILTER_SUCCESS => {
            value.insert(
                "status".to_string(),
                serde_json::Value::String("Success".to_string()),
            );
        }
        FIBER_PAYMENT_STATUS_FILTER_FAILED => {
            value.insert(
                "status".to_string(),
                serde_json::Value::String("Failed".to_string()),
            );
        }
        _ => {
            return Err(ffi_error(
                FiberFfiStatus::InvalidArgument,
                "status must be one of FIBER_PAYMENT_STATUS_FILTER_*",
            ));
        }
    };
    insert_optional_u64_hex(&mut value, "limit", options.has_limit, options.limit);
    insert_optional_string(&mut value, "after", options.after)?;
    deserialize_object(value)
}

pub(super) fn new_invoice_params_from_options(
    options: &FiberNewInvoiceOptions,
) -> FfiCallResult<fnn::rpc::invoice::NewInvoiceParams> {
    validate_options_struct::<FiberNewInvoiceOptions>(
        options.struct_size,
        options.flags,
        "FiberNewInvoiceOptions",
    )?;
    let mut value = serde_json::Map::new();
    value.insert("amount".to_string(), u128_hex_field(options.amount));
    insert_optional_string(&mut value, "description", options.description)?;
    let currency = match options.currency {
        FIBER_INVOICE_CURRENCY_DEFAULT => "Fibd",
        FIBER_INVOICE_CURRENCY_FIBB => "Fibb",
        FIBER_INVOICE_CURRENCY_FIBT => "Fibt",
        FIBER_INVOICE_CURRENCY_FIBD => "Fibd",
        _ => {
            return Err(ffi_error(
                FiberFfiStatus::InvalidArgument,
                "currency must be one of FIBER_INVOICE_CURRENCY_*",
            ));
        }
    };
    value.insert(
        "currency".to_string(),
        serde_json::Value::String(currency.to_string()),
    );
    insert_optional_string(&mut value, "payment_preimage", options.payment_preimage)?;
    insert_optional_string(&mut value, "payment_hash", options.payment_hash)?;
    insert_optional_u64_hex(&mut value, "expiry", options.has_expiry, options.expiry);
    insert_optional_string(&mut value, "fallback_address", options.fallback_address)?;
    insert_optional_u64_hex(
        &mut value,
        "final_expiry_delta",
        options.has_final_expiry_delta,
        options.final_expiry_delta,
    );
    insert_optional_json(&mut value, "udt_type_script", options.udt_type_script_json)?;
    match options.hash_algorithm {
        FIBER_HASH_ALGORITHM_DEFAULT => {}
        FIBER_HASH_ALGORITHM_CKB_HASH => {
            value.insert(
                "hash_algorithm".to_string(),
                serde_json::Value::String("ckb_hash".to_string()),
            );
        }
        FIBER_HASH_ALGORITHM_SHA256 => {
            value.insert(
                "hash_algorithm".to_string(),
                serde_json::Value::String("sha256".to_string()),
            );
        }
        _ => {
            return Err(ffi_error(
                FiberFfiStatus::InvalidArgument,
                "hash_algorithm must be one of FIBER_HASH_ALGORITHM_*",
            ));
        }
    }
    insert_optional_bool(
        &mut value,
        "allow_mpp",
        options.has_allow_mpp,
        options.allow_mpp,
    );
    insert_optional_bool(
        &mut value,
        "allow_trampoline_routing",
        options.has_allow_trampoline_routing,
        options.allow_trampoline_routing,
    );
    deserialize_object(value)
}

pub(super) fn required_hash_param(
    name: &str,
    ptr: *const c_char,
) -> FfiCallResult<serde_json::Value> {
    let mut value = serde_json::Map::new();
    value.insert(name.to_string(), string_field(name, ptr)?);
    Ok(serde_json::Value::Object(value))
}

pub(super) fn string_field(name: &str, ptr: *const c_char) -> FfiCallResult<serde_json::Value> {
    Ok(serde_json::Value::String(required_string(ptr, name)?))
}

fn json_field(name: &str, ptr: *const c_char) -> FfiCallResult<serde_json::Value> {
    let json = required_string(ptr, name)?;
    serde_json::from_str(&json).map_err(|err| {
        ffi_error(
            FiberFfiStatus::InvalidArgument,
            format!("invalid {name}: {err}"),
        )
    })
}

fn insert_optional_string(
    value: &mut serde_json::Map<String, serde_json::Value>,
    name: &str,
    ptr: *const c_char,
) -> FfiCallResult<()> {
    let Some(field) = optional_string(ptr)? else {
        return Ok(());
    };
    if field.is_empty() {
        return Ok(());
    }
    value.insert(name.to_string(), serde_json::Value::String(field));
    Ok(())
}

fn insert_optional_json(
    value: &mut serde_json::Map<String, serde_json::Value>,
    name: &str,
    ptr: *const c_char,
) -> FfiCallResult<()> {
    let Some(field) = optional_string(ptr)? else {
        return Ok(());
    };
    if field.is_empty() {
        return Ok(());
    }
    let json = serde_json::from_str(&field).map_err(|err| {
        ffi_error(
            FiberFfiStatus::InvalidArgument,
            format!("invalid {name}_json: {err}"),
        )
    })?;
    value.insert(name.to_string(), json);
    Ok(())
}

fn insert_optional_bool(
    value: &mut serde_json::Map<String, serde_json::Value>,
    name: &str,
    has_value: i32,
    field: i32,
) {
    if has_value != 0 {
        value.insert(name.to_string(), serde_json::Value::Bool(field != 0));
    }
}

fn insert_optional_u64_number(
    value: &mut serde_json::Map<String, serde_json::Value>,
    name: &str,
    has_value: i32,
    field: u64,
) {
    if has_value != 0 {
        value.insert(
            name.to_string(),
            serde_json::Value::Number(serde_json::Number::from(field)),
        );
    }
}

fn insert_optional_u64_hex(
    value: &mut serde_json::Map<String, serde_json::Value>,
    name: &str,
    has_value: i32,
    field: u64,
) {
    if has_value != 0 {
        value.insert(name.to_string(), u64_hex_field(field));
    }
}

fn insert_optional_u128_hex(
    value: &mut serde_json::Map<String, serde_json::Value>,
    name: &str,
    has_value: i32,
    field: FiberU128,
) {
    if has_value != 0 {
        value.insert(name.to_string(), u128_hex_field(field));
    }
}

fn u64_hex_field(value: u64) -> serde_json::Value {
    serde_json::Value::String(format!("0x{value:x}"))
}

fn u128_hex_field(value: FiberU128) -> serde_json::Value {
    serde_json::Value::String(format!("0x{:x}", fiber_u128_to_u128(value)))
}

fn fiber_u128_to_u128(value: FiberU128) -> u128 {
    ((value.high as u128) << 64) | value.low as u128
}

pub(super) fn deserialize_object<T: DeserializeOwned>(
    value: serde_json::Map<String, serde_json::Value>,
) -> FfiCallResult<T> {
    deserialize_value(serde_json::Value::Object(value))
}

pub(super) fn deserialize_value<T: DeserializeOwned>(value: serde_json::Value) -> FfiCallResult<T> {
    serde_json::from_value(value).map_err(|err| {
        ffi_error(
            FiberFfiStatus::InvalidArgument,
            format!("invalid native options: {err}"),
        )
    })
}

pub(super) fn validate_options_struct<T>(
    struct_size: u32,
    flags: u32,
    name: &str,
) -> FfiCallResult<()> {
    let expected_size = std::mem::size_of::<T>();
    if struct_size == 0 {
        return Err(ffi_error(
            FiberFfiStatus::InvalidArgument,
            format!("{name}.struct_size must be set to sizeof({name})"),
        ));
    }
    if (struct_size as usize) < expected_size {
        return Err(ffi_error(
            FiberFfiStatus::InvalidArgument,
            format!(
                "{name}.struct_size is too small: got {struct_size}, expected at least {expected_size}"
            ),
        ));
    }
    if flags != 0 {
        return Err(ffi_error(
            FiberFfiStatus::InvalidArgument,
            format!("{name}.flags must be 0"),
        ));
    }
    Ok(())
}

pub(super) fn funding_lock_from_discovery_options(
    options: &FiberCkbDiscoverHistoryStartBlockOptions,
) -> FfiCallResult<ckb_types::packed::Script> {
    let lock_args = optional_string(options.lock_args)?.filter(|value| !value.trim().is_empty());
    let pubkey = optional_string(options.pubkey)?.filter(|value| !value.trim().is_empty());
    let address = optional_string(options.address)?.filter(|value| !value.trim().is_empty());
    let supplied = usize::from(lock_args.is_some())
        + usize::from(pubkey.is_some())
        + usize::from(address.is_some());
    if supplied != 1 {
        return Err(ffi_error(
            FiberFfiStatus::InvalidArgument,
            "exactly one of lock_args, pubkey, and address must be supplied",
        ));
    }

    let payload = if let Some(lock_args) = lock_args {
        let value = lock_args.strip_prefix("0x").unwrap_or(&lock_args);
        let bytes = hex::decode(value).map_err(|err| {
            ffi_error(
                FiberFfiStatus::InvalidArgument,
                format!("invalid lock_args hex: {err}"),
            )
        })?;
        let hash = ckb_types::H160::from_slice(&bytes).map_err(|err| {
            ffi_error(
                FiberFfiStatus::InvalidArgument,
                format!("lock_args must be exactly 20 bytes: {err}"),
            )
        })?;
        CkbAddressPayload::from_pubkey_hash(hash)
    } else if let Some(pubkey) = pubkey {
        let value = pubkey.strip_prefix("0x").unwrap_or(&pubkey);
        let bytes = hex::decode(value).map_err(|err| {
            ffi_error(
                FiberFfiStatus::InvalidArgument,
                format!("invalid CKB pubkey hex: {err}"),
            )
        })?;
        let pubkey = secp256k1::PublicKey::from_slice(&bytes).map_err(|err| {
            ffi_error(
                FiberFfiStatus::InvalidArgument,
                format!("invalid CKB secp256k1 pubkey: {err}"),
            )
        })?;
        CkbAddressPayload::from_pubkey(&pubkey)
    } else {
        let address = address.expect("one identity was validated above");
        let address = address.parse::<CkbAddress>().map_err(|err| {
            ffi_error(
                FiberFfiStatus::InvalidArgument,
                format!("invalid CKB address: {err}"),
            )
        })?;
        address.payload().clone()
    };
    Ok((&payload).into())
}

pub(super) fn optional_u64(has_value: i32, value: u64, field: &str) -> FfiCallResult<Option<u64>> {
    match has_value {
        0 => Ok(None),
        1 => Ok(Some(value)),
        _ => Err(ffi_error(
            FiberFfiStatus::InvalidArgument,
            format!("{field} must be 0 or 1"),
        )),
    }
}

pub(super) fn parse_pubkey(value: &str) -> FfiCallResult<fnn::fiber_types::Pubkey> {
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

pub(super) fn parse_addr_type(value: Option<&str>) -> FfiCallResult<Option<TransportType>> {
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
