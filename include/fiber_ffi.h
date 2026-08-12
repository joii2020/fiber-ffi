#ifndef FIBER_FFI_H
#define FIBER_FFI_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct FiberHandle FiberHandle;

typedef void (*fiber_event_callback)(const char *event_json, void *user_data);

typedef struct FiberStartOptions {
  const char *config_path;
  const char *database_prefix;
  const char *log_level;
  fiber_event_callback event_callback;
  void *event_callback_user_data;
} FiberStartOptions;

typedef struct FiberConnectPeerOptions {
  const char *address;
  const char *pubkey;
  /* Optional: tcp, ws, or wss. Used only when pubkey is set. */
  const char *addr_type;
  /* Non-zero saves the peer address when address is set. */
  int save;
} FiberConnectPeerOptions;

typedef struct FiberU128 {
  uint64_t low;
  uint64_t high;
} FiberU128;

typedef int32_t FiberInvoiceCurrency;
#define FIBER_INVOICE_CURRENCY_DEFAULT 0
#define FIBER_INVOICE_CURRENCY_FIBB 1
#define FIBER_INVOICE_CURRENCY_FIBT 2
#define FIBER_INVOICE_CURRENCY_FIBD 3

typedef int32_t FiberHashAlgorithm;
#define FIBER_HASH_ALGORITHM_DEFAULT 0
#define FIBER_HASH_ALGORITHM_CKB_HASH 1
#define FIBER_HASH_ALGORITHM_SHA256 2

typedef int32_t FiberPaymentStatusFilter;
#define FIBER_PAYMENT_STATUS_FILTER_ALL 0
#define FIBER_PAYMENT_STATUS_FILTER_CREATED 1
#define FIBER_PAYMENT_STATUS_FILTER_INFLIGHT 2
#define FIBER_PAYMENT_STATUS_FILTER_SUCCESS 3
#define FIBER_PAYMENT_STATUS_FILTER_FAILED 4

typedef struct FiberOpenChannelOptions {
  uint32_t struct_size;
  uint32_t flags;
  const char *pubkey;
  FiberU128 funding_amount;
  int has_public;
  int public_;
  int has_one_way;
  int one_way;
  const char *funding_udt_type_script_json;
  const char *shutdown_script_json;
  uint64_t commitment_delay_epoch;
  int has_commitment_delay_epoch;
  uint64_t commitment_fee_rate;
  int has_commitment_fee_rate;
  uint64_t funding_fee_rate;
  int has_funding_fee_rate;
  uint64_t tlc_expiry_delta;
  int has_tlc_expiry_delta;
  FiberU128 tlc_min_value;
  int has_tlc_min_value;
  FiberU128 tlc_fee_proportional_millionths;
  int has_tlc_fee_proportional_millionths;
  FiberU128 max_tlc_value_in_flight;
  int has_max_tlc_value_in_flight;
  uint64_t max_tlc_number_in_flight;
  int has_max_tlc_number_in_flight;
} FiberOpenChannelOptions;

typedef struct FiberAcceptChannelOptions {
  uint32_t struct_size;
  uint32_t flags;
  const char *temporary_channel_id;
  FiberU128 funding_amount;
  const char *shutdown_script_json;
  FiberU128 max_tlc_value_in_flight;
  int has_max_tlc_value_in_flight;
  uint64_t max_tlc_number_in_flight;
  int has_max_tlc_number_in_flight;
  FiberU128 tlc_min_value;
  int has_tlc_min_value;
  FiberU128 tlc_fee_proportional_millionths;
  int has_tlc_fee_proportional_millionths;
  uint64_t tlc_expiry_delta;
  int has_tlc_expiry_delta;
} FiberAcceptChannelOptions;

typedef struct FiberOpenChannelWithExternalFundingOptions {
  uint32_t struct_size;
  uint32_t flags;
  const char *pubkey;
  FiberU128 funding_amount;
  int has_public;
  int public_;
  const char *funding_udt_type_script_json;
  const char *shutdown_script_json;
  const char *funding_lock_script_json;
  const char *funding_lock_script_cell_deps_json;
  uint64_t commitment_delay_epoch;
  int has_commitment_delay_epoch;
  uint64_t commitment_fee_rate;
  int has_commitment_fee_rate;
  uint64_t funding_fee_rate;
  int has_funding_fee_rate;
  uint64_t tlc_expiry_delta;
  int has_tlc_expiry_delta;
  FiberU128 tlc_min_value;
  int has_tlc_min_value;
  FiberU128 tlc_fee_proportional_millionths;
  int has_tlc_fee_proportional_millionths;
  FiberU128 max_tlc_value_in_flight;
  int has_max_tlc_value_in_flight;
  uint64_t max_tlc_number_in_flight;
  int has_max_tlc_number_in_flight;
} FiberOpenChannelWithExternalFundingOptions;

typedef struct FiberSubmitSignedFundingTxOptions {
  uint32_t struct_size;
  uint32_t flags;
  const char *channel_id;
  const char *signed_funding_tx_json;
} FiberSubmitSignedFundingTxOptions;

typedef struct FiberListChannelsOptions {
  uint32_t struct_size;
  uint32_t flags;
  const char *pubkey;
  int has_include_closed;
  int include_closed;
  int has_only_pending;
  int only_pending;
} FiberListChannelsOptions;

typedef struct FiberShutdownChannelOptions {
  uint32_t struct_size;
  uint32_t flags;
  const char *channel_id;
  const char *close_script_json;
  uint64_t fee_rate;
  int has_fee_rate;
  int has_force;
  int force;
} FiberShutdownChannelOptions;

typedef struct FiberUpdateChannelOptions {
  uint32_t struct_size;
  uint32_t flags;
  const char *channel_id;
  int has_enabled;
  int enabled;
  uint64_t tlc_expiry_delta;
  int has_tlc_expiry_delta;
  FiberU128 tlc_minimum_value;
  int has_tlc_minimum_value;
  FiberU128 tlc_fee_proportional_millionths;
  int has_tlc_fee_proportional_millionths;
} FiberUpdateChannelOptions;

typedef struct FiberSendPaymentOptions {
  uint32_t struct_size;
  uint32_t flags;
  const char *target_pubkey;
  FiberU128 amount;
  int has_amount;
  const char *payment_hash;
  uint64_t final_tlc_expiry_delta;
  int has_final_tlc_expiry_delta;
  uint64_t tlc_expiry_limit;
  int has_tlc_expiry_limit;
  const char *invoice;
  uint64_t timeout;
  int has_timeout;
  FiberU128 max_fee_amount;
  int has_max_fee_amount;
  uint64_t max_fee_rate;
  int has_max_fee_rate;
  uint64_t max_parts;
  int has_max_parts;
  const char *trampoline_hops_json;
  int has_keysend;
  int keysend;
  const char *udt_type_script_json;
  int has_allow_self_payment;
  int allow_self_payment;
  const char *custom_records_json;
  const char *hop_hints_json;
  int has_dry_run;
  int dry_run;
} FiberSendPaymentOptions;

typedef struct FiberBuildRouterOptions {
  uint32_t struct_size;
  uint32_t flags;
  FiberU128 amount;
  int has_amount;
  const char *udt_type_script_json;
  const char *hops_info_json;
  uint64_t final_tlc_expiry_delta;
  int has_final_tlc_expiry_delta;
} FiberBuildRouterOptions;

typedef struct FiberSendPaymentWithRouterOptions {
  uint32_t struct_size;
  uint32_t flags;
  const char *payment_hash;
  const char *router_json;
  const char *invoice;
  const char *custom_records_json;
  int has_keysend;
  int keysend;
  const char *udt_type_script_json;
  int has_dry_run;
  int dry_run;
} FiberSendPaymentWithRouterOptions;

typedef struct FiberListPaymentsOptions {
  uint32_t struct_size;
  uint32_t flags;
  FiberPaymentStatusFilter status;
  uint64_t limit;
  int has_limit;
  const char *after;
} FiberListPaymentsOptions;

typedef struct FiberNewInvoiceOptions {
  uint32_t struct_size;
  uint32_t flags;
  FiberU128 amount;
  const char *description;
  FiberInvoiceCurrency currency;
  const char *payment_preimage;
  const char *payment_hash;
  uint64_t expiry;
  int has_expiry;
  const char *fallback_address;
  uint64_t final_expiry_delta;
  int has_final_expiry_delta;
  const char *udt_type_script_json;
  FiberHashAlgorithm hash_algorithm;
  int has_allow_mpp;
  int allow_mpp;
  int has_allow_trampoline_routing;
  int allow_trampoline_routing;
} FiberNewInvoiceOptions;

#define FIBER_OPEN_CHANNEL_OPTIONS_INIT                                       \
  { sizeof(FiberOpenChannelOptions), 0 }
#define FIBER_ACCEPT_CHANNEL_OPTIONS_INIT                                     \
  { sizeof(FiberAcceptChannelOptions), 0 }
#define FIBER_OPEN_CHANNEL_WITH_EXTERNAL_FUNDING_OPTIONS_INIT                 \
  { sizeof(FiberOpenChannelWithExternalFundingOptions), 0 }
#define FIBER_SUBMIT_SIGNED_FUNDING_TX_OPTIONS_INIT                           \
  { sizeof(FiberSubmitSignedFundingTxOptions), 0 }
#define FIBER_LIST_CHANNELS_OPTIONS_INIT                                      \
  { sizeof(FiberListChannelsOptions), 0 }
#define FIBER_SHUTDOWN_CHANNEL_OPTIONS_INIT                                   \
  { sizeof(FiberShutdownChannelOptions), 0 }
#define FIBER_UPDATE_CHANNEL_OPTIONS_INIT                                     \
  { sizeof(FiberUpdateChannelOptions), 0 }
#define FIBER_SEND_PAYMENT_OPTIONS_INIT                                       \
  { sizeof(FiberSendPaymentOptions), 0 }
#define FIBER_BUILD_ROUTER_OPTIONS_INIT                                       \
  { sizeof(FiberBuildRouterOptions), 0 }
#define FIBER_SEND_PAYMENT_WITH_ROUTER_OPTIONS_INIT                           \
  { sizeof(FiberSendPaymentWithRouterOptions), 0 }
#define FIBER_LIST_PAYMENTS_OPTIONS_INIT                                      \
  { sizeof(FiberListPaymentsOptions), 0 }
#define FIBER_NEW_INVOICE_OPTIONS_INIT                                        \
  { sizeof(FiberNewInvoiceOptions), 0 }

typedef enum FiberFfiStatus {
  FIBER_FFI_STATUS_OK = 0,
  FIBER_FFI_STATUS_NULL_POINTER = 1,
  FIBER_FFI_STATUS_INVALID_ARGUMENT = 2,
  FIBER_FFI_STATUS_STARTUP_FAILED = 3,
  FIBER_FFI_STATUS_ALREADY_STOPPED = 4,
  FIBER_FFI_STATUS_PANIC = 5,
} FiberFfiStatus;

/* result_json is borrowed and is valid only for the duration of the callback. */
typedef void (*fiber_ckb_prepare_callback)(FiberFfiStatus status,
                                           const char *result_json,
                                           void *user_data);

const char *fiber_version(void);

/*
 * Asynchronously prepares the CKB backend used by the next fiber_start call.
 * A return value of OK means the request was accepted; completion is reported
 * exactly once through callback. The callback is never invoked inline.
 *
 * With disable-ckb-rpc, this starts and synchronizes the embedded Light Client
 * and keeps it alive for a matching fiber_start call. Without that feature, it
 * asynchronously reports {"ready":true,"mode":"external_rpc","skipped":true}.
 */
FiberFfiStatus fiber_prepare_ckb(const FiberStartOptions *options,
                                 fiber_ckb_prepare_callback callback,
                                 void *callback_user_data);

FiberFfiStatus fiber_start(const FiberStartOptions *options,
                           FiberHandle **out_handle);

FiberFfiStatus fiber_stop(FiberHandle *handle);

FiberFfiStatus fiber_node_info(FiberHandle *handle, char **out_json);

FiberFfiStatus fiber_list_peers(FiberHandle *handle, char **out_json);

FiberFfiStatus fiber_connect_peer(FiberHandle *handle,
                                  const FiberConnectPeerOptions *options);

FiberFfiStatus fiber_disconnect_peer(FiberHandle *handle, const char *pubkey);

FiberFfiStatus fiber_open_channel(FiberHandle *handle,
                                  const FiberOpenChannelOptions *options,
                                  char **out_temporary_channel_id);

FiberFfiStatus fiber_accept_channel(FiberHandle *handle,
                                    const FiberAcceptChannelOptions *options,
                                    char **out_channel_id);

FiberFfiStatus fiber_open_channel_with_external_funding(
    FiberHandle *handle, const FiberOpenChannelWithExternalFundingOptions *options,
    char **out_json);

FiberFfiStatus fiber_submit_signed_funding_tx(
    FiberHandle *handle, const FiberSubmitSignedFundingTxOptions *options,
    char **out_json);

FiberFfiStatus fiber_abandon_channel(FiberHandle *handle,
                                     const char *channel_id);

FiberFfiStatus fiber_list_channels(FiberHandle *handle,
                                   const FiberListChannelsOptions *options,
                                   char **out_json);

FiberFfiStatus fiber_shutdown_channel(
    FiberHandle *handle, const FiberShutdownChannelOptions *options);

FiberFfiStatus fiber_update_channel(
    FiberHandle *handle, const FiberUpdateChannelOptions *options);

FiberFfiStatus fiber_send_payment(FiberHandle *handle,
                                  const FiberSendPaymentOptions *options,
                                  char **out_json);

FiberFfiStatus fiber_build_router(FiberHandle *handle,
                                  const FiberBuildRouterOptions *options,
                                  char **out_json);

FiberFfiStatus fiber_send_payment_with_router(
    FiberHandle *handle, const FiberSendPaymentWithRouterOptions *options,
    char **out_json);

FiberFfiStatus fiber_get_payment(FiberHandle *handle,
                                 const char *payment_hash,
                                 char **out_json);

FiberFfiStatus fiber_list_payments(
    FiberHandle *handle, const FiberListPaymentsOptions *options,
    char **out_json);

FiberFfiStatus fiber_new_invoice(FiberHandle *handle,
                                 const FiberNewInvoiceOptions *options,
                                 char **out_invoice_address);

FiberFfiStatus fiber_parse_invoice(FiberHandle *handle,
                                   const char *invoice,
                                   char **out_json);

FiberFfiStatus fiber_get_invoice(FiberHandle *handle,
                                 const char *payment_hash,
                                 char **out_json);

FiberFfiStatus fiber_cancel_invoice(FiberHandle *handle,
                                    const char *payment_hash,
                                    char **out_json);

FiberFfiStatus fiber_settle_invoice(FiberHandle *handle,
                                    const char *payment_hash,
                                    const char *payment_preimage);

void fiber_string_free(char *string);

size_t fiber_last_error_message(char *buffer, size_t buffer_len);

#ifdef __cplusplus
}
#endif

#endif
