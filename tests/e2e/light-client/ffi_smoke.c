#include "fiber_ffi.h"

#include <inttypes.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static atomic_int ckb_prepare_finished = 0;
static atomic_int ckb_prepare_succeeded = 0;

static void on_ckb_prepared(FiberFfiStatus status, const char *result_json,
                            void *user_data) {
  (void)user_data;
  printf("fiber_prepare_ckb: %s\n", result_json != NULL ? result_json : "null");
  fflush(stdout);
  if (status == FIBER_FFI_STATUS_OK && result_json != NULL &&
      strstr(result_json, "\"ready\":true") == NULL) {
    return;
  }
  atomic_store(&ckb_prepare_succeeded,
               status == FIBER_FFI_STATUS_OK && result_json != NULL &&
                   strstr(result_json, "\"ready\":true") != NULL &&
                   strstr(result_json, "\"mode\":\"light_client\"") != NULL);
  atomic_store(&ckb_prepare_finished, 1);
}

static void print_last_error(const char *operation, FiberFfiStatus status) {
  size_t length = fiber_last_error_message(NULL, 0);
  char *message = calloc(length + 1, 1);
  if (message != NULL) {
    fiber_last_error_message(message, length + 1);
  }
  fprintf(stderr, "%s failed with status %d: %s\n", operation, (int)status,
          message != NULL ? message : "failed to allocate error buffer");
  free(message);
}

int main(int argc, char **argv) {
  if (argc != 3) {
    fprintf(stderr, "usage: %s CONFIG_PATH DISCOVERY_RPC_URL\n", argv[0]);
    return 2;
  }

  FiberStartOptions options = {0};
  options.config_path = argv[1];
  options.log_level = "info,fiber_ffi=debug";

  char *funding_address = NULL;
  FiberFfiStatus status =
      fiber_ckb_funding_address(&options, &funding_address);
  if (status != FIBER_FFI_STATUS_OK) {
    print_last_error("fiber_ckb_funding_address", status);
    return 1;
  }
  FiberCkbDiscoverHistoryStartBlockOptions discovery =
      FIBER_CKB_DISCOVER_HISTORY_START_BLOCK_OPTIONS_INIT;
  discovery.rpc_url = argv[2];
  discovery.address = funding_address;
  uint64_t history_start_block = 0;
  status = fiber_ckb_discover_history_start_block(&discovery,
                                                   &history_start_block);
  if (status != FIBER_FFI_STATUS_OK) {
    print_last_error("fiber_ckb_discover_history_start_block", status);
    fiber_string_free(funding_address);
    return 1;
  }
  printf("fiber_ckb_discover_history_start_block: address=%s height=%" PRIu64
         "\n",
         funding_address, history_start_block);
  fiber_string_free(funding_address);

  status = fiber_prepare_ckb_with_history_start_block(
      &options, history_start_block, on_ckb_prepared, NULL);
  if (status != FIBER_FFI_STATUS_OK) {
    print_last_error("fiber_prepare_ckb_with_history_start_block", status);
    return 1;
  }
  while (!atomic_load(&ckb_prepare_finished)) {
  }
  if (!atomic_load(&ckb_prepare_succeeded)) {
    fprintf(stderr,
            "fiber_prepare_ckb_with_history_start_block did not report Light "
            "Client readiness\n");
    return 1;
  }

  FiberHandle *handle = NULL;
  status = fiber_start(&options, &handle);
  if (status != FIBER_FFI_STATUS_OK) {
    print_last_error("fiber_start", status);
    return 1;
  }

  char *node_info = NULL;
  status = fiber_node_info(handle, &node_info);
  if (status != FIBER_FFI_STATUS_OK) {
    print_last_error("fiber_node_info", status);
    fiber_stop(handle);
    return 1;
  }
  printf("fiber_node_info: %s\n", node_info);
  fiber_string_free(node_info);

  char *ckb_readiness = NULL;
  status = fiber_ckb_readiness(handle, &ckb_readiness);
  if (status != FIBER_FFI_STATUS_OK || ckb_readiness == NULL ||
      strstr(ckb_readiness, "\"ready\":true") == NULL) {
    if (status != FIBER_FFI_STATUS_OK) {
      print_last_error("fiber_ckb_readiness", status);
    } else {
      fprintf(stderr, "CKB readiness check failed: %s\n",
              ckb_readiness != NULL ? ckb_readiness : "null");
    }
    fiber_string_free(ckb_readiness);
    fiber_stop(handle);
    return 1;
  }
  printf("fiber_ckb_readiness: %s\n", ckb_readiness);
  fiber_string_free(ckb_readiness);

  char *ckb_balance = NULL;
  status = fiber_ckb_balance(handle, &ckb_balance);
  if (status != FIBER_FFI_STATUS_OK || ckb_balance == NULL ||
      strstr(ckb_balance, "\"mode\":\"light_client\"") == NULL ||
      strstr(ckb_balance, "\"address\":\"ckt1") == NULL ||
      strstr(ckb_balance, "\"lock_args\":\"0x") == NULL ||
      strstr(ckb_balance, "\"capacity_shannons\":") == NULL) {
    if (status != FIBER_FFI_STATUS_OK) {
      print_last_error("fiber_ckb_balance", status);
    } else {
      fprintf(stderr, "CKB balance query failed: %s\n",
              ckb_balance != NULL ? ckb_balance : "null");
    }
    fiber_string_free(ckb_balance);
    fiber_stop(handle);
    return 1;
  }
  printf("fiber_ckb_balance: %s\n", ckb_balance);
  fiber_string_free(ckb_balance);

  status = fiber_stop(handle);
  if (status != FIBER_FFI_STATUS_OK) {
    print_last_error("fiber_stop", status);
    return 1;
  }

  puts("fiber-ffi light-client E2E smoke test passed");
  return 0;
}
