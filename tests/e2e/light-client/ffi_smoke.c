#include "fiber_ffi.h"

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
  if (argc != 2) {
    fprintf(stderr, "usage: %s CONFIG_PATH\n", argv[0]);
    return 2;
  }

  FiberStartOptions options = {0};
  options.config_path = argv[1];
  options.log_level = "info,fiber_ffi=debug";

  FiberFfiStatus status =
      fiber_prepare_ckb(&options, on_ckb_prepared, NULL);
  if (status != FIBER_FFI_STATUS_OK) {
    print_last_error("fiber_prepare_ckb", status);
    return 1;
  }
  while (!atomic_load(&ckb_prepare_finished)) {
  }
  if (!atomic_load(&ckb_prepare_succeeded)) {
    fprintf(stderr, "fiber_prepare_ckb did not report Light Client readiness\n");
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

  status = fiber_stop(handle);
  if (status != FIBER_FFI_STATUS_OK) {
    print_last_error("fiber_stop", status);
    return 1;
  }

  puts("fiber-ffi light-client E2E smoke test passed");
  return 0;
}
