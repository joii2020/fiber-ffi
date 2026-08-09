#include "fiber_ffi.h"

#include <stdio.h>
#include <stdlib.h>

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

  FiberHandle *handle = NULL;
  FiberFfiStatus status = fiber_start(&options, &handle);
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
