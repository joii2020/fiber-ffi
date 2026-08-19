#define _POSIX_C_SOURCE 200809L

#include "fiber_ffi.h"

#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

#define DEFAULT_CKB_RPC_URL "https://testnet.ckbapp.dev/"

typedef struct DemoArgs {
  const char *config_path;
  const char *data_dir;
  const char *ckb_rpc_url;
  const char *log_level;
  const char *log_file;
} DemoArgs;

typedef struct PrepareState {
  pthread_mutex_t mutex;
  pthread_cond_t condition;
  int done;
  FiberFfiStatus status;
  char *result_json;
} PrepareState;

static const char *status_name(FiberFfiStatus status) {
  switch (status) {
  case FIBER_FFI_STATUS_OK:
    return "OK";
  case FIBER_FFI_STATUS_NULL_POINTER:
    return "NULL_POINTER";
  case FIBER_FFI_STATUS_INVALID_ARGUMENT:
    return "INVALID_ARGUMENT";
  case FIBER_FFI_STATUS_STARTUP_FAILED:
    return "STARTUP_FAILED";
  case FIBER_FFI_STATUS_ALREADY_STOPPED:
    return "ALREADY_STOPPED";
  case FIBER_FFI_STATUS_PANIC:
    return "PANIC";
  case FIBER_FFI_STATUS_NOT_READY:
    return "NOT_READY";
  default:
    return "UNKNOWN";
  }
}

static void print_last_error(const char *operation, FiberFfiStatus status) {
  size_t required = fiber_last_error_message(NULL, 0);
  char *message = (char *)calloc(required + 1, 1);

  if (message != NULL) {
    fiber_last_error_message(message, required + 1);
  }
  fprintf(stderr, "%s failed (status=%s): %s\n", operation,
          status_name(status),
          message != NULL && message[0] != '\0' ? message : "unknown error");
  free(message);
}

static int print_json_result(const char *label, const char *operation,
                             FiberFfiStatus status, char *result_json) {
  if (status != FIBER_FFI_STATUS_OK) {
    if (result_json != NULL) {
      fiber_string_free(result_json);
    }
    print_last_error(operation, status);
    return 0;
  }

  printf("[%s] %s\n", label, result_json != NULL ? result_json : "null");
  if (result_json != NULL) {
    fiber_string_free(result_json);
  }
  return 1;
}

static char *join_path(const char *left, const char *right) {
  size_t left_length = strlen(left);
  size_t right_length = strlen(right);
  int needs_slash = left_length != 0 && left[left_length - 1] != '/';
  char *result =
      (char *)malloc(left_length + (size_t)needs_slash + right_length + 1);

  if (result == NULL) {
    return NULL;
  }
  memcpy(result, left, left_length);
  if (needs_slash) {
    result[left_length++] = '/';
  }
  memcpy(result + left_length, right, right_length + 1);
  return result;
}

static int is_regular_file(const char *path) {
  struct stat metadata;
  return stat(path, &metadata) == 0 && S_ISREG(metadata.st_mode);
}

static void on_fiber_event(const char *event_json, void *user_data) {
  FILE *log_file = (FILE *)user_data;

  if (log_file != NULL) {
    fprintf(log_file, "[event] %s\n", event_json != NULL ? event_json : "");
    fflush(log_file);
  }
}

static void on_ckb_prepare(FiberFfiStatus status, const char *result_json,
                           void *user_data) {
  PrepareState *state = (PrepareState *)user_data;
  const char *result = result_json != NULL ? result_json : "";
  int terminal = status != FIBER_FFI_STATUS_OK ||
                 strstr(result, "\"ready\":true") != NULL ||
                 strstr(result, "\"status\":\"failed\"") != NULL;

  if (state == NULL) {
    return;
  }
  if (!terminal) {
    printf("[light-client] %s\n", result);
    fflush(stdout);
    return;
  }

  pthread_mutex_lock(&state->mutex);
  state->status = status;
  state->result_json = strdup(result);
  state->done = 1;
  pthread_cond_signal(&state->condition);
  pthread_mutex_unlock(&state->mutex);
}

static int prepare_ckb(const FiberStartOptions *options,
                       int has_history_start_block,
                       uint64_t history_start_block) {
  PrepareState state;
  struct timespec started_at;
  struct timespec finished_at;
  FiberFfiStatus status;
  int ready = 0;
  int error;

  memset(&state, 0, sizeof(state));
  error = pthread_mutex_init(&state.mutex, NULL);
  if (error != 0) {
    fprintf(stderr, "Unable to initialize Light Client mutex: %s\n",
            strerror(error));
    return 0;
  }
  error = pthread_cond_init(&state.condition, NULL);
  if (error != 0) {
    fprintf(stderr, "Unable to initialize Light Client condition: %s\n",
            strerror(error));
    pthread_mutex_destroy(&state.mutex);
    return 0;
  }
  if (clock_gettime(CLOCK_MONOTONIC, &started_at) != 0) {
    fprintf(stderr, "Unable to record Light Client start time: %s\n",
            strerror(errno));
    pthread_cond_destroy(&state.condition);
    pthread_mutex_destroy(&state.mutex);
    return 0;
  }

  status = has_history_start_block
               ? fiber_prepare_ckb_with_history_start_block(
                     options, history_start_block, on_ckb_prepare, &state)
               : fiber_prepare_ckb(options, on_ckb_prepare, &state);
  if (status != FIBER_FFI_STATUS_OK) {
    print_last_error("fiber_prepare_ckb", status);
    pthread_cond_destroy(&state.condition);
    pthread_mutex_destroy(&state.mutex);
    return 0;
  }

  pthread_mutex_lock(&state.mutex);
  while (!state.done) {
    pthread_cond_wait(&state.condition, &state.mutex);
  }
  pthread_mutex_unlock(&state.mutex);

  if (state.result_json == NULL) {
    fprintf(stderr, "Unable to copy the Light Client prepare result\n");
  } else if (state.status != FIBER_FFI_STATUS_OK) {
    fprintf(stderr, "Light Client preparation failed (status=%s): %s\n",
            status_name(state.status), state.result_json);
  } else if (strstr(state.result_json, "\"mode\":\"light_client\"") ==
                 NULL ||
             strstr(state.result_json, "\"ready\":true") == NULL) {
    fprintf(stderr,
            "fiber-ffi was not built with the embedded Light Client: %s\n",
            state.result_json);
  } else if (clock_gettime(CLOCK_MONOTONIC, &finished_at) != 0) {
    fprintf(stderr, "Unable to record Light Client completion time: %s\n",
            strerror(errno));
  } else {
    double elapsed = (double)(finished_at.tv_sec - started_at.tv_sec) +
                     (double)(finished_at.tv_nsec - started_at.tv_nsec) /
                         1000000000.0;
    printf("[light-client] ready after %.3f seconds: %s\n", elapsed,
           state.result_json);
    ready = 1;
  }

  free(state.result_json);
  pthread_cond_destroy(&state.condition);
  pthread_mutex_destroy(&state.mutex);
  return ready;
}

static int discover_history_start_block(
    const DemoArgs *args, const FiberStartOptions *start_options,
    int *has_history_start_block, uint64_t *history_start_block) {
  FiberCkbDiscoverHistoryStartBlockOptions discovery =
      FIBER_CKB_DISCOVER_HISTORY_START_BLOCK_OPTIONS_INIT;
  FiberFfiStatus status;
  char *ckb_dir = NULL;
  char *birthday_path = NULL;
  char *funding_address = NULL;
  int ok = 0;

  *has_history_start_block = 0;
  *history_start_block = 0;
  ckb_dir = join_path(args->data_dir, "ckb");
  birthday_path =
      ckb_dir != NULL ? join_path(ckb_dir, "wallet-birthday.json") : NULL;
  if (birthday_path == NULL) {
    fprintf(stderr, "Unable to construct wallet birthday path\n");
    goto cleanup;
  }
  if (is_regular_file(birthday_path)) {
    printf("[discovery] using persisted wallet birthday: %s\n", birthday_path);
    ok = 1;
    goto cleanup;
  }

  status = fiber_ckb_funding_address(start_options, &funding_address);
  if (status != FIBER_FFI_STATUS_OK) {
    print_last_error("fiber_ckb_funding_address", status);
    goto cleanup;
  }

  discovery.rpc_url = args->ckb_rpc_url;
  discovery.address = funding_address;
  status = fiber_ckb_discover_history_start_block(&discovery,
                                                   history_start_block);
  if (status != FIBER_FFI_STATUS_OK) {
    print_last_error("fiber_ckb_discover_history_start_block", status);
    goto cleanup;
  }

  *has_history_start_block = 1;
  printf("[discovery] funding address: %s\n"
         "[discovery] external CKB RPC/Indexer: %s\n"
         "[discovery] safe history start: %" PRIu64 " (0x%" PRIx64 ")\n",
         funding_address, args->ckb_rpc_url, *history_start_block,
         *history_start_block);
  ok = 1;

cleanup:
  if (funding_address != NULL) {
    fiber_string_free(funding_address);
  }
  free(birthday_path);
  free(ckb_dir);
  return ok;
}

static int validate_args(const DemoArgs *args) {
  char *ckb_dir = NULL;
  char *key_path = NULL;
  int ok = 0;

  if (!is_regular_file(args->config_path)) {
    fprintf(stderr, "Configuration file not found: %s\n", args->config_path);
    return 0;
  }
  ckb_dir = join_path(args->data_dir, "ckb");
  key_path = ckb_dir != NULL ? join_path(ckb_dir, "key") : NULL;
  if (key_path == NULL) {
    fprintf(stderr, "Unable to construct CKB key path\n");
  } else if (!is_regular_file(key_path)) {
    fprintf(stderr, "CKB key not found: %s (run make setup first)\n", key_path);
  } else if (getenv("FIBER_SECRET_KEY_PASSWORD") == NULL) {
    fprintf(stderr,
            "FIBER_SECRET_KEY_PASSWORD is not set; use make run or export it\n");
  } else {
    ok = 1;
  }
  free(key_path);
  free(ckb_dir);
  return ok;
}

static void print_help(const char *program) {
  printf("fiber-ffi background C demo\n\n"
         "Usage:\n  %s [options]\n\n"
         "Options:\n"
         "  --config PATH     Fiber YAML configuration\n"
         "  --data PATH       data directory containing ckb/key\n"
         "  --ckb-rpc URL     external CKB RPC/Indexer for first-run discovery\n"
         "  --log-level TEXT  fiber-ffi log filter\n"
         "  --log-file PATH   log file [default: <data>/fiber-ffi.log]\n"
         "  -h, --help        show this help\n",
         program);
}

/* Returns 1 to run, 0 for help, and -1 for invalid arguments. */
static int parse_args(int argc, char **argv, DemoArgs *args) {
  int index;

  args->config_path = "examples/c-demo/config.testnet.yml";
  args->data_dir = "examples/c-demo/data";
  args->ckb_rpc_url = DEFAULT_CKB_RPC_URL;
  args->log_level = "info,fiber_ffi=debug";
  args->log_file = NULL;

  for (index = 1; index < argc; ++index) {
    const char *argument = argv[index];
    const char **destination = NULL;

    if (strcmp(argument, "-h") == 0 || strcmp(argument, "--help") == 0) {
      print_help(argv[0]);
      return 0;
    }
    if (strcmp(argument, "--config") == 0) {
      destination = &args->config_path;
    } else if (strcmp(argument, "--data") == 0) {
      destination = &args->data_dir;
    } else if (strcmp(argument, "--ckb-rpc") == 0 ||
               strcmp(argument, "--ckb-discovery-rpc") == 0) {
      destination = &args->ckb_rpc_url;
    } else if (strcmp(argument, "--log-level") == 0) {
      destination = &args->log_level;
    } else if (strcmp(argument, "--log-file") == 0) {
      destination = &args->log_file;
    } else {
      fprintf(stderr, "Unknown option: %s (use --help)\n", argument);
      return -1;
    }
    if (++index >= argc) {
      fprintf(stderr, "%s requires a value\n", argument);
      return -1;
    }
    *destination = argv[index];
  }
  return 1;
}

static int block_shutdown_signals(sigset_t *shutdown_signals) {
  int error;

  sigemptyset(shutdown_signals);
  sigaddset(shutdown_signals, SIGINT);
  sigaddset(shutdown_signals, SIGTERM);
  error = pthread_sigmask(SIG_BLOCK, shutdown_signals, NULL);
  if (error != 0) {
    fprintf(stderr, "Unable to block shutdown signals: %s\n", strerror(error));
    return 0;
  }
  return 1;
}

int main(int argc, char **argv) {
  DemoArgs args;
  FiberStartOptions options = {0};
  FiberHandle *node = NULL;
  FiberFfiStatus status;
  char *default_log_file = NULL;
  char *result_json = NULL;
  FILE *event_log = NULL;
  sigset_t shutdown_signals;
  uint64_t history_start_block = 0;
  int has_history_start_block = 0;
  int received_signal = 0;
  int signal_error;
  int exit_code = EXIT_FAILURE;
  int argument_result = parse_args(argc, argv, &args);

  if (argument_result <= 0) {
    return argument_result == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
  }
  if (args.log_file == NULL) {
    default_log_file = join_path(args.data_dir, "fiber-ffi.log");
    if (default_log_file == NULL) {
      fprintf(stderr, "Unable to construct default log path\n");
      goto cleanup;
    }
    args.log_file = default_log_file;
  }
  if (!validate_args(&args) || !block_shutdown_signals(&shutdown_signals)) {
    goto cleanup;
  }

  event_log = fopen(args.log_file, "a");
  if (event_log == NULL) {
    fprintf(stderr, "Unable to open log file %s: %s\n", args.log_file,
            strerror(errno));
    goto cleanup;
  }
  if (setenv("FIBER_FFI_LOG_FILE", args.log_file, 1) != 0) {
    fprintf(stderr, "Unable to set FIBER_FFI_LOG_FILE: %s\n",
            strerror(errno));
    goto cleanup;
  }

  printf("Fiber FFI C Demo\n"
         "  config:  %s\n"
         "  data:    %s\n"
         "  CKB RPC: %s\n"
         "  log:     %s\n"
         "  version: %s\n",
         args.config_path, args.data_dir, args.ckb_rpc_url, args.log_file,
         fiber_version());

  options.config_path = args.config_path;
  options.database_prefix = args.data_dir;
  options.log_level = args.log_level;

  if (!discover_history_start_block(&args, &options,
                                    &has_history_start_block,
                                    &history_start_block)) {
    goto cleanup;
  }
  printf("[startup] synchronizing the embedded CKB Light Client...\n");
  if (!prepare_ckb(&options, has_history_start_block, history_start_block)) {
    goto cleanup;
  }

  options.event_callback = on_fiber_event;
  options.event_callback_user_data = event_log;
  printf("[startup] starting Fiber and its JSON-RPC service...\n");
  status = fiber_start(&options, &node);
  if (status != FIBER_FFI_STATUS_OK) {
    print_last_error("fiber_start", status);
    goto cleanup;
  }
  if (node == NULL) {
    fprintf(stderr, "fiber_start returned a null handle\n");
    goto cleanup;
  }

  status = fiber_node_info(node, &result_json);
  if (!print_json_result("node-info", "fiber_node_info", status,
                         result_json)) {
    result_json = NULL;
    goto shutdown;
  }
  result_json = NULL;
  status = fiber_ckb_readiness(node, &result_json);
  if (!print_json_result("ckb-readiness", "fiber_ckb_readiness", status,
                         result_json)) {
    result_json = NULL;
    goto shutdown;
  }
  result_json = NULL;

  printf("[ready] Fiber RPC: http://127.0.0.1:8227\n"
         "[ready] Try: fnn-cli -u http://127.0.0.1:8227 info\n"
         "[ready] Press Ctrl-C to stop.\n");
  signal_error = sigwait(&shutdown_signals, &received_signal);
  if (signal_error != 0) {
    fprintf(stderr, "Unable to wait for a shutdown signal: %s\n",
            strerror(signal_error));
    goto shutdown;
  }
  printf("[shutdown] received signal %d, stopping Fiber...\n", received_signal);
  exit_code = EXIT_SUCCESS;

shutdown:
  status = fiber_stop(node);
  node = NULL;
  if (status != FIBER_FFI_STATUS_OK) {
    print_last_error("fiber_stop", status);
    exit_code = EXIT_FAILURE;
  } else {
    printf("[shutdown] Fiber stopped\n");
  }

cleanup:
  if (node != NULL) {
    status = fiber_stop(node);
    if (status != FIBER_FFI_STATUS_OK) {
      print_last_error("fiber_stop", status);
    }
  }
  if (event_log != NULL) {
    fclose(event_log);
  }
  free(default_log_file);
  return exit_code;
}
