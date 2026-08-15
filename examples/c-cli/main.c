#define _POSIX_C_SOURCE 200809L

#include "../../include/fiber_ffi.h"

#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <time.h>

#define INPUT_SIZE 8192
#define DEFAULT_CHANNEL_FUNDING_SHANNONS UINT64_C(50000000000)
#define DEFAULT_CKB_DISCOVERY_RPC_URL "https://testnet.ckbapp.dev/"

typedef struct CliArgs {
  const char *config;
  const char *data;
  const char *log_level;
  const char *log_file;
  const char *ckb_discovery_rpc;
} CliArgs;

typedef struct PrepareState {
  pthread_mutex_t mutex;
  pthread_cond_t condition;
  int done;
  FiberFfiStatus status;
  char *result;
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
  if (message != NULL && message[0] != '\0') {
    fprintf(stderr, "%s failed (status=%s): %s\n", operation,
            status_name(status), message);
  } else {
    fprintf(stderr, "%s failed (status=%s)\n", operation,
            status_name(status));
  }
  free(message);
}

static int print_string_result(const char *label, const char *operation,
                               FiberFfiStatus status, char *output) {
  if (status != FIBER_FFI_STATUS_OK) {
    if (output != NULL) {
      fiber_string_free(output);
    }
    printf("[%s/error] ", label);
    fflush(stdout);
    print_last_error(operation, status);
    return 0;
  }
  if (output == NULL) {
    printf("[%s/error] %s succeeded without a string result\n", label,
           operation);
    return 0;
  }

  printf("[%s/ok]\n%s\n", label, output);
  fiber_string_free(output);
  return 1;
}

static void print_local_time(void) {
  time_t now = time(NULL);
  struct tm local;
  char formatted[64];

  if (now == (time_t)-1 || localtime_r(&now, &local) == NULL ||
      strftime(formatted, sizeof(formatted), "%Y-%m-%d %H:%M:%S %Z",
               &local) == 0) {
    printf("[local time] unavailable\n");
    return;
  }
  printf("[local time] %s\n", formatted);
}

static int print_unit_result(const char *label, const char *operation,
                             FiberFfiStatus status) {
  if (status == FIBER_FFI_STATUS_OK) {
    printf("[%s/ok] operation submitted\n", label);
    return 1;
  }

  printf("[%s/error] ", label);
  fflush(stdout);
  print_last_error(operation, status);
  return 0;
}

static void prepare_callback(FiberFfiStatus status, const char *result_json,
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
    printf("[startup/1] CKB Light Client status: %s\n", result);
    fflush(stdout);
    return;
  }
  pthread_mutex_lock(&state->mutex);
  state->status = status;
  state->result = strdup(result);
  state->done = 1;
  pthread_cond_signal(&state->condition);
  pthread_mutex_unlock(&state->mutex);
}

static void event_callback(const char *event_json, void *user_data) {
  FILE *log_file = (FILE *)user_data;

  if (log_file != NULL) {
    fprintf(log_file, "[event] %s\n", event_json != NULL ? event_json : "");
    fflush(log_file);
  }
}

static int prepare_ckb(const FiberStartOptions *options,
                       int has_history_start_block,
                       uint64_t history_start_block) {
  PrepareState state;
  struct timespec started_at;
  struct timespec ready_at;
  FiberFfiStatus accepted;
  double elapsed_seconds;
  int ready;

  memset(&state, 0, sizeof(state));
  if (pthread_mutex_init(&state.mutex, NULL) != 0) {
    fprintf(stderr, "Unable to initialize CKB prepare synchronization state\n");
    return 0;
  }
  if (pthread_cond_init(&state.condition, NULL) != 0) {
    fprintf(stderr, "Unable to initialize CKB prepare synchronization state\n");
    pthread_mutex_destroy(&state.mutex);
    return 0;
  }

  if (clock_gettime(CLOCK_MONOTONIC, &started_at) != 0) {
    fprintf(stderr, "Unable to record CKB Light Client initialization start time: %s\n",
            strerror(errno));
    pthread_cond_destroy(&state.condition);
    pthread_mutex_destroy(&state.mutex);
    return 0;
  }

  accepted = has_history_start_block
                 ? fiber_prepare_ckb_with_history_start_block(
                       options, history_start_block, prepare_callback, &state)
                 : fiber_prepare_ckb(options, prepare_callback, &state);
  if (accepted != FIBER_FFI_STATUS_OK) {
    print_last_error("fiber_prepare_ckb", accepted);
    pthread_cond_destroy(&state.condition);
    pthread_mutex_destroy(&state.mutex);
    return 0;
  }

  pthread_mutex_lock(&state.mutex);
  while (!state.done) {
    pthread_cond_wait(&state.condition, &state.mutex);
  }
  pthread_mutex_unlock(&state.mutex);

  if (state.result == NULL) {
    fprintf(stderr, "Unable to allocate memory for the fiber_prepare_ckb callback result\n");
    ready = 0;
  } else if (state.status != FIBER_FFI_STATUS_OK) {
    fprintf(stderr, "fiber_prepare_ckb failed (status=%s): %s\n",
            status_name(state.status), state.result);
    ready = 0;
  } else {
    ready = strstr(state.result, "\"mode\":\"light_client\"") != NULL &&
            strstr(state.result, "\"ready\":true") != NULL;
    if (ready) {
      if (clock_gettime(CLOCK_MONOTONIC, &ready_at) != 0) {
        fprintf(stderr, "Unable to record CKB Light Client initialization completion time: %s\n",
                strerror(errno));
        ready = 0;
      } else {
        elapsed_seconds = (double)(ready_at.tv_sec - started_at.tv_sec) +
                          (double)(ready_at.tv_nsec - started_at.tv_nsec) /
                              1000000000.0;
        printf("[startup/1] CKB Light Client is ready (elapsed: %.3f seconds): %s\n",
               elapsed_seconds, state.result);
      }
    } else {
      fprintf(stderr, "The loaded library was not built with disable-ckb-rpc; prepare result: %s\n",
              state.result);
    }
  }

  free(state.result);
  pthread_cond_destroy(&state.condition);
  pthread_mutex_destroy(&state.mutex);
  return ready;
}

static void trim(char *value) {
  size_t start = 0;
  size_t end = strlen(value);

  while (start < end && isspace((unsigned char)value[start])) {
    ++start;
  }
  while (end > start && isspace((unsigned char)value[end - 1])) {
    --end;
  }
  if (start != 0) {
    memmove(value, value + start, end - start);
  }
  value[end - start] = '\0';
}

/* Returns 1 for input, 0 for EOF, and -1 for an input error. */
static int prompt_line(const char *label, char *output, size_t output_size) {
  int ch;

  printf("%s> ", label);
  fflush(stdout);
  if (fgets(output, (int)output_size, stdin) == NULL) {
    if (!feof(stdin)) {
      perror("stdin");
      return -1;
    }
    return 0;
  }
  if (strchr(output, '\n') == NULL && !feof(stdin)) {
    while ((ch = getchar()) != '\n' && ch != EOF) {
    }
    printf("[input] Input is too long (maximum: %d bytes)\n", INPUT_SIZE - 2);
    return -1;
  }
  trim(output);
  return 1;
}

static int prompt_required(const char *label, char *output,
                           size_t output_size) {
  int result;

  for (;;) {
    result = prompt_line(label, output, output_size);
    if (result <= 0) {
      return 0;
    }
    if (output[0] != '\0') {
      return 1;
    }
    printf("[input] This field is required\n");
  }
}

static int prompt_yes_no(const char *label, int default_value, int *value) {
  char input[32];
  char prompt[256];

  snprintf(prompt, sizeof(prompt), "%s %s", label,
           default_value ? "[Y/n]" : "[y/N]");
  for (;;) {
    if (prompt_line(prompt, input, sizeof(input)) <= 0) {
      return 0;
    }
    if (input[0] == '\0') {
      *value = default_value;
      return 1;
    }
    if (strcasecmp(input, "y") == 0 || strcasecmp(input, "yes") == 0) {
      *value = 1;
      return 1;
    }
    if (strcasecmp(input, "n") == 0 || strcasecmp(input, "no") == 0) {
      *value = 0;
      return 1;
    }
    printf("[input] Please enter y or n\n");
  }
}

static int prompt_optional_bool(const char *label, int *has_value,
                                int *value) {
  char input[32];

  for (;;) {
    if (prompt_line(label, input, sizeof(input)) <= 0) {
      return 0;
    }
    if (input[0] == '\0') {
      *has_value = 0;
      return 1;
    }
    if (strcasecmp(input, "y") == 0 || strcasecmp(input, "yes") == 0) {
      *has_value = 1;
      *value = 1;
      return 1;
    }
    if (strcasecmp(input, "n") == 0 || strcasecmp(input, "no") == 0) {
      *has_value = 1;
      *value = 0;
      return 1;
    }
    printf("[input] Please enter y, n, or leave the field blank\n");
  }
}

static int parse_u64(const char *input, uint64_t *value) {
  char *end = NULL;
  unsigned long long parsed;
  const char *cursor;

  if (input[0] == '\0') {
    return 0;
  }
  for (cursor = input; *cursor != '\0'; ++cursor) {
    if (!isdigit((unsigned char)*cursor)) {
      return 0;
    }
  }
  errno = 0;
  parsed = strtoull(input, &end, 10);
  if (errno == ERANGE || end == input || *end != '\0' ||
      parsed > UINT64_MAX) {
    return 0;
  }
  *value = (uint64_t)parsed;
  return 1;
}

static int prompt_optional_u64(const char *label, int *has_value,
                               uint64_t *value) {
  char input[64];

  for (;;) {
    if (prompt_line(label, input, sizeof(input)) <= 0) {
      return 0;
    }
    if (input[0] == '\0') {
      *has_value = 0;
      return 1;
    }
    if (parse_u64(input, value)) {
      *has_value = 1;
      return 1;
    }
    printf("[input] Please enter a valid non-negative integer\n");
  }
}

static int parse_u128(const char *input, FiberU128 *value) {
  FiberU128 parsed = {0, 0};
  const char *cursor;

  if (input[0] == '\0') {
    return 0;
  }
  for (cursor = input; *cursor != '\0'; ++cursor) {
    uint64_t low_times_eight;
    uint64_t low_times_two;
    uint64_t next_low;
    uint64_t carry;
    uint64_t next_high;
    uint64_t digit;

    if (!isdigit((unsigned char)*cursor)) {
      return 0;
    }
    digit = (uint64_t)(*cursor - '0');

    low_times_eight = parsed.low << 3;
    low_times_two = parsed.low << 1;
    next_low = low_times_eight + low_times_two;
    carry = (parsed.low >> 61) + (parsed.low >> 63) +
            (next_low < low_times_eight ? 1u : 0u);

    if (parsed.high > UINT64_MAX / 10) {
      return 0;
    }
    next_high = parsed.high * 10;
    if (UINT64_MAX - next_high < carry) {
      return 0;
    }
    next_high += carry;

    if (UINT64_MAX - next_low < digit) {
      if (next_high == UINT64_MAX) {
        return 0;
      }
      ++next_high;
    }
    next_low += digit;
    parsed.low = next_low;
    parsed.high = next_high;
  }

  *value = parsed;
  return 1;
}

static int prompt_u128(const char *label, int *has_value, FiberU128 *value) {
  char input[64];

  for (;;) {
    if (prompt_line(label, input, sizeof(input)) <= 0) {
      return 0;
    }
    if (input[0] == '\0') {
      *has_value = 0;
      return 1;
    }
    if (parse_u128(input, value)) {
      *has_value = 1;
      return 1;
    }
    printf("[input] Please enter a valid non-negative integer (maximum: 2^128-1)\n");
  }
}

static int fiber_u128_is_zero(FiberU128 value) {
  return value.low == 0 && value.high == 0;
}

static int peer_connect(FiberHandle *node) {
  char mode[16];
  char address[INPUT_SIZE];
  char pubkey[INPUT_SIZE];
  char addr_type[32];
  FiberConnectPeerOptions options = {0};
  int save;

  printf("\nConnection method:\n1. Full multiaddr (recommended)\n2. Peer public key\n");
  if (prompt_line("Select", mode, sizeof(mode)) <= 0) {
    return 0;
  }
  if (strcmp(mode, "1") == 0) {
    if (!prompt_required("multiaddr", address, sizeof(address)) ||
        !prompt_yes_no("Save peer address", 1, &save)) {
      return 0;
    }
    options.address = address;
    options.save = save;
  } else if (strcmp(mode, "2") == 0) {
    if (!prompt_required("Peer public key", pubkey, sizeof(pubkey))) {
      return 0;
    }
    if (prompt_line("Connection type [tcp/ws/wss]", addr_type, sizeof(addr_type)) <=
        0) {
      return 0;
    }
    if (addr_type[0] == '\0') {
      strcpy(addr_type, "tcp");
    }
    options.pubkey = pubkey;
    options.addr_type = addr_type;
  } else {
    printf("[input] Invalid connection method\n");
    return 1;
  }

  print_unit_result("peer/connect", "fiber_connect_peer",
                    fiber_connect_peer(node, &options));
  return 1;
}

static int peer_disconnect(FiberHandle *node) {
  char pubkey[INPUT_SIZE];

  if (!prompt_required("Peer public key", pubkey, sizeof(pubkey))) {
    return 0;
  }
  print_unit_result("peer/disconnect", "fiber_disconnect_peer",
                    fiber_disconnect_peer(node, pubkey));
  return 1;
}

static int peer_menu(FiberHandle *node) {
  char choice[16];

  for (;;) {
    char *output = NULL;
    FiberFfiStatus status;

    print_local_time();
    printf("\n---------- Peer Menu ----------\n"
           "1. Connect\n2. Disconnect\n3. List\n0. Back\n");
    if (prompt_line("Select", choice, sizeof(choice)) <= 0) {
      return 0;
    }
    if (strcmp(choice, "1") == 0) {
      if (!peer_connect(node)) {
        return 0;
      }
    } else if (strcmp(choice, "2") == 0) {
      if (!peer_disconnect(node)) {
        return 0;
      }
    } else if (strcmp(choice, "3") == 0) {
      status = fiber_list_peers(node, &output);
      print_string_result("peer/list", "fiber_list_peers", status, output);
    } else if (strcmp(choice, "0") == 0) {
      return 1;
    } else {
      printf("[input] Invalid menu choice\n");
    }
  }
}

static int channel_open(FiberHandle *node) {
  char pubkey[INPUT_SIZE];
  char udt_json[INPUT_SIZE];
  FiberOpenChannelOptions options = {0};
  FiberU128 amount = {0, 0};
  uint64_t funding_fee_rate = 0;
  int has_amount;
  int has_public;
  int public_value = 0;
  int has_one_way;
  int one_way = 0;
  int has_funding_fee_rate;
  char *output = NULL;
  FiberFfiStatus status;

  if (!prompt_required("Remote peer public key", pubkey, sizeof(pubkey)) ||
      !prompt_u128("Funding amount (shannons; blank defaults to 500 CKB)", &has_amount,
                   &amount)) {
    return 0;
  }
  if (!has_amount) {
    amount.low = DEFAULT_CHANNEL_FUNDING_SHANNONS;
    amount.high = 0;
    printf("[input] Using the default funding amount: 500 CKB (50000000000 shannons)\n");
  } else if (fiber_u128_is_zero(amount)) {
    printf("[channel/open/error] Funding amount must be greater than 0\n");
    return 1;
  }
  if (!prompt_optional_bool("Announce channel publicly [y/n; blank uses the Fiber default]",
                            &has_public, &public_value) ||
      !prompt_optional_bool("Funded by one side only [y/n; blank uses the Fiber default]",
                            &has_one_way, &one_way) ||
      prompt_line("UDT type script JSON [leave blank for a CKB channel]", udt_json,
                  sizeof(udt_json)) <= 0 ||
      !prompt_optional_u64("Funding fee rate [blank uses the Fiber default]",
                           &has_funding_fee_rate, &funding_fee_rate)) {
    return 0;
  }

  options.struct_size = sizeof(options);
  options.pubkey = pubkey;
  options.funding_amount = amount;
  options.has_public = has_public;
  options.public_ = public_value;
  options.has_one_way = has_one_way;
  options.one_way = one_way;
  options.funding_udt_type_script_json =
      udt_json[0] != '\0' ? udt_json : NULL;
  options.has_funding_fee_rate = has_funding_fee_rate;
  options.funding_fee_rate = funding_fee_rate;

  status = fiber_open_channel(node, &options, &output);
  print_string_result("channel/open temporary_channel_id",
                      "fiber_open_channel", status, output);
  return 1;
}

static int channel_close(FiberHandle *node) {
  char channel_id[INPUT_SIZE];
  char close_script[INPUT_SIZE];
  FiberShutdownChannelOptions options = {0};
  uint64_t fee_rate = 0;
  int has_fee_rate;
  int force;

  if (!prompt_required("channel_id", channel_id, sizeof(channel_id)) ||
      !prompt_yes_no("Force close", 0, &force) ||
      prompt_line("Close script JSON [blank uses the default]", close_script,
                  sizeof(close_script)) <= 0 ||
      !prompt_optional_u64("Fee rate [blank uses the default]", &has_fee_rate,
                           &fee_rate)) {
    return 0;
  }

  options.struct_size = sizeof(options);
  options.channel_id = channel_id;
  options.close_script_json =
      close_script[0] != '\0' ? close_script : NULL;
  options.has_force = 1;
  options.force = force;
  options.has_fee_rate = has_fee_rate;
  options.fee_rate = fee_rate;
  print_unit_result("channel/close", "fiber_shutdown_channel",
                    fiber_shutdown_channel(node, &options));
  return 1;
}

static int channel_menu(FiberHandle *node) {
  char choice[16];

  for (;;) {
    print_local_time();
    printf("\n---------- Channel Menu ----------\n"
           "1. Open\n2. Close\n3. List (all except failed history)\n0. Back\n");
    if (prompt_line("Select", choice, sizeof(choice)) <= 0) {
      return 0;
    }
    if (strcmp(choice, "1") == 0) {
      if (!channel_open(node)) {
        return 0;
      }
    } else if (strcmp(choice, "2") == 0) {
      if (!channel_close(node)) {
        return 0;
      }
    } else if (strcmp(choice, "3") == 0) {
      FiberListChannelsOptions options = {0};
      char *output = NULL;
      FiberFfiStatus status;

      options.struct_size = sizeof(options);
      options.has_include_closed = 1;
      options.include_closed = 1;
      status = fiber_list_channels(node, &options, &output);
      print_string_result("channel/list", "fiber_list_channels", status,
                          output);
    } else if (strcmp(choice, "0") == 0) {
      return 1;
    } else {
      printf("[input] Invalid menu choice\n");
    }
  }
}

static int pay_create_invoice(FiberHandle *node) {
  char description[INPUT_SIZE];
  char currency_input[16];
  char udt_json[INPUT_SIZE];
  FiberNewInvoiceOptions options = {0};
  FiberU128 amount = {0, 0};
  uint64_t expiry = 0;
  int has_amount;
  int has_expiry;
  int has_allow_mpp;
  int allow_mpp = 0;
  char *output = NULL;
  FiberFfiStatus status;

  if (!prompt_u128("Invoice amount (shannons)", &has_amount, &amount)) {
    return 0;
  }
  if (!has_amount || fiber_u128_is_zero(amount)) {
    printf("[pay/invoice/error] Invoice amount must be greater than 0\n");
    return 1;
  }
  if (prompt_line("Description [optional]", description, sizeof(description)) <= 0) {
    return 0;
  }
  printf("Currency: 1. Fibb/mainnet CKB  2. Fibt/testnet CKB  3. Fibd/UDT\n");
  if (prompt_line("Select currency", currency_input, sizeof(currency_input)) <= 0) {
    return 0;
  }
  if (currency_input[0] == '\0') {
    strcpy(currency_input, "2");
  }
  if (strcmp(currency_input, "1") == 0) {
    options.currency = FIBER_INVOICE_CURRENCY_FIBB;
  } else if (strcmp(currency_input, "2") == 0) {
    options.currency = FIBER_INVOICE_CURRENCY_FIBT;
  } else if (strcmp(currency_input, "3") == 0) {
    options.currency = FIBER_INVOICE_CURRENCY_FIBD;
  } else {
    printf("[pay/invoice/error] Invalid currency\n");
    return 1;
  }

  udt_json[0] = '\0';
  if (options.currency == FIBER_INVOICE_CURRENCY_FIBD &&
      prompt_line("UDT type script JSON", udt_json, sizeof(udt_json)) <= 0) {
    return 0;
  }
  if (!prompt_optional_u64("Expiry in seconds [blank uses the Fiber default]", &has_expiry,
                           &expiry) ||
      !prompt_optional_bool("Allow multi-path payments (MPP) [y/n; blank uses the default]",
                            &has_allow_mpp, &allow_mpp)) {
    return 0;
  }

  options.struct_size = sizeof(options);
  options.amount = amount;
  options.description = description[0] != '\0' ? description : NULL;
  options.udt_type_script_json = udt_json[0] != '\0' ? udt_json : NULL;
  options.has_expiry = has_expiry;
  options.expiry = expiry;
  options.has_allow_mpp = has_allow_mpp;
  options.allow_mpp = allow_mpp;

  status = fiber_new_invoice(node, &options, &output);
  print_string_result("pay/invoice", "fiber_new_invoice", status, output);
  return 1;
}

static int pay_invoice(FiberHandle *node) {
  char invoice[INPUT_SIZE];
  FiberSendPaymentOptions options = {0};
  FiberU128 max_fee = {0, 0};
  uint64_t timeout = 0;
  int has_timeout;
  int has_max_fee;
  int dry_run;
  char *output = NULL;
  FiberFfiStatus status;

  if (!prompt_required("invoice", invoice, sizeof(invoice)) ||
      !prompt_optional_u64("Timeout in seconds [blank uses the Fiber default]", &has_timeout,
                           &timeout) ||
      !prompt_u128("Maximum fee in shannons [blank uses the Fiber default]",
                   &has_max_fee, &max_fee) ||
      !prompt_yes_no("Dry run only (do not send payment)", 0, &dry_run)) {
    return 0;
  }

  options.struct_size = sizeof(options);
  options.invoice = invoice;
  options.has_timeout = has_timeout;
  options.timeout = timeout;
  options.has_max_fee_amount = has_max_fee;
  options.max_fee_amount = max_fee;
  options.has_dry_run = 1;
  options.dry_run = dry_run;

  status = fiber_send_payment(node, &options, &output);
  print_string_result("pay/send", "fiber_send_payment", status, output);
  return 1;
}

static int pay_menu(FiberHandle *node) {
  char choice[16];

  for (;;) {
    print_local_time();
    printf("\n---------- Pay Menu ----------\n"
           "1. Create invoice\n2. Pay invoice\n0. Back\n");
    if (prompt_line("Select", choice, sizeof(choice)) <= 0) {
      return 0;
    }
    if (strcmp(choice, "1") == 0) {
      if (!pay_create_invoice(node)) {
        return 0;
      }
    } else if (strcmp(choice, "2") == 0) {
      if (!pay_invoice(node)) {
        return 0;
      }
    } else if (strcmp(choice, "0") == 0) {
      return 1;
    } else {
      printf("[input] Invalid menu choice\n");
    }
  }
}

static void menu_loop(FiberHandle *node) {
  char choice[16];

  for (;;) {
    print_local_time();
    printf("\n========== Fiber Main Menu ==========\n"
           "1. Peer\n2. Channel\n3. Pay\nq. Exit\n");
    if (prompt_line("Select", choice, sizeof(choice)) <= 0 ||
        strcmp(choice, "q") == 0) {
      return;
    }
    if (strcmp(choice, "1") == 0) {
      if (!peer_menu(node)) {
        return;
      }
    } else if (strcmp(choice, "2") == 0) {
      if (!channel_menu(node)) {
        return;
      }
    } else if (strcmp(choice, "3") == 0) {
      if (!pay_menu(node)) {
        return;
      }
    } else {
      printf("[input] Invalid menu choice\n");
    }
  }
}

static int mkdir_one(const char *path) {
  struct stat metadata;

  if (mkdir(path, 0700) == 0) {
    return 1;
  }
  if (errno == EEXIST && stat(path, &metadata) == 0 &&
      S_ISDIR(metadata.st_mode)) {
    return 1;
  }
  fprintf(stderr, "Unable to create data directory %s: %s\n", path,
          strerror(errno));
  return 0;
}

static int mkdir_all(const char *path) {
  char *copy = strdup(path);
  char *cursor;
  int ok = 1;

  if (copy == NULL) {
    return 0;
  }
  for (cursor = copy + 1; *cursor != '\0'; ++cursor) {
    if (*cursor == '/') {
      *cursor = '\0';
      if (copy[0] != '\0' && !mkdir_one(copy)) {
        ok = 0;
        break;
      }
      *cursor = '/';
    }
  }
  if (ok) {
    ok = mkdir_one(copy);
  }
  free(copy);
  return ok;
}

static char *join_path(const char *left, const char *right) {
  size_t left_length = strlen(left);
  size_t right_length = strlen(right);
  int needs_slash = left_length != 0 && left[left_length - 1] != '/';
  char *result = (char *)malloc(left_length + (size_t)needs_slash +
                                right_length + 1);

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

static int discover_initial_history_start_block(
    const CliArgs *args, const FiberStartOptions *start_options,
    int *has_history_start_block, uint64_t *history_start_block) {
  FiberCkbDiscoverHistoryStartBlockOptions discover_options =
      FIBER_CKB_DISCOVER_HISTORY_START_BLOCK_OPTIONS_INIT;
  FiberFfiStatus status;
  char *ckb_directory = NULL;
  char *birthday_path = NULL;
  char *funding_address = NULL;
  int ok = 0;

  *has_history_start_block = 0;
  *history_start_block = 0;
  ckb_directory = join_path(args->data, "ckb");
  birthday_path =
      ckb_directory != NULL ? join_path(ckb_directory, "wallet-birthday.json")
                            : NULL;
  if (birthday_path == NULL) {
    fprintf(stderr, "Unable to construct the wallet birthday file path\n");
    goto cleanup;
  }
  if (is_regular_file(birthday_path)) {
    printf("[startup/0] Using persisted wallet birthday: %s\n", birthday_path);
    ok = 1;
    goto cleanup;
  }

  status = fiber_ckb_funding_address(start_options, &funding_address);
  if (status != FIBER_FFI_STATUS_OK) {
    print_last_error("fiber_ckb_funding_address", status);
    goto cleanup;
  }
  discover_options.rpc_url = args->ckb_discovery_rpc;
  discover_options.address = funding_address;
  status = fiber_ckb_discover_history_start_block(&discover_options,
                                                   history_start_block);
  if (status != FIBER_FFI_STATUS_OK) {
    print_last_error("fiber_ckb_discover_history_start_block", status);
    goto cleanup;
  }
  *has_history_start_block = 1;
  printf("[startup/0] Funding address: %s\n"
         "[startup/0] External RPC: %s\n"
         "[startup/0] Suggested history start block: %" PRIu64 " (0x%" PRIx64 ")\n",
         funding_address, args->ckb_discovery_rpc, *history_start_block,
         *history_start_block);
  ok = 1;

cleanup:
  if (funding_address != NULL) {
    fiber_string_free(funding_address);
  }
  free(birthday_path);
  free(ckb_directory);
  return ok;
}

static int validate_startup(const CliArgs *args) {
  char *ckb_directory;
  char *key_path;
  int ok = 0;

  if (!is_regular_file(args->config)) {
    fprintf(stderr, "Configuration file not found: %s\n", args->config);
    return 0;
  }
  ckb_directory = join_path(args->data, "ckb");
  if (ckb_directory == NULL || !mkdir_all(ckb_directory)) {
    free(ckb_directory);
    return 0;
  }
  key_path = join_path(ckb_directory, "key");
  if (key_path == NULL) {
    free(ckb_directory);
    return 0;
  }
  if (!is_regular_file(key_path)) {
    fprintf(stderr, "Test wallet private key not found at %s; run make setup first\n",
            key_path);
  } else if (getenv("FIBER_SECRET_KEY_PASSWORD") == NULL) {
    fprintf(stderr,
            "FIBER_SECRET_KEY_PASSWORD is not set; use make run or set it manually\n");
  } else {
    ok = 1;
  }
  free(key_path);
  free(ckb_directory);
  return ok;
}

static void print_help(const char *program) {
  printf("fiber-ffi C CLI example\n\n"
         "Usage:\n  %s [options]\n\n"
         "Options:\n"
         "  --config PATH     Path to the Fiber YAML configuration\n"
         "  --data PATH       Fiber/CKB Light Client data directory\n"
         "  --log-level TEXT  fiber-ffi log filter\n"
         "  --log-file PATH   Log file [default: <data>/fiber-ffi.log]\n"
         "  --ckb-discovery-rpc URL\n"
         "                    CKB RPC/Indexer used for initial wallet history discovery\n"
         "  -h, --help        Show this help\n",
         program);
}

/* Returns 1 to run, 0 for --help, and -1 for invalid arguments. */
static int parse_args(int argc, char **argv, CliArgs *args) {
  int index;

  args->config = "examples/c-cli/config.testnet.yml";
  args->data = "examples/c-cli/data";
  args->log_level = "info,fiber_ffi=debug";
  args->log_file = NULL;
  args->ckb_discovery_rpc = DEFAULT_CKB_DISCOVERY_RPC_URL;
  for (index = 1; index < argc; ++index) {
    const char *argument = argv[index];
    const char **destination = NULL;

    if (strcmp(argument, "-h") == 0 || strcmp(argument, "--help") == 0) {
      print_help(argv[0]);
      return 0;
    }
    if (strcmp(argument, "--config") == 0) {
      destination = &args->config;
    } else if (strcmp(argument, "--data") == 0) {
      destination = &args->data;
    } else if (strcmp(argument, "--log-level") == 0) {
      destination = &args->log_level;
    } else if (strcmp(argument, "--log-file") == 0) {
      destination = &args->log_file;
    } else if (strcmp(argument, "--ckb-discovery-rpc") == 0) {
      destination = &args->ckb_discovery_rpc;
    } else {
      fprintf(stderr, "Unknown argument: %s (use --help for usage)\n", argument);
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

int main(int argc, char **argv) {
  CliArgs args;
  FiberStartOptions options = {0};
  FiberHandle *node = NULL;
  FiberFfiStatus status;
  char *default_log_file = NULL;
  char *output = NULL;
  FILE *log_file;
  int has_history_start_block = 0;
  uint64_t history_start_block = 0;
  int argument_result = parse_args(argc, argv, &args);

  if (argument_result <= 0) {
    return argument_result == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
  }
  if (args.log_file == NULL) {
    default_log_file = join_path(args.data, "fiber-ffi.log");
    if (default_log_file == NULL) {
      fprintf(stderr, "Unable to construct the default log file path\n");
      return EXIT_FAILURE;
    }
    args.log_file = default_log_file;
  }
  if (!validate_startup(&args)) {
    free(default_log_file);
    return EXIT_FAILURE;
  }
  log_file = fopen(args.log_file, "a");
  if (log_file == NULL) {
    fprintf(stderr, "Unable to open log file %s: %s\n", args.log_file,
            strerror(errno));
    free(default_log_file);
    return EXIT_FAILURE;
  }
  if (setenv("FIBER_FFI_LOG_FILE", args.log_file, 1) != 0) {
    fprintf(stderr, "Unable to set FIBER_FFI_LOG_FILE: %s\n", strerror(errno));
    fclose(log_file);
    free(default_log_file);
    return EXIT_FAILURE;
  }

  printf("Fiber FFI C CLI Example\n"
         "  Config:   %s\n"
         "  Data:     %s\n"
         "  Log:      %s\n"
         "  Initial wallet discovery RPC: %s\n"
         "  Version:  %s\n",
         args.config, args.data, args.log_file, args.ckb_discovery_rpc,
         fiber_version());

  options.config_path = args.config;
  options.database_prefix = args.data;
  options.log_level = args.log_level;

  printf("\n[startup/0] Determining the wallet history start block...\n");
  if (!discover_initial_history_start_block(
          &args, &options, &has_history_start_block, &history_start_block)) {
    fclose(log_file);
    free(default_log_file);
    return EXIT_FAILURE;
  }
  printf("[startup/1] Synchronizing the built-in CKB Light Client...\n");
  if (!prepare_ckb(&options, has_history_start_block,
                   history_start_block)) {
    fclose(log_file);
    free(default_log_file);
    return EXIT_FAILURE;
  }

  printf("[startup/2] Initializing and starting Fiber...\n");
  options.event_callback = event_callback;
  options.event_callback_user_data = log_file;
  status = fiber_start(&options, &node);
  if (status != FIBER_FFI_STATUS_OK) {
    print_last_error("fiber_start", status);
    fclose(log_file);
    free(default_log_file);
    return EXIT_FAILURE;
  }
  if (node == NULL) {
    fprintf(stderr, "fiber_start succeeded with a null handle\n");
    fclose(log_file);
    free(default_log_file);
    return EXIT_FAILURE;
  }

  printf("[startup/2] Fiber started successfully\n");
  status = fiber_node_info(node, &output);
  print_string_result("node-info", "fiber_node_info", status, output);

  printf("[startup/3] Querying the funding wallet balance through the CKB Light Client...\n");
  output = NULL;
  status = fiber_ckb_balance(node, &output);
  if (print_string_result("ckb/wallet-balance", "fiber_ckb_balance", status,
                          output)) {
    printf("[startup/3] When opening a channel, reserve capacity for a change Cell and transaction fees.\n");
  } else {
    fprintf(stderr,
            "[startup/3] Balance query failed. Fiber is running, but verify the wallet balance before opening a channel.\n");
  }

  menu_loop(node);
  printf("\n[shutdown] Stopping Fiber...\n");
  status = fiber_stop(node);
  if (status != FIBER_FFI_STATUS_OK) {
    print_last_error("fiber_stop", status);
    fclose(log_file);
    free(default_log_file);
    return EXIT_FAILURE;
  }
  fclose(log_file);
  free(default_log_file);
  printf("[shutdown] Fiber stopped\n");
  return EXIT_SUCCESS;
}
