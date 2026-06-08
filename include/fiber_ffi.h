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

typedef enum FiberFfiStatus {
  FIBER_FFI_STATUS_OK = 0,
  FIBER_FFI_STATUS_NULL_POINTER = 1,
  FIBER_FFI_STATUS_INVALID_ARGUMENT = 2,
  FIBER_FFI_STATUS_STARTUP_FAILED = 3,
  FIBER_FFI_STATUS_ALREADY_STOPPED = 4,
  FIBER_FFI_STATUS_PANIC = 5,
} FiberFfiStatus;

const char *fiber_version(void);

FiberFfiStatus fiber_start(const FiberStartOptions *options,
                           FiberHandle **out_handle);

FiberFfiStatus fiber_stop(FiberHandle *handle);

FiberFfiStatus fiber_node_info(FiberHandle *handle, char **out_json);

FiberFfiStatus fiber_list_peers(FiberHandle *handle, char **out_json);

FiberFfiStatus fiber_connect_peer(FiberHandle *handle,
                                  const FiberConnectPeerOptions *options);

FiberFfiStatus fiber_disconnect_peer(FiberHandle *handle, const char *pubkey);

void fiber_string_free(char *string);

size_t fiber_last_error_message(char *buffer, size_t buffer_len);

#ifdef __cplusplus
}
#endif

#endif
