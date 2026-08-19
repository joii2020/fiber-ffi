CC ?= cc
CARGO ?= cargo
OPENSSL ?= openssl

ROOT_DIR := $(abspath ../..)
CARGO_TARGET_DIR ?= $(ROOT_DIR)/target
BUILD_DIR ?= $(CARGO_TARGET_DIR)/c-demo
LIB_DIR := $(CARGO_TARGET_DIR)/release
BINARY := $(BUILD_DIR)/fiber-ffi-c-demo
FIBER_FEATURES ?= sqlite,ckb-light-client-portable
DATA_DIR ?= $(ROOT_DIR)/examples/c-demo/data
CKB_DATA_DIR := $(DATA_DIR)/ckb
CKB_KEY := $(CKB_DATA_DIR)/key
DEMO_SECRET_KEY_PASSWORD ?= fiber-ffi-c-demo-password
DEMO_ARGS ?= --data "$(DATA_DIR)"

CPPFLAGS += -I$(ROOT_DIR)/include
CFLAGS ?= -O2 -g
CFLAGS += -std=c11 -Wall -Wextra -Wpedantic -pthread
LDLIBS += -L$(LIB_DIR) -lfiber_ffi

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
RPATH := -Wl,-rpath,'$$ORIGIN/../release'
else ifeq ($(UNAME_S),Darwin)
RPATH := -Wl,-rpath,@loader_path/../release
else
$(error The C demo supports Linux and macOS only)
endif

.PHONY: all library setup run clean

all: setup $(BINARY)

setup: $(CKB_KEY)

$(CKB_KEY):
	@mkdir -p "$(CKB_DATA_DIR)"
	@chmod 700 "$(DATA_DIR)" "$(CKB_DATA_DIR)"
	@set -e; \
		tmp=$$(mktemp "$(CKB_KEY).tmp.XXXXXX"); \
		trap 'rm -f "$$tmp"' EXIT HUP INT TERM; \
		umask 077; \
		"$(OPENSSL)" rand -hex 32 > "$$tmp"; \
		chmod 600 "$$tmp"; \
		if [ -e "$(CKB_KEY)" ]; then \
			echo "CKB key already exists: $(CKB_KEY)"; \
		else \
			mv "$$tmp" "$(CKB_KEY)"; \
			tmp=; \
			echo "Generated test-only CKB key: $(CKB_KEY)"; \
		fi; \
		if [ -n "$$tmp" ]; then rm -f "$$tmp"; fi; \
		trap - EXIT HUP INT TERM

library:
	cd $(ROOT_DIR) && CARGO_TARGET_DIR=$(CARGO_TARGET_DIR) $(CARGO) build \
		--release --locked --no-default-features --features "$(FIBER_FEATURES)"

$(BUILD_DIR):
	mkdir -p $@

$(BINARY): main.c $(ROOT_DIR)/include/fiber_ffi.h library | $(BUILD_DIR)
	$(CC) $(CPPFLAGS) $(CFLAGS) $< $(LDFLAGS) $(RPATH) $(LDLIBS) -o $@

run: all
	cd $(ROOT_DIR) && \
		FIBER_SECRET_KEY_PASSWORD="$${FIBER_SECRET_KEY_PASSWORD:-$(DEMO_SECRET_KEY_PASSWORD)}" \
		$(BINARY) $(DEMO_ARGS)

clean:
	$(RM) $(BINARY)
