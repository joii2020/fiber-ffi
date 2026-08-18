CARGO ?= cargo
OPENSSL ?= openssl

ROOT_DIR := $(abspath ../..)
CARGO_TARGET_DIR ?= $(ROOT_DIR)/target
BINARY := $(CARGO_TARGET_DIR)/release/fiber-demo-cli
FIBER_FEATURES ?= sqlite,ckb-light-client-portable
DATA_DIR ?= $(ROOT_DIR)/tools/fiber-demo-cli/data
CKB_DATA_DIR := $(DATA_DIR)/ckb
CKB_KEY := $(CKB_DATA_DIR)/key
DEMO_SECRET_KEY_PASSWORD ?= fiber-ffi-rust-cli-demo-password
CLI_ARGS ?= --data "$(DATA_DIR)"

.PHONY: all build setup run clean

all: setup build

build:
	cd $(ROOT_DIR) && CARGO_TARGET_DIR=$(CARGO_TARGET_DIR) $(CARGO) build \
		--release --locked --bin fiber-demo-cli \
		--no-default-features --features "$(FIBER_FEATURES)"

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
			echo "CKB test key already exists: $(CKB_KEY)"; \
		else \
			mv "$$tmp" "$(CKB_KEY)"; \
			tmp=; \
			echo "Generated CKB test key: $(CKB_KEY)"; \
		fi; \
		if [ -n "$$tmp" ]; then rm -f "$$tmp"; fi; \
		trap - EXIT HUP INT TERM

run: all
	cd $(ROOT_DIR) && \
		FIBER_SECRET_KEY_PASSWORD="$${FIBER_SECRET_KEY_PASSWORD:-$(DEMO_SECRET_KEY_PASSWORD)}" \
		$(BINARY) $(CLI_ARGS)

clean:
	$(RM) $(BINARY)
