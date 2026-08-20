CARGO ?= cargo

ROOT_DIR := $(abspath ../..)
CARGO_TARGET_DIR ?= $(ROOT_DIR)/target
BINARY := $(CARGO_TARGET_DIR)/release/fiber-demo-cli
FIBER_FEATURES ?= sqlite,ckb-light-client-portable
DATA_DIR ?= $(ROOT_DIR)/tools/fiber-demo-cli/data
DEMO_SECRET_KEY_PASSWORD ?= fiber-ffi-rust-cli-demo-password
CLI_ARGS ?= --data "$(DATA_DIR)"

.PHONY: all build run clean

all: build

build:
	cd $(ROOT_DIR) && CARGO_TARGET_DIR=$(CARGO_TARGET_DIR) $(CARGO) build \
		--release --locked --bin fiber-demo-cli \
		--no-default-features --features "$(FIBER_FEATURES)"

run: build
	cd $(ROOT_DIR) && \
		FIBER_SECRET_KEY_PASSWORD="$${FIBER_SECRET_KEY_PASSWORD:-$(DEMO_SECRET_KEY_PASSWORD)}" \
		$(BINARY) $(CLI_ARGS)

clean:
	$(RM) $(BINARY)
