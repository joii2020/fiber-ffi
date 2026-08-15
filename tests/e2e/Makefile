CARGO ?= cargo
FIBER_SOURCE_DIR ?= ../../../fiber

.PHONY: test light-client light-client-check

test: light-client

light-client:
	FIBER_SOURCE_DIR="$(abspath $(FIBER_SOURCE_DIR))" CARGO="$(CARGO)" ./light-client/run.sh

light-client-check:
	FIBER_SOURCE_DIR="$(abspath $(FIBER_SOURCE_DIR))" CARGO="$(CARGO)" ./light-client/run.sh --check
