CARGO_TARGET_DIR ?= target
CARGO ?= cargo

ANDROID_API ?= 23
ANDROID_FEATURES ?= sqlite,watchtower
ANDROID_NDK_HOME ?= $(NDK_HOME)
ANDROID_OUT_DIR ?= $(CARGO_TARGET_DIR)/android

UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)
ANDROID_HOST_TAG ?= $(if $(filter Linux,$(UNAME_S)),linux-x86_64,$(if $(filter Darwin,$(UNAME_S)),$(if $(filter arm64,$(UNAME_M)),darwin-arm64,darwin-x86_64),unknown))

.PHONY: build-android
build-android:
	@set -e; \
	ndk_bin="$(ANDROID_NDK_HOME)/toolchains/llvm/prebuilt/$(ANDROID_HOST_TAG)/bin"; \
	abi="arm64-v8a"; \
	target="aarch64-linux-android"; \
	env_target="AARCH64_LINUX_ANDROID"; \
	linker="$$ndk_bin/aarch64-linux-android$(ANDROID_API)-clang"; \
	linkerxx="$$ndk_bin/aarch64-linux-android$(ANDROID_API)-clang++"; \
	echo "Building fiber-ffi for $$abi ($$target)"; \
	rustup target add "$$target"; \
	env \
		ANDROID_NDK_HOME="$(ANDROID_NDK_HOME)" \
		ANDROID_NDK_ROOT="$(ANDROID_NDK_HOME)" \
		CC_$$(printf '%s' "$$target" | tr '-' '_')="$$linker" \
		CXX_$$(printf '%s' "$$target" | tr '-' '_')="$$linkerxx" \
		AR_$$(printf '%s' "$$target" | tr '-' '_')="$$ndk_bin/llvm-ar" \
		RANLIB_$$(printf '%s' "$$target" | tr '-' '_')="$$ndk_bin/llvm-ranlib" \
		AR="$$ndk_bin/llvm-ar" \
		RANLIB="$$ndk_bin/llvm-ranlib" \
		CARGO_TARGET_$${env_target}_LINKER="$$linker" \
		BINDGEN_EXTRA_CLANG_ARGS="--sysroot=$(ANDROID_NDK_HOME)/toolchains/llvm/prebuilt/$(ANDROID_HOST_TAG)/sysroot --target=$$target -D__ANDROID_API__=$(ANDROID_API)" \
		$(CARGO) build --release --locked --target "$$target" --no-default-features --features "$(ANDROID_FEATURES)"; \
