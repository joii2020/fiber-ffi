CARGO ?= cargo
RUSTUP ?= rustup

ANDROID_API ?= 23
ANDROID_TARGET ?= aarch64-linux-android
ANDROID_RUSTFLAGS ?= -C link-arg=-Wl,-z,max-page-size=16384

ANDROID_HOST_TAG ?= $(notdir $(firstword $(wildcard $(ANDROID_NDK_HOME)/toolchains/llvm/prebuilt/*)))
ANDROID_TOOLCHAIN_DIR ?= $(ANDROID_NDK_HOME)/toolchains/llvm/prebuilt/$(ANDROID_HOST_TAG)

.PHONY: build-android
build-android:
	@set -e; \
	ndk_bin="$(ANDROID_TOOLCHAIN_DIR)/bin"; \
	linker="$$ndk_bin/$(ANDROID_TARGET)$(ANDROID_API)-clang"; \
	linkerxx="$$ndk_bin/$(ANDROID_TARGET)$(ANDROID_API)-clang++"; \
	target_env=$$(printf '%s' "$(ANDROID_TARGET)" | tr '[:lower:]-' '[:upper:]_'); \
	$(RUSTUP) target add "$(ANDROID_TARGET)"; \
	env \
		ANDROID_NDK_HOME="$(ANDROID_NDK_HOME)" \
		ANDROID_NDK_ROOT="$(ANDROID_NDK_HOME)" \
		CC_$$(printf '%s' "$(ANDROID_TARGET)" | tr '-' '_')="$$linker" \
		CXX_$$(printf '%s' "$(ANDROID_TARGET)" | tr '-' '_')="$$linkerxx" \
		AR_$$(printf '%s' "$(ANDROID_TARGET)" | tr '-' '_')="$$ndk_bin/llvm-ar" \
		RANLIB_$$(printf '%s' "$(ANDROID_TARGET)" | tr '-' '_')="$$ndk_bin/llvm-ranlib" \
		AR="$$ndk_bin/llvm-ar" \
		RANLIB="$$ndk_bin/llvm-ranlib" \
		CARGO_TARGET_$${target_env}_LINKER="$$linker" \
		RUSTFLAGS="$(ANDROID_RUSTFLAGS) $${RUSTFLAGS:-}" \
		BINDGEN_EXTRA_CLANG_ARGS="--sysroot=$(ANDROID_TOOLCHAIN_DIR)/sysroot --target=$(ANDROID_TARGET) -D__ANDROID_API__=$(ANDROID_API)" \
		$(CARGO) build --release --locked --target "$(ANDROID_TARGET)" --no-default-features --features sqlite; \
