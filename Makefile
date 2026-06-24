CARGO ?= cargo
RUSTUP ?= rustup

IOS_RUSTFLAGS ?= -C link-arg=-Wl,-install_name,@rpath/libfiber_ffi.dylib

ANDROID_API ?= 23
ANDROID_TARGET ?= aarch64-linux-android
ANDROID_RUSTFLAGS ?= -C link-arg=-Wl,-z,max-page-size=16384

ANDROID_HOST_TAG ?= $(notdir $(firstword $(wildcard $(ANDROID_NDK_HOME)/toolchains/llvm/prebuilt/*)))
ANDROID_TOOLCHAIN_DIR ?= $(ANDROID_NDK_HOME)/toolchains/llvm/prebuilt/$(ANDROID_HOST_TAG)

.PHONY: build-ios build-ios-sim build-android

build-ios:
	@set -e; \
	target="aarch64-apple-ios"; \
	sdk=iphoneos; \
	clang_target=arm64-apple-ios; \
	echo "Building $$target"; \
	target_key=$$(printf '%s' "$$target" | tr '-' '_'); \
	target_env=$$(printf '%s' "$$target" | tr '[:lower:]-' '[:upper:]_'); \
	sdkroot=$$(xcrun --sdk "$$sdk" --show-sdk-path); \
	deployment_target=$$(xcrun --sdk "$$sdk" --show-sdk-platform-version); \
	min_flag="-miphoneos-version-min=$$deployment_target"; \
	clang=$$(xcrun --sdk "$$sdk" --find clang); \
	clangxx=$$(xcrun --sdk "$$sdk" --find clang++); \
	ar=$$(xcrun --sdk "$$sdk" --find ar); \
	ranlib=$$(xcrun --sdk "$$sdk" --find ranlib); \
	$(RUSTUP) target add "$$target"; \
	env \
		IPHONEOS_DEPLOYMENT_TARGET="$$deployment_target" \
		SDKROOT="$$sdkroot" \
		CC_$${target_key}="$$clang" \
		CXX_$${target_key}="$$clangxx" \
		AR_$${target_key}="$$ar" \
		RANLIB_$${target_key}="$$ranlib" \
		CFLAGS_$${target_key}="--target=$$clang_target -isysroot $$sdkroot $$min_flag" \
		CXXFLAGS_$${target_key}="--target=$$clang_target -isysroot $$sdkroot $$min_flag" \
		CARGO_TARGET_$${target_env}_LINKER="$$clang" \
		OPENSSL_STATIC=1 \
		RUSTFLAGS="$(IOS_RUSTFLAGS) -C link-arg=$$min_flag $${RUSTFLAGS:-}" \
		$(CARGO) build --release --locked --target "$$target" --no-default-features --features "sqlite"

build-ios-sim:
	@set -e; \
	target="aarch64-apple-ios-sim"; \
	sdk=iphonesimulator; \
	clang_target=arm64-apple-ios-simulator; \
	echo "Building $$target"; \
	target_key=$$(printf '%s' "$$target" | tr '-' '_'); \
	target_env=$$(printf '%s' "$$target" | tr '[:lower:]-' '[:upper:]_'); \
	sdkroot=$$(xcrun --sdk "$$sdk" --show-sdk-path); \
	deployment_target=$$(xcrun --sdk "$$sdk" --show-sdk-platform-version); \
	min_flag="-mios-simulator-version-min=$$deployment_target"; \
	clang=$$(xcrun --sdk "$$sdk" --find clang); \
	clangxx=$$(xcrun --sdk "$$sdk" --find clang++); \
	ar=$$(xcrun --sdk "$$sdk" --find ar); \
	ranlib=$$(xcrun --sdk "$$sdk" --find ranlib); \
	$(RUSTUP) target add "$$target"; \
	env \
		IPHONEOS_DEPLOYMENT_TARGET="$$deployment_target" \
		SDKROOT="$$sdkroot" \
		CC_$${target_key}="$$clang" \
		CXX_$${target_key}="$$clangxx" \
		AR_$${target_key}="$$ar" \
		RANLIB_$${target_key}="$$ranlib" \
		CFLAGS_$${target_key}="--target=$$clang_target -isysroot $$sdkroot $$min_flag" \
		CXXFLAGS_$${target_key}="--target=$$clang_target -isysroot $$sdkroot $$min_flag" \
		CARGO_TARGET_$${target_env}_LINKER="$$clang" \
		OPENSSL_STATIC=1 \
		RUSTFLAGS="$(IOS_RUSTFLAGS) -C link-arg=$$min_flag $${RUSTFLAGS:-}" \
		$(CARGO) build --release --locked --target "$$target" --no-default-features --features "sqlite"

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
