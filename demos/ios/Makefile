CONFIGURATION ?= Debug
SCHEME := FiberDemo
DEVELOPMENT_TEAM ?=
PRODUCT_BUNDLE_IDENTIFIER ?= com.example.fiberdemo.ios

DEVICE_SIGNING_ARGS := CODE_SIGNING_ALLOWED=NO PRODUCT_BUNDLE_IDENTIFIER=$(PRODUCT_BUNDLE_IDENTIFIER)
ifneq ($(strip $(DEVELOPMENT_TEAM)),)
DEVICE_SIGNING_ARGS := DEVELOPMENT_TEAM=$(DEVELOPMENT_TEAM) PRODUCT_BUNDLE_IDENTIFIER=$(PRODUCT_BUNDLE_IDENTIFIER)
endif

.PHONY: build build-sim

build:
	case "$(CONFIGURATION)" in Debug|Release) ;; *) echo "Usage: make build CONFIGURATION=Debug|Release"; exit 2;; esac
	$(MAKE) -C "../.." build-ios
	mkdir -p FiberDemo/Libs/iphoneos
	cp ../../target/aarch64-apple-ios/release/libfiber_ffi.dylib FiberDemo/Libs/iphoneos/libfiber_ffi.dylib
	xcodebuild \
		-project FiberDemo.xcodeproj \
		-scheme "$(SCHEME)" \
		-configuration "$(CONFIGURATION)" \
		-sdk iphoneos \
		-derivedDataPath build/DerivedData \
		SYMROOT=build/XcodeBuild \
		ENABLE_USER_SCRIPT_SANDBOXING=NO \
		$(DEVICE_SIGNING_ARGS) \
		build
	echo "Built app: build/XcodeBuild/$(CONFIGURATION)-iphoneos/$(SCHEME).app"

build-sim:
	case "$(CONFIGURATION)" in Debug|Release) ;; *) echo "Usage: make build-sim CONFIGURATION=Debug|Release"; exit 2;; esac
	$(MAKE) -C "../.." build-ios-sim
	mkdir -p FiberDemo/Libs/iphonesimulator
	cp ../../target/aarch64-apple-ios-sim/release/libfiber_ffi.dylib FiberDemo/Libs/iphonesimulator/libfiber_ffi.dylib
	xcodebuild \
		-project FiberDemo.xcodeproj \
		-scheme "$(SCHEME)" \
		-configuration "$(CONFIGURATION)" \
		-sdk iphonesimulator \
		-derivedDataPath build/DerivedData \
		SYMROOT=build/XcodeBuild \
		ENABLE_USER_SCRIPT_SANDBOXING=NO \
		CODE_SIGNING_ALLOWED=NO \
		ARCHS=arm64 \
		ONLY_ACTIVE_ARCH=YES \
		build
	echo "Built app: build/XcodeBuild/$(CONFIGURATION)-iphonesimulator/$(SCHEME).app"
