# Fiber iOS Demo

This demo mirrors the Android sample with a native UIKit UI and direct calls into
`fiber_ffi.h`.

## Build native libraries

From the repository root:

```sh
make build-ios build-ios-sim
```

Or build the app from the iOS demo directory; the Makefile builds and prepares
the required native library first:

```sh
make -C demos/ios build
make -C demos/ios build-sim
```

## Run the app

Open `demos/ios/FiberDemo.xcodeproj` in Xcode, select the `FiberDemo` target,
and run it on an iOS simulator or device that matches the dylib you built.
For physical devices, choose your development team in the target signing
settings.

You can also build the iOS app from the command line:

```sh
make -C demos/ios build
make -C demos/ios build-sim
```

By default, the device build disables code signing so it can produce a local app
bundle without an Apple development team. To sign the app for installation on a
physical device, pass your team ID:

```sh
make -C demos/ios build DEVELOPMENT_TEAM=YOURTEAMID
```

If needed, also pass a bundle identifier registered to your team:

```sh
make -C demos/ios build DEVELOPMENT_TEAM=YOURTEAMID PRODUCT_BUNDLE_IDENTIFIER=com.yourcompany.fiberdemo
```

`build` builds a Debug device app at:

```text
demos/ios/build/XcodeBuild/Debug-iphoneos/FiberDemo.app
```

`build-sim` builds a Debug simulator app at:

```text
demos/ios/build/XcodeBuild/Debug-iphonesimulator/FiberDemo.app
```

The simulator build targets `arm64`, matching the Rust
`aarch64-apple-ios-sim` dylib produced by `make build-ios-sim`.

Optional arguments:

```sh
make -C demos/ios build CONFIGURATION=Debug
make -C demos/ios build CONFIGURATION=Release
make -C demos/ios build-sim CONFIGURATION=Debug
make -C demos/ios build-sim CONFIGURATION=Release
```

The Rust iOS library defaults to the same minimum deployment target as the demo
app, iOS 15.0. If you change the Xcode target's deployment version, pass the
same value when building the native library:

```sh
make -C demos/ios build IOS_DEPLOYMENT_TARGET=16.0
make -C demos/ios build-sim IOS_DEPLOYMENT_TARGET=16.0
```

The Xcode target expects:

- `FiberDemo/Libs/iphonesimulator/libfiber_ffi.dylib` for simulator builds.
- `FiberDemo/Libs/iphoneos/libfiber_ffi.dylib` for device builds.

## Demo coverage

- Set and store the CKB private key expected by Fiber. Unlike the Android demo,
  this sample does not derive a CKB lock arg or query CKB balance, so it avoids
  adding a BigInteger/secp256k1 dependency to the iOS demo.
- Start and stop the embedded Fiber node while the app is active.
- Read node info and native event callbacks.
- List and connect peers.
- Create and send invoices.
- List, create, and close channels.

iOS does not have an Android-style foreground service. This sample keeps the
Fiber handle alive only while the app scene is connected.
