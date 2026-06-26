# Fiber FFI dynamic libraries

Run `make -C demos/ios build` or `make -C demos/ios build-sim` from the
repository root to build and copy the matching Rust library.
The Makefile copies the dylibs into platform-specific folders:

- `iphoneos/libfiber_ffi.dylib`
- `iphonesimulator/libfiber_ffi.dylib`

The folders are intentionally ignored by git because the dylibs are build
artifacts.
