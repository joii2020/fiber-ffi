cmake_minimum_required(VERSION 3.22.1)

project(fiber_bridge)

add_library(fiber_ffi SHARED IMPORTED)
set_target_properties(fiber_ffi PROPERTIES
    IMPORTED_LOCATION ${CMAKE_SOURCE_DIR}/../jniLibs/${ANDROID_ABI}/libfiber_ffi.so
    IMPORTED_NO_SONAME TRUE
)

add_library(fiber_bridge SHARED fiber_bridge.cpp)

target_include_directories(fiber_bridge PRIVATE include)
target_link_options(fiber_bridge PRIVATE "-Wl,-z,max-page-size=16384")

find_library(log-lib log)

target_link_libraries(fiber_bridge fiber_ffi ${log-lib})
