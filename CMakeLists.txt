cmake_minimum_required(VERSION 3.9)
project(libvgio VERSION 0.0.0 LANGUAGES CXX)

# Optimize by default, but also include debug info
set(CMAKE_CXX_FLAGS "-O3 -g")

# Use C++14, so that Protobuf headers that use lambdas will work.
set(CMAKE_CXX_STANDARD 14)

# Declare dependencies and explain how to find them
find_package(PkgConfig REQUIRED)

# We need to find Protobuf normally to get the protobuf_generate_cpp command
find_package(Protobuf REQUIRED)
# But we also need to find it via pkg-config to get both the static and dynamic libraries.
# The default find_package implementation can only give us one or the other.
pkg_check_modules(Protobuf REQUIRED protobuf)

# Find threads
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# Find HTSlib. We find it with pkg-config, so it has static and dynamic libs by default.
pkg_check_modules(HTSlib REQUIRED htslib)

# Set link directories. we can't use target_link_directories to keep these
# straight between static and dynamic libraries since it's not available until
# cmake 3.13, which isn't out in distros yet.
link_directories(
    ${Protobuf_LIBRARY_DIRS} ${HTSlib_LIBRARY_DIRS}
    ${Protobuf_STATIC_LIBRARY_DIRS} ${HTSlib_STATIC_LIBRARY_DIRS}
)

# Set where the LC_ID_DYLIB install name for Mac dylib files ought to point.
# It ought to point to where the dylibs will actually be installed
# Only takes effect after installation
set(CMAKE_INSTALL_NAME_DIR "${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}")

# Make Protobuf headers and code
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS "deps/vg.proto")

# The Protobuf compiler dumps the vg.pb.h file in the root directory.
# But we need to have it in somewhere accessible as <vg/vg.pb.h>
# because that's where our code will want it to be after installation.
# So hook it up with a symlink. See <https://stackoverflow.com/a/35765320>
add_custom_target(link_target ALL
    COMMAND ${CMAKE_COMMAND} -E create_symlink . vg)

# Find all the CPP files
file(GLOB SOURCES "src/**.cpp")

# Build that into both shared and static libraies, with shared as the main one.
# Don't use an object library because we want the static library to be position-dependent code.
add_library(vgio SHARED ${PROTO_SRCS} ${SOURCES})
set_property(TARGET vgio PROPERTY POSITION_INDEPENDENT_CODE ON)
add_library(vgio_static STATIC ${PROTO_SRCS} ${SOURCES})
set_target_properties(vgio_static PROPERTIES OUTPUT_NAME vgio)
set_property(TARGET vgio_static PROPERTY POSITION_INDEPENDENT_CODE OFF)

# Don't build any object files until the Protobuf include symlink is set up
add_dependencies(vgio link_target)
add_dependencies(vgio_static link_target)

# Add an alias so that library can be used inside the build tree, e.g. when testing
add_library(VGio::vgio ALIAS vgio)

# Set target properties

# If we pass exactly ${HTSlib_INCLUDEDIR} or ${Protobuf_INCLUDEDIR} they manage
# to get filtered out somehow and they aren't passed as -I optiosn to the
# compiler. So we throw a /. on the end.
target_include_directories(vgio
    PUBLIC
        $<INSTALL_INTERFACE:include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}> # Capture the Protobuf generated header that lands here.
        ${HTSlib_INCLUDEDIR}/.
        ${Protobuf_INCLUDEDIR}/.
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)
target_include_directories(vgio_static
    PUBLIC
        $<INSTALL_INTERFACE:include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}> # Capture the Protobuf generated header that lands here.
        ${HTSlib_INCLUDEDIR}/.
        ${Protobuf_INCLUDEDIR}/.
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_compile_features(vgio PUBLIC cxx_std_14)
target_compile_features(vgio_static PUBLIC cxx_std_14)

# We need to repeat these linking rules for both shared and static because they don't propagate from the object library.
# But we need to carry through transitive library dependencies in static mode.
# Also note that target_link_directories needs cmake 3.13+
target_link_libraries(vgio
    PUBLIC
        ${Protobuf_LIBRARIES} Threads::Threads ${HTSlib_LIBRARIES}
)
target_link_libraries(vgio_static
    PUBLIC
        ${Protobuf_STATIC_LIBRARIES} Threads::Threads ${HTSlib_STATIC_LIBRARIES}
)

# Installation instructions

include(GNUInstallDirs)
set(INSTALL_CONFIGDIR ${CMAKE_INSTALL_LIBDIR}/cmake/VGio)

install(TARGETS vgio vgio_static
    EXPORT vgio-targets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

# Make the exported targets have the name VGio and not vgio
set_target_properties(vgio PROPERTIES EXPORT_NAME VGio)
set_target_properties(vgio_static PROPERTIES EXPORT_NAME VGio_static)

install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(FILES ${PROTO_HDRS} DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/vg)

# Export the targets to a script
install(EXPORT vgio-targets
  FILE
    VGioTargets.cmake
  NAMESPACE
    VGio::
  DESTINATION
    ${INSTALL_CONFIGDIR}
)

# Create a ConfigVersion.cmake file
include(CMakePackageConfigHelpers)
write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/VGioConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY AnyNewerVersion
)

configure_package_config_file(${CMAKE_CURRENT_LIST_DIR}/cmake/VGioConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/VGioConfig.cmake
    INSTALL_DESTINATION ${INSTALL_CONFIGDIR}
)

# Install the config and configversion
install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/VGioConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/VGioConfigVersion.cmake
    DESTINATION ${INSTALL_CONFIGDIR}
)

# Export from the build tree
export(EXPORT vgio-targets FILE ${CMAKE_CURRENT_BINARY_DIR}/VGioTargets.cmake NAMESPACE VGio::)

# Register package in user's package registry
export(PACKAGE VGio)

# TODO: Auto-generate a pkg-config file so non-cmake code can depend on us
