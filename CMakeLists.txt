cmake_minimum_required(VERSION 3.10)
project(libvgio VERSION 0.0.0 LANGUAGES CXX)

OPTION(USE_INSTALLED_LIBHANDLEGRAPH "Use the version of libhandlegraph installed on the system, if it exists, instead of the bundled version [default: off]" OFF)


# Optimize by default, but also include debug info
set(CMAKE_CXX_FLAGS "-O3 -g ${CMAKE_CXX_FLAGS}")
# Don't allow any -std= flags in
string(REGEX REPLACE " *-std=[^ ]*" "" CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS})
find_package(OpenMP REQUIRED)

if (NOT DEFINED CMAKE_CXX_STANDARD)
    # Use C++14, so that Protobuf headers that use lambdas will work.
    set(CMAKE_CXX_STANDARD 14)
endif()
message("Building libvgio for C++ ${CMAKE_CXX_STANDARD}")

# Declare dependencies and explain how to find them
find_package(PkgConfig REQUIRED)

# On Mac, Homebrew ships a CMake with a FindProtobuf.cmake that can't get all
# the dependencies fro the Protobuf it ships.
# But luckily Homebrew also ships the Protobuf project's CMake config.
# Since Protobuf depends on Abseil, and CMake hasn't yet updated FindProtobuf
# to expose this dependency, we need to sniff out Protobuf using its own
# installed CMake config and not CMake's finder.
find_package(Protobuf CONFIG)

if (Protobuf_FOUND)
    message("Found Protobuf using its own protobuf-config.cmake")
    # The compatible mode is supposed to set Protobuf_LIBRARIES, but doesn't, so we don't use it.
    # INTERFACE_LINK_LIBRARIES on protobuf::libprotobuf is full of targets and not actual lib files.
    # Also the targets are only SHARED and not STATIC, as installed by Homebrew.

    # Instead of protobuf_generate_cpp which FindProtobuf makes, Protobuf itself
    # makes a different and incompatible protobuf_generate function.

    # Make Protobuf headers and code.
    # See <https://github.com/protocolbuffers/protobuf/blob/d800c5f08b184d261e6c47662035d554f109eb3b/docs/cmake_protobuf_generate.md>
    protobuf_generate(
        LANGUAGE cpp
        PROTOS "deps/vg.proto"
        IMPORT_DIRS "deps/"
        OUT_VAR PROTO_SRCS
    )
    # Find just the headers to install
    foreach (FILENAME ${PROTO_SRCS})
        string(REGEX MATCH ".*\.h$" HEADER ${FILENAME})
        if (HEADER)
            list(APPEND PROTO_HDRS ${HEADER})
        endif()
    endforeach()
else()
    message("No installed Protobuf has registered itself with CMake.")

    # Fall back to FindProtobuf.cmake and hope it is a version that works with the installed Protobuf
    find_package(Protobuf REQUIRED)

    message("Found Protobuf using FindProtobuf.cmake")

    # Generate the code using the FindProtobuf.cmake function
    protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS "deps/vg.proto")
endif()





# Find threads
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads)

if (Threads_FOUND)
    message("Using real Threads library")
else()
    message("Faking Threads library; does your CMake know about pthreads?")
    set(CMAKE_THREAD_LIBS_INIT "-lpthread")
    set(CMAKE_HAVE_THREADS_LIBRARY 1)
    set(CMAKE_USE_WIN32_THREADS_INIT 0)
    set(CMAKE_USE_PTHREADS_INIT 1)
    set(THREADS_PREFER_PTHREAD_FLAG ON)
    add_library(Threads::Threads INTERFACE IMPORTED)
    set_property(TARGET Threads::Threads PROPERTY INTERFACE_LINK_LIBRARIES "${CMAKE_THREAD_LIBS_INIT}")
endif()

# Find HTSlib. We find it with pkg-config, so it has static and dynamic libs by default.
pkg_check_modules(HTSlib REQUIRED htslib)

# Find Jansson
pkg_check_modules(Jansson REQUIRED jansson)

# Find or build libhandlegraph
if (USE_INSTALLED_LIBHANDLEGRAPH)
    find_package(libhandlegraph)
endif()

if (${libhandlegraph_FOUND})
    message("Using installed libhandlegraph")
elseif (TARGET handlegraph_objs)
    message("Using libhandlegraph built by another CMake")
else ()
    message("Using bundled libhandlegraph")
    add_subdirectory("${bdsg_DIR}/deps/libhandlegraph")
endif()

add_library(libhandlegraph::handlegraph_shared ALIAS handlegraph_shared)
add_library(libhandlegraph::handlegraph_static ALIAS handlegraph_static)

if (CMAKE_MAJOR_VERSION EQUAL "3" AND (CMAKE_MINOR_VERSION EQUAL "10" OR CMAKE_MINOR_VERSION EQUAL "11"))
    # Set link directories. We can't yet use target_link_directories to keep
    # these straight between static and dynamic libraries since it's not
    # available until cmake 3.13.
    link_directories(
        ${HTSlib_LIBRARY_DIRS} ${Jansson_LIBRARY_DIRS}
        ${HTSlib_STATIC_LIBRARY_DIRS} ${Jansson_STATIC_LIBRARY_DIRS}
    )
endif()

# Set where the LC_ID_DYLIB install name for Mac dylib files ought to point.
# It ought to point to where the dylibs will actually be installed
# Only takes effect after installation
include(GNUInstallDirs)
set(CMAKE_INSTALL_NAME_DIR "${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}")

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

# Don't build any object files until the Protobuf include symlink is set up and handlegraph is built
add_dependencies(vgio link_target)
add_dependencies(vgio_static link_target)

# Add an alias so that library can be used inside the build tree, e.g. when testing
add_library(VGio::vgio ALIAS vgio)

# Set target properties
target_include_directories(vgio
    PUBLIC
        $<INSTALL_INTERFACE:include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}> # Capture the Protobuf generated header that lands here.
        ${HTSlib_INCLUDEDIR}
        ${Jansson_INCLUDEDIR}
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)
target_include_directories(vgio_static
    PUBLIC
        $<INSTALL_INTERFACE:include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}> # Capture the Protobuf generated header that lands here.
        ${HTSlib_INCLUDEDIR}
        ${Jansson_INCLUDEDIR}
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_compile_features(vgio PUBLIC cxx_std_${CMAKE_CXX_STANDARD})
target_compile_features(vgio_static PUBLIC cxx_std_${CMAKE_CXX_STANDARD})

# We need to repeat these linking rules for both shared and static because they don't propagate from the object library.
# But we need to carry through transitive library dependencies in static mode.
# Also note that target_link_directories needs cmake 3.13+
target_link_libraries(vgio
    PUBLIC
        protobuf::libprotobuf Threads::Threads ${HTSlib_LIBRARIES} ${Jansson_LIBRARIES} libhandlegraph::handlegraph_shared ${PLATFORM_EXTRA_LIB_FLAGS} OpenMP::OpenMP_CXX
)
target_link_libraries(vgio_static
    PUBLIC
        protobuf::libprotobuf Threads::Threads ${HTSlib_STATIC_LIBRARIES} ${Jansson_LIBRARIES} libhandlegraph::handlegraph_static ${PLATFORM_EXTRA_LIB_FLAGS} OpenMP::OpenMP_CXX
)

if (NOT (CMAKE_MAJOR_VERSION EQUAL "3" AND (CMAKE_MINOR_VERSION EQUAL "10" OR CMAKE_MINOR_VERSION EQUAL "11")))
    target_link_directories(vgio
        PUBLIC
            ${HTSlib_LIBRARY_DIRS} ${Jansson_LIBRARY_DIRS}
    )
    target_link_directories(vgio_static
        PUBLIC
            ${HTSlib_STATIC_LIBRARY_DIRS} ${Jansson_STATIC_LIBRARY_DIRS}
    )
endif()

# Test
# TODO: This doesn't link on Mac yet due to missing vtables for STL streams
# that Protobuf wants.
add_executable(test_libvgio EXCLUDE_FROM_ALL test.cpp)
target_link_libraries(test_libvgio vgio_static)
set_target_properties(test_libvgio PROPERTIES OUTPUT_NAME "test_libvgio")
set_target_properties(test_libvgio PROPERTIES INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}")

# Installation instructions

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
