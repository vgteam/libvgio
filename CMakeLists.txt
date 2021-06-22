cmake_minimum_required(VERSION 3.9)
project(libvgio VERSION 0.0.0 LANGUAGES CXX)

# Optimize by default, but also include debug info
set(CMAKE_CXX_FLAGS "-O3 -g ${CMAKE_CXX_FLAGS}")
if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")

  # assumes clang build
  # we can't reliably detect when we're using clang, so for the time being we assume
  # TODO: can't we though?

  # adapted from https://stackoverflow.com/questions/46414660/macos-cmake-and-openmp
  # find_package(OpenMP) does not work reliably on macOS, so we do its work ourselves
  set (OpenMP_C "${CMAKE_C_COMPILER}")
  set (OpenMP_C_FLAGS " -Xpreprocessor -fopenmp -I/opt/local/include/libomp -I/usr/local/include -L/opt/local/lib/libomp -L/usr/local/lib")
  set (OpenMP_C_LIB_NAMES "libomp" "libgomp" "libiomp5")
  set (OpenMP_CXX "${CMAKE_CXX_COMPILER}")
  set (OpenMP_CXX_FLAGS " -Xpreprocessor -fopenmp -I/opt/local/include/libomp -I/usr/local/include -L/opt/local/lib/libomp -L/usr/local/lib")
  set (OpenMP_CXX_LIB_NAMES "libomp" "libgomp" "libiomp5")
  set (OpenMP_libomp_LIBRARY "omp")
  set (OpenMP_libgomp_LIBRARY "gomp")
  set (OpenMP_libiomp5_LIBRARY "iomp5")

  # and now add the OpenMP parameters to the compile flags
  set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
  set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
  set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OpenMP_EXE_LINKER_FLAGS} -lomp")

  # Mac needs libdl and libomp when linking the library
  set(PLATFORM_EXTRA_LIB_FLAGS -ldl -lomp)

elseif (${CMAKE_SYSTEM_NAME} MATCHES "Linux")

  find_package(OpenMP REQUIRED)

  # add the flags it detects to the compile flags
  set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS} -fopenmp")
  set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS} -fopenmp")
  set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OpenMP_EXE_LINKER_FLAGS}")

  # Linux only needs libdl when linking the library
  set(PLATFORM_EXTRA_LIB_FLAGS -ldl)

endif()

# Use C++14, so that Protobuf headers that use lambdas will work.
set(CMAKE_CXX_STANDARD 14)

# Declare dependencies and explain how to find them
find_package(PkgConfig REQUIRED)

# We need to find Protobuf normally to get the protobuf_generate_cpp command.
# We can only get one version of the libraries this way (static or dynamic). We
# used to also look with pkg_config to find both versions, but that allows us
# to choose two different Protobufs with the different methods, which causes
# Problems.
# No amount of unsetting variables seems to let us invoke this twice to get
# different flavors, and we don't really want to fork FindProtobuf.cmake. So we
# find the dynamic library and assume the static library is a .a next to it.
set(Protobuf_USE_STATIC_LIBS OFF)
# To investigate why we pick the Protobuf we do, do this:
set(Protobuf_DEBUG ON)
find_package(Protobuf REQUIRED)
string(REGEX REPLACE "(\\.so|\\.dylib)$" ".a" Protobuf_STATIC_LIBRARIES "${Protobuf_LIBRARIES}")
message("Protobuf will be ${Protobuf_LIBRARIES} for PIC dynamic code and ${Protobuf_STATIC_LIBRARIES} for non-PIC static code")
# TODO: This is *supposed* to define a protobuf::libprotobuf target, but it doesn't.
# Just use old-style ${Protobuf_INCLUDE_DIRS} and ${Protobuf_LIBRARIES}
# instead. Also, with the targets we probably can't redo find_package.


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

# Add external projects
include(${CMAKE_ROOT}/Modules/ExternalProject.cmake)

# libhandlegraph (full build using its cmake config)
ExternalProject_Add(handlegraph
  SOURCE_DIR "${CMAKE_SOURCE_DIR}/deps/libhandlegraph"
  CMAKE_ARGS "${CMAKE_ARGS};-DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>")
ExternalProject_Get_property(handlegraph INSTALL_DIR)
set(handlegraph_INCLUDE "${INSTALL_DIR}/include")
set(handlegraph_LIB "${INSTALL_DIR}/lib")

# Set link directories. We can't use target_link_directories to keep these
# straight between static and dynamic libraries since it's not available until
# cmake 3.13, which isn't out in distros yet.
link_directories(
    ${HTSlib_LIBRARY_DIRS} ${Jansson_LIBRARY_DIRS}
    ${HTSlib_STATIC_LIBRARY_DIRS} ${Jansson_STATIC_LIBRARY_DIRS}
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

# Don't build any object files until the Protobuf include symlink is set up and handlegraph is built
add_dependencies(vgio link_target)
add_dependencies(vgio_static link_target)
add_dependencies(vgio handlegraph)

# Add an alias so that library can be used inside the build tree, e.g. when testing
add_library(VGio::vgio ALIAS vgio)

# Set target properties
target_include_directories(vgio
    PUBLIC
        $<INSTALL_INTERFACE:include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}> # Capture the Protobuf generated header that lands here.
        ${HTSlib_INCLUDEDIR}
        ${Protobuf_INCLUDE_DIRS}
        ${Jansson_INCLUDEDIR}
        $<BUILD_INTERFACE:${handlegraph_INCLUDE}>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)
target_include_directories(vgio_static
    PUBLIC
        $<INSTALL_INTERFACE:include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}> # Capture the Protobuf generated header that lands here.
        ${HTSlib_INCLUDEDIR}
        ${Protobuf_INCLUDE_DIRS}
        ${Jansson_INCLUDEDIR}
        $<BUILD_INTERFACE:${handlegraph_INCLUDE}>
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
        ${Protobuf_LIBRARIES} Threads::Threads ${HTSlib_LIBRARIES} ${Jansson_LIBRARIES} ${handlegraph_LIB}/libhandlegraph${CMAKE_SHARED_LIBRARY_SUFFIX} ${PLATFORM_EXTRA_LIB_FLAGS}
)
target_link_libraries(vgio_static
    PUBLIC
        ${Protobuf_STATIC_LIBRARIES} Threads::Threads ${HTSlib_STATIC_LIBRARIES} ${Jansson_LIBRARIES} ${handlegraph_LIB}/libhandlegraph${CMAKE_STATIC_LIBRARY_SUFFIX} ${PLATFORM_EXTRA_LIB_FLAGS}
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
