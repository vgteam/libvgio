get_filename_component(VGIO_CMAKE_DIR "${CMAKE_CURRENT_LIST_FILE}" PATH)
include(CMakeFindDependencyMacro)

list(APPEND CMAKE_MODULE_PATH ${VGIO_CMAKE_DIR})

find_dependency(Protobuf REQUIRED)
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_dependency(Threads REQUIRED)
find_dependency(PkgConfig REQUIRED)
pkg_check_modules(Protobuf REQUIRED protobuf)
pkg_check_modules(HTSlib REQUIRED htslib)
list(REMOVE_AT CMAKE_MODULE_PATH -1)

if(NOT TARGET VGio::VGio)
    include("${VGIO_CMAKE_DIR}/VGioTargets.cmake")
endif()

set(VGio_LIBRARIES VGio::VGio)
set(VGio_STATIC_LIBRARIES VGio::VGio_static)
