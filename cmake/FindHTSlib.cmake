# Explain how to find HTSlib, since CMake by default does not know what HTSlib is.

# Do it with pkg-config
find_package(PkgConfig REQUIRED)
pkg_check_modules(HTSlib REQUIRED htslib)

# As in https://cmake.org/cmake/help/v3.0/module/FindPkgConfig.html we put our
# libraries in HTSlib_LIBRARIES and our static libraries in HTSlib_STATIC_LIBRARIES.
