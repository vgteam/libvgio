# Explain how to find HTSlib, since CMake by default does not know what HTSlib is.
# See <https://github.com/Intel-HLS/GenomicsDB/blob/0ad5451950661900c8e6c9db0c084ebecdf2351f/cmake/Modules/Findhtslib.cmake>

# Get the standard package finder.
# See https://cmake.org/cmake/help/v3.6/module/FindPackageHandleStandardArgs.html
include(FindPackageHandleStandardArgs)

# The include directory is wherever has this header under an "htslib" folder.
find_path(HTSLIB_INCLUDE_DIR htslib/hts.h)

# The library is going to be what we would link with -lhts
find_library(HTSLIB_LIBRARY hts)

# Say we found the library if we found those things, and fail otherwise.
find_package_handle_standard_args(HTSlib DEFAULT_MSG HTSLIB_INCLUDE_DIR HTSLIB_LIBRARY)
