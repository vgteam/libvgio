#!/bin/bash
# This script builds all dependencies of libvgio
# and installs the library on a LINUX or Mac OS X system

CUR_DIR=`pwd`
VGIO_INSTALL_PREFIX=${HOME}
if [ $# -ge 1 ]; then
    VGIO_INSTALL_PREFIX=${1}
fi

# Get absolute path name of install directory
mkdir -p "${VGIO_INSTALL_PREFIX}" 2> /dev/null
cd "${VGIO_INSTALL_PREFIX}" > /dev/null 2>&1
if [ $? != 0 ] ; then
    echo "ERROR: directory '${VGIO_INSTALL_PREFIX}' does not exist nor could be created."
    echo "Please choose another directory."
    exit 1
else
    VGIO_INSTALL_PREFIX=`pwd -P`
fi

echo "Library will be installed in '${VGIO_INSTALL_PREFIX}'"

cd "${CUR_DIR}"
OLD_DIR="$( cd "$( dirname "$0" )" && pwd )" # gets the directory where the script is located in
cd "${OLD_DIR}"
OLD_DIR=`pwd`

# Build
cd build # change into the build directory
if [ $? != 0 ]; then
    exit 1
fi
./clean.sh # clean-up build directory
if [ $? != 0 ]; then
    exit 1
fi

cmake -DCMAKE_INSTALL_PREFIX="${VGIO_INSTALL_PREFIX}" .. # run cmake 
if [ $? != 0 ]; then
    echo "ERROR: CMake build failed."
    exit 1
fi
make # run make
if [ $? != 0 ]; then
    echo "ERROR: Build failed."
    exit 1
fi
echo "Removing old files"
echo "rm -rf '${VGIO_INSTALL_PREFIX}/include/vg/vg.pb.h'"
rm -rf "${VGIO_INSTALL_PREFIX}/include/vg/vg.pb.h"
echo "rm -rf '${VGIO_INSTALL_PREFIX}/include/vg/io/*'"
rm -rf "${VGIO_INSTALL_PREFIX}/include/vg/io/*"
if [ $? != 0 ]; then
    echo "WARNING: Could not remove old header files."
fi
echo "rm -f '${VGIO_INSTALL_PREFIX}/lib/libvgio*'"
rm -f "${VGIO_INSTALL_PREFIX}/lib/libvgio*"
if [ $? != 0 ]; then
    echo "WARNING: Could not remove old library file."
fi
make install # install library
if [ $? != 0 ]; then
    echo "ERROR: Installation failed."
    exit 1
fi

cd ..

if [ "`pwd`" != "${OLD_DIR}" ]; then
    echo "ERROR: we are not in the original dir ${OLD_DIR} now."
    exit 1
fi

echo "SUCCESS: libvgio was installed successfully!"
echo "The libvgio protobuf header is located in '${VGIO_INSTALL_PREFIX}/include/vg'."
echo "The libvgio include files are located in '${VGIO_INSTALL_PREFIX}/include/vg/io'."
echo "The library files are located in '${VGIO_INSTALL_PREFIX}/lib'."
