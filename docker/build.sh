#!/bin/bash
set -e

echo "**********************************"
echo "building $1 for $BACKEND with" 
echo "$CXX using $BUILDCHAIN"
echo "**********************************"

cd /usr/local/src/$1

if [ "$BUILDCHAIN" == "make" ] 
then
    if [ "$SKIPTESTS" == "true"]
    then
        make clean
        make -e test
    fi
    make clean
    make -e install
else
    mkdir -p build
    cd build

    if [ "$BACKEND" == "libevent" ]
    then
        cmake .. -DCMAKE_CXX_COMPILER=$CXX
    else
        cmake .. -DCMAKE_CXX_COMPILER=$CXX -DWITH_LIBEVENT=Off
    fi 
    make
    make test
    make install
fi

