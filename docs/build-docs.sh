#!/bin/bash

echo "Run 'build-docs.sh' from rubicon/docs"

if [ "$1" != "--no-clone" ]; then
    rm -rf build
    mkdir build
    cd build
    git clone -b gh-pages https://github.com/capitalone/rubicon.git html
    cd ..
else
    rm -rf build/html
    mkdir build/html
fi
make html
open build/html/index.html
echo "Opening $(pwd)/build/html/index.html..."
