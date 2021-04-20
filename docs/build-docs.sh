#!/bin/bash

if [[ $(pwd) =~ .*/rubicon/docs$ ]]; then
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
    make html && echo "Open with 'open $(pwd)/build/html/index.html'"
else
    echo "'build-docs.sh' must be run from rubicon/docs"
    exit 1
fi
