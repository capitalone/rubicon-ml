#!/bin/bash

if [[ $(pwd) =~ .*/rubicon-ml/docs$ ]]; then
    rm -rf build/html
    mkdir build/html

    make html && echo "Open with 'open $(pwd)/build/html/index.html'"
else
    echo "'build-docs.sh' must be run from rubicon-ml/docs"
    exit 1
fi
