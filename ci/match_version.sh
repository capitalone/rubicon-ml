#!/bin/bash

VERSION=$1
FILENAME=$2

VERSION_CLEAN=$(cut -d 'v' -f2- <<< ${VERSION})

INIT_VERSION=$(grep '__version__' ${FILENAME})
INIT_VERSION_CLEAN=$(cut -d '"' -f2 <<< ${INIT_VERSION})

if [[ ${VERSION_CLEAN} != ${INIT_VERSION_CLEAN} ]]; then
  echo "Release version (${VERSION_CLEAN}) and __init__.py version (${INIT_VERSION_CLEAN}) MISMATCH."
  exit 1
fi

echo "Release version and __init__.py version match."
