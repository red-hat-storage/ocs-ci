#!/usr/bin/env bash

URL=$1
TEMPLATE=$2
PATCH_FILE="${TEMPLATE}.patch"

if [ -z "$URL" ]; then
    echo "ERROR: First parameter is empty and it has to be URL of template" 1>&2
    exit 1
fi
if [ -z "$TEMPLATE" ]; then
    echo "ERROR: second parameter is empty and it has be template file path" 1>&2
    exit 1
fi

diff -u --label=$URL <(curl -s $URL) --label=$TEMPLATE $TEMPLATE > ${PATCH_FILE}

if grep -q "$URL" $PATCH_FILE; then
    echo "Your patch file is created here: $PATCH_FILE"
else
    echo "ERROR: Something went wrong while creating the patch!"
fi
