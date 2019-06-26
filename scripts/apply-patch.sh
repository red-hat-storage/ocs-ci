#!/usr/bin/env bash

# If you pass patch file as first parameter this script apply patch to just
# particular file.

# If you don't provide any parameter:
# This script looks for all .patch files in ocs-ci repo and tries to download
# original file written in the first line of patch file. (Count that patch was
# created by create-patch.sh script with labeling and contains URL)

set -e

script_dir=$(dirname "$0")
echo $script_dir
download_and_patch_file () {
    patch_file=$1
    file_name=${patch_file%".patch"}
    rm -f $file_name
    read -r download_link<$patch_file
    download_link=$(echo  $download_link | cut -d " " -f 2)
    echo "Downloading new version of file: $file_name from source: $download_link"
    wget $download_link -O $file_name
    patch $file_name < $patch_file
    echo "File: $file_name was sucessfully patched"
}

PATCH_FILE=$1

if [ -n "$PATCH_FILE" ]; then
    download_and_patch_file "$PATCH_FILE"
else
    # If you will move the script to some different location you will need to
    # change following line!
    pushd $script_dir/../
    find ./ -name '*.patch' | while read patch_file
    do
        echo "Found patch file: $patch_file"
        download_and_patch_file "$patch_file"
    done
    popd
fi
