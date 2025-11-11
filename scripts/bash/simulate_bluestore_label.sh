#!/bin/bash

set -euo pipefail

DEVICE="$1"
VERIFY_DISK_EMPTY="${2:-true}"  # optional second argument (default: true)

if [[ -z "$DEVICE" ]]; then
    echo "Usage: $0 /dev/sdX [verify_disk_empty:true|false]"
    exit 1
fi
echo ">>> Running BlueStore label simulation on $DEVICE"
echo ">>> Verify disk empty: $VERIFY_DISK_EMPTY"

# Stage 0 — Optional disk emptiness verification
if [[ "$VERIFY_DISK_EMPTY" == "true" ]]; then
    echo ">>> Precheck: Verifying device is empty (first 22 bytes)..."
    if dd if="$DEVICE" bs=1 count=22 status=none | tr -d '\000' | grep -q .; then
        echo "ERROR: $DEVICE contains non-zero data at LBA0 — refusing to overwrite."
        echo ">>> Please wipe or use a clean test disk."
        exit 1
    fi
fi

# Stage 1 — Prechecks
echo ">>> Precheck: Validating device..."

[ -b "$DEVICE" ] || { echo "ERROR: $DEVICE is not a block device"; exit 1; }
[ "$(lsblk -no TYPE "$DEVICE")" = "disk" ] || { echo "ERROR: $DEVICE is not a whole disk"; exit 1; }
[ "$(blockdev --getro "$DEVICE")" -eq 0 ] || { echo "ERROR: $DEVICE is read-only"; exit 1; }

# Stage 2 — Prepare disk
echo ">>> Preparing disk (wiping signatures and partition table)..."
sgdisk -Z "$DEVICE" || true
wipefs -a "$DEVICE" || true
blockdev --rereadpt "$DEVICE" || true
udevadm settle || true

# Stage 3 — Write BlueStore label
echo ">>> Writing BlueStore label..."
UUID=$(cat /proc/sys/kernel/random/uuid)

# Build the label reliably with trailing newline preserved
LC_ALL=C printf -v LABEL 'bluestore block device\n%.36s\n' "$UUID"
LEN=$(printf '%s' "$LABEL" | wc -c)

echo "Label length: $LEN"
if [[ "$LEN" -ne 60 ]]; then
    echo "ERROR: Label length=$LEN (expected 60)"
    echo "LABEL content (hex):"
    printf '%s' "$LABEL" | hexdump -C
    exit 1
fi

printf '%s' "$LABEL" | dd of="$DEVICE" bs=1 seek=0 conv=notrunc,fsync status=none
blockdev --flushbufs "$DEVICE" || true
udevadm settle || true

echo ">>> Label stamped: $DEVICE:$UUID"

# Stage 4 — Verify label
echo ">>> Verifying label..."
VERIFY_OK=1
head -c 22 "$DEVICE" | grep -qx 'bluestore block device' || VERIFY_OK=0
[ "$(dd if="$DEVICE" bs=1 count=60 status=none | wc -c)" -eq 60 ] || VERIFY_OK=0

if [[ "$VERIFY_OK" -eq 1 ]]; then
    echo ">>> Verification PASSED"
    echo ">>> BlueStore UUID: $UUID"
else
    echo ">>> Verification FAILED"
    echo ">>> Hexdump of first 60 bytes:"
    hexdump -C -n 60 -s 0 "$DEVICE" || true
    exit 1
fi
