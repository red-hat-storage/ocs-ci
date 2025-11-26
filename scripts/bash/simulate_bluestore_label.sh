#!/bin/bash
set -euo pipefail

DEVICE="${1:-}"
TAG="${2:-}"                     # Optional: Ceph container tag
VERIFY_DISK_EMPTY="${3:-true}"   # Optional: verify disk is empty

# Stage 0 — Argument validation
if [[ -z "$DEVICE" ]]; then
    echo "Usage: $0 /dev/sdX <ceph_tag> [verify_disk_empty:true|false]"
    exit 1
fi

# If TAG not provided, auto-detect from cephadm
if [[ -z "$TAG" ]]; then
    detected_tag="v$(cephadm version | awk '/cephadm version/ {print $3}')"
    echo ">>> No TAG specified, using detected version: $detected_tag"
    TAG="$detected_tag"
fi

echo ">>> Simulating BlueStore label on $DEVICE"
echo ">>> Ceph image tag: $TAG"
echo ">>> Verify disk empty: $VERIFY_DISK_EMPTY"

# Stage 1 — Optional disk emptiness check
if [[ "$VERIFY_DISK_EMPTY" == "true" ]]; then
    echo ">>> Precheck: Verifying device is empty (first 22 bytes)..."
    if dd if="$DEVICE" bs=1 count=22 status=none | tr -d '\000' | grep -q .; then
        echo "ERROR: $DEVICE contains non-zero data at LBA0 — refusing to overwrite."
        echo ">>> Please wipe or use a clean test disk."
        exit 1
    fi
fi

# Stage 2 — Device validation
[ -b "$DEVICE" ] || { echo "ERROR: $DEVICE is not a block device"; exit 1; }
[ "$(lsblk -no TYPE "$DEVICE")" = "disk" ] || { echo "ERROR: $DEVICE is not a whole disk"; exit 1; }
[ "$(blockdev --getro "$DEVICE")" -eq 0 ] || { echo "ERROR: $DEVICE is read-only"; exit 1; }

# Stage 3 — Disk cleanup
echo ">>> Cleaning disk signatures and partition table..."
sgdisk -Z "$DEVICE" || true
wipefs -a "$DEVICE" || true
blockdev --rereadpt "$DEVICE" || true
udevadm settle || true


## Stage 4 — Simulate BlueStore label using ceph-volume
echo ">>> Simulating BlueStore label using ceph-volume..."
podman run --rm --privileged --net=host \
  -v /dev:/dev \
  -v /sys:/sys \
  -v /run/udev:/run/udev:ro \
  -v /etc/ceph:/etc/ceph:ro \
  -v /var/lib/ceph/bootstrap-osd:/var/lib/ceph/bootstrap-osd:ro \
  quay.io/ceph/ceph:$TAG \
  ceph-volume --log-path /tmp/ceph-log \
    raw prepare --bluestore --data "$DEVICE" --crush-device-class ssd

## Stage 5 — Verification
echo ">>> Verifying with ceph-volume..."
podman run --rm --privileged --net=host \
  -v /dev:/dev \
  -v /sys:/sys \
  -v /run/udev:/run/udev:ro \
  -v /etc/ceph:/etc/ceph:ro \
  -v /var/lib/ceph:/var/lib/ceph:ro \
  quay.io/ceph/ceph:$TAG \
  ceph-volume raw list "$DEVICE" --format json

echo ">>> BlueStore simulation complete."
