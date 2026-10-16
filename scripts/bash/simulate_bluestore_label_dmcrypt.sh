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

DEVICE_BASENAME=$(basename "$DEVICE")
MAPPER_NAME="ceph-sim-${DEVICE_BASENAME}"
KEY_FILE=$(mktemp /tmp/ceph-sim-key.XXXXXX)
# Ensure the key file and open mapper are always cleaned up on exit
trap 'rm -f "$KEY_FILE"; cryptsetup luksClose "$MAPPER_NAME" 2>/dev/null || true' EXIT

echo ">>> Simulating encrypted BlueStore OSD on $DEVICE"
echo ">>> Ceph image tag: $TAG"
echo ">>> LUKS mapper name: /dev/mapper/$MAPPER_NAME"
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

# Stage 4 — Get Ceph FSID (from ceph.conf if present, else from cephadm shell)
echo ">>> Getting Ceph FSID..."
CEPH_FSID=""
if [[ -f /etc/ceph/ceph.conf ]]; then
    CEPH_FSID=$(awk '/^fsid[[:space:]]*=/{print $3}' /etc/ceph/ceph.conf | tr -d '[:space:]')
    [[ -n "$CEPH_FSID" ]] && echo ">>> Read FSID from /etc/ceph/ceph.conf"
fi
if [[ -z "$CEPH_FSID" ]]; then
    CEPH_FSID=$(cephadm shell -- ceph fsid 2>/dev/null | tr -d '[:space:]')
    [[ -n "$CEPH_FSID" ]] && echo ">>> Got FSID via cephadm shell"
fi
if [[ -z "$CEPH_FSID" ]]; then
    echo "ERROR: Failed to get Ceph FSID"
    exit 1
fi
echo ">>> Ceph FSID: $CEPH_FSID"

# Stage 5 — Generate random LUKS key and create LUKS container
echo ">>> Generating random LUKS encryption key..."
dd if=/dev/urandom bs=64 count=1 status=none > "$KEY_FILE"

echo ">>> Creating LUKS container on $DEVICE..."
cryptsetup --batch-mode --key-size 512 --key-file "$KEY_FILE" luksFormat "$DEVICE"

# Stage 6 — Open the LUKS container
echo ">>> Opening LUKS container as /dev/mapper/$MAPPER_NAME..."
cryptsetup --key-file "$KEY_FILE" luksOpen "$DEVICE" "$MAPPER_NAME"

# Stage 7 — Write BlueStore metadata inside the LUKS container
echo ">>> Writing BlueStore metadata inside LUKS container via ceph-volume..."
podman run --rm --privileged --net=host \
  -v /dev:/dev \
  -v /sys:/sys \
  -v /run/udev:/run/udev:ro \
  -v /etc/ceph:/etc/ceph:ro \
  -v /var/lib/ceph/bootstrap-osd:/var/lib/ceph/bootstrap-osd:ro \
  quay.io/ceph/ceph:"$TAG" \
  ceph-volume --log-path /tmp/ceph-log \
    raw prepare --bluestore --data "/dev/mapper/$MAPPER_NAME" --crush-device-class ssd

# Stage 8 — Verify BlueStore metadata inside the container
echo ">>> Verifying BlueStore metadata inside LUKS container..."
podman run --rm --privileged --net=host \
  -v /dev:/dev \
  -v /sys:/sys \
  -v /run/udev:/run/udev:ro \
  -v /etc/ceph:/etc/ceph:ro \
  -v /var/lib/ceph:/var/lib/ceph:ro \
  quay.io/ceph/ceph:"$TAG" \
  ceph-volume raw list "/dev/mapper/$MAPPER_NAME" --format json

# Stage 9 — Set LUKS header metadata to match a real Rook encrypted OSD
# (subsystem carries ceph_fsid; label carries pvc_name — both readable without key)
echo ">>> Setting LUKS header metadata to simulate Rook encrypted OSD..."
PVC_LABEL="pvc_name=sim-ocs-deviceset-localblock-0-data-${DEVICE_BASENAME}"
cryptsetup config \
  --subsystem "ceph_fsid=${CEPH_FSID}" \
  --label "$PVC_LABEL" \
  "$DEVICE"
echo ">>> LUKS subsystem set to: ceph_fsid=${CEPH_FSID}"
echo ">>> LUKS label set to: ${PVC_LABEL}"

# Stage 10 — Close the LUKS container (key is discarded by the EXIT trap)
echo ">>> Closing LUKS container..."
cryptsetup luksClose "$MAPPER_NAME"
# Remove the mapper from the trap now that it's already closed
trap 'rm -f "$KEY_FILE"' EXIT

# Stage 11 — Verify LUKS header metadata is intact (readable without key)
echo ">>> Verifying LUKS header metadata..."
cryptsetup luksDump "$DEVICE" | grep -E "Label|Subsystem|UUID"

echo ">>> Encrypted BlueStore simulation complete."
