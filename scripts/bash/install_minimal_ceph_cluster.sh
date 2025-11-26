#!/bin/bash
set -euo pipefail

# Download cephadm
curl -fsSL "https://download.ceph.com/rpm-tentacle/el9/noarch/cephadm" -o /tmp/cephadm
# Make it executable and move to PATH
chmod +x /tmp/cephadm
mv /tmp/cephadm /usr/local/bin/

# Verify cephadm version
cephadm version
# Check that cephadm version starts with 'cephadm version 20.'
if ! cephadm version | grep -q '^cephadm version 20\.'; then
  echo "Error: cephadm version is not 20.x"
  exit 1
fi

# Get the node IP address
NODE_IP=$(ip route get 1.1.1.1 | awk '{print $7}')
# Bootstrap the Ceph cluster (skip monitoring stack)
cephadm bootstrap --mon-ip ${NODE_IP} --skip-monitoring-stack

if cephadm shell -- ceph health | grep -qE 'HEALTH_OK|HEALTH_WARN'; then
    echo "Cluster is operational (OK or WARN)."
else
    echo "Cluster is in a critical state (HEALTH_ERR)."
    exit 1
fi

# Create bootstrap keyring directory
mkdir -p /var/lib/ceph/bootstrap-osd

# Extract bootstrap keyring
cephadm shell -- ceph auth get client.bootstrap-osd -o /var/lib/ceph/bootstrap-osd/ceph.keyring

# Get the FSID
FSID=$(cephadm shell -- ceph fsid)
# Get the ceph keyring
KEYRING=$(cephadm shell -- ceph auth get client.admin)

echo "Bootstrap the Ceph cluster (skip monitoring stack) completed successfully."
