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
echo "Node IP address: ${NODE_IP}"

# Check if Bootstrap is already done
if cephadm shell -- ceph health &> /dev/null; then
    echo "Ceph cluster is already bootstrapped."
else
    echo "Ceph cluster is not bootstrapped yet. Proceeding with bootstrap."
    # Bootstrap the Ceph cluster (skip monitoring stack)
    cephadm bootstrap --mon-ip ${NODE_IP} --skip-monitoring-stack
fi

if cephadm shell -- ceph health | grep -qE 'HEALTH_OK|HEALTH_WARN'; then
    echo "Cluster is operational (OK or WARN)."
else
    echo "Cluster is in a critical state (HEALTH_ERR)."
    exit 1
fi

# Get the ceph keyring
KEY=$(cephadm shell -- ceph auth get-key client.bootstrap-osd)

# Create bootstrap keyring
mkdir -p /var/lib/ceph/bootstrap-osd
cat <<EOF > /var/lib/ceph/bootstrap-osd/ceph.keyring
[client.bootstrap-osd]
    key = $KEY
    caps mon = "allow profile bootstrap-osd"
EOF

# Set permissions
chown root:root /var/lib/ceph/bootstrap-osd/ceph.keyring
chmod 644 /var/lib/ceph/bootstrap-osd/ceph.keyring

echo "Bootstrap the Ceph cluster (skip monitoring stack) completed successfully."
