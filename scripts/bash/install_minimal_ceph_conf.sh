#!/bin/bash

 if [ "$#" -ne 3 ]; then
   echo "Usage: $0 <fsid> <bootstrap-osd-key> <mon_host>"
   exit 1
 fi

 fsid="$1"
 key="$2"
 host_node_ip="$3"

 if [ -z "$fsid" ] || [ -z "$key" ] || [ -z "$host_node_ip" ]; then
   echo "None of the arguments can be empty."
   exit 1
 fi

# Create ceph.conf
mkdir -p /etc/ceph
cat <<EOF > /etc/ceph/ceph.conf
[global]
fsid = $fsid
mon_host = $host_node_ip
EOF

# Create bootstrap keyring
mkdir -p /var/lib/ceph/bootstrap-osd
cat <<EOF > /var/lib/ceph/bootstrap-osd/ceph.keyring
[client.bootstrap-osd]
    key = $key
    caps mon = "allow profile bootstrap-osd"
EOF

# Set permissions
chown root:root /var/lib/ceph/bootstrap-osd/ceph.keyring
chmod 644 /var/lib/ceph/bootstrap-osd/ceph.keyring

echo "Minimal Ceph configuration and bootstrap keyring created successfully."
