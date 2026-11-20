# NFS External Client Testing Setup

This document describes how to set up an external NFS client VM for testing NFS exports from ODF/OCS clusters.

## Overview

The NFS test framework (`ocs_ci.utility.nfs_base.NFSClientTestBase`) uses an external VM to validate that NFS exports can be mounted and accessed from outside the OpenShift cluster. This VM:

- **Purpose**: Mount and test NFS exports from ODF/OCS clusters
- **Platform**: OpenShift Virtualization (CNV/KubeVirt)
- **User**: root (current implementation is designed for root user)
- **Access**: SSH with key-based authentication
- **Auto-recovery**: Automatic VM restart via Kubernetes API when SSH becomes unavailable

## Configuration

### Required Configuration Parameters

Add these to your test configuration file (e.g., `conf/ocsci/nfs-testing.yaml`):

```yaml
ENV_DATA:
  # SSH connection to NFS client VM
  nfs_client_ip: "10.x.x.x"              # VM IP address (node IP, LoadBalancer, or pod network)
  nfs_client_user: "root"                # Current implementation requires root user
  nfs_client_private_key: "~/.ssh/openshift-dev.pem"  # SSH private key path

  # VM identification for restart (non-auth config)
  nfs_client_vm_namespace: "nfs-testing"   # Namespace where VM runs
  nfs_client_vm_name: "nfs-client-vm"      # VirtualMachine resource name

  # Optional: NooBaa DB NFS mount testing (separate from external client)
  nb_nfs_server: "nfs-server.example.com"
  nb_nfs_mount: "/export/path"

AUTH:
  # NFS client VM restart credentials (OpenShift Virtualization)
  nfs:
    api_server: "https://api.cluster.example.com:6443"  # OpenShift API server
    token: "eyJhbGciOiJSUzI1..."  # ServiceAccount token (see below) # gitleaks:allow
```

### Configuration Notes

- **nfs_client_ip**: Must be reachable from the test runner. Can be:
  - Node IP if using NodePort service for SSH
  - LoadBalancer IP if VM has LoadBalancer service
  - Pod network IP (requires VPN or network access to pod network)

- **nfs_client_user**: Current implementation uses `root` user. This simplifies permissions for mount/umount operations which require root privileges.

- **AUTH.nfs.api_server**: OpenShift cluster API server URL where the NFS client VM runs

- **AUTH.nfs.token**: Long-lived ServiceAccount token with permissions to restart the VM. See "ServiceAccount Setup" below.

## VM Requirements

### Operating System
- RHEL 8/9, CentOS Stream 8/9/10, or Fedora
- Any Linux distribution with NFS client tools

### Required Packages
```bash
# RHEL/CentOS/Fedora
dnf install -y nfs-utils

# Verify installation
systemctl enable nfs-client.target
systemctl start nfs-client.target
```

### SSH Access
- SSH key-based authentication configured
- Public key added to `/root/.ssh/authorized_keys`
- SSH service enabled and running

### Network Access
- Can reach NFS LoadBalancer services on test ODF/OCS clusters
- Firewall allows NFS traffic (TCP port 2049)
- DNS resolution for NFS hostnames (or /etc/hosts will be updated automatically)

## ServiceAccount Setup for VM Restart

When SSH to the VM fails, the test framework can automatically restart the VM via the Kubernetes API. This requires a ServiceAccount with restricted permissions.

### Step 1: Create RBAC Resources

Create a file `vm-restarter-rbac.yaml` with the following content, replacing placeholders:

- `<SERVICE_ACCOUNT_NAME>` → e.g., `nfs-vm-restarter`
- `<NAMESPACE_PLACEHOLDER>` → e.g., `nfs-testing`
- `<VM_NAME>` → e.g., `nfs-client-vm`

```yaml
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: <SERVICE_ACCOUNT_NAME>
  namespace: <NAMESPACE_PLACEHOLDER>
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: <SERVICE_ACCOUNT_NAME>-role
  namespace: <NAMESPACE_PLACEHOLDER>
rules:
# Allow getting VM status
- apiGroups: ["kubevirt.io"]
  resources: ["virtualmachines"]
  resourceNames: ["<VM_NAME>"]  # ONLY this specific VM
  verbs: ["get"]
# Allow starting the VM if stopped
- apiGroups: ["subresources.kubevirt.io"]
  resources: ["virtualmachines/start"]
  resourceNames: ["<VM_NAME>"]
  verbs: ["update"]
# Allow stopping the VM if running
- apiGroups: ["subresources.kubevirt.io"]
  resources: ["virtualmachines/stop"]
  resourceNames: ["<VM_NAME>"]
  verbs: ["update"]
# Allow restarting the VM if running
- apiGroups: ["subresources.kubevirt.io"]
  resources: ["virtualmachines/restart"]
  resourceNames: ["<VM_NAME>"]
  verbs: ["update"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: <SERVICE_ACCOUNT_NAME>-binding
  namespace: <NAMESPACE_PLACEHOLDER>
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: <SERVICE_ACCOUNT_NAME>-role
subjects:
- kind: ServiceAccount
  name: <SERVICE_ACCOUNT_NAME>
  namespace: <NAMESPACE_PLACEHOLDER>
---
# Create a long-lived token secret for the service account
apiVersion: v1
kind: Secret
metadata:
  name: <SERVICE_ACCOUNT_NAME>-token
  namespace: <NAMESPACE_PLACEHOLDER>
  annotations:
    kubernetes.io/service-account.name: <SERVICE_ACCOUNT_NAME>
type: kubernetes.io/service-account-token
```

### Step 2: Deploy the Resources

Log in to the OpenShift cluster where the NFS client VM runs:

```bash
oc login https://api.cluster.example.com:6443

# Apply the RBAC configuration
oc apply -f vm-restarter-rbac.yaml
```

### Step 3: Extract the Token

```bash
# Set your values
SERVICE_ACCOUNT_NAME="nfs-vm-restarter"
NAMESPACE="nfs-testing"

# Extract token and API server
TOKEN=$(oc get secret ${SERVICE_ACCOUNT_NAME}-token -n ${NAMESPACE} -o jsonpath='{.data.token}' | base64 -d)
API_SERVER=$(oc whoami --show-server)

echo "Token: ${TOKEN}"
echo "API Server: ${API_SERVER}"
```

### Step 4: Update Configuration

Add the extracted values to your test configuration:

```yaml
AUTH:
  nfs:
    api_server: "https://api.cluster.example.com:6443"  # From: oc whoami --show-server
    token: "eyJhbGciOiJSUzI1..."  # From: oc get secret ... (full token) # gitleaks:allow
```

**Security Note**: Store the token securely (e.g., in Jenkins credentials vault, environment variables). Do not commit tokens to git repositories.

### Step 5: Verify Permissions

Test that the ServiceAccount can restart the VM:

```bash
# Using curl (Kubernetes REST API)
curl -k -X PUT \
  -H "Authorization: Bearer ${TOKEN}" \
  "${API_SERVER}/apis/subresources.kubevirt.io/v1/namespaces/${NAMESPACE}/virtualmachines/<VM_NAME>/restart"

# Expected: HTTP 200 OK with VM resource JSON
```

Verify the ServiceAccount cannot access other resources:

```bash
# Should fail with 403 Forbidden
curl -k \
  -H "Authorization: Bearer ${TOKEN}" \
  "${API_SERVER}/api/v1/namespaces/${NAMESPACE}/pods"
```

## How Auto-Restart Works

When running NFS tests:

1. **Test attempts SSH connection** to `nfs_client_ip`
2. **If SSH fails** (timeout or connection refused):
   - Code calls `_restart_vm_openshift_virt()` method
   - Makes HTTPS PUT request to Kubernetes API: `/apis/subresources.kubevirt.io/v1/namespaces/{namespace}/virtualmachines/{name}/restart`
   - Uses Bearer token authentication
3. **VM restarts** via KubeVirt controller
4. **Code waits** for SSH to become available (300s timeout)
5. **Reconnects** and continues test

This is implemented in:
- `ocs_ci/utility/nfs_base.py` - `NFSClientTestBase._restart_vm_openshift_virt()`
- `tests/functional/nfs_feature/test_nfs_feature_enable_for_ODF_clusters.py` - `TestNfsEnable._restart_vm_openshift_virt()`

## Test Classes

### Using NFSClientTestBase

For new tests, inherit from `NFSClientTestBase`:

```python
from ocs_ci.utility.nfs_base import NFSClientTestBase

class TestMyNFSFeature(NFSClientTestBase):
    """Test NFS feature with out-of-cluster client"""

    def test_nfs_export(self, pod_factory):
        # self.con - SSH connection to NFS client VM (automatically managed)
        # self.nfs_client_ip - VM IP address
        # self.test_folder - Mount point path

        # Create PVC with NFS storage class
        pvc = create_pvc(sc_name=constants.NFS_STORAGECLASS_NAME)

        # Get NFS export details
        share = self.get_nfs_export_details(pvc)

        # Mount on external client
        self.mount_nfs_export(self.con, share, self.test_folder)

        # Test file operations
        self.con.exec_cmd(f"echo 'test' > {self.test_folder}/file.txt")

        # Verify from pod
        pod = pod_factory(pvc=pvc)
        result = pod.exec_cmd_on_pod("cat /mnt/file.txt")
        assert result.strip() == "test"
```

The `NFSClientTestBase` class provides:
- Automatic SSH connection management
- VM restart on connection failure
- NFS mount/unmount helpers
- Data integrity verification methods

## Troubleshooting

### SSH Connection Fails

```bash
# Check VM is running
oc get vm nfs-client-vm -n nfs-testing
oc get vmi nfs-client-vm -n nfs-testing

# Access VM console
virtctl console nfs-client-vm -n nfs-testing

# Check SSH service
systemctl status sshd

# Check firewall
firewall-cmd --list-all
```

### VM Restart Not Working

```bash
# Verify token is valid
curl -k \
  -H "Authorization: Bearer ${TOKEN}" \
  "${API_SERVER}/apis/subresources.kubevirt.io/v1/namespaces/${NAMESPACE}/virtualmachines/<VM_NAME>"

# Check RBAC permissions
oc auth can-i update virtualmachines/restart \
  --as=system:serviceaccount:${NAMESPACE}:${SERVICE_ACCOUNT_NAME} \
  --resource-name=<VM_NAME> \
  -n ${NAMESPACE}
# Should return: yes
```

### NFS Mount Fails

```bash
# SSH to VM and test manually
ssh root@<vm-ip>

# Check NFS client tools
rpm -qa | grep nfs-utils
systemctl status nfs-client.target

# Test network connectivity to NFS server
ping <nfs-loadbalancer-ip>
nc -zv <nfs-loadbalancer-ip> 2049

# Test mount manually
mkdir -p /tmp/test-mount
mount -t nfs4 -o proto=tcp <nfs-server>:<share> /tmp/test-mount
ls -la /tmp/test-mount
umount /tmp/test-mount
```

## Security Considerations

### ServiceAccount Permissions

The ServiceAccount has **minimal permissions**:
- ✅ Can restart/start/stop **only** the specified VM
- ❌ Cannot access other VMs in the same namespace
- ❌ Cannot access other namespaces
- ❌ Cannot read secrets, configmaps, or other resources

### Token Storage

- Store tokens in secure credential vaults (Jenkins credentials, Vault, etc.)
- Do not commit tokens to git repositories
- Rotate tokens if compromised (delete and recreate the Secret)
- Token does not expire (type `kubernetes.io/service-account-token`)

### VM Security

For test VMs:
- SSH key-only authentication (recommended)
- Restrict SSH access to test runner IPs (firewall rules)
- Keep OS and packages updated
- Use root user for simplicity (dedicated test VM, not production)

## References

- [OpenShift Virtualization Documentation](https://docs.openshift.com/container-platform/latest/virt/about-virt.html)
- [KubeVirt API Reference](https://kubevirt.io/api-reference/)
- [Kubernetes ServiceAccounts](https://kubernetes.io/docs/concepts/security/service-accounts/)
- [NFS Utils Package](https://linux.die.net/man/8/mount.nfs)

## Example VM Definition

For reference, here's a minimal VM definition using cloud-init:

```yaml
apiVersion: kubevirt.io/v1
kind: VirtualMachine
metadata:
  name: nfs-client-vm
  namespace: nfs-testing
spec:
  running: true
  template:
    spec:
      domain:
        devices:
          disks:
          - disk:
              bus: virtio
            name: containerdisk
          - disk:
              bus: virtio
            name: cloudinitdisk
        resources:
          requests:
            memory: 4Gi
            cpu: 2
      volumes:
      - name: containerdisk
        containerDisk:
          image: quay.io/centos/centos:stream9
      - name: cloudinitdisk
        cloudInitNoCloud:
          userData: |
            #cloud-config
            users:
              - name: root
                ssh_authorized_keys:
                  - ssh-rsa AAAAB3NzaC... <YOUR_PUBLIC_KEY>
            packages:
              - nfs-utils
            runcmd:
              - systemctl enable nfs-client.target
              - systemctl start nfs-client.target
```
