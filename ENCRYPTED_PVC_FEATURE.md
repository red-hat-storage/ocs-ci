# Encrypted PVC Support for Krkn Chaos and Resiliency Testing

## Overview

This feature adds support for using encrypted PVCs (Persistent Volume Claims) in Krkn chaos testing and resiliency testing workloads. When enabled, VDBENCH workloads will use encrypted storage classes for RBD (Ceph Block Pool) volumes.

## Features

- Configurable encrypted PVC option in both Krkn and Resiliency test configurations
- Automatic creation of encrypted storage classes when encryption is enabled
- Support for RBD (Ceph Block Pool) encryption
- Backward compatible - defaults to non-encrypted PVCs if not configured
- Graceful fallback to default storage classes if encrypted storage class creation fails

## Configuration

### Krkn Chaos Testing

To enable encrypted PVCs in Krkn chaos testing, update your configuration file (`conf/ocsci/krkn_chaos_config.yaml`):

```yaml
ENV_DATA:
  krkn_config:
    workloads:
      - VDBENCH

    vdbench_config:
      num_pvcs_per_interface: 5
      pvc_size: 50

      # Enable encrypted PVCs (requires KMS to be configured)
      use_encrypted_pvc: true  # Set to true to use encrypted PVCs
```

### Resiliency Testing

To enable encrypted PVCs in resiliency testing, update your configuration file (`conf/ocsci/resiliency_tests_config.yaml`):

```yaml
ENV_DATA:
  resiliency_config:
    workloads:
      - VDBENCH

    vdbench_config:
      num_pvcs_per_interface: 5
      pvc_size: 50

      # Enable encrypted PVCs (requires KMS to be configured)
      use_encrypted_pvc: true  # Set to true to use encrypted PVCs
```

## Prerequisites

**Important:** Encrypted PVCs require a Key Management System (KMS) to be configured in your OCS cluster. Without KMS, encrypted storage class creation will fail, and the system will fall back to default (non-encrypted) storage classes.

Ensure your cluster has:
1. KMS configured (e.g., Vault, HPCS, etc.)
2. Encryption enabled in the StorageCluster
3. Valid KMS connection details in the `csi-kms-connection-details` ConfigMap

## How It Works

1. **Configuration Loading**: The system reads the `use_encrypted_pvc` option from the configuration file
2. **Storage Class Creation**: When enabled and KMS is available:
   - Creates an encrypted RBD storage class with `encrypted: true` and appropriate `encryptionKMSID`
   - The storage class is created using the existing `storageclass_factory` fixture
3. **PVC Creation**: The encrypted storage class is passed to `multi_pvc_factory` when creating PVCs
4. **Fallback**: If encrypted storage class creation fails, the system logs a warning and uses default storage classes

## Implementation Details

### Modified Files

1. **Configuration Files**:
   - `conf/ocsci/krkn_chaos_config.yaml` - Added `use_encrypted_pvc` option
   - `conf/ocsci/resiliency_tests_config.yaml` - Added `use_encrypted_pvc` option

2. **Configuration Classes**:
   - `ocs_ci/krkn_chaos/krkn_workload_config.py` - Added `use_encrypted_pvc()` method
   - `ocs_ci/resiliency/resiliency_workload_config.py` - Added `use_encrypted_pvc()` method

3. **Workload Factories**:
   - `ocs_ci/krkn_chaos/krkn_workload_factory.py` - Updated to create and use encrypted storage classes
   - `ocs_ci/resiliency/resiliency_workload_factory.py` - Updated to create and use encrypted storage classes

4. **Test Fixtures**:
   - `tests/cross_functional/krkn_chaos/conftest.py` - Added `storageclass_factory` parameter
   - `tests/cross_functional/resilience/conftest.py` - Added `storageclass_factory` parameter

### Code Flow

```
Test Configuration (YAML)
    ↓
Config Class (use_encrypted_pvc())
    ↓
Workload Factory (create_workload_ops)
    ↓
VDBENCH Workload Creation (_create_vdbench_workloads_for_project)
    ↓
Encrypted Storage Class Creation (if use_encrypted_pvc=true)
    ↓
PVC Creation with Encrypted SC (multi_pvc_factory)
    ↓
VDBENCH Workload with Encrypted PVCs
```

## Storage Types

### RBD (Ceph Block Pool)
✅ **SUPPORTED** - Encrypted storage classes are created with:
- `encrypted: "true"`
- `encryptionKMSID: <kms_id>` (automatically retrieved from KMS configuration)
- **Per-PVC encryption** via storage class parameters
- Each RBD PVC gets its own encryption key from KMS

### CephFS (Ceph Filesystem)
❌ **NOT SUPPORTED (Per-PVC)** - CephFS does **NOT** support per-PVC encryption via storage class parameters.
- **Cluster-wide encryption only** - Must be enabled at the StorageCluster CR level
- All CephFS volumes share the same encryption configuration
- The `use_encrypted_pvc` option only affects RBD PVCs
- CephFS PVCs will always use the default (non-encrypted) storage class

**Important**: When `use_encrypted_pvc: true` is set:
- ✅ RBD PVCs → Use encrypted storage class with KMS
- ❌ CephFS PVCs → Use default storage class (no change)

## Usage Examples

### Example 1: Krkn Chaos Test with Encrypted PVCs

```bash
# Run Krkn chaos tests with encrypted PVCs
pytest tests/cross_functional/krkn_chaos/test_krkn_node_scenarios.py \
    --ocsci-conf conf/ocsci/krkn_chaos_config.yaml \
    -v -s
```

### Example 2: Resiliency Test with Encrypted PVCs

```bash
# Run resiliency tests with encrypted PVCs
run-ci \
    --ocsci-conf conf/ocsci/resiliency_tests_config.yaml \
    tests/cross_functional/resilience/test_storage_component_failures.py
```

## Logging

The implementation includes comprehensive logging:

```
INFO - Encrypted PVCs are enabled - will create encrypted storage classes
INFO - Creating encrypted storage classes for VDBENCH workloads
INFO - ✓ Created encrypted RBD storage class: test-rbd-storageclass-xyz
INFO - CephFS encryption uses cluster-wide encryption if configured
INFO - Using encrypted storage class: test-rbd-storageclass-xyz
```

If encryption is not available:
```
ERROR - Failed to create encrypted storage classes: <error details>
WARNING - Falling back to default storage classes
```

## Backward Compatibility

- The feature is **disabled by default** (`use_encrypted_pvc: false`)
- Existing tests and configurations continue to work without changes
- No impact on tests that don't need encryption

## Testing

To verify the feature:

1. **Check PVC StorageClass**:
```bash
oc get pvc -n <namespace> -o yaml | grep storageClassName
```

2. **Verify StorageClass Encryption**:
```bash
oc get storageclass <sc-name> -o yaml | grep encrypted
```

3. **Check PV Encryption**:
```bash
oc get pv <pv-name> -o yaml | grep -A 5 volumeAttributes
```

## Limitations

1. **KMS Required**: Encrypted PVCs require KMS configuration
2. **RBD Only**: **ONLY RBD (Ceph Block Pool) supports per-PVC encryption**
   - CephFS does **NOT** support per-PVC encryption via storage class
   - CephFS can only use cluster-wide encryption (configured at StorageCluster level)
   - The feature will create encrypted RBD storage classes but NOT for CephFS
3. **Performance**: Encrypted volumes may have slightly different performance characteristics
4. **No Mixed Encryption**: Cannot have some CephFS PVCs encrypted and others not (cluster-wide only)

## Troubleshooting

### Encrypted Storage Class Creation Fails

**Symptom**: Logs show "Failed to create encrypted storage classes"

**Solutions**:
1. Verify KMS is configured: `oc get configmap csi-kms-connection-details -n openshift-storage`
2. Check StorageCluster encryption: `oc get storagecluster -o yaml | grep -A 10 encryption`
3. Review KMS connectivity and credentials

### PVCs Remain in Pending State

**Symptom**: PVCs with encrypted storage class don't bind

**Solutions**:
1. Check CSI driver logs: `oc logs -n openshift-storage <csi-provisioner-pod>`
2. Verify encryption KMS ID is valid
3. Ensure KMS service is accessible from the cluster

### Falls Back to Default Storage Class

**Symptom**: "Falling back to default storage classes" warning appears

**Solutions**:
1. This is expected behavior when KMS is not configured
2. Check the error message for specific failure reason
3. Set `use_encrypted_pvc: false` if encryption is not needed

## Future Enhancements

Potential improvements for future versions:
- Support for different encryption KMS IDs per storage class
- CephFS per-PVC encryption (when supported by CSI driver)
- Encryption performance metrics and monitoring
- Additional workload types (CNV, FIO, etc.)

## References

- [OCS Encryption Documentation](https://access.redhat.com/documentation/en-us/red_hat_openshift_data_foundation/)
- [Ceph RBD Encryption](https://docs.ceph.com/en/latest/rbd/rbd-encryption/)
- [OCS KMS Integration](https://access.redhat.com/documentation/en-us/red_hat_openshift_data_foundation/4.14/html/managing_and_allocating_storage_resources/encrypting-persistent-volume-claims)
