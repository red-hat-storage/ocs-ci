# RGW Workload for Krkn Chaos Testing

## Overview

The **RGWWorkload** class provides comprehensive S3 workload management for RADOS Gateway (RGW) testing in Krkn chaos scenarios. It enables stress testing of object storage operations during cluster disruptions, validating the resilience of RGW and Ceph object storage.

### Key Features
- **Continuous S3 Operations**: Upload, download, list, and delete objects
- **Lifecycle Management**: Complete workload control with start/stop/pause/resume
- **Chaos Integration**: Seamlessly integrates with Krkn chaos testing framework
- **Health Monitoring**: Real-time workload status and validation
- **Configurable Patterns**: Customizable operation types and intervals
- **Background Execution**: Non-blocking workload operations

---

## Architecture

### Components

```
┌─────────────────────────────────────────────────────┐
│           Krkn Chaos Testing Framework               │
│                                                       │
│  ┌──────────────────────────────────────────────┐  │
│  │      WorkloadOps (krkn_workload_factory)    │  │
│  │  - Manages multiple workload types          │  │
│  │  - Validates workload health                │  │
│  │  - Coordinates background operations         │  │
│  └──────────────────┬───────────────────────────┘  │
│                     │                                │
│  ┌──────────────────▼───────────────────────────┐  │
│  │       RGW Workload (rgw_workload.py)        │  │
│  │  ┌────────────────────────────────────────┐  │  │
│  │  │  • Upload objects to RGW buckets       │  │  │
│  │  │  • Download objects for verification   │  │  │
│  │  │  • List operations for metadata stress │  │  │
│  │  │  • Delete operations for lifecycle     │  │  │
│  │  └────────────────┬───────────────────────┘  │  │
│  └───────────────────┼───────────────────────────┘  │
└────────────────────┼──────────────────────────────┘
                      │
         ┌────────────▼─────────────┐
         │  MCG Stress Helper Utils │
         │  (mcg_stress_helper.py)   │
         └──────────────┬────────────┘
                        │
         ┌──────────────▼─────────────┐
         │     RGW Buckets (Ceph)     │
         │  • Object uploads           │
         │  • Object retrieval         │
         │  • Metadata operations      │
         └─────────────────────────────┘
```

---

## Usage

### 1. Basic Usage in Tests

```python
from ocs_ci.resiliency.resiliency_workload import RGWWorkload
from ocs_ci.ocs.resources.objectbucket import RGWOCBucket

# Create RGW bucket
rgw_bucket = RGWOCBucket("my-test-bucket", namespace="openshift-storage")

# Configure workload
workload_config = {
    "iteration_count": 10,
    "operation_types": ["upload", "download", "list", "delete"],
    "delay_between_iterations": 30,
}

# Create workload
workload = RGWWorkload(
    rgw_bucket=rgw_bucket,
    awscli_pod=awscli_pod,
    workload_config=workload_config,
)

# Start workload
workload.start_workload()

# ... perform chaos operations ...

# Stop and cleanup
workload.stop_workload()
workload.cleanup_workload()
```

### 2. Krkn Chaos Integration

Update `conf/ocsci/krkn_chaos_config.yaml`:

```yaml
ENV_DATA:
  krkn_config:
    workloads:
      - RGW_WORKLOAD

    rgw_config:
      num_buckets: 3
      iteration_count: 10
      operation_types:
        - upload
        - download
        - list
        - delete
      upload_multiplier: 1
      metadata_ops_enabled: false
      delay_between_iterations: 30
```

Then run Krkn chaos tests:

```bash
pytest tests/cross_functional/krkn_chaos/test_krkn_scenarios.py \
    --ocsci-conf conf/ocsci/krkn_chaos_config.yaml \
    -v -s
```

### 3. Multiple RGW Workloads

```yaml
ENV_DATA:
  krkn_config:
    workloads:
      - VDBENCH
      - RGW_WORKLOAD
      - CNV_WORKLOAD

    rgw_config:
      num_buckets: 5           # Create 5 RGW workloads
      iteration_count: 20       # Run 20 iterations
      operation_types:          # All S3 operations
        - upload
        - download
        - list
        - delete
```

---

## Configuration Parameters

### RGW Config Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `num_buckets` | int | 3 | Number of RGW buckets/workloads to create |
| `iteration_count` | int | 10 | Number of workload iterations (0 = infinite) |
| `operation_types` | list | All | S3 operations to perform: `upload`, `download`, `list`, `delete` |
| `upload_multiplier` | int | 1 | Multiplier for object uploads |
| `metadata_ops_enabled` | bool | false | Enable metadata-intensive operations |
| `delay_between_iterations` | int | 30 | Seconds to wait between iterations |

### Workload Operations

#### Upload
- Syncs test directory to S3 bucket
- Creates objects under iteration-specific prefix
- Uses `sync_object_directory` for efficiency

####  Download
- Downloads objects from previous iteration
- Verifies data integrity (implicit)
- Cleans up downloaded files automatically

#### List
- Lists objects under iteration prefix
- Stresses metadata operations
- Validates bucket accessibility

#### Delete
- Removes objects from 2 iterations ago
- Prevents interference with downloads
- Uses recursive delete for efficiency

---

## Workload Lifecycle

### States

```
┌─────────┐
│ Created │
└────┬────┘
     │
     │ start_workload()
     ▼
┌─────────┐     pause_workload()     ┌────────┐
│ Running ├────────────────────────►│ Paused │
└────┬────┘                          └───┬────┘
     │                                   │
     │                    resume_workload()
     │◄──────────────────────────────────┘
     │
     │ stop_workload()
     ▼
┌─────────┐     cleanup_workload()  ┌──────────┐
│ Stopped ├────────────────────────►│ Cleaned  │
└─────────┘                          └──────────┘
```

### Methods

#### `start_workload()`
- Prepares test objects in awscli pod
- Starts background workload thread
- Begins continuous S3 operations

#### `stop_workload()`
- Signals workload thread to stop
- Waits for graceful shutdown
- Updates workload state

#### `cleanup_workload()`
- Stops workload if running
- Cleans up test directories
- Preserves bucket (managed by factory)

#### `pause_workload()` / `resume_workload()`
- Temporarily suspend/resume operations
- Maintains workload state
- Useful for specific chaos scenarios

---

## Integration with MCG Stress Helper

The RGW workload leverages `mcg_stress_helper.py` utilities:

### Helper Functions Used

| Function | Purpose |
|----------|---------|
| `get_mcg_obj()` | Retrieve S3 credentials for RGW |
| `sync_object_directory_with_retry()` | Reliable upload/download with retry |
| `delete_objs_from_bucket()` | Efficient bulk deletion |
| `upload_objs_to_buckets()` | Concurrent uploads to multiple buckets |

### Example with Stress Helper

```python
from ocs_ci.helpers.mcg_stress_helper import upload_objs_to_buckets

# Upload to multiple RGW buckets concurrently
buckets = {
    constants.RGW_PLATFORM: rgw_bucket1,
    constants.RGW_PLATFORM: rgw_bucket2,
}

upload_objs_to_buckets(
    mcg_obj=None,  # Not needed for RGW
    pod_obj=awscli_pod,
    buckets=buckets,
    current_iteration=1,
    multiplier=1,
)
```

---

## Workload Validation

### Health Checks

The workload supports comprehensive health validation:

```python
# Get workload status
status = workload.get_workload_status()
# Returns:
# {
#     'bucket_name': 'rgw-bucket-xyz',
#     'is_running': True,
#     'is_paused': False,
#     'current_iteration': 5,
#     'total_iterations': 10,
#     'operations': ['upload', 'download', 'list', 'delete']
# }

# Check if running
is_running = workload.is_workload_running()
```

### Krkn Integration

During chaos testing, workload health is validated:

```python
def _validate_rgw_workload(self, workload):
    """Validate RGW workload health during chaos."""
    if not workload.is_running():
        log.warning("RGW workload is not running")

    status = workload.get_workload_status()
    if not status.get("is_running", False):
        log.warning("RGW workload reports as not running")
```

---

## Platform Requirements

### Supported Platforms
- ✅ **vSphere** (on-prem)
- ✅ **Baremetal** (on-prem)
- ✅ **RHV** (on-prem)
- ❌ **AWS** (RGW not available)
- ❌ **Azure** (RGW not available)
- ❌ **GCP** (RGW not available)

### Prerequisites
- RGW endpoint available
- AWSCLI pod with S3 capabilities
- RGW storage class configured
- Sufficient storage capacity

---

## Examples

### Minimal Configuration

```yaml
ENV_DATA:
  krkn_config:
    workloads: ["RGW_WORKLOAD"]
    rgw_config:
      num_buckets: 1
      iteration_count: 5
```

### Intensive Testing

```yaml
ENV_DATA:
  krkn_config:
    workloads: ["RGW_WORKLOAD"]
    rgw_config:
      num_buckets: 10
      iteration_count: 100
      operation_types: ["upload", "download", "list", "delete"]
      upload_multiplier: 5
      metadata_ops_enabled: true
      delay_between_iterations: 10
```

### Mixed Workload Testing

```yaml
ENV_DATA:
  krkn_config:
    workloads:
      - VDBENCH          # Block/File storage
      - RGW_WORKLOAD     # Object storage
      - CNV_WORKLOAD     # Virtual machines

    rgw_config:
      num_buckets: 3
      iteration_count: 20
```

---

## Troubleshooting

### Common Issues

#### 1. Workload Not Starting
```
Error: RGW workload is not running
```
**Solution**: Check RGW endpoint and bucket creation:
```bash
oc get cephobjectstores -n openshift-storage
oc get route -n openshift-storage | grep rgw
```

#### 2. Upload Failures
```
Error: Upload operation failed
```
**Solution**: Verify OBC credentials and bucket access:
```python
obc_obj = OBC(bucket_name)
print(f"Access Key: {obc_obj.access_key_id}")
print(f"Endpoint: {obc_obj.s3_endpoint}")
```

#### 3. Pod Not Found
```
Error: Failed to prepare test objects
```
**Solution**: Ensure awscli_pod is available:
```bash
oc get pods -n openshift-storage | grep awscli
```

---

## Best Practices

1. **Start Small**: Begin with 1-2 buckets and low iteration count
2. **Monitor Resources**: Watch Ceph cluster health during workload
3. **Adjust Delays**: Increase `delay_between_iterations` for stability
4. **Enable Background Ops**: Use with background cluster operations
5. **Cleanup After Tests**: Always call `cleanup_workload()`

---

## Future Enhancements

- [ ] Multipart upload support
- [ ] Versioning operations
- [ ] Lifecycle policy testing
- [ ] Cross-region replication validation
- [ ] Performance metrics collection
- [ ] Integration with bucket policies

---

## See Also

- [VDBENCH Workload Documentation](README.md)
- [Krkn Chaos Testing Guide](../krkn_chaos/README.md)
- [MCG Stress Helper Utils](../../helpers/mcg_stress_helper.py)
- [RGW Test Examples](../../tests/functional/object/rgw/)
