# CephFS Filesystem Monitoring for Stress Tests

## Overview

The CephFS Filesystem Monitoring feature provides continuous monitoring of filesystem operations during stress tests to detect genuine hangs and issues in CephFS/ODF. It implements a three-level monitoring strategy that runs concurrently with stress workloads to identify problems that could indicate bugs in the underlying storage system.

## Features

- **Three-Level Monitoring Strategy**:
  - Full Filesystem Monitoring - Comprehensive checks on entire mount
  - Sampled Directory Monitoring - Random sampling of subdirectories
  - Current Iteration Monitoring - Focus on actively written directories

- **Hang Detection**: Timeout-based detection with configurable thresholds
- **Automatic Alerting**: Creates marker files when hangs are detected
- **Integration**: Seamlessly integrates with existing CephFS stress test framework
- **Configurable**: All monitoring parameters can be tuned via environment variables
- **Non-Intrusive**: Runs in background without interfering with stress workload

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Stress Test Pod                          │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────────┐         ┌─────────────────────────┐ │
│  │  Main Workload   │         │  Monitoring Manager     │ │
│  │  Thread          │         │  Thread                 │ │
│  │                  │         │                         │ │
│  │  - Smallfile     │         │  ┌──────────────────┐  │ │
│  │    Operations    │         │  │ Full FS Monitor  │  │ │
│  │  - File Creation │         │  │ (every 30 min)   │  │ │
│  │  - Metadata Ops  │         │  └──────────────────┘  │ │
│  │                  │         │                         │ │
│  └──────────────────┘         │  ┌──────────────────┐  │ │
│                                │  │ Sampled Monitor  │  │ │
│                                │  │ (every 5 min)    │  │ │
│                                │  └──────────────────┘  │ │
│                                │                         │ │
│                                │  ┌──────────────────┐  │ │
│                                │  │ Current Monitor  │  │ │
│                                │  │ (every 3 min)    │  │ │
│                                │  └──────────────────┘  │ │
│                                │                         │ │
│                                │  ┌──────────────────┐  │ │
│                                │  │ Hang Detection   │  │ │
│                                │  │ Logic            │  │ │
│                                │  └──────────────────┘  │ │
│                                └─────────────────────────┘ │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐ │
│  │              /mnt (CephFS Mount)                     │ │
│  │  - /mnt/base/iter0/                                  │ │
│  │  - /mnt/base/iter1/                                  │ │
│  │  - /mnt/output/monitoring_logs/                      │ │
│  │  - /mnt/output/hang_markers/                         │ │
│  └──────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## Monitoring Levels

### 1. Full Filesystem Monitoring

**Purpose**: Detect global filesystem hangs affecting the entire mount

**Commands**:
- `ls -lRf /mnt` - Recursive directory listing
- `find /mnt -type f -exec stat -c '%n' {} +` - Find and stat all files

**Interval**: Every 30 minutes (configurable)
**Timeout**: 15 minutes (configurable)

**Detects**:
- MDS (Metadata Server) hangs
- Distributed lock deadlocks
- Cache coherency issues
- Global filesystem freezes

### 2. Sampled Directory Monitoring

**Purpose**: Detect localized issues in specific directory subtrees

**Commands**:
- `ls -lRf {sampled_dir}` - Listing on random sample
- `find {sampled_dir} -type f -exec stat -c '%n' {} +` - Stat on sample

**Interval**: Every 5 minutes (configurable)
**Timeout**: 10 minutes (configurable)
**Sample Size**: 10 directories (configurable)

**Detects**:
- Localized directory hangs
- Issues with specific inode ranges
- Problems with particular file patterns
- Early indicators of developing issues

### 3. Current Iteration Monitoring

**Purpose**: Detect write-path hangs during active file creation

**Commands**:
- `ls -lRf {current_iter_dir}` - Listing on active directory
- `find {current_iter_dir} -type f | head -1000` - Quick find check

**Interval**: Every 3 minutes (configurable)
**Timeout**: 10 minutes (configurable)

**Detects**:
- Write-path hangs during file creation
- Issues with concurrent operations
- Problems with specific workload patterns
- Real-time detection of developing hangs

## Configuration

### Environment Variables

All monitoring parameters can be configured via environment variables in the pod/job templates:

```yaml
env:
  # Enable/Disable Monitoring
  - name: ENABLE_FS_MONITORING
    value: "true"  # Set to "false" to disable all monitoring
  
  # Full Filesystem Monitoring
  - name: MONITOR_FULL_FS_ENABLED
    value: "true"
  - name: MONITOR_FULL_FS_INTERVAL
    value: "1800"  # 30 minutes in seconds
  - name: MONITOR_FULL_FS_TIMEOUT
    value: "900"   # 15 minutes in seconds
  
  # Sampled Directory Monitoring
  - name: MONITOR_SAMPLE_ENABLED
    value: "true"
  - name: MONITOR_SAMPLE_INTERVAL
    value: "300"   # 5 minutes in seconds
  - name: MONITOR_SAMPLE_TIMEOUT
    value: "600"   # 10 minutes in seconds
  - name: MONITOR_SAMPLE_SIZE
    value: "10"    # Number of directories to sample
  
  # Current Iteration Monitoring
  - name: MONITOR_CURRENT_ENABLED
    value: "true"
  - name: MONITOR_CURRENT_INTERVAL
    value: "180"   # 3 minutes in seconds
  - name: MONITOR_CURRENT_TIMEOUT
    value: "600"   # 10 minutes in seconds
  
  # Hang Detection
  - name: HANG_DETECTION_CONSECUTIVE_FAILURES
    value: "2"     # Number of consecutive failures before declaring hang
```

### Tuning Guidelines

**For Aggressive Bug Detection** (Recommended for initial testing):
```yaml
MONITOR_FULL_FS_INTERVAL: "1800"  # 30 min
MONITOR_SAMPLE_INTERVAL: "300"    # 5 min
MONITOR_CURRENT_INTERVAL: "180"   # 3 min
HANG_DETECTION_CONSECUTIVE_FAILURES: "2"
```

**For Reduced Overhead** (If monitoring impacts workload):
```yaml
MONITOR_FULL_FS_INTERVAL: "3600"  # 60 min
MONITOR_SAMPLE_INTERVAL: "600"    # 10 min
MONITOR_CURRENT_INTERVAL: "300"   # 5 min
HANG_DETECTION_CONSECUTIVE_FAILURES: "3"
```

**For Maximum Coverage** (Long-running tests):
```yaml
MONITOR_FULL_FS_INTERVAL: "900"   # 15 min
MONITOR_SAMPLE_INTERVAL: "180"    # 3 min
MONITOR_CURRENT_INTERVAL: "120"   # 2 min
MONITOR_SAMPLE_SIZE: "15"         # More samples
```

## Usage

### Automatic Integration

The monitoring is automatically enabled in CephFS stress test pods and jobs. No code changes are required in test cases.

### Example Test Case

```python
from ocs_ci.helpers.cephfs_stress_helpers import CephFSStressTestManager

def test_cephfs_stress_with_monitoring(project_factory):
    """
    CephFS stress test with automatic filesystem monitoring
    """
    proj_name = "cephfs-stress-test"
    project_factory(project_name=proj_name)
    
    stress_mgr = CephFSStressTestManager(namespace=proj_name)
    
    try:
        # Setup stress test environment
        pvc_obj, _ = stress_mgr.setup_stress_test_environment(pvc_size="500Gi")
        
        # Start background health checks (includes hang detection)
        stress_mgr.start_background_checks(interval_minutes=30)
        
        # Create stress job (monitoring starts automatically in pods)
        stress_job_obj = stress_mgr.create_cephfs_stress_job(
            pvc_name=pvc_obj.name,
            multiplication_factors="1,2,3",
            parallelism=4,
            completions=4,
            base_file_count=50000,
        )
        
        # Wait for job completion
        # Monitoring runs continuously in background
        # If hang detected, test will fail automatically
        
    finally:
        stress_mgr.teardown()
```

### Manual Hang Check

You can also manually check for hangs:

```python
from ocs_ci.helpers.cephfs_stress_helpers import check_for_filesystem_hangs

# Check for hang markers
hang_detected, hang_details = check_for_filesystem_hangs(
    namespace="cephfs-stress-test"
)

if hang_detected:
    print(f"Hangs detected: {len(hang_details)}")
    for hang in hang_details:
        print(f"  Pod: {hang['pod_name']}")
        print(f"  Monitor: {hang['monitor_type']}")
        print(f"  Command: {hang['command']}")
```

## Hang Detection Logic

### Detection Criteria

A hang is detected when:

1. **Timeout Exceeded**: Command runs longer than configured timeout
2. **Consecutive Failures**: Multiple consecutive monitoring attempts fail (default: 2)
3. **Process Unresponsive**: Command process becomes unresponsive

### Hang Marker Files

When a hang is detected, the monitoring script creates a marker file:

**Location**: `/mnt/output/hang_markers/HANG_DETECTED_{monitor_type}_{timestamp}.json`

**Content**:
```json
{
  "timestamp": "20260427_143052",
  "monitor_type": "FullFilesystem",
  "command": "ls -lRf /mnt",
  "details": "Command timed out after 905.23s. System state: {...}",
  "base_dir": "/mnt/base",
  "hostname": "cephfs-stress-job-abc123"
}
```

### Test Failure

When hang markers are detected:

1. **Pod Exit**: Stress test pod exits with error code 1
2. **Background Check**: Background verification detects hang markers
3. **Test Failure**: Test framework fails the test with detailed hang information
4. **Log Collection**: Monitoring logs are automatically collected

## Monitoring Logs

### Log Locations

**Inside Pod**:
- `/mnt/output/monitoring_logs/filesystem_monitor_{timestamp}.log`

**Collected Logs** (after test):
- `ocs-ci-logs-{run_id}/{test_name}/monitoring_logs/`

### Log Content

Monitoring logs include:

- Monitoring configuration
- Command execution details
- Success/failure status
- Execution duration
- Hang detection events
- System state captures

### Example Log Entry

```
2026-04-27 14:30:52 - INFO - FullFS-Monitor - [FullFilesystem] Starting full filesystem scan
2026-04-27 14:30:52 - INFO - FullFS-Monitor - [FullFilesystem] Running: ls -lRf /mnt
2026-04-27 14:35:23 - INFO - FullFS-Monitor - [FullFilesystem] ls -lRf completed successfully in 271.45s, output lines: 2547893
2026-04-27 14:35:23 - INFO - FullFS-Monitor - [FullFilesystem] Running: find /mnt -type f -exec stat
2026-04-27 14:40:15 - INFO - FullFS-Monitor - [FullFilesystem] find+stat completed successfully in 292.18s, files found: 1273946
```

## Integration with Test Framework

### Background Verification

The monitoring integrates with `CephFSStressTestManager` background checks:

```python
# In CephFSStressTestManager._run_strict_verifications()
verifications_to_run = [
    check_ceph_health,
    verify_openshift_storage_ns_pods_in_running_state,
    verify_no_filesystem_hangs,  # <-- Hang detection check
]
```

### Verification Pause/Resume

During intentional disruptions, verification checks (including hang detection) can be paused:

```python
# Pause during disruption
stress_mgr.pause_background_checks()

# Induce failure (e.g., node restart)
induce_failure()

# Resume after recovery
stress_mgr.resume_background_checks()
```

## Troubleshooting

### Monitoring Not Running

**Check**:
1. Verify `ENABLE_FS_MONITORING=true` in pod environment
2. Check pod logs for monitoring startup messages
3. Verify monitoring script exists in container image

**Debug**:
```bash
# Check if monitoring process is running
kubectl exec -it <pod-name> -- ps aux | grep cephfs_filesystem_monitor

# Check monitoring logs
kubectl exec -it <pod-name> -- ls -la /mnt/output/monitoring_logs/
```

### False Positive Hangs

If monitoring detects hangs that aren't genuine issues:

**Increase Timeouts**:
```yaml
MONITOR_FULL_FS_TIMEOUT: "1800"  # 30 minutes
MONITOR_SAMPLE_TIMEOUT: "900"    # 15 minutes
MONITOR_CURRENT_TIMEOUT: "900"   # 15 minutes
```

**Increase Consecutive Failures**:
```yaml
HANG_DETECTION_CONSECUTIVE_FAILURES: "3"  # Require 3 failures
```

### High Resource Usage

If monitoring impacts workload performance:

**Reduce Frequency**:
```yaml
MONITOR_FULL_FS_INTERVAL: "3600"  # Run less often
MONITOR_SAMPLE_INTERVAL: "600"
MONITOR_CURRENT_INTERVAL: "300"
```

**Reduce Sample Size**:
```yaml
MONITOR_SAMPLE_SIZE: "5"  # Sample fewer directories
```

**Disable Levels**:
```yaml
MONITOR_FULL_FS_ENABLED: "false"  # Disable full FS monitoring
```

## Best Practices

1. **Start Aggressive**: Begin with default settings for maximum bug detection
2. **Tune Based on Results**: Adjust intervals and timeouts based on observed behavior
3. **Monitor Logs**: Regularly review monitoring logs for patterns
4. **Collect Evidence**: Always collect monitoring logs when hangs are detected
5. **Correlate Events**: Cross-reference hang detection with cluster events
6. **Document Findings**: Record hang patterns and conditions for bug reports

## Container Image Requirements

The monitoring script must be included in the stress test container image:

**Dockerfile**:
```dockerfile
# Copy monitoring script
COPY cephfs_filesystem_monitor.py /script/cephfs_filesystem_monitor.py
RUN chmod +x /script/cephfs_filesystem_monitor.py
```

**Script Location**: `/script/cephfs_filesystem_monitor.py`

## Performance Impact

Expected resource usage with millions of files:

| Monitoring Level | Duration per Run | Frequency | Time Overhead |
|-----------------|------------------|-----------|---------------|
| Full FS         | 5-15 minutes     | 30 min    | ~30-50%       |
| Sampled         | 1-3 minutes      | 5 min     | ~20-60%       |
| Current Iter    | 30-90 seconds    | 3 min     | ~15-50%       |

**Note**: Overhead percentages indicate the portion of time a monitoring command is running. Since commands run sequentially and are staggered, typically only one monitoring command runs at any given time.

## Future Enhancements

Potential improvements for future versions:

- [ ] Adaptive timeout adjustment based on file count
- [ ] Machine learning-based hang prediction
- [ ] Real-time alerting via webhooks
- [ ] Prometheus metrics export
- [ ] Distributed monitoring across multiple pods
- [ ] Historical hang pattern analysis
- [ ] Integration with cluster event correlation

## Support

For issues or questions:

1. Check monitoring logs in `/mnt/output/monitoring_logs/`
2. Review hang marker files in `/mnt/output/hang_markers/`
3. Examine pod logs for monitoring startup/errors
4. Consult test framework logs for verification failures
5. Open an issue with collected logs and hang details

## References

- [CephFS Stress Test Helpers](../ocs_ci/helpers/cephfs_stress_helpers.py)
- [Monitoring Script](../ocs_ci/templates/workloads/cephfs_stress/cephfs_filesystem_monitor.py)
- [Job Template](../ocs_ci/templates/workloads/cephfs_stress/cephfs_stress_job.yaml)
- [Pod Template](../ocs_ci/templates/workloads/cephfs_stress/cephfs_stress_pod.yaml)