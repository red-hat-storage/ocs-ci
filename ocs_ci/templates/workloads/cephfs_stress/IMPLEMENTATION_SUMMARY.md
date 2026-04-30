# CephFS Filesystem Monitoring - Implementation Summary

## Architecture Overview

The implementation follows a **clean separation of concerns**:

1. **Container (cephfs-stress-pod)**: Runs monitoring script in background
2. **ocs-ci Framework**: Validates hang detection and fails tests

```
┌─────────────────────────────────────────────────────────────────┐
│                    Container Image                              │
│  (quay.io/ocsci/cephfs-stress-pod)                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  /script/                                                       │
│  ├── cephfs_stress_script.py      # Main stress workload       │
│  ├── cephfs_filesystem_monitor.py # Monitoring script (NEW)    │
│  └── smallfile_cli.py              # Smallfile tool             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                            │
                            │ Runs in Pod
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Kubernetes Pod                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Shell Script (in YAML):                                       │
│  1. Start monitoring in background                             │
│  2. Run stress test                                            │
│  3. Exit (monitoring creates markers if hangs detected)        │
│                                                                 │
│  /mnt/output/                                                   │
│  ├── monitoring_logs/          # Monitoring logs               │
│  └── hang_markers/             # Hang detection markers        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                            │
                            │ Validates
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                    ocs-ci Framework                             │
│  (CephFSStressTestManager)                                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Background Verification Checks (every 30 min):                │
│  1. check_ceph_health()                                        │
│  2. verify_openshift_storage_ns_pods_in_running_state()       │
│  3. verify_no_filesystem_hangs()  ← NEW                        │
│                                                                 │
│  On Hang Detection:                                            │
│  - Collect monitoring logs                                     │
│  - Fail test with detailed hang information                    │
│  - Report to test framework                                    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Implementation Components

### 1. Monitoring Script (In Container)

**File**: `cephfs_filesystem_monitor.py`
**Location**: `/script/cephfs_filesystem_monitor.py` in container
**Purpose**: Continuously monitor filesystem operations

**What it does**:
- Runs three levels of monitoring (Full FS, Sampled, Current Iteration)
- Detects hangs based on command timeouts
- Creates marker files when hangs detected
- Logs all monitoring activity

**What it does NOT do**:
- Does not fail the test (that's ocs-ci's job)
- Does not stop the stress workload
- Does not communicate with ocs-ci directly

### 2. Pod/Job Templates (Simple Shell Script)

**Files**: `cephfs_stress_job.yaml`, `cephfs_stress_pod.yaml`
**Purpose**: Start monitoring and run stress test

**Shell script does**:
```bash
# 1. Start monitoring in background
python3 /script/cephfs_filesystem_monitor.py &

# 2. Run stress test
python3 /script/cephfs_stress_script.py

# 3. Exit (ocs-ci will check for hangs)
```

**Shell script does NOT do**:
- Does not check for hang markers (ocs-ci does this)
- Does not fail the pod based on hangs
- Does not collect logs (ocs-ci does this)

### 3. ocs-ci Framework Integration

**File**: `ocs_ci/helpers/cephfs_stress_helpers.py`
**Purpose**: Validate hang detection and fail tests

**Functions added**:

#### `check_for_filesystem_hangs(namespace)`
- Checks all pods for hang marker files
- Reads and parses hang details
- Returns hang status and details

#### `verify_no_filesystem_hangs(namespace, stress_manager)`
- Verification function called by background checks
- Raises exception if hangs detected
- Respects pause/resume during disruptions

#### `collect_monitoring_logs(stress_job_obj)`
- Collects monitoring logs from pods
- Saves to ocs-ci log directory
- Called automatically on test completion

**Integration point**:
```python
# In CephFSStressTestManager._run_strict_verifications()
verifications_to_run = [
    check_ceph_health,
    verify_openshift_storage_ns_pods_in_running_state,
    verify_no_filesystem_hangs,  # ← Checks for hang markers
]
```

## Why This Approach is Best

### ✅ Separation of Concerns
- **Container**: Runs monitoring, creates markers
- **ocs-ci**: Validates markers, fails tests
- Each component does what it's best at

### ✅ Simple and Maintainable
- Shell script in YAML is minimal (just 6 lines)
- No complex logic in YAML
- All validation logic in Python (testable, debuggable)

### ✅ Flexible
- Easy to enable/disable monitoring
- Easy to tune parameters
- Easy to add new verification checks

### ✅ Robust
- Monitoring runs independently of stress test
- Hang detection doesn't interfere with workload
- Framework can pause/resume checks during disruptions

### ✅ Integrated
- Works with existing test framework
- Uses existing background check mechanism
- Follows existing patterns (like ceph_health_check)

## Data Flow

### Normal Operation (No Hangs)

```
1. Pod starts
   └─> Shell script starts monitoring in background
   └─> Shell script runs stress test
   └─> Stress test completes successfully
   └─> Pod exits with code 0

2. ocs-ci background checks (every 30 min)
   └─> verify_no_filesystem_hangs() checks for markers
   └─> No markers found
   └─> Verification passes

3. Test completes
   └─> collect_monitoring_logs() collects logs
   └─> Test passes
```

### Hang Detected

```
1. Pod starts
   └─> Shell script starts monitoring in background
   └─> Shell script runs stress test
   └─> Monitoring detects hang (command timeout)
       └─> Creates marker file: /mnt/output/hang_markers/HANG_DETECTED_*.json
       └─> Logs hang details
       └─> Continues monitoring
   └─> Stress test continues (unaware of hang)
   └─> Stress test completes
   └─> Pod exits with code 0 (doesn't know about hang)

2. ocs-ci background checks (every 30 min)
   └─> verify_no_filesystem_hangs() checks for markers
   └─> Finds hang marker files
   └─> Reads hang details
   └─> Raises exception with hang information
   └─> Background check thread signals stop_event
   └─> Main test thread detects verification failure

3. Test fails
   └─> collect_monitoring_logs() collects logs
   └─> Test framework reports failure with hang details
   └─> Logs include:
       - Monitoring logs (what commands hung)
       - Hang marker files (when and why)
       - System state at time of hang
```

## Configuration

All configuration via environment variables in pod/job YAML:

```yaml
env:
  # Enable/disable monitoring
  - name: ENABLE_FS_MONITORING
    value: "true"
  
  # Full filesystem monitoring
  - name: MONITOR_FULL_FS_ENABLED
    value: "true"
  - name: MONITOR_FULL_FS_INTERVAL
    value: "1800"  # 30 minutes
  - name: MONITOR_FULL_FS_TIMEOUT
    value: "900"   # 15 minutes
  
  # Sampled directory monitoring
  - name: MONITOR_SAMPLE_ENABLED
    value: "true"
  - name: MONITOR_SAMPLE_INTERVAL
    value: "300"   # 5 minutes
  - name: MONITOR_SAMPLE_TIMEOUT
    value: "600"   # 10 minutes
  - name: MONITOR_SAMPLE_SIZE
    value: "10"
  
  # Current iteration monitoring
  - name: MONITOR_CURRENT_ENABLED
    value: "true"
  - name: MONITOR_CURRENT_INTERVAL
    value: "180"   # 3 minutes
  - name: MONITOR_CURRENT_TIMEOUT
    value: "600"   # 10 minutes
  
  # Hang detection
  - name: HANG_DETECTION_CONSECUTIVE_FAILURES
    value: "2"
```

## Container Image Requirements

The monitoring script must be added to the container image:

```dockerfile
# In your Dockerfile
COPY cephfs_filesystem_monitor.py /script/cephfs_filesystem_monitor.py
RUN chmod +x /script/cephfs_filesystem_monitor.py
```

Then rebuild and push:
```bash
docker build -t quay.io/ocsci/cephfs-stress-pod:latest .
docker push quay.io/ocsci/cephfs-stress-pod:latest
```

## Usage in Tests

No changes needed to existing test code! The monitoring is automatic:

```python
def test_cephfs_stress_with_monitoring(project_factory):
    """
    CephFS stress test with automatic filesystem monitoring
    """
    proj_name = "cephfs-stress-test"
    project_factory(project_name=proj_name)
    
    stress_mgr = CephFSStressTestManager(namespace=proj_name)
    
    try:
        # Setup
        pvc_obj, _ = stress_mgr.setup_stress_test_environment(pvc_size="500Gi")
        
        # Start background checks (includes hang detection)
        stress_mgr.start_background_checks(interval_minutes=30)
        
        # Create stress job (monitoring starts automatically in pods)
        stress_job_obj = stress_mgr.create_cephfs_stress_job(
            pvc_name=pvc_obj.name,
            multiplication_factors="1,2,3",
            parallelism=4,
            completions=4,
            base_file_count=50000,
        )
        
        # Wait for completion
        # Monitoring runs in background
        # Test fails automatically if hangs detected
        
    finally:
        stress_mgr.teardown()
```

## Files Modified/Created

### Created:
1. `ocs_ci/templates/workloads/cephfs_stress/cephfs_filesystem_monitor.py` - Monitoring script
2. `docs/cephfs_filesystem_monitoring.md` - Comprehensive documentation
3. `ocs_ci/templates/workloads/cephfs_stress/IMPLEMENTATION_SUMMARY.md` - This file

### Modified:
1. `ocs_ci/templates/workloads/cephfs_stress/cephfs_stress_job.yaml` - Added monitoring startup
2. `ocs_ci/templates/workloads/cephfs_stress/cephfs_stress_pod.yaml` - Added monitoring startup
3. `ocs_ci/helpers/cephfs_stress_helpers.py` - Added hang detection functions

## Testing the Implementation

### 1. Verify Monitoring Starts

```bash
# Get pod name
POD=$(kubectl get pods -n test-ns | grep cephfs-stress | awk '{print $1}')

# Check monitoring process
kubectl exec -it $POD -n test-ns -- ps aux | grep cephfs_filesystem_monitor

# Should see:
# python3 /script/cephfs_filesystem_monitor.py
```

### 2. Verify Monitoring Logs

```bash
# Check logs are being created
kubectl exec -it $POD -n test-ns -- ls -la /mnt/output/monitoring_logs/

# View logs
kubectl exec -it $POD -n test-ns -- tail -f /mnt/output/monitoring_logs/filesystem_monitor_*.log
```

### 3. Verify Hang Detection

To test hang detection, temporarily reduce timeout:

```yaml
env:
  - name: MONITOR_CURRENT_TIMEOUT
    value: "10"  # Very short for testing
```

Then create a large directory that takes >10 seconds to list.

## Summary

**What happens in the container**:
- Monitoring script runs in background
- Creates hang markers if timeouts occur
- Logs all monitoring activity

**What happens in ocs-ci**:
- Background checks look for hang markers
- Fails test if markers found
- Collects logs for debugging

**Result**:
- Clean separation of concerns
- Simple, maintainable code
- Robust hang detection
- Integrated with existing framework

This approach gives you the best of both worlds: simple container logic and powerful validation in the test framework.