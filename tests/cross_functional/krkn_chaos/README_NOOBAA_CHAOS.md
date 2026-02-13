# NooBaa Chaos Testing with KRKN

This document describes the NooBaa pod disruption chaos tests using the KRKN chaos engineering framework.

## Overview

The NooBaa chaos tests repeatedly kill NooBaa pods at fixed intervals while S3 metadata workload is running, validating NooBaa's resilience and data integrity under aggressive disruption conditions.

## Test Scenarios

### 1. Basic Pod Disruption Tests (`test_krkn_noobaa_pod_disruption_with_s3_workload`)

These tests target individual NooBaa components with repeated pod kills at configurable intervals:

#### Target Pods

| Pod | Description | Criticality | Test Configurations |
|-----|-------------|-------------|---------------------|
| **noobaa-db-pg-0** | PostgreSQL database | **CRITICAL** | 20min/180s, 30min/240s, 60min/300s |
| **noobaa-core-0** | NooBaa core S3 service | **HIGH** | 20min/180s, 30min/240s |
| **noobaa-operator** | NooBaa operator | **MEDIUM** | 20min/180s |

#### Test Parameterization

```python
@pytest.mark.parametrize(
    "target_pod,duration_seconds,kill_interval_seconds",
    [
        ("noobaa-db-pg-0", 1200, 180),   # 20 min, kill every 3 min
        ("noobaa-db-pg-0", 1800, 240),   # 30 min, kill every 4 min
        ("noobaa-db-pg-0", 3600, 300),   # 60 min, kill every 5 min
        ("noobaa-core-0", 1200, 180),    # 20 min, kill every 3 min
        ("noobaa-core-0", 1800, 240),    # 30 min, kill every 4 min
        ("noobaa-operator.*", 1200, 180), # 20 min, kill every 3 min
    ]
)
```

#### Chaos Behavior

- **Pod Kill Method**: Force delete (no graceful shutdown)
- **Kill Frequency**: Configurable interval (180-300 seconds)
- **Total Duration**: Configurable (20-60 minutes)
- **Expected Pod Kills**: ~6-20 kills per test
- **Workload Overlap**: S3 operations run continuously during all chaos

### 2. Strength Testing (`test_krkn_noobaa_strength_testing`)

Extreme stress tests that push NooBaa to its limits by targeting multiple components simultaneously with aggressive kill intervals:

#### Stress Levels

| Level | Duration Multiplier | Kill Interval | Success Rate Threshold |
|-------|-------------------|---------------|----------------------|
| **high** | 3x (30 min) | 120s | 70% |
| **extreme** | 5x (50 min) | 90s | 60% |
| **ultimate** | 8x (80 min) | 60s | 50% |

#### Target Components (All Simultaneously)

1. **noobaa-db-pg-0** - Database pod
2. **noobaa-core-0** - Core service pod
3. **noobaa-operator** - Operator pod

#### Example: Ultimate Stress Test

- **Duration**: 80 minutes (8x base)
- **Kill Interval**: Every 60 seconds
- **Total Disruptions**: ~80 pod kills per component (240 total)
- **Concurrent Failures**: All three NooBaa components disrupted simultaneously

## Configuration

### Using Configuration File

Create a configuration file or use the provided `conf/ocsci/krkn_noobaa_chaos_config.yaml`:

```yaml
ENV_DATA:
  krkn_config:
    workloads:
      - "MCG_WORKLOAD"  # NooBaa S3 workload

    run_workload: true
    enable_verification: true

    mcg_config:
      num_buckets: 3
      iteration_count: 20
      operation_types:
        - upload
        - download
        - list
        - delete
      upload_multiplier: 2
      metadata_ops_enabled: true  # Critical for NooBaa testing
      delay_between_iterations: 20
      delete_bucket_on_cleanup: true
```

### Running Tests

#### Run All NooBaa Chaos Tests

```bash
pytest --ocsci-conf conf/ocsci/krkn_noobaa_chaos_config.yaml \
    tests/cross_functional/krkn_chaos/test_krkn_noobaa_chaos.py
```

#### Run Specific Test Scenario

```bash
# Test NooBaa DB pod disruption (20min, 180s interval)
pytest --ocsci-conf conf/ocsci/krkn_noobaa_chaos_config.yaml \
    tests/cross_functional/krkn_chaos/test_krkn_noobaa_chaos.py::TestKrKnNooBaaChaos::test_krkn_noobaa_pod_disruption_with_s3_workload[noobaa-db-20min-180s-interval]

# Test strength testing (extreme level)
pytest --ocsci-conf conf/ocsci/krkn_noobaa_chaos_config.yaml \
    tests/cross_functional/krkn_chaos/test_krkn_noobaa_chaos.py::TestKrKnNooBaaChaos::test_krkn_noobaa_strength_testing[noobaa-extreme-stress]
```

#### Run Only Database Tests

```bash
pytest --ocsci-conf conf/ocsci/krkn_noobaa_chaos_config.yaml \
    tests/cross_functional/krkn_chaos/test_krkn_noobaa_chaos.py \
    -k "noobaa-db"
```

## KRKN Pod-Disruption Scenario Details

### How It Works

The tests use KRKN's pod-disruption scenario (`regex_openshift_pod_kill`) to:

1. **Match Pods**: Use regex patterns to identify target pods
   - Namespace pattern: `^openshift-storage$`
   - Pod name pattern: `^noobaa-db-pg-0$`, `^noobaa-core-0$`, `^noobaa-operator.*$`

2. **Kill Configuration**:
   - Number of pods to kill: 1 (per iteration)
   - Kill action: Force delete (equivalent to `kubectl delete pod --force --grace-period=0`)
   - Recovery time: Set to kill interval (time before next kill)

3. **Iteration Control**:
   - Number of iterations: `duration / kill_interval`
   - Wait between iterations: `kill_interval` seconds
   - Example: 1200s duration / 180s interval = ~6-7 pod kills

### KRKN Configuration Generated

```yaml
pod_scenarios:
  - namespace_pattern: "^openshift-storage$"
    name_pattern: "^noobaa-db-pg-0$"
    kill: 1
    krkn_pod_recovery_time: 180

tunings:
  wait_duration: 180
  iterations: 6
```

## S3 Workload Details

### Workload Characteristics

The MCG_WORKLOAD creates multiple S3 buckets on NooBaa and performs continuous operations:

1. **Bucket Creation**:
   - Creates 3 MCG buckets (configurable)
   - Uses `MCGOCBucket` for NooBaa-specific buckets
   - Waits for bucket binding and health verification

2. **S3 Operations** (per bucket):
   - **Upload**: Multiple objects per iteration (configurable multiplier)
   - **Download**: Retrieve uploaded objects
   - **List**: List bucket contents (metadata operation)
   - **Delete**: Clean up objects

3. **Metadata Operations**:
   - Enabled by default for NooBaa testing
   - Stresses the NooBaa database with metadata-heavy operations
   - Critical for validating database resilience

4. **Operation Cycle**:
   - Runs 20 iterations per bucket (configurable)
   - 20s delay between iterations (configurable)
   - Operations continue throughout chaos duration

### Workload Validation

After chaos completion:

1. **Health Checks**:
   - Verify all workload threads are still running
   - Check for S3 operation failures
   - Validate bucket accessibility

2. **Pod Status Verification**:
   - NooBaa DB pods: Running or recovering
   - NooBaa Core pods: Running or recovering
   - NooBaa Operator: Running

3. **Expected Behavior**:
   - ✅ Temporary S3 failures during pod kills (503, timeouts)
   - ✅ Pod restarts and recovery
   - ✅ Workload resumes after pod recovery
   - ❌ Permanent database corruption
   - ❌ Unrecoverable NooBaa failures

## Expected Results

### Success Criteria

| Test Type | Min Success Rate | Expected Behavior |
|-----------|-----------------|-------------------|
| **Basic Disruption** | 80% | Most iterations succeed, temporary failures acceptable |
| **High Strength** | 70% | Some failures expected due to aggressive testing |
| **Extreme Strength** | 60% | Significant failures acceptable, focus on recovery |
| **Ultimate Strength** | 50% | High failure rate acceptable, validates extreme resilience |

### Key Metrics

- **Pod Recovery Time**: Time for pod to return to Running state
- **S3 Operation Success Rate**: Percentage of successful S3 requests
- **Database Consistency**: No corruption after recovery
- **Service Availability**: S3 endpoints return to normal after chaos

## Failure Modes

### Expected Failures (Non-Critical)

1. **Temporary S3 Errors**:
   - HTTP 503 Service Unavailable
   - Connection timeouts
   - Request timeouts
   - ➡️ Expected during pod restart, should resolve automatically

2. **Pod Restart**:
   - Pod phase: `Terminating` → `Pending` → `ContainerCreating` → `Running`
   - ➡️ Normal behavior, pod should reach Running state

3. **Database Connection Loss**:
   - Brief loss of connection to PostgreSQL
   - ➡️ NooBaa core should reconnect automatically

### Critical Failures (Test Failure)

1. **Database Corruption**:
   - NooBaa DB unable to start after chaos
   - Data inconsistency errors in logs
   - ➡️ Indicates database did not handle abrupt termination

2. **Permanent Service Degradation**:
   - NooBaa pods stuck in CrashLoopBackOff
   - S3 service not recovering after chaos
   - ➡️ Indicates unrecoverable failure

3. **Data Loss**:
   - Objects missing after chaos
   - Bucket metadata lost
   - ➡️ Indicates storage consistency issue

## Troubleshooting

### Common Issues

#### 1. NooBaa Pods Not Running

**Error**: `No NooBaa core pods found - NooBaa may not be deployed`

**Solution**: Verify NooBaa is deployed and healthy:
```bash
oc get pods -n openshift-storage | grep noobaa
oc get noobaa -n openshift-storage
```

#### 2. MCG Bucket Creation Fails

**Error**: `Failed to create MCG workloads: Bucket failed to become healthy`

**Solution**: Check NooBaa operator and OBC controller:
```bash
oc logs -n openshift-storage deployment/noobaa-operator
oc get obc -n <test-namespace>
```

#### 3. S3 Operations Timeout

**Error**: `Workload validation/cleanup issue: Request timeout`

**Solution**: This is expected during pod disruption. Check if pods recovered:
```bash
oc get pods -n openshift-storage | grep noobaa
# Wait for pods to reach Running state
```

### Debug Mode

Enable detailed logging:

```python
import logging
logging.getLogger('ocs_ci.krkn_chaos').setLevel(logging.DEBUG)
logging.getLogger('ocs_ci.workloads').setLevel(logging.DEBUG)
```

## Architecture

### Test Flow

```
1. Setup Phase
   ├── Initialize KRKN framework
   ├── Create MCG workloads (3 buckets)
   ├── Start S3 operations (background threads)
   └── Validate initial health

2. Chaos Execution Phase
   ├── Generate KRKN pod-disruption scenario
   ├── Configure iterations and intervals
   ├── Execute pod kills (repeated)
   │   ├── Force delete target pod
   │   ├── Wait for recovery time
   │   └── Repeat for configured iterations
   └── Monitor workload health

3. Validation Phase
   ├── Stop workload operations
   ├── Validate S3 workload health
   ├── Check NooBaa pod status
   ├── Verify no database corruption
   └── Analyze chaos results

4. Cleanup Phase
   ├── Delete S3 objects
   ├── Delete MCG buckets
   └── Clean up test resources
```

### Component Interaction

```
Test Framework
    │
    ├── KRKN Pod-Disruption Scenario
    │   └── Repeatedly kills NooBaa pods
    │
    ├── MCG S3 Workload
    │   ├── Bucket 1: S3 operations
    │   ├── Bucket 2: S3 operations
    │   └── Bucket 3: S3 operations
    │
    └── NooBaa Components
        ├── noobaa-db-pg-0 (target)
        ├── noobaa-core-0 (target)
        └── noobaa-operator (target)
```

## Best Practices

1. **Start with Basic Tests**: Run database tests first before strength testing
2. **Monitor Cluster Health**: Check overall cluster health before testing
3. **Adjust Kill Intervals**: Lower intervals = more aggressive, higher failure risk
4. **Review Logs**: Check NooBaa logs for database errors after chaos
5. **Allow Recovery Time**: Ensure pods fully recover between test runs
6. **Scale Workload**: Adjust `num_buckets` and `iteration_count` based on cluster size

## References

- [KRKN Chaos Engineering](https://github.com/krkn-chaos/krkn)
- [NooBaa Documentation](https://www.noobaa.io/)
- [KRKN Chaos Framework README](../../../ocs_ci/krkn_chaos/README.md)
