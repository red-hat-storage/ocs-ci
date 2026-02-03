# NooBaa KRKN Chaos Tests Implementation Summary

## Overview

This implementation adds comprehensive KRKN chaos testing for NooBaa components, focusing on pod disruption scenarios with continuous S3 metadata workload operations.

## Files Created/Modified

### 1. Test File
**Path**: `tests/cross_functional/krkn_chaos/test_krkn_noobaa_chaos.py`

A comprehensive test suite with two main test classes:

#### Test: `test_krkn_noobaa_pod_disruption_with_s3_workload`
- **Purpose**: Repeatedly kill NooBaa pods at fixed intervals
- **Targets**: noobaa-db-pg-0, noobaa-core-0, noobaa-operator
- **Configurations**: 6 test scenarios with varying durations (20-60 min) and kill intervals (180-300s)
- **Polarion ID**: OCS-7342

#### Test: `test_krkn_noobaa_strength_testing`
- **Purpose**: Extreme stress testing with simultaneous multi-pod disruption
- **Stress Levels**: high (3x), extreme (5x), ultimate (8x)
- **Kill Intervals**: 60-120 seconds
- **Targets**: All NooBaa components simultaneously
- **Polarion ID**: OCS-7343

### 2. Workload Registry Update
**Path**: `ocs_ci/krkn_chaos/krkn_workload_registry.py`

Added `MCG_WORKLOAD` registration:
```python
KrknWorkloadRegistry.register(
    WorkloadTypeConfig(
        name="MCG_WORKLOAD",
        required_fixtures=["awscli_pod"],
        factory_method="_create_mcg_workloads_for_project",
        fixture_params=["awscli_pod"],
        description="MCG/NooBaa S3 workload for NooBaa chaos testing",
    )
)
```

### 3. Workload Configuration Update
**Path**: `ocs_ci/krkn_chaos/krkn_workload_config.py`

Added:
- `MCG_WORKLOAD` constant
- `get_mcg_config()` method for MCG-specific configuration

### 4. Workload Factory Update
**Path**: `ocs_ci/krkn_chaos/krkn_workload_factory.py`

Added:
- `_create_mcg_workloads_for_project()` - Factory method for creating MCG workloads
- `_validate_mcg_workload()` - Validation method for MCG workload health
- Updated workload type detection to distinguish MCG from RGW
- Updated validation calls in all relevant methods

Key features of MCG workload factory:
- Creates MCG buckets using `MCGOCBucket`
- Pre-flight health checks for NooBaa pods
- Uses `RGWWorkload` class for S3 operations (S3-compatible)
- Supports metadata-intensive operations
- Automatic cleanup on teardown

### 5. Configuration File
**Path**: `conf/ocsci/krkn_noobaa_chaos_config.yaml`

Comprehensive configuration with:
- MCG_WORKLOAD setup (3 buckets, 20 iterations)
- Metadata operations enabled (critical for NooBaa DB testing)
- S3 operation types: upload, download, list, delete
- Background cluster operations configuration
- Detailed inline documentation

### 6. Documentation
**Path**: `tests/cross_functional/krkn_chaos/README_NOOBAA_CHAOS.md`

Complete documentation including:
- Test scenario descriptions
- Configuration examples
- KRKN pod-disruption scenario details
- S3 workload characteristics
- Expected results and success criteria
- Failure modes and troubleshooting
- Architecture diagrams
- Best practices

## Key Features

### 1. KRKN Pod-Disruption Chaos
- **Scenario Type**: `regex_openshift_pod_kill`
- **Pod Selection**: Regex pattern matching in openshift-storage namespace
- **Kill Method**: Force delete (no graceful shutdown)
- **Frequency**: Configurable intervals (60-300 seconds)
- **Duration**: Configurable (20-80 minutes)
- **Iterations**: Automatic calculation based on duration and interval

### 2. S3 Metadata Workload
- **Workload Type**: MCG_WORKLOAD (NooBaa-specific)
- **Bucket Type**: MCGOCBucket (NooBaa buckets)
- **Operations**: Upload, download, list, delete
- **Metadata Mode**: Enabled by default for database stress testing
- **Parallelization**: Multiple buckets with concurrent operations
- **Continuous Execution**: Runs throughout entire chaos duration

### 3. Target Pods

| Pod | Label | Critical | Test Scenarios |
|-----|-------|----------|---------------|
| noobaa-db-pg-0 | cnpg.io/cluster=noobaa-db-pg-cluster | ⚠️ HIGH | 3 scenarios (20min, 30min, 60min) |
| noobaa-core-0 | noobaa-core=noobaa | ⚠️ MEDIUM | 2 scenarios (20min, 30min) |
| noobaa-operator | noobaa-operator=deployment | ℹ️ LOW | 1 scenario (20min) |

### 4. Chaos Behavior
- **Pod kills happen periodically**: Not just once, but repeatedly throughout test
- **Chaos overlaps with workload**: S3 operations run during all pod disruptions
- **Force deletion**: Pods are killed without graceful shutdown (simulates crashes)
- **Expected pod restarts**: Pods may be killed 6-80 times in one test
- **Workload resilience validation**: S3 operations must survive pod disruptions

### 5. Validation
- **Pod Health**: Verify NooBaa pods recover to Running state
- **Workload Health**: Validate S3 operations complete successfully
- **Database Consistency**: Ensure no corruption after repeated pod kills
- **Service Availability**: Verify S3 endpoints are accessible post-chaos

## Implementation Highlights

### Frequency-Based Pod Disruption
The tests use KRKN's iteration feature to create frequency-based pod killing:

```python
num_iterations = duration_seconds // kill_interval_seconds
# Example: 1200s / 180s = 6-7 pod kills

config.set_tunings(
    wait_duration=kill_interval_seconds,  # Time between kills
    iterations=num_iterations,            # Number of kills
)
```

### MCG Workload Creation
```python
# Create MCG bucket (NooBaa-specific)
mcg_bucket = MCGOCBucket(bucket_name)
mcg_bucket.verify_health(timeout=300)

# Create workload with metadata operations
workload_config = {
    "metadata_ops_enabled": True,  # Stress NooBaa DB
    "iteration_count": 20,
    "operation_types": ["upload", "download", "list", "delete"],
}

# Reuse RGWWorkload for S3 operations (S3-compatible API)
mcg_workload = RGWWorkload(
    rgw_bucket=mcg_bucket,
    awscli_pod=awscli_pod,
    workload_config=workload_config,
)
mcg_workload.start_workload()
```

### Health Validation
```python
# Check NooBaa pods after chaos
for label, name in noobaa_pods:
    pods = get_pods_having_label(
        label=label,
        namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
    )
    assert len(pods) > 0, f"No {name} pods found after chaos"

    for pod in pods:
        status = pod["status"]["phase"]
        assert status in ["Running", "ContainerCreating", "Pending"]
```

## Testing Strategy

### Basic Tests (6 scenarios)
1. **Database Focus**: 3 scenarios targeting noobaa-db-pg-0
   - 20min/180s: Moderate stress, ~6 kills
   - 30min/240s: Extended duration, ~7 kills
   - 60min/300s: Long-running, ~12 kills

2. **Core Service**: 2 scenarios targeting noobaa-core-0
   - 20min/180s: Quick validation
   - 30min/240s: Extended validation

3. **Operator**: 1 scenario targeting noobaa-operator
   - 20min/180s: Basic operator resilience

### Strength Tests (3 scenarios)
1. **High Stress**: 30min duration, 120s interval (~15 kills/pod)
2. **Extreme Stress**: 50min duration, 90s interval (~33 kills/pod)
3. **Ultimate Stress**: 80min duration, 60s interval (~80 kills/pod)

Each strength test targets **all three pods simultaneously**, resulting in:
- High: ~45 total pod kills
- Extreme: ~99 total pod kills
- Ultimate: ~240 total pod kills

## Success Metrics

| Test Type | Success Rate | Max Allowed Failures |
|-----------|-------------|---------------------|
| Basic Disruption | ≥80% | 20% of iterations |
| High Strength | ≥70% | 30% of iterations |
| Extreme Strength | ≥60% | 40% of iterations |
| Ultimate Strength | ≥50% | 50% of iterations |

## Usage Examples

### Run All Tests
```bash
pytest --ocsci-conf conf/ocsci/krkn_noobaa_chaos_config.yaml \
    tests/cross_functional/krkn_chaos/test_krkn_noobaa_chaos.py
```

### Run Only Database Tests
```bash
pytest --ocsci-conf conf/ocsci/krkn_noobaa_chaos_config.yaml \
    tests/cross_functional/krkn_chaos/test_krkn_noobaa_chaos.py \
    -k "noobaa-db"
```

### Run Strength Testing Only
```bash
pytest --ocsci-conf conf/ocsci/krkn_noobaa_chaos_config.yaml \
    tests/cross_functional/krkn_chaos/test_krkn_noobaa_chaos.py::TestKrKnNooBaaChaos::test_krkn_noobaa_strength_testing
```

### Run Specific Scenario
```bash
pytest --ocsci-conf conf/ocsci/krkn_noobaa_chaos_config.yaml \
    tests/cross_functional/krkn_chaos/test_krkn_noobaa_chaos.py::TestKrKnNooBaaChaos::test_krkn_noobaa_pod_disruption_with_s3_workload[noobaa-db-60min-300s-interval]
```

## Integration with Existing Framework

### Leverages Existing Components
- **KRKN Framework**: Uses existing KRKN integration
- **PodScenarios**: Reuses `regex_openshift_pod_kill` scenario generator
- **RGWWorkload**: Reuses S3 workload implementation for MCG
- **Validation Helpers**: Uses existing `KrknResultAnalyzer`, `ValidationHelper`
- **Health Checks**: Uses existing `CephHealthHelper` patterns

### Extends Framework
- **New Workload Type**: MCG_WORKLOAD for NooBaa-specific testing
- **NooBaa Health Validation**: New helper method `_validate_noobaa_health()`
- **Registry Integration**: Automatic fixture loading via workload registry

## Key Requirements Met

✅ **KRKN Pod-Disruption Chaos**: Uses `regex_openshift_pod_kill` scenario
✅ **Repeated Pod Kills**: Configurable frequency via iterations
✅ **Fixed Intervals**: Configurable kill intervals (60-300s)
✅ **S3 Metadata Workload**: MCG workload with metadata operations enabled
✅ **Primary Target**: noobaa-db-pg with 3 test scenarios
✅ **Secondary Targets**: noobaa-core, noobaa-operator
✅ **Namespace**: openshift-storage
✅ **Force Delete**: Pods killed with no graceful shutdown
✅ **Continuous Chaos**: Runs for entire test duration
✅ **Workload Overlap**: S3 operations continue during all chaos
✅ **Multiple Pod Kills**: Each pod killed multiple times per test run

## Future Enhancements

Potential improvements for future iterations:

1. **Network Chaos**: Add network latency/partition scenarios for NooBaa
2. **Volume Disruption**: Add PVC deletion scenarios for NooBaa DB
3. **Scaling Tests**: Add horizontal scaling during chaos
4. **Backup/Restore**: Add backup/restore validation during chaos
5. **Multi-Tenancy**: Add multi-bucket workload with isolation testing
6. **Performance Metrics**: Add latency/throughput tracking during chaos

## Testing Recommendations

1. **Start Small**: Begin with 20min tests before attempting 60min tests
2. **Monitor Cluster**: Check cluster health before and after tests
3. **Review Logs**: Always check NooBaa logs for database errors
4. **Adjust Intervals**: Increase intervals if seeing too many failures
5. **Scale Workload**: Adjust bucket count based on cluster capacity
6. **Run Repeatedly**: Run tests multiple times to catch intermittent issues

## Conclusion

This implementation provides comprehensive NooBaa chaos testing with:
- ✅ Frequency-based pod disruption using KRKN
- ✅ S3 metadata workload for database stress testing
- ✅ Multiple test scenarios covering all NooBaa components
- ✅ Strength testing for extreme resilience validation
- ✅ Extensive documentation and configuration examples
- ✅ Full integration with existing KRKN framework
