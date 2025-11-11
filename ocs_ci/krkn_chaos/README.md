# Krkn Chaos Testing Framework

This directory contains the **enhanced** Krkn chaos testing framework integration for OCS-CI, providing comprehensive chaos engineering capabilities with **extreme strength testing**, intelligent workload management, and advanced post-chaos data verification.

## Overview

The Krkn chaos testing framework allows you to inject various types of chaos into your OpenShift Data Foundation (ODF) clusters to test resilience and data integrity. The framework supports multiple workload types, **extreme cluster strength testing**, and includes advanced post-chaos verification capabilities.

## 🚀 Latest Enhancements (2025)

### ⚡ Extreme Strength Testing
- **Application Outage Scenarios**: Multi-pattern chaos with cascading failures, rapid-fire disruptions, and recovery stress testing
- **Container Chaos Scenarios**: Container kill/pause patterns with progressive escalation and mixed chaos approaches
- **Resource Hog Scenarios**: Apocalyptic resource exhaustion testing with CPU/Memory/IO devastation patterns
- **Cluster Strength Scoring**: Advanced metrics with success rates and resilience scoring

### 🛠️ Technical Improvements
- **Duplicate Scenario Prevention**: Fixed unique filename generation and duplicate detection
- **Jinja2 Template Optimization**: Eliminated empty lines in generated YAML configurations
- **Enhanced Safety Controls**: Component-aware testing with critical vs resilient component handling
- **Advanced Logging**: Comprehensive logging with emojis and detailed scenario analysis

### 🔧 Critical Bug Fixes (September 2025)
- **NetworkPolicy Configuration Fix**: Resolved 400 Bad Request error in application outage scenarios
  - Fixed Jinja2 template to properly format `block` parameter as YAML list instead of boolean
  - Template now correctly generates `block: [Ingress, Egress]` instead of `block: true`
  - Eliminates "cannot unmarshal bool into Go struct field NetworkPolicySpec.spec.policyTypes" error
- **Dynamic Instance Detection**: Added support for automatic pod instance detection using `get_pods_having_label()`
- **Pod Name Extraction Fix**: Corrected pod name access from `pod.name` to `pod["metadata"]["name"]` for dictionary objects
- **VDBENCH Performance Enhancement**: Increased thread count from 10 to 32 for both block and CephFS workloads
  - Provides more intensive I/O stress testing during chaos scenarios
  - Better simulates real-world high-load conditions

## 🏗️ Extreme Strength Testing

### Test Types and Intensity Levels

#### 1. Application Outage Scenarios
**Location**: `tests/cross_functional/krkn_chaos/test_krkn_application_outage_scenarios.py`

**Supported Components**:
- **Ceph Components**: MON, MGR, MDS, OSD, RGW
- **CSI Plugins**: CephFS Node Plugin, RBD Node Plugin, CephFS Controller Plugin, RBD Controller Plugin
- **Rook Operator**: Rook Ceph Operator pods

**Basic Test**: `test_run_krkn_application_outage_scenarios`
- 🎯 Primary Outage, 🔥 Extended Outage, ⚡ Rapid-Fire Outage, 💥 Stress Test Outage
- **Critical Components** (MON, MGR, MDS, CSI Controllers, Rook Operator): 4 scenarios with conservative settings
- **Resilient Components** (OSD, RGW, CSI Node Plugins): 9 scenarios with high-intensity testing
- **Dynamic Instance Detection**: Automatically detects all available pod instances for each component

**Extreme Test**: `test_krkn_application_strength_testing`
- **OSD Extreme**: 6x duration multiplier, 13+ scenarios
- **RGW High**: 4x duration multiplier, conservative approach
- **CSI Node Plugin Extreme**: High-intensity testing with multiple scenarios
- **OSD Ultimate**: 8x duration multiplier, maximum stress
- **Patterns**: Cascading, Rapid-Fire, Wave, Endurance, Recovery Stress

#### 2. Container Chaos Scenarios
**Location**: `tests/cross_functional/krkn_chaos/test_krkn_container_chaos.py`

**Basic Test**: `test_krkn_container_chaos`
- 🎯 Primary Kill, 🔥 Aggressive Kill, ⏸️ Primary Pause, 💥 Extended Pause, ⚡ Rapid-Fire Kill
- **Critical Components**: 5 scenarios with SIGTERM preference
- **Resilient Components**: 10 scenarios with SIGKILL and extended pauses

**Extreme Test**: `test_krkn_container_strength_testing`
- **OSD Extreme**: 3x duration, 4x pause multipliers, 17+ scenarios
- **RGW High**: 2x duration, 3x pause multipliers
- **OSD Ultimate**: 5x duration, 6x pause multipliers, maximum stress
- **Patterns**: Cascading Kills, Rapid-Fire, Pause Waves, Mixed Chaos, Recovery Stress

#### 3. Resource Hog Scenarios (APOCALYPTIC)
**Location**: `tests/cross_functional/krkn_chaos/test_krkn_hog_scenarios.py`

**Basic Test**: `test_run_krkn_hog_scenarios`
- 🎯 Primary CPU/Memory/IO Hogs, 🔥 Intensive variants, 🌪️ Extreme variants
- **Master Nodes**: 6 scenarios with conservative settings (70% CPU, 60s duration)
- **Worker Nodes**: 13 scenarios with apocalyptic settings (95% CPU, 360s duration)

**EXTREME Test**: `test_krkn_extreme_cluster_strength_testing`
- **EXTREME**: 4x duration, 2x intensity, 18+ scenarios
- **ULTIMATE**: 6x duration, 3x intensity, endurance testing
- **APOCALYPSE**: 8x duration, 4x intensity, maximum cluster destruction
- **Patterns**: Cascading Resource Apocalypse, Rapid-Fire Bombardment, Resource Tsunami, Endurance Apocalypse

### Strength Testing Success Criteria

| Test Type | Extreme | Ultimate | Apocalypse |
|-----------|---------|----------|------------|
| **Application Strength** | 60% success rate | 60% success rate | 60% success rate |
| **Container Strength** | 65% success rate | 65% success rate | 65% success rate |
| **Resource Hog (Cluster)** | 50% success rate | 40% success rate | 30% success rate |

### Safety Controls

#### Component-Aware Testing
- **Critical Components** (MON, MGR, MDS, CSI Controllers, Rook Operator): Conservative settings, shorter durations, fewer scenarios
- **Resilient Components** (OSD, RGW, CSI Node Plugins): Aggressive testing, longer durations, more scenarios
- **Master Nodes**: Resource-limited testing to prevent cluster instability
- **Worker Nodes**: Full apocalyptic testing with maximum resource exhaustion

#### Intelligent Scenario Generation
- **Unique Filenames**: MD5-based hashing prevents scenario file overwrites
- **Duplicate Prevention**: Configuration generator prevents duplicate scenario paths
- **Progressive Escalation**: Scenarios build from baseline to extreme levels
- **Recovery Testing**: Validates system behavior during recovery phases

## Configuration

### Configuration Priority and Sources

The Krkn chaos testing framework follows the same configuration pattern as other OCS-CI components (like `sc_encryption.yaml`). Configuration is loaded in this priority order:

1. **🥇 Jenkins/Runtime ENV_DATA** (Highest Priority): Configuration exported from Jenkins jobs or test environments
2. **🥈 Configuration File**: Default configuration file at `conf/ocsci/krkn_chaos_config.yaml`
3. **🥉 Built-in Defaults**: Fallback defaults when no configuration is found

### Jenkins/Runtime Configuration (Recommended)

For Jenkins jobs or runtime configuration, export your settings in `ENV_DATA`:

```yaml
ENV_DATA:
  krkn_config:
    # Workload type to use for chaos testing
    workload: "CNV_WORKLOAD"  # Options: VDBENCH, CNV_WORKLOAD, FIO

    # Enable/disable post-chaos data verification (default: true)
    enable_verification: false

    # Additional workload-specific configuration
    vdbench_config:
      threads: 32
      size: "20g"
      elapsed: 1200
    cnv_config:
      vm_count: 6
      encrypted: true
```

### Configuration File (Fallback)

The fallback configuration file is located at `conf/ocsci/krkn_chaos_config.yaml`:

```yaml
ENV_DATA:
  krkn_config:
    # Workload type to use for chaos testing
    workload: "VDBENCH"  # Options: VDBENCH, CNV_WORKLOAD, FIO

    # Enable/disable post-chaos data verification (default: true)
    enable_verification: true

    # Additional workload-specific configuration can be added here
    # vdbench_config:
    #   threads: 10
    #   size: "10g"
    #   elapsed: 3600
    # cnv_config:
    #   vm_count: 4
    #   encrypted: false
```

### Configuration Loading Behavior

The framework automatically detects and loads configuration:

```
INFO - Using krkn_config from runtime ENV_DATA (Jenkins exports or test configuration)
INFO - Using workload type: CNV_WORKLOAD
```

Or if no runtime config is found:

```
INFO - Loading Krkn chaos config from /path/to/krkn_chaos_config.yaml
INFO - Merged krkn_config from config file into ENV_DATA
INFO - Using workload type: VDBENCH
```

### Workload Types

#### 1. VDBENCH (Default)
- **Description**: Traditional VDBENCH workloads on CephFS and RBD storage
- **Background Operations**: ✅ Fully integrated - runs during workload execution
- **Use Case**: Storage performance and data integrity testing
- **Configuration Example**:
```yaml
ENV_DATA:
  krkn_config:
    workloads: ["VDBENCH"]
    vdbench_config:
      num_pvcs_per_interface: 4  # Number of PVCs per storage interface (default: 4)
      pvc_size: 50  # PVC size in GiB (default: 50)
      threads: 32
      size: "15g"
      elapsed: 600
      interval: 60
```

#### 2. CNV_WORKLOAD
- **Description**: Container Native Virtualization workloads with VMs
- **Background Operations**: ✅ Fully integrated - validates VM resilience
- **Use Case**: Virtual machine resilience testing
- **Configuration Example**:
```yaml
ENV_DATA:
  krkn_config:
    workloads: ["CNV_WORKLOAD"]
    cnv_config:
      vm_count: 6
      encrypted: true
```

#### 3. RGW Workload
- **Description**: RGW S3 workload for object storage stress testing
- **Background Operations**: ✅ Fully integrated - validates S3 operations on RGW
- **Use Case**: RADOS Gateway resilience and performance testing
- **Platform**: On-prem only (vSphere, Baremetal)
- **Configuration Example**:
```yaml
ENV_DATA:
  krkn_config:
    workloads: ["RGW_WORKLOAD"]
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
      delete_bucket_on_cleanup: true  # Delete buckets when workload completes
```

#### 4. Multiple Workloads
- **Description**: Run multiple workload types simultaneously
- **Background Operations**: ✅ Validates all workload types
- **Use Case**: Comprehensive cluster testing
- **Configuration Example**:
```yaml
ENV_DATA:
  krkn_config:
    workloads: ["VDBENCH", "CNV_WORKLOAD", "RGW_WORKLOAD"]
```

### Background Operations Configuration

#### Enable/Disable Background Operations

```yaml
ENV_DATA:
  krkn_config:
    background_cluster_operations:
      # Enable continuous validation (default: true)
      enabled: true
      # How often to run operations (seconds)
      operation_interval: 60
      # Maximum concurrent operations
      max_concurrent_operations: 3
```

#### Supported Operations

| Operation | Description | Validates |
|-----------|-------------|-----------|
| `snapshot_lifecycle` | PVC snapshot creation/deletion | Snapshot functionality |
| `clone_lifecycle` | PVC clone creation/deletion | Clone functionality |
| `node_taint_churn` | Node taint/untaint operations | Node scheduling |
| `osd_operations` | OSD start/stop/restart | Ceph OSD resilience |
| `mds_failover` | MDS failover testing | CephFS availability |
| `rgw_restart` | RGW restart operations | S3 service resilience |
| `reclaim_space` | Space reclamation testing | Storage reclaim |
| `longevity_operations` | Comprehensive snapshot/restore/expand workflow | Long-running storage operations |

#### Longevity Operations Details

The `longevity_operations` background operation performs comprehensive long-running storage validation by combining multiple storage operations in a single workflow. This operation is inspired by and uses patterns from the longevity helper functions.

**What It Does:**
1. **Snapshot Creation**: Creates volume snapshots from a subset of workload PVCs (max 3 PVCs to avoid excessive resources)
2. **Snapshot Restoration**: Restores new PVCs from the created snapshots with correct size handling
3. **PVC Expansion**: Expands original workload PVCs by 5GB (if supported by the storage class)
4. **Resource Cleanup**: Automatically cleans up all temporary resources (restored PVCs, snapshots)

**Key Features:**
- Uses actual PVC capacity (not converted size) to avoid size mismatch errors
- Gracefully handles storage classes that don't support expansion
- Continues with remaining operations even if individual steps fail
- Automatically cleans up all temporary resources
- Validates data integrity through snapshot/restore cycle

**Example Log Output:**
```
INFO - Executing longevity operations (snapshot/restore/expand)
INFO - Selected 3 PVCs for longevity testing
INFO - Creating snapshots from workload PVCs
INFO - Created snapshot pvc-xxx-snapshot-abc from PVC pvc-test-xxx
INFO - Created 3 snapshots successfully
INFO - Restoring PVCs from snapshots
INFO - Restored PVC restored-longevity-xyz from snapshot pvc-xxx-snapshot-abc
INFO - Restored 3 PVCs from snapshots successfully
INFO - Attempting to expand original PVCs
INFO - Expanding PVC pvc-test-xxx from 50Gi to 55Gi
INFO - ✓ Successfully expanded PVC pvc-test-xxx
INFO - Successfully expanded 2 PVCs
INFO - Cleaning up longevity operation resources
INFO - ✓ Longevity operations completed successfully
```

## Background Cluster Operations (Continuous Validation)

### How It Works

The framework now uses **continuous validation** during chaos testing instead of post-chaos verification:

1. **Workload Initialization**:
   - Workloads start and begin I/O operations
   - Background cluster operations initialize
   - Continuous validation begins

2. **Chaos Injection** (with continuous validation):
   - Krkn executes chaos scenarios
   - **Background operations run continuously**:
     - PVC snapshot/clone lifecycle operations
     - Node taint churn testing
     - OSD operations (start/stop/restart)
     - MDS failover testing
     - RGW restart operations
     - Reclaim space operations
   - Validates cluster health in real-time

3. **Validation During Chaos**:
   - Operations execute at configured intervals (default: 60s)
   - Multiple operations run concurrently (default: 3)
   - Detects issues immediately as they occur
   - Better validation than post-chaos checks

4. **Error Detection**:
   - Real-time cluster health monitoring
   - Operation success/failure tracking
   - Immediate failure detection
   - Comprehensive validation report

### Configuration

```yaml
ENV_DATA:
  krkn_config:
    background_cluster_operations:
      enabled: true
      operation_interval: 60  # Seconds between operations
      max_concurrent_operations: 3
      enabled_operations:
        - snapshot_lifecycle
        - clone_lifecycle
        - node_taint_churn
        - osd_operations
        - mds_failover
        - rgw_restart
        - reclaim_space
        - longevity_operations
```

### Benefits Over Post-Chaos Verification

| Aspect | Post-Chaos Verification | Background Operations |
|--------|------------------------|----------------------|
| **Detection Speed** | After chaos completes | Real-time during chaos |
| **Coverage** | Storage only | Storage + cluster operations |
| **Validation Types** | Data integrity | Data + operational health |
| **Failure Detection** | Delayed | Immediate |
| **Overhead** | High (full re-verification) | Low (continuous sampling) |

## Usage Examples

### Jenkins/Runtime Configuration (Recommended)

#### Basic CNV Workloads for Jenkins Jobs

```yaml
# Jenkins ENV_DATA export
ENV_DATA:
  krkn_config:
    workload: "CNV_WORKLOAD"
    enable_verification: false  # CNV doesn't support verification
    cnv_config:
      vm_count: 6
      encrypted: true
```

#### VDBENCH with Custom Configuration

```yaml
# Jenkins ENV_DATA export
ENV_DATA:
  krkn_config:
    workload: "VDBENCH"
    enable_verification: true
    vdbench_config:
      num_pvcs_per_interface: 6  # Create 6 PVCs per storage type (12 total workloads)
      pvc_size: 100  # 100 GiB per PVC
      threads: 32
      size: "20g"
      elapsed: 1200
```

#### VDBENCH with Minimal Deployment Count

```yaml
# Jenkins ENV_DATA export - Faster testing with fewer deployments
ENV_DATA:
  krkn_config:
    workload: "VDBENCH"
    enable_verification: true
    vdbench_config:
      num_pvcs_per_interface: 2  # Only 2 PVCs per storage type (4 total workloads)
      pvc_size: 30  # Smaller PVCs for faster provisioning
      threads: 16
      elapsed: 300
```

### Configuration File Examples

#### Basic VDBENCH with Verification

```yaml
# conf/ocsci/krkn_chaos_config.yaml
ENV_DATA:
  krkn_config:
    workload: "VDBENCH"
    enable_verification: true
```

#### CNV Workloads (No Verification)

```yaml
# conf/ocsci/krkn_chaos_config.yaml
ENV_DATA:
  krkn_config:
    workload: "CNV_WORKLOAD"
    enable_verification: true  # Ignored - CNV doesn't support verification
```

#### VDBENCH without Verification

```yaml
# conf/ocsci/krkn_chaos_config.yaml
ENV_DATA:
  krkn_config:
    workload: "VDBENCH"
    enable_verification: false  # Skip verification
```

## 🚀 Extreme Strength Testing Examples

### Application Outage Scenarios

- **Basic Enhanced Testing**: Runs 4-9 application outage scenarios per component with multiple chaos patterns (Primary, Extended, Rapid-Fire, Stress Test)
- **Extreme Strength Testing**: Executes 13+ scenarios with cascading failures, rapid-fire disruptions, wave patterns, and recovery stress testing
- **Ultimate Stress Testing**: Maximum application resilience testing with 8x duration multiplier and sustained outage patterns

### Container Chaos Scenarios

- **Basic Enhanced Testing**: Runs 5-10 container chaos scenarios including kill/pause patterns with progressive escalation
- **Extreme Strength Testing**: Executes 17+ scenarios with cascading kills, rapid-fire patterns, pause waves, and mixed chaos approaches
- **Ultimate Container Testing**: Maximum container resilience with 5x duration and 6x pause multipliers for sustained container stress

### Resource Hog Scenarios (APOCALYPTIC)

- **Basic Enhanced Testing**: Runs 6-13 resource exhaustion scenarios targeting CPU, Memory, and I/O with varying intensity levels
- **EXTREME Cluster Testing**: Executes 18+ scenarios with 4x duration and 2x intensity for cascading resource apocalypse patterns
- **APOCALYPSE Destruction Testing**: Ultimate cluster destruction with 8x duration, 4x intensity, and 30% minimum success rate threshold

### Network Chaos Scenarios

- **Enhanced Network Testing**: Multiple network chaos scenarios per component with latency, packet loss, and bandwidth limitations
- **Network Ingress Testing**: Capacity stress scenarios targeting ingress traffic with high-intensity network degradation patterns

## Architecture

### Key Components

1. **`KrknWorkloadConfig`**: Configuration parser - reads and manages settings using ENV_DATA pattern
2. **`KrknWorkloadRegistry`**: Central registry for workload types - makes it easy to add new workloads
3. **`KrknWorkloadFactory`**: Workload factory - creates appropriate workload types with automatic dispatch
4. **`WorkloadOps`**: Workload operations manager - validates health and integrates background operations
5. **`BackgroundClusterOperations`**: Continuous validation during chaos testing (replaces post-chaos verification)

### Configuration Architecture

The framework follows OCS-CI's standard configuration pattern:

```
┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐
│  Jenkins ENV_DATA   │ -> │  Load Config File   │ -> │   Set Defaults      │
│   (Priority 1)      │    │   (Priority 2)      │    │   (Priority 3)      │
└─────────────────────┘    └─────────────────────┘    └─────────────────────┘
                                        │
                           ┌─────────────────────┐
                           │  Merge into         │
                           │  config.ENV_DATA    │
                           │  krkn_config        │
                           └─────────────────────┘
```

### Workflow

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Load Config   │ -> │  Create Workload │ -> │  Start Workload │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Cleanup & Exit  │ <- │ Run Verification │ <- │  Execute Chaos  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Logging

The framework provides comprehensive logging:

### Configuration Loading
```
INFO: Loading Krkn workload configuration from: /path/to/krkn_chaos_config.yaml
INFO: Post-chaos verification enabled for VDBENCH workloads
```

### Workload Creation
```
INFO: Creating VDBENCH workloads for chaos testing
INFO: Creating 4 CNV workloads for chaos testing
```

### Verification Process
```
INFO: 🔍 Starting post-chaos data verification with VDBENCH forx=verify
INFO: Creating verification workload 1/2
INFO: ✅ No data validation errors found in verification workload 1
```

### Error Detection
```
ERROR: 🚨 DATA CORRUPTION DETECTED in verification workload 1!
ERROR: Validation Error: Data Validation error at offset 0x0000001000 | Expected: 0x12345678 | Found: 0x87654321
```

## Best Practices

### 1. Use Jenkins/Runtime Configuration (Recommended)
For Jenkins jobs and automated testing, always use runtime `ENV_DATA` configuration:
```yaml
ENV_DATA:
  krkn_config:
    workload: "CNV_WORKLOAD"
    enable_verification: false
```
This takes priority over config files and provides better control.

### 2. Enable Verification for Production Testing
```yaml
ENV_DATA:
  krkn_config:
    workload: "VDBENCH"
    enable_verification: true  # Always enable for production
```

### 3. Use Appropriate Workload Types
- **VDBENCH**: For storage integrity testing
- **CNV_WORKLOAD**: For VM resilience testing
- Choose based on your testing objectives

### 4. Monitor Configuration Loading
Check logs to confirm configuration source:
```
INFO - Using krkn_config from runtime ENV_DATA (Jenkins exports or test configuration)
```
or
```
INFO - Loading Krkn chaos config from /path/to/krkn_chaos_config.yaml
```

### 5. Monitor Verification Results
- Check logs for verification completion
- Investigate any verification failures immediately
- Use verification results to improve system resilience

### 6. Configure Timeouts Appropriately
```yaml
ENV_DATA:
  krkn_config:
    vdbench_config:
      verification_timeout: 300  # 10 minutes for verification
```

## Troubleshooting

### Common Issues

#### 1. Verification Skipped for CNV Workloads
**Expected Behavior**: CNV workloads automatically skip verification
```
INFO: Post-chaos verification not supported for CNV_WORKLOAD workloads
INFO: Skipping post-chaos verification (disabled in configuration or unsupported workload type)
```

#### 2. Verification Disabled
**Check Configuration**:
```yaml
ENV_DATA:
  krkn_config:
    enable_verification: false  # Change to true
```

#### 3. Missing Verification Config Function
**Error**:
```
WARNING: Post-chaos verification requested but no verification config function provided
```
**Solution**: Ensure `get_fs_verify_config` function is properly defined

### 🚨 Extreme Testing Issues

#### 1. Extreme Testing Warnings
**Expected Behavior**: Extreme tests show apocalyptic warnings
```
⚠️  APOCALYPSE TESTING WARNING: This test will push the cluster to its absolute limits.
🚨 Starting APOCALYPSE cluster strength testing - PREPARE FOR EXTREME RESOURCE EXHAUSTION!
```

#### 2. Low Success Rates in Extreme Testing
**Expected Behavior**: Extreme testing has lower success rate thresholds
- **EXTREME**: 50% success rate minimum
- **ULTIMATE**: 40% success rate minimum
- **APOCALYPSE**: 30% success rate minimum

**Example Success**:
```
🎉 APOCALYPSE CLUSTER STRENGTH TEST PASSED!
Cluster demonstrated 35.2% resilience under apocalypse conditions!
✅ NO CEPH CRASHES - CLUSTER SURVIVED APOCALYPSE RESOURCE APOCALYPSE!
```

### Debug Mode

Enable debug logging for detailed information:
```python
import logging
logging.getLogger('ocs_ci.krkn_chaos').setLevel(logging.DEBUG)
```

## 🔧 Troubleshooting

### NetworkPolicy Configuration Errors

**Problem**: Application outage scenarios fail with 400 Bad Request error:
```
NetworkPolicy in version "v1" cannot be handled as a NetworkPolicy:
json: cannot unmarshal bool into Go struct field NetworkPolicySpec.spec.policyTypes
```

**Solution**: This error was fixed in September 2025. Ensure you have the latest version with:
- Fixed Jinja2 template in `ocs_ci/krkn_chaos/template/scenarios/openshift/app_outage.yml.j2`
- Template correctly formats `block` parameter as YAML list: `[Ingress, Egress]`
- No `block=True` boolean parameters in test scenarios

**Verification**: Check generated YAML contains:
```yaml
application_outage:
  block:
    - Ingress
    - Egress
```

### Pod Detection Issues

**Problem**: Tests fail with `'dict' object has no attribute 'name'` error.

**Solution**: Fixed pod name extraction from `get_pods_having_label()` return values:
- Use `pod["metadata"]["name"]` instead of `pod.name`
- Function returns list of dictionaries, not Pod objects

### Dynamic Instance Detection

**Problem**: Tests use fixed instance counts instead of detecting available pods.

**Solution**: Updated tests to use dynamic detection:
- `_detect_component_instances()` method automatically counts available pods
- Tests adapt to varying cluster sizes and configurations
- No need to manually specify instance counts

## Contributing

### Adding New Workload Types or Verification Methods

1. Update `KrknWorkloadConfig` with new workload constants
2. Add verification support check in `should_run_verification()`
3. Implement workload-specific verification if needed
4. Update this README with new configuration options
5. Add appropriate test cases

### Adding New Extreme Strength Testing Scenarios

1. **Scenario Generation**: Use unique filename generation with MD5 hashing
```python
import hashlib
import json

config_str = json.dumps(config, sort_keys=True)
config_hash = hashlib.md5(config_str.encode()).hexdigest()[:8]
unique_filename = f"scenario_{config_hash}.yaml"
```

2. **Component Safety**: Implement component-aware testing
```python
is_critical = component_name in ["mon", "mgr", "mds"]
if is_critical:
    # Conservative settings for critical components
    scenarios = basic_scenarios
else:
    # Aggressive testing for resilient components
    scenarios = basic_scenarios + extreme_scenarios
```

3. **Progressive Escalation**: Build scenarios from baseline to extreme
```python
scenarios = [
    # 🎯 BASELINE: Standard scenario
    create_scenario(duration=base_duration),
    # 🔥 ESCALATED: 2x intensity
    create_scenario(duration=base_duration * 2),
    # 💀 EXTREME: Maximum intensity
    create_scenario(duration=base_duration * 4),
]
```

4. **Success Rate Configuration**: Set appropriate thresholds
```python
min_success_rates = {
    "extreme": 50,     # 50% for extreme testing
    "ultimate": 40,    # 40% for ultimate testing
    "apocalypse": 30,  # 30% for apocalypse testing
}
```

5. **Jinja2 Templates**: Use whitespace control for clean YAML
```jinja2
{%- if condition %}
  field: {{ value }}
{%- endif %}
{%- for item in items %}
  - {{ item }}
{%- endfor %}
```

6. **Large Port Ranges**: Automatic filename optimization for extreme scenarios
```python
# Large port ranges automatically use hash-based filenames
egress_ports = list(range(1, 65536))  # All ports
# Generates: pod_network_outage_egress_range_65535ports_abc123.yaml
# Instead of extremely long filename with all port numbers
```

### Testing Guidelines

- **Critical Components**: Always use conservative settings for MON, MGR, MDS
- **Resilient Components**: Push OSD, RGW to extreme limits safely
- **Master Nodes**: Limit resource exhaustion to prevent cluster instability
- **Worker Nodes**: Enable full apocalyptic testing
- **Success Rates**: Set realistic thresholds based on chaos intensity
- **Safety Warnings**: Include appropriate warnings for extreme tests

## Security Considerations

- Verification processes have access to storage data
- Ensure proper cleanup of verification workloads
- Monitor verification logs for sensitive information
- Use appropriate timeouts to prevent resource exhaustion

---

For more information about Krkn chaos engineering, visit: https://github.com/krkn-chaos/krkn
