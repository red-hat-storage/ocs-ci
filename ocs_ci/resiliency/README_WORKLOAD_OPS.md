# Resiliency Tests Workload Mechanism

This document describes the workload mechanism for resiliency tests, which follows the same pattern as the krkn chaos tests automation.

## Overview

The resiliency tests now use a unified workload management system that provides:

- **Automated workload creation and management** - No need to manually create PVCs and workloads
- **Configuration-driven testing** - All workload settings in a single YAML config file
- **Background cluster operations** - Automated validation during test execution
- **Optional workload scaling** - Configurable scaling operations in parallel with failure injection
- **Multiple workload types** - Support for VDBENCH, GOSBENCH, CNV, and FIO workloads

## Architecture

The workload mechanism consists of three main components:

1. **ResiliencyWorkloadConfig** (`resiliency_workload_config.py`)
   - Loads configuration from `resiliency_tests_config.yaml`
   - Provides methods to access workload settings

2. **ResiliencyWorkloadFactory** (`resiliency_workload_factory.py`)
   - Creates workloads based on configuration
   - Manages workload lifecycle

3. **workload_ops Fixture** (`tests/cross_functional/resilience/conftest.py`)
   - Provides unified interface for test methods
   - Handles setup, validation, and cleanup

## Configuration File

Configuration is stored in `conf/ocsci/resiliency_tests_config.yaml`:

```yaml
ENV_DATA:
  resiliency_config:
    # Workload types to run
    workloads:
      - VDBENCH

    # Run workloads during testing
    run_workload: true

    # Enable data verification
    enable_verification: true

    # VDBENCH Configuration
    vdbench_config:
      threads: 16
      elapsed: 1200
      interval: 60

      block:
        size: "20g"
        patterns:
          - name: "random_write"
            rdpct: 0
            seekpct: 100
            xfersize: "4k"

      filesystem:
        size: "10m"
        depth: 4
        width: 5
        patterns:
          - name: "random_write"
            fileio: "random"
            rdpct: 0
            xfersize: "4k"

    # Scaling Configuration
    scaling_config:
      enabled: true
      min_replicas: 1
      max_replicas: 5
      delay: 30

    # Background Operations
    background_cluster_operations:
      enabled: true
      operation_interval: 60
      max_concurrent_operations: 3
      enabled_operations:
        - snapshot_lifecycle
        - clone_lifecycle
        - node_taint_churn
        - osd_operations
        - mds_failover
        - rgw_restart
        - reclaim_space
```

## Usage in Tests

### Basic Usage

```python
@pytest.mark.parametrize(
    argnames=["scenario_name", "failure_case"],
    argvalues=[
        pytest.param(
            "STORAGECLUSTER_COMPONENT_FAILURES",
            "OSD_POD_FAILURES",
            marks=polarion_id("OCS-6821"),
        ),
    ],
)
def test_storage_component_failure(
    self,
    scenario_name,
    failure_case,
    workload_ops,  # workload_ops fixture
):
    """
    Test storage component failures with workloads.
    """
    # Setup workloads (starts workloads, background ops, and scaling)
    workload_ops.setup_workloads()

    # Run failure injection
    resiliency_runner = Resiliency(scenario_name, failure_method=failure_case)
    resiliency_runner.start()
    resiliency_runner.cleanup()

    # Validate and cleanup workloads
    workload_ops.validate_and_cleanup()
```

### What the workload_ops Fixture Provides

The `workload_ops` fixture automatically:

1. **Creates workloads** based on configuration
   - VDBENCH workloads on CephFS and RBD PVCs
   - Multiple access modes (RWO, RWX, Block)
   - Configurable patterns and parameters

2. **Starts background operations** (if enabled)
   - Snapshot lifecycle operations
   - Clone operations
   - Node taints and drains
   - OSD/MDS/RGW operations
   - CSI-Addons operations

3. **Starts background scaling** (if enabled)
   - Random scale up/down operations
   - Configurable replica limits
   - Runs in parallel with failure injection

4. **Validates workloads** after test
   - Checks for I/O errors
   - Validates background operations
   - Ensures data integrity

5. **Cleans up resources**
   - Stops workloads
   - Deletes PVCs
   - Cleans up projects

## Running Tests

### With Configuration File

Pass the configuration file using `--ocsci-conf`:

```bash
pytest tests/cross_functional/resilience/test_app_scale_on_storage_component_failure.py \
    --ocsci-conf conf/ocsci/resiliency_tests_config.yaml
```

### Disable Workloads

To run tests without workloads:

```yaml
ENV_DATA:
  resiliency_config:
    run_workload: false
```

### Disable Scaling

To run tests without background scaling:

```yaml
ENV_DATA:
  resiliency_config:
    scaling_config:
      enabled: false
```

### Disable Background Operations

To run tests without background cluster operations:

```yaml
ENV_DATA:
  resiliency_config:
    background_cluster_operations:
      enabled: false
```

## Comparison with Krkn Tests

The resiliency workload mechanism follows the same pattern as krkn tests:

| Feature | Krkn Tests | Resiliency Tests |
|---------|-----------|------------------|
| Config File | `krkn_chaos_config.yaml` | `resiliency_tests_config.yaml` |
| Config Class | `KrknWorkloadConfig` | `ResiliencyWorkloadConfig` |
| Factory Class | `KrknWorkloadFactory` | `ResiliencyWorkloadFactory` |
| Fixture Name | `workload_ops` | `workload_ops` |
| Workload Types | VDBENCH, GOSBENCH, CNV | VDBENCH, GOSBENCH, CNV, FIO |
| Background Ops | ✅ | ✅ |
| Scaling | ✅ | ✅ |

## Migration Guide

### Migrating Existing Tests

To migrate an existing resiliency test to use the workload_ops fixture:

**Before:**
```python
def test_example(
    self,
    project_factory,
    multi_pvc_factory,
    resiliency_workload,
    vdbench_block_config,
    vdbench_filesystem_config,
):
    # Manual workload creation
    project = project_factory()
    pvcs = multi_pvc_factory(...)
    workloads = []
    for pvc in pvcs:
        workload = resiliency_workload("VDBENCH", pvc, ...)
        workload.start_workload()
        workloads.append(workload)

    # Manual validation and cleanup
    for workload in workloads:
        result = workload.get_results()
        assert "error" not in result
        workload.cleanup()
```

**After:**
```python
def test_example(self, workload_ops):
    # Automated workload creation
    workload_ops.setup_workloads()

    # Run test logic...

    # Automated validation and cleanup
    workload_ops.validate_and_cleanup()
```

### Benefits of Migration

1. **Less boilerplate code** - No manual PVC/workload creation
2. **Consistent configuration** - All settings in one place
3. **Better maintainability** - Changes to workload config don't require test code changes
4. **Enhanced testing** - Background operations and scaling built-in
5. **Reusability** - Same fixture across all resiliency tests

## Future Enhancements

- Add support for FIO workload type
- Add support for custom workload patterns
- Add support for multiple projects
- Add workload metrics collection
- Add workload performance validation

## Related Files

- `conf/ocsci/resiliency_tests_config.yaml` - Configuration file
- `ocs_ci/resiliency/resiliency_workload_config.py` - Config loader
- `ocs_ci/resiliency/resiliency_workload_factory.py` - Workload factory
- `tests/cross_functional/resilience/conftest.py` - Fixture definitions
- `tests/cross_functional/resilience/test_app_scale_on_storage_component_failure.py` - Example test

## Similar Patterns

This pattern is inspired by and compatible with:

- Krkn chaos tests (`tests/cross_functional/krkn_chaos/`)
- Krkn workload factory (`ocs_ci/krkn_chaos/krkn_workload_factory.py`)
- Krkn configuration (`conf/ocsci/krkn_chaos_config.yaml`)
- Resiliency tests configuration (`conf/ocsci/resiliency_tests_config.yaml`)
