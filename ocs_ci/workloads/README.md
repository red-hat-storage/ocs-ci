# VdbenchWorkload Class – README

## Overview

This module provides an advanced `VdbenchWorkload` class designed to automate and manage Vdbench workloads.

Key capabilities include:
- YAML-based config support with Jinja2 templating
- Support for both **Filesystem** and **Block** PVCs
- Auto-generation of deployment and ConfigMap YAMLs
- Scaling, pausing, resuming, and cleanup operations
- Config-to-Vdbench syntax conversion
- Detailed logging and error handling

---

## Class: `VdbenchWorkload`

### Initialization
```python
VdbenchWorkload(pvc, vdbench_config_file, namespace=None, image=None)
```
- **pvc**: PVC object (OCS)
- **vdbench_config_file**: Path to YAML file describing workload
- **namespace** (optional): Target Kubernetes namespace
- **image** (optional): Custom vdbench container image

---

## Features & Methods


### 🚀 Workload Control
- `start_workload()`: Apply config and start workload
- `pause_workload()`: Pause (scale to 0)
- `resume_workload()`: Resume to original replicas
- `stop_workload()`: Stop and remove deployment
- `cleanup_workload()`: Cleanup all resources and temp files
- `scale_up_pods(n) / scale_down_pods(n)`: Scale replica count

### 🔍 Monitoring & Debugging
- `_wait_for_pods_ready(timeout=300)`: Waits for workload to be running
- `_check_pods_ready()`: Internal check for pod status
- `_capture_pod_logs()`: Capture logs for debugging failures
- `get_workload_status()`: Returns a status dict of workload state

---

## Deployment Template Directory
Templates are expected under:
```
ocs_ci/templates/workloads/vdbench/
```
- `deployment.yaml.j2`
- `configmap.yaml.j2`

If missing, fallback to inline templates will be used.

---

## Available Configuration Fixtures

### 1. Default Configuration

```python
@pytest.fixture
def vdbench_default_config()
```

- Basic configuration suitable for general testing
- **Configurable parameters**:
  - `lun`: Storage device path (default: `/vdbench-data/testfile`)
  - `size`: Test file size (default: `1g`)
  - `threads`: Number of worker threads (default: `1`)
  - `rdpct`: Read percentage (default: `50`)
  - `seekpct`: Seek percentage (default: `100`)
  - `xfersize`: Transfer size (default: `4k`)
  - `elapsed`: Test duration in seconds (default: `60`)
  - `interval`: Reporting interval (default: `5`)
  - `iorate`: I/O rate (default: `max`)

---

### 2. Performance Configuration

```python
@pytest.fixture
def vdbench_performance_config()
```

- Optimized for performance benchmarking
- **Configurable parameters**:
  - `lun`: Storage device path (default: `/vdbench-data/perftest`)
  - `size`: Test file size (default: `10g`)
  - `threads`: Number of worker threads (default: `4`)
  - `workloads`: Custom workload definitions (optional)
  - `runs`: Custom run definitions (optional)

---

### 3. Block Device Configuration

```python
@pytest.fixture
def vdbench_block_config()
```

- Specialized for block device testing
- **Configurable parameters**:
  - `lun`: Block device path (default: `/dev/vdbench-device`)
  - `size`: Device size (default: `100%`)
  - `threads`: Number of worker threads (default: `2`)
  - `rdpct`: Read percentage (default: `50`)
  - `seekpct`: Seek percentage (default: `100`)
  - `xfersize`: Transfer size (default: `8k`)
  - `elapsed`: Test duration (default: `120`)
  - `interval`: Reporting interval (default: `5`)
  - `iorate`: I/O rate (default: `max`)

---

### 4. Filesystem Configuration

```python
@pytest.fixture
def vdbench_filesystem_config()
```

- Designed for filesystem testing
- **Configurable parameters**:
  - `anchor`: Base directory (default: `/vdbench-data/fs-test`)
  - `depth`: Directory depth (default: `2`)
  - `width`: Files per directory (default: `4`)
  - `files`: Total files (default: `10`)
  - `size`: File size (default: `1g`)
  - `threads`: Worker threads (default: `2`)
  - `rdpct`: Read percentage (default: `50`)
  - `xfersize`: Transfer size (default: `8k`)
  - `elapsed`: Test duration (default: `120`)
  - `interval`: Reporting interval (default: `5`)
  - `iorate`: I/O rate (default: `max`)

---

## How to Run Tests

### Basic Usage

1. **Import the required fixtures** in your test file:

```python
from your_module import (
    vdbench_workload_factory,
    vdbench_default_config,
    create_vdbench_test_scenario
)
```

2. **Create a simple test**:

```python
def test_basic_performance(vdbench_workload_factory, vdbench_default_config):
    config = vdbench_default_config(xfersize="64k", elapsed=300)
    workload = vdbench_workload_factory(vdbench_config=config, auto_start=True)
    # Add your assertions here
```
---

**Technical Implementation**:

```python
VdbenchWorkload Class Features
python# Complete lifecycle management
workload = VdbenchWorkload(pvc=pvc, vdbench_config_file=config_file)
workload.start_workload()           # Start with health validation
workload.scale_up_pods(3)           # Dynamic scaling
workload.pause_workload()           # Pause operations
workload.resume_workload()          # Resume from pause
workload.scale_down_pods(1)         # Scale down
workload.stop_workload()            # Clean stop
workload.cleanup_workload()         # Resource cleanup
```

**Configuration Support**
```yaml
# Filesystem workload example

storage_definitions:
  - id: 1
    fsd: true
    anchor: "/vdbench-data/test"
    depth: 3
    width: 4
    files: 50
    size: "100m"

workload_definitions:
  - id: 1
    sd_id: 1
    rdpct: 70
    xfersize: "32k"
    threads: 4

run_definitions:
  - id: 1
    wd_id: 1
    elapsed: 300
    interval: 15
    iorate: "max"
```
Monitoring & Reporting
```python
# Real-time monitoring
metrics = monitor_vdbench_workload(
    workload=workload,
    interval=30,
    duration=300
)

# Performance reporting
report = create_vdbench_performance_report(
    metrics,
    output_file="performance_report.yaml"
)
```
---

### Running Different Configurations

#### 1. Default Configuration

```python
def test_default_config(vdbench_workload_factory, vdbench_default_config):
    config = vdbench_default_config(
        size="2g",
        threads=4,
        rdpct=70,
        elapsed=120
    )
    workload = vdbench_workload_factory(vdbench_config=config, auto_start=True)
```

#### 2. Performance Test

```python
def test_performance(vdbench_workload_factory, vdbench_performance_config):
    config = vdbench_performance_config(
        size="20g",
        threads=8,
        workloads=[
            {"id": 1, "sd_id": 1, "rdpct": 100, "xfersize": "128k"},
            {"id": 2, "sd_id": 1, "rdpct": 0, "xfersize": "256k"}
        ]
    )
    workload = vdbench_workload_factory(vdbench_config=config, auto_start=True)

    performance_metrics = []

    # === LIFECYCLE PHASE 1: START & BASELINE ===
    log.info("Phase 1: Starting performance workload - Baseline")

    # Baseline performance monitoring
    log.info("Collecting baseline performance metrics")
    baseline_metrics = monitor_vdbench_workload(
        workload=workload,
        interval=30,
        duration=120  # 2 minutes baseline
    )
    performance_metrics.extend(baseline_metrics)

    validate_vdbench_workload_health(workload)

    # === LIFECYCLE PHASE 2: SCALE UP FOR PEAK PERFORMANCE ===
    log.info("Phase 2: Scaling up for peak performance testing")
    workload.scale_up_pods(6)

    # === LIFECYCLE PHASE 3: PAUSE & ANALYZE ===
    log.info("Phase 3: Pausing for interim analysis")
    workload.pause_workload()

    # Generate interim performance report
    interim_report = create_vdbench_performance_report(
        performance_metrics,
        output_file=f"/tmp/interim_performance_{workload.deployment_name}.yaml"
    )

    log.info(f"Interim performance summary: {interim_report.get('performance', {})}")

    # Analysis pause
    time.sleep(60)

    # === LIFECYCLE PHASE 4: RESUME & SUSTAINED LOAD ===
    log.info("Phase 4: Resuming for sustained load testing")
    workload.resume_workload()

    # Sustained performance monitoring
    log.info("Collecting sustained performance metrics")
    sustained_metrics = monitor_vdbench_workload(
        workload=workload,
        interval=25,
        duration=300  # 5 minutes sustained load
    )
    performance_metrics.extend(sustained_metrics)

    validate_vdbench_workload_health(workload)

    # === LIFECYCLE PHASE 5: SCALE DOWN & EFFICIENCY TEST ===
    log.info("Phase 5: Scaling down for efficiency testing")
    workload.scale_down_pods(2)

    # Efficiency monitoring
    log.info("Collecting efficiency metrics")
    efficiency_metrics = monitor_vdbench_workload(
        workload=workload,
        interval=30,
        duration=150  # 2.5 minutes efficiency test
    )
    performance_metrics.extend(efficiency_metrics)

    # === LIFECYCLE PHASE 6: GENERATE FINAL REPORT & STOP ===
    log.info("Phase 6: Generating final performance report")

    final_report = create_vdbench_performance_report(
        performance_metrics,
        output_file=f"/tmp/final_performance_{workload.deployment_name}.yaml"
    )

    # Performance assertions
    assert len(performance_metrics) > 10, "Should have collected multiple metric snapshots"
    assert final_report.get("performance", {}).get("peak_rate", 0) > 0, "Should have recorded performance data"

    log.info("Performance test results:")
    log.info(f"  Total snapshots: {final_report.get('summary', {}).get('total_snapshots', 0)}")
    log.info(f"  Peak rate: {final_report.get('performance', {}).get('peak_rate', 0)}")
    log.info(f"  Average rate: {final_report.get('performance', {}).get('average_rate', 0)}")
    log.info(f"  Min rate: {final_report.get('performance', {}).get('min_rate', 0)}")

    # Stop workload
    workload.stop_workload()
    assert not workload.is_running

    log.info("Performance workload lifecycle test completed successfully")

    return final_report
```

#### 3. Block Device Test

```python
def test_block_device(vdbench_workload_factory, vdbench_block_config):
    config = vdbench_block_config(
        lun="/dev/sdb",
        threads=8,
        xfersize="64k",
        elapsed=600
    )
    workload = vdbench_workload_factory(vdbench_config=config, auto_start=True)
```

#### 4. Filesystem Test

```python
def test_filesystem(vdbench_workload_factory, vdbench_filesystem_config):
    config = vdbench_filesystem_config(
        anchor="/mnt/testfs",
        depth=3, # directory depth
        width=10, # Directory Width
        files=100, # num files per  dir
        size="100m"
    )
    workload = vdbench_workload_factory(vdbench_config=config, auto_start=True)

    workload.start()

    sleep(30)

    workload.stop()
```

---

### Complete Test Scenario

```python
def test_complete_scenario(
    vdbench_workload_factory,
    pvc_factory,
    vdbench_performance_config
):
    # Create configuration
    config = vdbench_performance_config(size="50g", threads=16)

    # Create PVC and workload
    pvc, workload = create_vdbench_test_scenario(
        vdbench_workload_factory,
        pvc_factory,
        config,
        pvc_size="100Gi",
        storage_class="premium-rwo",
        access_mode="ReadWriteOnce"
    )

    # Start workload
    workload.start_workload()

    # Add your test assertions here
```

---

## Best Practices

1. **Resource Management**:
   - Adjust PVC sizes according to your test requirements
   - Monitor resource usage during long-running tests

2. **Configuration**:
   - Start with smaller tests and scale up
   - Use appropriate transfer sizes for your storage system

3. **Cleanup**:
   - All fixtures include automatic cleanup
   - Manual cleanup is also available via `workload.cleanup_workload()`


---

## Troubleshooting

- **PVC Creation Issues**: Verify storage class and access modes
- **Workload Failures**: Check container logs for VDBench errors
- **Performance Problems**: Adjust thread counts and transfer sizes

---

For more advanced configurations, refer to the [VDBench documentation](https://www.oracle.com/downloads/server-storage/vdbench-downloads.html) and customize the configuration dictionaries as needed.
