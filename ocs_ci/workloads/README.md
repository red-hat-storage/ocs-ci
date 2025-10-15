# VdbenchWorkload Class – Comprehensive Documentation

## Overview

The `VdbenchWorkload` class is a comprehensive Python automation framework for managing Vdbench workloads in Kubernetes environments. It provides full lifecycle management, dynamic scaling, and sophisticated configuration handling for storage performance testing.

### Key Features
- **Multi-format Support**: YAML configuration with Jinja2 templating
- **Storage Compatibility**: Supports both Filesystem and Block PVCs with automatic detection
- **Lifecycle Management**: Complete workload lifecycle with pause/resume capabilities
- **Dynamic Scaling**: Runtime scaling operations with health monitoring
- **Template Engine**: Jinja2-based template rendering with fallback mechanisms
- **Configuration Conversion**: Automatic YAML-to-Vdbench format conversion
- **Comprehensive Logging**: Detailed logging and error handling throughout

---

## Class: `VdbenchWorkload`

### Initialization

```python
VdbenchWorkload(pvc, vdbench_config_file, namespace=None, image=None)
```

**Parameters:**
- **pvc** (OCS): PVC object to attach the workload to
- **vdbench_config_file** (str): Path to YAML configuration file for Vdbench
- **namespace** (str, optional): Kubernetes namespace (defaults to PVC namespace)
- **image** (str, optional): Container image for Vdbench workload

**Auto-detected Properties:**
- Volume mode (Filesystem/Block) from PVC spec
- Access modes and storage class from PVC
- Unique deployment naming with random suffix
- Mount paths based on volume mode

---

## Core Methods

### 🚀 Workload Lifecycle Management

#### `start_workload()`
Deploys the Vdbench workload by creating ConfigMap and Deployment resources.
- Creates ConfigMap with Vdbench configuration
- Deploys workload with proper volume mounts
- Waits for pods to reach Ready state
- Captures logs on failure for debugging

#### `pause_workload()`
Pauses workload by scaling replicas to 0 while preserving configuration.
- Stores current replica count for resume
- Scales deployment to 0 replicas
- Updates internal state flags

#### `resume_workload()`
Resumes paused workload by restoring previous replica count.
- Restores previous replica count
- Waits for pods to become ready
- Updates state to running

#### `stop_workload()`
Stops workload by deleting the deployment while preserving ConfigMap.
- Gracefully deletes deployment
- Resets internal state
- Preserves configuration for potential restart

#### `cleanup_workload()`
Complete cleanup of all resources including temporary files.
- Deletes deployment and ConfigMap
- Removes temporary YAML files
- Comprehensive error handling

### 📊 Scaling Operations

#### `scale_up_pods(desired_count)`
Dynamically scales workload to higher replica count.
- Validates desired count is greater than current
- Updates deployment replica specification
- Waits for new pods to become ready

#### `scale_down_pods(desired_count)`
Scales workload down to lower replica count.
- Validates desired count constraints
- Handles scaling to zero gracefully
- Maintains pod readiness checks when applicable

### 🔍 Monitoring & Status

#### `get_workload_status()`
Returns comprehensive workload status information.

**Returns:**
```python
{
    "deployment_name": str,
    "namespace": str,
    "is_running": bool,
    "is_paused": bool,
    "current_replicas": int,
    "pod_phases": list,
    "deployment_status": str
}
```

#### `get_all_deployment_pod_logs()`
Retrieves logs from all pods in the deployment.
- Fetches logs from each pod individually
- Combines logs with pod identification
- Handles pod access errors gracefully

#### `_wait_for_pods_ready(timeout=300)`
Waits for all workload pods to reach Ready state.
- Configurable timeout (default: 5 minutes)
- Periodic readiness checks every 10 seconds
- Comprehensive error logging and pod log capture

---

## Configuration System

### YAML Configuration Format

The system uses a structured YAML format that automatically converts to Vdbench syntax:

```yaml
storage_definitions:
  # Block Storage Definition (SD)
  - id: 1
    lun: "/dev/vdbench-device"        # Block device path
    size: "10g"
    threads: 4
    openflags: "o_direct"             # Direct I/O flags
    open_flags: "o_direct"            # Alternative flag format
    align: "1k"                       # Alignment parameter
    offset: "0"                       # Offset parameter

  # Filesystem Storage Definition (FSD)
  - id: 2
    fsd: true                         # Marks as filesystem definition
    anchor: "/vdbench-data/fs-test"   # Root directory
    depth: 3                          # Directory depth
    width: 10                         # Files per directory
    files: 100                        # Total files
    size: "1g"                        # Per-file size
    open_flags: "o_direct"            # File opening flags

workload_definitions:
  # Block Workload Definition (WD)
  - id: 1
    sd_id: 1                          # References block storage
    rdpct: 70                         # Read percentage
    seekpct: 100                      # Random vs sequential
    xfersize: "64k"                   # Transfer size
    threads: 2                        # Thread count
    skew: 10                          # Load skewing

  # Filesystem Workload Definition (FWD)
  - id: 2
    fsd_id: 2                         # References filesystem storage
    fileio: "random"                  # I/O pattern (random/sequential)
    rdpct: 50                         # Read percentage
    xfersize: "32k"                   # Transfer size
    threads: 4                        # Thread count
    skew: 5                           # Load skewing

run_definitions:
  # Block Run Definition
  - id: 1
    wd_id: 1                          # References block workload
    elapsed: 300                      # Duration in seconds
    interval: 60                      # Reporting interval
    iorate: "max"                     # I/O rate limit
    format: "yes"                     # Format control

  # Filesystem Run Definition
  - id: 2
    fwd_id: 2                         # References filesystem workload
    elapsed: 300                      # Duration in seconds
    interval: 60                      # Reporting interval
    iorate: "1000"                    # Fixed rate limit (uses fwdrate internally)
    format: "no"                      # Skip formatting
```

### Automatic Configuration Conversion

The system automatically converts YAML to native Vdbench format with full parameter support:

**Block Storage YAML Input:**
```yaml
storage_definitions:
  - id: 1
    lun: "/dev/sdb"
    size: "100%"
    threads: 4
    openflags: "o_direct"
    align: "1k"
    offset: "0"

workload_definitions:
  - id: 1
    sd_id: 1
    rdpct: 70
    seekpct: 100
    xfersize: "64k"
    threads: 2
    skew: 10

run_definitions:
  - id: 1
    wd_id: 1
    elapsed: 300
    interval: 15
    iorate: "max"
    format: "yes"
```

**Generated Vdbench Format:**
```
sd=sd1,lun=/dev/sdb,size=100%,threads=4,openflags=o_direct,align=1k,offset=0

wd=wd1,sd=sd1,rdpct=70,seekpct=100,xfersize=64k,threads=2,skew=10

rd=rd1,wd=wd1,elapsed=300,interval=60,iorate=max,format=yes
```

**Filesystem Storage YAML Input:**
```yaml
storage_definitions:
  - id: 1
    fsd: true
    anchor: "/vdbench-data/fs-test"
    depth: 3
    width: 10
    files: 100
    size: "1g"
    open_flags: "o_direct"

workload_definitions:
  - id: 1
    fsd_id: 1
    fileio: "random"
    rdpct: 70
    xfersize: "64k"
    threads: 4
    skew: 5

run_definitions:
  - id: 1
    fwd_id: 1
    elapsed: 300
    interval: 15
    iorate: "max"
    format: "no"
```

**Generated Vdbench Format:**
```
fsd=fsd1,anchor=/vdbench-data/fs-test,depth=3,width=10,files=100,size=1g,openflags=o_direct

fwd=fwd1,fsd=fsd1,fileio=random,rdpct=70,xfersize=64k,threads=4,skew=5

rd=rd1,fwd=fwd1,elapsed=300,interval=60,fwdrate=max,format=no
```

---

## Template System

### Directory Structure
Templates are located in:
```
ocs_ci/templates/workloads/vdbench/
├── deployment.yaml.j2
└── configmap.yaml.j2
```

### Template Variables
Templates receive comprehensive context data:
```python
{
    "deployment_name": "vdbench-workload-abc12345",
    "namespace": "openshift-storage",
    "pvc_name": "test-pvc",
    "volume_mode": "Filesystem",  # or "Block"
    "mount_path": "/vdbench-data",  # or "/dev/vdbench-device"
    "device_path": "/vdbench-data",
    "image": "quay.io/pakamble/vdbench:latest",
    "vdbench_config_content": "sd=sd1,lun=...",
    "replicas": 1
}
```

### Fallback System
If Jinja2 templates are not found, the system uses inline template generation with the same Jinja2 environment for consistency.

---

## Available Test Fixtures

### Core Factory Fixture

#### `vdbench_workload_factory(request, project_factory)`
Primary factory for creating VdbenchWorkload instances with automatic cleanup.

**Usage:**
```python
def test_example(vdbench_workload_factory, vdbench_default_config):
    config = vdbench_default_config(size="5g", threads=4)
    workload = vdbench_workload_factory(
        pvc=pvc,
        vdbench_config=config,
        auto_start=True
    )
```

**Parameters:**
- `pvc` (OCS): PVC object
- `vdbench_config` (dict, optional): Configuration dictionary
- `config_file` (str, optional): Path to existing config file
- `namespace` (str, optional): Target namespace
- `image` (str, optional): Container image
- `pvc_access_mode` (str): PVC access mode
- `pvc_volume_mode` (str): PVC volume mode
- `auto_start` (bool): Whether to automatically start workload

### Configuration Fixtures

#### `vdbench_default_config()`
Basic configuration suitable for general testing and development.

**Configurable Parameters:**
```python
vdbench_default_config(
    lun="/vdbench-data/testfile",
    size="1g",
    threads=1,
    rdpct=50,
    seekpct=100,
    xfersize="4k",
    elapsed=60,
    interval=60,
    iorate="max"
)
```

#### `vdbench_performance_config()`
Optimized configuration for performance benchmarking with multiple workload patterns.

**Configurable Parameters:**
```python
vdbench_performance_config(
    lun="/vdbench-data/perftest",
    size="10g",
    threads=4,
    workloads=[
        {"id": 1, "sd_id": 1, "rdpct": 70, "xfersize": "64k"},
        {"id": 2, "sd_id": 1, "rdpct": 0, "xfersize": "1m"}
    ],
    runs=[
        {"id": 1, "wd_id": 1, "elapsed": 300, "iorate": "1000"},
        {"id": 2, "wd_id": 2, "elapsed": 180, "iorate": "max"}
    ]
)
```

#### `vdbench_block_config()`
Specialized configuration for block device testing with direct I/O.

**Configurable Parameters:**
```python
vdbench_block_config(
    lun="/dev/vdbench-device",
    size="1g",
    threads=2,
    open_flags="o_direct",
    sd_align="",                    # Alignment parameter
    sd_offset="",                   # Offset parameter
    sd_name="sd1",                  # Storage definition name
    elapsed=120,
    interval=60,
    patterns=[                      # Custom workload patterns
        {
            "name": "random_mixed",
            "rdpct": 50,            # 50% reads, 50% writes
            "seekpct": 100,         # Random I/O
            "xfersize": "8k",       # Transfer size
            "skew": 0,              # Load skewing
        }
    ],
)
```

#### `vdbench_filesystem_config()`
Configuration designed for filesystem testing with file structure definition.

**Configurable Parameters:**
```python
vdbench_filesystem_config(
    anchor="/vdbench-data/fs-test", # Root directory
    depth=2,                        # Directory depth
    width=4,                        # Files per directory
    files=10,                       # Total files
    size="1g",                      # Per-file size
    open_flags="o_direct",          # File opening flags
    default_threads=2,             # Default thread count
    elapsed=120,
    interval=60,
    default_rdpct=50,              # Default read percentage
    precreate_then_run=True,       # Two-phase execution
    precreate_elapsed=120,         # Precreate duration (must be >= 2*interval)
    precreate_interval=60,         # Precreate reporting interval
    precreate_iorate="max",        # Precreate I/O rate (cannot be 0 for filesystem)
    patterns=[                     # Custom workload patterns
        {
            "name": "random_mixed",
            "fileio": "random",
            "rdpct": 50,
            "xfersize": "8k",
            "threads": 2,
            "skew": 0,
        }
    ],
)
```

#### `vdbench_mixed_workload_config()`
Advanced configuration for mixed I/O pattern testing.

**Configurable Parameters:**
```python
vdbench_mixed_workload_config(
    patterns=[
        {"name": "sequential_read", "rdpct": 100, "seekpct": 0, "xfersize": "1m"},
        {"name": "random_read", "rdpct": 100, "seekpct": 100, "xfersize": "4k"},
        {"name": "mixed_rw", "rdpct": 70, "seekpct": 100, "xfersize": "64k"},
        {"name": "sequential_write", "rdpct": 0, "seekpct": 0, "xfersize": "1m"}
    ],
    lun="/vdbench-data/mixed",
    size="5g",
    threads=2,
    elapsed=300,
    interval=60
)
```

---

## Enhanced Parameter Support

### Comprehensive Parameter Handling

The VdbenchWorkload class now supports all standard Vdbench parameters with intelligent conversion between YAML and native Vdbench formats:

#### Storage Definition Parameters

**Block Storage (SD):**
- `lun` - Device or file path
- `size` - Storage size
- `threads` - Thread count
- `openflags` / `open_flags` - Opening flags (both formats supported)
- `align` - Alignment parameter (omitted if empty)
- `offset` - Offset parameter (omitted if empty)

**Filesystem Storage (FSD):**
- `anchor` - Root directory path
- `depth` - Directory depth
- `width` - Files per directory
- `files` - Total file count
- `size` - Per-file size
- `open_flags` - File opening flags

#### Workload Definition Parameters

**Block Workloads (WD):**
- `sd_id` - Storage definition reference
- `rdpct` - Read percentage (0-100)
- `seekpct` - Random seek percentage
- `xfersize` - Transfer size
- `threads` - Thread count
- `skew` - Load skewing parameter
- `openflags` - Opening flags

**Filesystem Workloads (FWD):**
- `fsd_id` - Filesystem storage reference
- `fileio` - I/O pattern (random/sequential)
- `rdpct` - Read percentage (0-100) - Cannot be used with fileio=sequential
- `xfersize` - Transfer size
- `threads` - Thread count
- `skew` - Load skewing parameter

#### Run Definition Parameters

**Block Runs:**
- `wd_id` - Workload definition reference
- `elapsed` - Duration in seconds
- `interval` - Reporting interval
- `iorate` - I/O rate limit
- `format` - Format control (yes/no)

**Filesystem Runs:**
- `fwd_id` - Filesystem workload reference
- `elapsed` - Duration in seconds
- `interval` - Reporting interval
- `iorate` - I/O rate limit (converted to fwdrate)
- `format` - Format control (yes/no)

### Intelligent Parameter Conversion

The system automatically:
- Detects storage type (block vs filesystem) and applies appropriate parameter handling
- Converts between different parameter naming conventions (`openflags` vs `open_flags`)
- Maps rate parameters correctly (`iorate` for block, `fwdrate` for filesystem)
- Handles both single and multiple workload/run definitions
- Supports precreate operations with proper format control
- Enforces vdbench constraints (e.g., excludes `rdpct` when `fileio=sequential`)
- Omits empty parameter values (e.g., skips `align=` and `offset=` when empty)

---

## Helper Functions

### Configuration Management

#### `create_temp_config_file(vdbench_config)`
Creates temporary YAML configuration file from dictionary.
- Generates temporary file with proper YAML formatting
- Returns file path for VdbenchWorkload initialization
- Automatic cleanup handled by test fixtures

#### `validate_vdbench_config(config_dict)`
Validates configuration dictionary structure.
- Checks for required sections and fields
- Validates ID references between sections
- Raises descriptive errors for invalid configurations

#### `auto_add_openflags_if_raw_device(config)`
Automatically adds `openflags=o_direct` for raw device paths.
- Detects LUN paths starting with `/dev/`
- Adds appropriate flags for block device access
- Preserves existing openflags settings

### Monitoring & Analysis

#### `monitor_vdbench_workload(workload, interval=30, duration=300)`
Continuous monitoring of running workload with metric collection.

**Returns:**
```python
[
    {
        "timestamp": 1234567890.123,
        "status": {...},
        "metrics": {
            "pod-name-1": {"avg_rate": 1234.5, "resp": 5.67},
            "pod-name-2": {"avg_rate": 1456.7, "resp": 4.23}
        }
    },
    ...
]
```

#### `create_vdbench_performance_report(metrics_history, output_file=None)`
Generates comprehensive performance analysis report.

**Report Structure:**
```python
{
    "summary": {
        "total_snapshots": 20,
        "duration": 300.0,
        "start_time": 1234567890.123,
        "end_time": 1234568190.123
    },
    "performance": {
        "peak_rate": 1500.0,
        "min_rate": 800.0,
        "average_rate": 1200.0,
        "average_response_time": 5.2
    }
}
```

#### `validate_vdbench_workload_health(workload, timeout=300)`
Validates workload health with comprehensive checks.
- Verifies running state and replica count
- Checks pod phases and readiness
- Provides detailed assertion errors for debugging

### Scenario Helpers

#### `create_vdbench_performance_scenario()`
Complete performance testing scenario setup.
```python
pvc, workload = create_vdbench_performance_scenario(
    vdbench_workload_factory,
    pvc_factory,
    vdbench_performance_config,
    storage_class="fast-ssd",
    pvc_size="50Gi",
    auto_start=True
)
```

#### `create_vdbench_block_scenario()`
Block device testing scenario with Block volume mode PVC.

#### `create_vdbench_rwx_scenario()`
ReadWriteMany scenario with automatic scaling for shared volume testing.

---

## Test Examples

### Basic Functionality Test

```python
def test_basic_workload_lifecycle(vdbench_workload_factory, vdbench_default_config):
    """Test complete workload lifecycle with basic configuration."""
    config = vdbench_default_config(
        size="2g",
        threads=2,
        elapsed=120
    )

    workload = vdbench_workload_factory(vdbench_config=config)

    # Lifecycle testing
    workload.start_workload()
    assert workload.is_running
    assert not workload.is_paused

    # Validate health
    validate_vdbench_workload_health(workload)

    # Pause and resume
    workload.pause_workload()
    assert workload.is_paused
    assert not workload.is_running

    workload.resume_workload()
    assert workload.is_running
    assert not workload.is_paused

    # Stop workload
    workload.stop_workload()
    assert not workload.is_running
```

### Performance Testing Example

```python
def test_comprehensive_performance_analysis(
    vdbench_workload_factory,
    vdbench_performance_config
):
    """Comprehensive performance testing with monitoring and reporting."""
    config = vdbench_performance_config(
        size="20g",
        threads=8,
        workloads=[
            {"id": 1, "sd_id": 1, "rdpct": 100, "xfersize": "128k"},  # Read test
            {"id": 2, "sd_id": 1, "rdpct": 0, "xfersize": "256k"}    # Write test
        ]
    )

    workload = vdbench_workload_factory(vdbench_config=config, auto_start=True)

    # === PHASE 1: BASELINE PERFORMANCE ===
    baseline_metrics = monitor_vdbench_workload(
        workload=workload,
        interval=30,
        duration=120
    )

    validate_vdbench_workload_health(workload)

    # === PHASE 2: SCALE UP FOR PEAK PERFORMANCE ===
    workload.scale_up_pods(6)

    peak_metrics = monitor_vdbench_workload(
        workload=workload,
        interval=25,
        duration=300
    )

    # === PHASE 3: SCALE DOWN FOR EFFICIENCY ===
    workload.scale_down_pods(2)

    efficiency_metrics = monitor_vdbench_workload(
        workload=workload,
        interval=30,
        duration=150
    )

    # === ANALYSIS AND REPORTING ===
    all_metrics = baseline_metrics + peak_metrics + efficiency_metrics

    final_report = create_vdbench_performance_report(
        all_metrics,
        output_file=f"/tmp/performance_report_{workload.deployment_name}.yaml"
    )

    # Performance assertions
    assert final_report["performance"]["peak_rate"] > 0
    assert final_report["summary"]["total_snapshots"] > 10
    assert len(all_metrics) > 15

    log.info(f"Peak Performance: {final_report['performance']['peak_rate']} ops/sec")
    log.info(f"Average Performance: {final_report['performance']['average_rate']} ops/sec")

    return final_report
```

### Block Device Testing

```python
def test_block_device_workload(vdbench_workload_factory, vdbench_block_config):
    """Test block device workload with direct I/O."""
    config = vdbench_block_config(
        lun="/dev/vdbench-device",
        threads=8,
        elapsed=300,
        interval=60,
        open_flags="o_direct",
        patterns=[
            {
                "name": "high_performance",
                "rdpct": 70,
                "seekpct": 100,
                "xfersize": "64k",
                "skew": 0,
            }
        ],
    )

    workload = vdbench_workload_factory(
        pvc=block_pvc,  # Block mode PVC
        vdbench_config=config,
        auto_start=True
    )

    # Monitor block device performance
    metrics = monitor_vdbench_workload(workload, interval=30, duration=300)

    # Validate specific block device behavior
    assert workload.volume_mode == "Block"
    assert "/dev/vdbench-device" in workload.vdbench_config_content

    validate_vdbench_workload_health(workload)
```

### Filesystem Testing

```python
def test_filesystem_workload(vdbench_workload_factory, vdbench_filesystem_config):
    """Test filesystem workload with file structure."""
    config = vdbench_filesystem_config(
        anchor="/vdbench-data/fs-test",
        depth=3,
        width=10,
        files=100,
        size="100m",
        default_threads=4,
        elapsed=300,
        interval=60,
        default_rdpct=50,
        precreate_then_run=True,
        precreate_elapsed=120,
        precreate_interval=60,
        precreate_iorate="max",
        patterns=[
            {
                "name": "mixed_workload",
                "fileio": "random",
                "rdpct": 50,
                "xfersize": "64k",
                "threads": 4,
                "skew": 0,
            }
        ],
    )

    workload = vdbench_workload_factory(vdbench_config=config, auto_start=True)

    # Verify filesystem configuration
    assert "fsd=fsd1" in workload.vdbench_config_content
    assert "fwd=fwd1" in workload.vdbench_config_content

    # Test filesystem operations
    time.sleep(60)  # Allow filesystem operations

    logs = workload.get_all_deployment_pod_logs()
    assert any("files=" in log for log in logs.values())

    validate_vdbench_workload_health(workload)
```

### Mixed Workload Pattern Testing

```python
def test_mixed_workload_patterns(vdbench_workload_factory, vdbench_mixed_workload_config):
    """Test multiple I/O patterns in sequence."""
    config = vdbench_mixed_workload_config(
        patterns=[
            {"name": "seq_read", "rdpct": 100, "seekpct": 0, "xfersize": "1m"},
            {"name": "rand_read", "rdpct": 100, "seekpct": 100, "xfersize": "4k"},
            {"name": "mixed_rw", "rdpct": 70, "seekpct": 100, "xfersize": "64k"},
            {"name": "seq_write", "rdpct": 0, "seekpct": 0, "xfersize": "1m"}
        ],
        elapsed=180  # 3 minutes per pattern
    )

    workload = vdbench_workload_factory(vdbench_config=config, auto_start=True)

    # Monitor each pattern phase
    pattern_results = {}
    for pattern in ["seq_read", "rand_read", "mixed_rw", "seq_write"]:
        phase_metrics = monitor_vdbench_workload(
            workload, interval=20, duration=180
        )
        pattern_results[pattern] = phase_metrics

    # Analyze pattern-specific performance
    for pattern, metrics in pattern_results.items():
        report = create_vdbench_performance_report(metrics)
        log.info(f"{pattern} - Avg Rate: {report['performance']['average_rate']}")

        assert report["performance"]["peak_rate"] > 0
        assert len(metrics) > 5
```

---

## Advanced Use Cases

### Resiliency Testing Integration

```python
def test_vdbench_with_platform_stress(
    vdbench_workload_factory,
    run_platform_stress,
    vdbench_performance_config
):
    """Test Vdbench workload resilience under platform stress."""
    # Setup performance workload
    config = vdbench_performance_config(size="10g", threads=6)
    workload = vdbench_workload_factory(vdbench_config=config, auto_start=True)

    # Start platform stress
    stress = run_platform_stress([constants.WORKER_MACHINE])

    try:
        # Monitor workload under stress
        stress_metrics = monitor_vdbench_workload(
            workload, interval=30, duration=600
        )

        # Validate workload maintains health under stress
        validate_vdbench_workload_health(workload)

        # Verify performance degradation is within acceptable limits
        report = create_vdbench_performance_report(stress_metrics)
        assert report["performance"]["min_rate"] > 100  # Minimum threshold

    finally:
        stress.stop()
```

### Multi-PVC Scenario

```python
def test_multiple_concurrent_workloads(vdbench_workload_factory, pvc_factory):
    """Test multiple concurrent Vdbench workloads."""
    workloads = []

    for i in range(3):
        pvc = pvc_factory(size="5Gi", storageclass="fast-ssd")
        config = vdbench_default_config(
            lun=f"/vdbench-data/test{i}",
            size="2g",
            threads=2
        )

        workload = vdbench_workload_factory(
            pvc=pvc,
            vdbench_config=config,
            auto_start=True
        )
        workloads.append(workload)

    # Monitor all workloads concurrently
    all_metrics = []
    for workload in workloads:
        metrics = monitor_vdbench_workload(workload, interval=20, duration=120)
        all_metrics.extend(metrics)
        validate_vdbench_workload_health(workload)

    # Analyze combined performance
    combined_report = create_vdbench_performance_report(all_metrics)
    assert combined_report["summary"]["total_snapshots"] > 15
```

---

## Best Practices

### Resource Management
- **PVC Sizing**: Ensure PVC size is adequate for your test data requirements
- **Thread Configuration**: Start with lower thread counts and scale based on system capacity
- **Monitoring Intervals**: Use appropriate intervals (20-30s) for meaningful metrics
- **Cleanup**: Leverage automatic cleanup through fixtures for consistent test environments

### Performance Testing
- **Baseline Establishment**: Always establish baseline performance before scaling tests
- **Gradual Scaling**: Scale up gradually to identify performance inflection points
- **Multiple Patterns**: Test various I/O patterns (sequential, random, mixed) for comprehensive analysis
- **Duration Planning**: Allow sufficient time for workload stabilization (60s minimum)

### Configuration Strategy
- **Start Simple**: Begin with default configurations and customize incrementally
- **Transfer Size Optimization**: Match transfer sizes to your storage system characteristics
- **Thread Tuning**: Optimize thread counts based on storage backend capabilities
- **Rate Limiting**: Use rate limiting for controlled performance testing

### Troubleshooting
- **Log Analysis**: Use `get_all_deployment_pod_logs()` for detailed error diagnosis
- **Health Validation**: Regular health checks with `validate_vdbench_workload_health()`
- **Resource Monitoring**: Monitor Kubernetes resource usage during long-running tests
- **Configuration Validation**: Validate configurations with `validate_vdbench_config()`

---

## Error Handling & Debugging

### Common Issues
1. **Pod Startup Failures**: Check PVC binding and storage class availability
2. **Configuration Errors**: Validate YAML structure and Vdbench syntax
3. **Performance Issues**: Verify storage backend capacity and network throughput
4. **Scaling Problems**: Ensure sufficient cluster resources for desired replica counts

### Debugging Tools
- Comprehensive logging throughout all operations
- Pod log capture on failures
- Status reporting with detailed state information
- Template fallback mechanisms for missing files

---

For additional information on Vdbench configuration options, refer to the [Oracle Vdbench Documentation](https://www.oracle.com/downloads/server-storage/vdbench-downloads.html).
