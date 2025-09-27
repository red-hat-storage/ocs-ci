# GOSBench Workload Management for NooBaa/ODF

GOSBench is a high-performance S3 benchmark tool that can be used to test NooBaa (Multi-Cloud Gateway) performance in OpenShift Data Foundation (ODF). This module provides comprehensive workload management capabilities for GOSBench testing.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [API Reference](#api-reference)
- [Configuration Options](#configuration-options)
- [Usage Examples](#usage-examples)
- [Advanced Usage](#advanced-usage)
- [Monitoring and Metrics](#monitoring-and-metrics)
- [Troubleshooting](#troubleshooting)
- [Best Practices](#best-practices)

## Overview

The GOSBench workload management system provides three main functions:

1. **Start Workload** - Deploy and configure GOSBench server and workers
2. **Modify Workload** - Update benchmark configuration dynamically
3. **Stop Workload** - Clean up all resources and stop the workload

The system automatically:
- Discovers NooBaa S3 endpoints
- Retrieves NooBaa credentials
- Creates Kubernetes resources (Deployments, Services, ConfigMaps, Secrets)
- Manages server/worker coordination
- Handles resource cleanup

## Features

✅ **Automatic NooBaa Integration** - Discovers S3 endpoints and credentials
✅ **Kubernetes Native** - Creates all necessary K8s resources
✅ **Dynamic Scaling** - Scale workers up/down during execution
✅ **Configuration Management** - Update benchmark parameters without restart
✅ **Status Monitoring** - Monitor pod status and deployment health
✅ **Error Handling** - Comprehensive error handling and cleanup
✅ **Flexible Configuration** - Support for custom benchmark scenarios
✅ **Resource Cleanup** - Automatic cleanup of all created resources

## Prerequisites

- OpenShift cluster with ODF/NooBaa deployed
- Access to `openshift-storage` namespace
- Proper RBAC permissions for creating Deployments, Services, ConfigMaps, Secrets
- NooBaa S3 service available and accessible

## Quick Start

### Simple Usage

```python
from ocs_ci.workloads.gosbench_workload import (
    start_gosbench_workload,
    stop_gosbench_workload
)

# Start workload with defaults
workload = start_gosbench_workload(
    workload_name="my-test",
    worker_replicas=5
)

# Run benchmark
results = workload.run_benchmark(timeout=1800)
print(f"Results: {results}")

# Stop workload
stop_gosbench_workload("my-test")
```

### Custom Configuration

```python
from ocs_ci.workloads.gosbench_workload import start_gosbench_workload

# Custom benchmark configuration
custom_config = {
    "benchmark": {
        "name": "custom-performance-test",
        "object": {
            "size": "10MiB",
            "count": 5000
        },
        "stages": [
            {"name": "ramp", "duration": "60s", "op": "none"},
            {"name": "write", "duration": "5m", "op": "put", "concurrency": 32},
            {"name": "read", "duration": "5m", "op": "get", "concurrency": 64},
            {"name": "cleanup", "duration": "2m", "op": "delete", "concurrency": 32}
        ]
    }
}

# Start with custom configuration
workload = start_gosbench_workload(
    workload_name="custom-test",
    benchmark_config=custom_config,
    worker_replicas=8
)
```

## API Reference

### Convenience Functions

#### `start_gosbench_workload()`

Start a GOSBench workload with specified configuration.

```python
def start_gosbench_workload(
    workload_name="gosbench",
    namespace=None,
    benchmark_config=None,
    worker_replicas=5,
    timeout=300
):
```

**Parameters:**
- `workload_name` (str): Name for the workload resources
- `namespace` (str): Kubernetes namespace (default: openshift-storage)
- `benchmark_config` (dict): Custom benchmark configuration
- `worker_replicas` (int): Number of worker pods
- `timeout` (int): Timeout for pod readiness in seconds

**Returns:** `GOSBenchWorkload` instance

#### `stop_gosbench_workload()`

Stop and cleanup a GOSBench workload.

```python
def stop_gosbench_workload(workload_name="gosbench", namespace=None):
```

**Parameters:**
- `workload_name` (str): Name of the workload to stop
- `namespace` (str): Kubernetes namespace

**Returns:** `bool` - True if stopped successfully

#### `modify_gosbench_config()`

Modify the configuration of a running workload.

```python
def modify_gosbench_config(
    workload_name="gosbench",
    namespace=None,
    benchmark_config=None
):
```

**Parameters:**
- `workload_name` (str): Name of the workload
- `namespace` (str): Kubernetes namespace
- `benchmark_config` (dict): New benchmark configuration

**Returns:** `bool` - True if configuration updated successfully

### GOSBenchWorkload Class

For advanced usage, use the `GOSBenchWorkload` class directly:

```python
from ocs_ci.workloads.gosbench_workload import GOSBenchWorkload

workload = GOSBenchWorkload(
    workload_name="advanced-test",
    namespace="openshift-storage"
)
```

#### Key Methods

- `start_workload(benchmark_config, worker_replicas, timeout)` - Start the workload
- `stop_workload()` - Stop and cleanup the workload
- `run_benchmark(timeout)` - Execute the benchmark
- `scale_workers(replicas)` - Scale worker deployment
- `update_workload_config(benchmark_config)` - Update configuration
- `get_workload_status()` - Get current status
- `wait_for_workload_ready(timeout)` - Wait for pods to be ready

## Configuration Options

### Benchmark Configuration Structure

```yaml
s3:
  endpoint: "https://s3-route-host"
  region: "us-east-1"
  access_key: "$AWS_ACCESS_KEY_ID"
  secret_key: "$AWS_SECRET_ACCESS_KEY"
  bucket: "benchmark-bucket"
  insecure_tls: false

benchmark:
  name: "test-name"
  object:
    size: "1MiB"      # Object size: 1KiB, 1MiB, 10MiB, 64MiB, etc.
    count: 10000      # Number of objects
  stages:
    - name: "ramp"
      duration: "30s"
      op: "none"
      concurrency: 1
    - name: "put"
      duration: "2m"
      op: "put"
      concurrency: 64
    - name: "get"
      duration: "2m"
      op: "get"
      concurrency: 64
    - name: "delete"
      duration: "1m"
      op: "delete"
      concurrency: 64
```

### Operation Types

- `none` - No operation (ramp/warmup)
- `put` - Write objects to S3
- `get` - Read objects from S3
- `delete` - Delete objects from S3
- `mixed` - Mixed read/write operations

### Object Sizes

- `1KiB`, `4KiB`, `16KiB` - Small objects (metadata intensive)
- `1MiB`, `4MiB`, `10MiB` - Medium objects (balanced)
- `64MiB`, `128MiB`, `1GiB` - Large objects (throughput focused)

## Usage Examples

### Example 1: Basic Performance Test

```python
from ocs_ci.workloads.gosbench_workload import start_gosbench_workload, stop_gosbench_workload

# Start basic workload
workload = start_gosbench_workload(
    workload_name="basic-perf",
    worker_replicas=4
)

# Check status
status = workload.get_workload_status()
print(f"Server pods: {len(status['server']['pods'])}")
print(f"Worker pods: {len(status['workers']['pods'])}")

# Run benchmark
try:
    results = workload.run_benchmark(timeout=1200)
    print("Benchmark completed successfully")
except Exception as e:
    print(f"Benchmark failed: {e}")

# Cleanup
stop_gosbench_workload("basic-perf")
```

### Example 2: Throughput Testing with Large Objects

```python
from ocs_ci.workloads.gosbench_workload import start_gosbench_workload

# Configuration for throughput testing
throughput_config = {
    "benchmark": {
        "name": "throughput-test",
        "object": {
            "size": "64MiB",
            "count": 500
        },
        "stages": [
            {"name": "upload", "duration": "10m", "op": "put", "concurrency": 16},
            {"name": "download", "duration": "10m", "op": "get", "concurrency": 32},
            {"name": "cleanup", "duration": "5m", "op": "delete", "concurrency": 16}
        ]
    }
}

workload = start_gosbench_workload(
    workload_name="throughput-test",
    benchmark_config=throughput_config,
    worker_replicas=6
)

# Run and monitor
results = workload.run_benchmark(timeout=2400)
workload.stop_workload()
```

### Example 3: IOPS Testing with Small Objects

```python
from ocs_ci.workloads.gosbench_workload import start_gosbench_workload

# Configuration for IOPS testing
iops_config = {
    "benchmark": {
        "name": "iops-test",
        "object": {
            "size": "4KiB",
            "count": 50000
        },
        "stages": [
            {"name": "write_iops", "duration": "5m", "op": "put", "concurrency": 128},
            {"name": "read_iops", "duration": "5m", "op": "get", "concurrency": 256},
            {"name": "mixed_iops", "duration": "3m", "op": "mixed", "concurrency": 192},
            {"name": "cleanup", "duration": "2m", "op": "delete", "concurrency": 64}
        ]
    }
}

workload = start_gosbench_workload(
    workload_name="iops-test",
    benchmark_config=iops_config,
    worker_replicas=10
)

results = workload.run_benchmark(timeout=1800)
workload.stop_workload()
```

### Example 4: Dynamic Scaling During Test

```python
from ocs_ci.workloads.gosbench_workload import GOSBenchWorkload

workload = GOSBenchWorkload(workload_name="scaling-test")

# Start with minimal workers
workload.start_workload(worker_replicas=2)

# Run initial test
print("Running with 2 workers...")
workload.run_benchmark(timeout=300)

# Scale up for intensive test
print("Scaling to 8 workers...")
workload.scale_workers(8)
workload.wait_for_workload_ready(timeout=120)

# Update config for intensive test
intensive_config = {
    "benchmark": {
        "object": {"size": "10MiB", "count": 2000},
        "stages": [
            {"name": "intensive", "duration": "8m", "op": "put", "concurrency": 64}
        ]
    }
}
workload.update_workload_config(intensive_config)

# Run intensive test
print("Running intensive test with 8 workers...")
workload.run_benchmark(timeout=1200)

# Cleanup
workload.stop_workload()
```

## Advanced Usage

### Custom Credentials

```python
from ocs_ci.workloads.gosbench_workload import GOSBenchWorkload

workload = GOSBenchWorkload(workload_name="custom-creds")

# Create secret with custom credentials
workload.create_credentials_secret(
    access_key_id="custom-access-key",
    secret_access_key="custom-secret-key"
)

# Start workload
workload.start_workload()
```

### Multi-Stage Testing

```python
# Configuration with multiple test phases
multi_stage_config = {
    "benchmark": {
        "name": "multi-stage-test",
        "object": {"size": "1MiB", "count": 10000},
        "stages": [
            # Ramp up
            {"name": "ramp", "duration": "60s", "op": "none"},

            # Write phase
            {"name": "write_phase", "duration": "5m", "op": "put", "concurrency": 32},

            # Read phase with higher concurrency
            {"name": "read_phase", "duration": "5m", "op": "get", "concurrency": 64},

            # Mixed workload
            {"name": "mixed_phase", "duration": "3m", "op": "mixed", "concurrency": 48},

            # Stress test with maximum concurrency
            {"name": "stress_phase", "duration": "2m", "op": "get", "concurrency": 128},

            # Cleanup
            {"name": "cleanup", "duration": "3m", "op": "delete", "concurrency": 32}
        ]
    }
}
```

### Error Handling and Retry Logic

```python
import time
from ocs_ci.workloads.gosbench_workload import GOSBenchWorkload

def run_benchmark_with_retry(workload_name, max_retries=3):
    workload = GOSBenchWorkload(workload_name=workload_name)

    for attempt in range(max_retries):
        try:
            # Start workload
            workload.start_workload(worker_replicas=5)

            # Wait for readiness
            workload.wait_for_workload_ready(timeout=300)

            # Run benchmark
            results = workload.run_benchmark(timeout=1800)

            print(f"Benchmark completed successfully on attempt {attempt + 1}")
            return results

        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}")

            # Cleanup before retry
            try:
                workload.stop_workload()
            except:
                pass

            if attempt < max_retries - 1:
                print(f"Retrying in 30 seconds...")
                time.sleep(30)
            else:
                print("All attempts failed")
                raise

    return None
```

## Monitoring and Metrics

### Status Monitoring

```python
workload = GOSBenchWorkload(workload_name="monitored-test")
workload.start_workload()

# Get detailed status
status = workload.get_workload_status()

print(f"Server deployment status: {status['server']['deployment']}")
print(f"Worker deployment status: {status['workers']['deployment']}")
print(f"Server pods: {status['server']['pods']}")
print(f"Worker pods: {status['workers']['pods']}")
print(f"ConfigMap: {status['config']}")
print(f"Secret: {status['secret']}")

# Check individual pod statuses
for pod in status['server']['pods']:
    print(f"Server pod {pod['name']}: {pod['status']}")

for pod in status['workers']['pods']:
    print(f"Worker pod {pod['name']}: {pod['status']}")
```

### Prometheus Metrics

GOSBench server exposes Prometheus metrics on port 2112 at `/metrics` endpoint:

```bash
# Port-forward to access metrics
oc port-forward svc/gosbench-server 2112:2112 -n openshift-storage

# Access metrics
curl http://localhost:2112/metrics
```

Key metrics:
- `gosbench_operations_total` - Total operations performed
- `gosbench_operation_duration_seconds` - Operation latency histogram
- `gosbench_bytes_transferred_total` - Total bytes transferred
- `gosbench_errors_total` - Total errors encountered

### Log Monitoring

```bash
# View server logs
oc logs -f deployment/gosbench-server -n openshift-storage

# View worker logs
oc logs -f deployment/gosbench-worker -n openshift-storage

# View all GOSBench logs
oc logs -f -l app=gosbench-server -n openshift-storage
oc logs -f -l app=gosbench-worker -n openshift-storage
```

## Troubleshooting

### Common Issues

#### 1. Pods Not Starting

```python
# Check pod status
status = workload.get_workload_status()
if status['server']['pods']:
    pod_name = status['server']['pods'][0]['name']
    # Describe pod for more details
    print(f"Check pod details: oc describe pod {pod_name} -n openshift-storage")
```

#### 2. S3 Endpoint Not Found

```python
# Check if S3 route exists
try:
    endpoint = workload.get_noobaa_s3_endpoint()
    print(f"S3 endpoint: {endpoint}")
except Exception as e:
    print(f"S3 endpoint error: {e}")
    # Check: oc get route s3 -n openshift-storage
```

#### 3. Credential Issues

```python
# Verify credentials
try:
    access_key, secret_key = workload.get_noobaa_credentials()
    print("Credentials retrieved successfully")
except Exception as e:
    print(f"Credential error: {e}")
    # Check: oc get secret noobaa-admin -n openshift-storage
```

#### 4. Benchmark Execution Fails

```python
# Check server pod logs
status = workload.get_workload_status()
if status['server']['pods']:
    server_pod = status['server']['pods'][0]['name']
    print(f"Check logs: oc logs {server_pod} -n openshift-storage")
```

### Debug Commands

```bash
# Check all GOSBench resources
oc get all -l app=gosbench-server -n openshift-storage
oc get all -l app=gosbench-worker -n openshift-storage

# Check ConfigMap
oc get configmap gosbench-config -n openshift-storage -o yaml

# Check Secret
oc get secret gosbench-aws -n openshift-storage -o yaml

# Check NooBaa status
oc get noobaa -n openshift-storage
oc get route s3 -n openshift-storage

# Port-forward for direct access
oc port-forward svc/gosbench-server 2000:2000 -n openshift-storage
```

## Best Practices

### Performance Testing

1. **Start Small**: Begin with small object sizes and low concurrency
2. **Gradual Scaling**: Increase load gradually to find limits
3. **Monitor Resources**: Watch CPU/Memory usage on NooBaa pods
4. **Network Considerations**: Consider network bandwidth limitations
5. **Storage Backend**: Understand underlying storage performance

### Resource Management

1. **Proper Cleanup**: Always call `stop_workload()` to clean up resources
2. **Namespace Isolation**: Use dedicated namespaces for large tests
3. **Resource Limits**: Set appropriate resource limits on worker pods
4. **Node Affinity**: Consider node placement for distributed testing

### Configuration

1. **Realistic Scenarios**: Design tests that match real-world usage
2. **Staged Testing**: Use multiple stages to simulate different phases
3. **Concurrency Tuning**: Adjust concurrency based on cluster capacity
4. **Duration vs Count**: Use duration for steady-state, count for fixed work

### Monitoring

1. **Baseline Measurements**: Establish performance baselines
2. **Trend Analysis**: Monitor performance over time
3. **Resource Correlation**: Correlate performance with resource usage
4. **Error Tracking**: Monitor and investigate error rates

### Example Test Suite

```python
def comprehensive_performance_test():
    """Comprehensive performance test suite."""

    test_configs = [
        {
            "name": "small-objects-iops",
            "config": {
                "benchmark": {
                    "object": {"size": "4KiB", "count": 10000},
                    "stages": [
                        {"name": "write", "duration": "3m", "op": "put", "concurrency": 64},
                        {"name": "read", "duration": "3m", "op": "get", "concurrency": 128},
                        {"name": "cleanup", "duration": "1m", "op": "delete", "concurrency": 32}
                    ]
                }
            },
            "workers": 8
        },
        {
            "name": "medium-objects-balanced",
            "config": {
                "benchmark": {
                    "object": {"size": "1MiB", "count": 5000},
                    "stages": [
                        {"name": "write", "duration": "4m", "op": "put", "concurrency": 32},
                        {"name": "read", "duration": "4m", "op": "get", "concurrency": 64},
                        {"name": "mixed", "duration": "2m", "op": "mixed", "concurrency": 48},
                        {"name": "cleanup", "duration": "2m", "op": "delete", "concurrency": 32}
                    ]
                }
            },
            "workers": 6
        },
        {
            "name": "large-objects-throughput",
            "config": {
                "benchmark": {
                    "object": {"size": "64MiB", "count": 200},
                    "stages": [
                        {"name": "write", "duration": "8m", "op": "put", "concurrency": 8},
                        {"name": "read", "duration": "8m", "op": "get", "concurrency": 16},
                        {"name": "cleanup", "duration": "4m", "op": "delete", "concurrency": 8}
                    ]
                }
            },
            "workers": 4
        }
    ]

    results = {}

    for test in test_configs:
        print(f"Running test: {test['name']}")

        try:
            workload = start_gosbench_workload(
                workload_name=test['name'],
                benchmark_config=test['config'],
                worker_replicas=test['workers']
            )

            result = workload.run_benchmark(timeout=2400)
            results[test['name']] = {
                "status": "success",
                "result": result
            }

        except Exception as e:
            results[test['name']] = {
                "status": "failed",
                "error": str(e)
            }

        finally:
            try:
                stop_gosbench_workload(test['name'])
            except:
                pass

    return results

# Run comprehensive test
results = comprehensive_performance_test()
for test_name, result in results.items():
    print(f"{test_name}: {result['status']}")
```

This comprehensive documentation provides everything needed to effectively use the GOSBench workload management system for NooBaa/ODF performance testing.
