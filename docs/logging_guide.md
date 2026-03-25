# OCS-CI Logging Guide for Contributors

This guide explains how to use logging effectively in the OCS-CI framework. Following these guidelines ensures consistent, readable logs that make debugging easier for everyone.

## Table of Contents

1. [Quick Start](#quick-start)
2. [Log Level Selection Guide](#log-level-selection-guide)
3. [Usage by Code Type](#usage-by-code-type)
4. [Common Patterns](#common-patterns)
5. [Anti-Patterns to Avoid](#anti-patterns-to-avoid)
6. [Special Topics](#special-topics)
7. [Quick Reference](#quick-reference)
8. [Summary](#summary)

---

## Quick Start

### Getting the Logger

All code should use the standard Python logging pattern:

```python
import logging

logger = logging.getLogger(__name__)
```

The framework automatically provides a custom `OCSCILogger` with extended log levels.

### Available Log Levels

From highest to lowest severity:

| Level | Value | Method | Use For |
|-------|-------|--------|---------|
| **CRITICAL** | 50 | `logger.critical()` | System failures, framework crashes |
| **ERROR** | 40 | `logger.error()` | Test failures, exceptions, errors |
| **WARNING** | 30 | `logger.warning()` | Deprecations, retries, potential issues |
| **TEST_STEP** | 25 | `logger.test_step()` | Major test workflow phases |
| **INFO** | 20 | `logger.info()` | General progress, operations |
| **ASSERTION** | 15 | `logger.assertion()` | Test validations and assertions |
| **DEBUG** | 10 | `logger.debug()` | Detailed diagnostic info |
| **AI_DATA** | 5 | `logger.ai_data()` | ML predictions, metrics, analysis |

---

## Log Level Selection Guide

### CRITICAL (50) - System Failures

**When to use:**
- Framework initialization failures
- Unrecoverable cluster state
- Critical resource exhaustion
- Infrastructure failures requiring immediate attention

**Examples:**

```python
# Framework initialization
try:
    initialize_framework()
except Exception as e:
    logger.critical(f"Framework initialization failed: {e}")
    raise

# Critical resource state
if not check_cluster_api_accessible():
    logger.critical("Cluster API is unreachable - cannot continue testing")
    sys.exit(1)

# OCP/OCS critical failures
if storage_cluster_state == "Failed":
    logger.critical(
        f"StorageCluster in Failed state: {get_cluster_conditions()}"
    )
```

**Output:**
```
CRITICAL - Framework initialization failed: Cannot load config from /path/to/config
CRITICAL - Cluster API is unreachable - cannot continue testing
CRITICAL - StorageCluster in Failed state: Phase=Failed, Message='Ceph cluster down'
```

---

### ERROR (40) - Test Failures & Exceptions

**When to use:**
- Test failures (caught by pytest)
- Exceptions during operations
- Resource creation/deletion failures
- Validation failures that block progress

**Examples:**

```python
# Resource creation failure
try:
    pvc = create_pvc(sc_name="ocs-storagecluster-ceph-rbd", size="10Gi")
except Exception as e:
    logger.error(f"Failed to create PVC: {e}")
    raise

# Timeout errors
if not wait_for_resource_state(pvc, "Bound", timeout=300):
    logger.error(
        f"PVC {pvc.name} did not reach Bound state after 300s. "
        f"Current state: {pvc.status.phase}"
    )
    raise TimeoutError("PVC creation timeout")

# Deployment failures
if deployment_failed:
    logger.error(
        f"Cluster deployment failed at stage '{current_stage}': {error_msg}"
    )
    logger.error(f"Deployment logs: {get_deployment_logs()}")
```

**Output:**
```
ERROR - Failed to create PVC: API server returned 500: Internal Server Error
ERROR - PVC test-pvc-rbd did not reach Bound state after 300s. Current state: Pending
ERROR - Cluster deployment failed at stage 'bootstrap': timeout waiting for bootstrap complete
```

---

### WARNING (30) - Potential Issues

**When to use:**
- Deprecated features/parameters
- Retry attempts
- Non-critical misconfigurations
- Performance concerns (not failures)
- Skipped optional operations

**Examples:**

```python
# Deprecation warnings
def old_function(param):
    logger.warning(
        "old_function() is deprecated and will be removed in v5.0. "
        "Use new_function() instead."
    )
    return new_function(param)

# Retry attempts
for attempt in range(1, max_retries + 1):
    try:
        return perform_operation()
    except TemporaryError as e:
        logger.warning(
            f"Operation failed (attempt {attempt}/{max_retries}): {e}. "
            f"Retrying in {retry_delay}s..."
        )
        time.sleep(retry_delay)

# Configuration issues
if not config.get("optimal_setting"):
    logger.warning(
        "optimal_setting not configured. Using default value which may "
        "impact performance. Consider setting in your ocsci-conf file."
    )

# Resource constraints
current_memory = get_available_memory()
if current_memory < recommended_memory:
    logger.warning(
        f"Available memory ({current_memory}GB) is below recommended "
        f"({recommended_memory}GB). Tests may run slower."
    )

# Skipped operations
if not managed_service:
    logger.warning("Skipping CephFS replica check on non-managed deployments")
    return
```

**Output:**
```
WARNING - old_function() is deprecated and will be removed in v5.0. Use new_function() instead.
WARNING - Operation failed (attempt 2/3): Connection timeout. Retrying in 5s...
WARNING - optimal_setting not configured. Using default value which may impact performance.
WARNING - Available memory (8GB) is below recommended (16GB). Tests may run slower.
```

---

### TEST_STEP (25) - Test Workflow Phases

**When to use:**
- Major phases in test execution
- Setup/teardown operations
- Deployment stages
- Workflow checkpoints

**Key features:**
- Automatic step numbering per logger
- Helps visualize test flow
- Makes logs scannable

**Examples:**

```python
# Test workflow
def test_pvc_snapshot_restore(self):
    """Test PVC snapshot creation and restoration"""

    logger.test_step("Create source PVC and write test data")
    source_pvc = create_pvc(size="5Gi")
    write_test_data(source_pvc, pattern="random", size="1Gi")
    original_checksum = calculate_checksum(source_pvc)

    logger.test_step("Create volume snapshot")
    snapshot = create_snapshot(source_pvc)
    wait_for_snapshot_ready(snapshot, timeout=300)

    logger.test_step("Restore snapshot to new PVC")
    restored_pvc = restore_from_snapshot(snapshot, size="5Gi")
    wait_for_pvc_bound(restored_pvc, timeout=300)

    logger.test_step("Verify data integrity in restored PVC")
    restored_checksum = calculate_checksum(restored_pvc)
    assert original_checksum == restored_checksum, "Data mismatch after restore"

    logger.test_step("Cleanup test resources")
    delete_pvc(restored_pvc)
    delete_snapshot(snapshot)
    delete_pvc(source_pvc)

# Setup/teardown
def setup_method(self):
    """Setup test environment"""

    logger.test_step("Create dedicated test namespace")
    self.namespace = create_namespace(name="test-mcg")

    logger.test_step("Deploy MCG resources")
    deploy_mcg_components(namespace=self.namespace)

    logger.test_step("Configure S3 test credentials")
    self.s3_creds = create_s3_credentials()

def teardown_method(self):
    """Cleanup test environment"""

    logger.test_step("Delete test buckets and objects")
    cleanup_s3_resources(self.s3_creds)

    logger.test_step("Remove MCG resources")
    delete_mcg_components(namespace=self.namespace)

    logger.test_step("Delete test namespace")
    delete_namespace(self.namespace)

# Deployment stages
def deploy_ocp_cluster(self):
    """Deploy OpenShift cluster"""

    logger.test_step("Generate cluster installation manifests")
    generate_manifests(cluster_name=self.cluster_name)

    logger.test_step("Deploy bootstrap node")
    deploy_bootstrap_node()

    logger.test_step("Deploy control plane nodes")
    deploy_control_plane(node_count=3)

    logger.test_step("Wait for bootstrap complete")
    wait_for_bootstrap_complete(timeout=1800)

    logger.test_step("Deploy worker nodes")
    deploy_worker_nodes(node_count=3)

    logger.test_step("Finalize cluster installation")
    finalize_installation()

# Complex multi-phase test
def test_cluster_upgrade(self):
    """Test ODF cluster upgrade from 4.15 to 4.16"""

    logger.test_step("Verify pre-upgrade cluster health")
    verify_cluster_health()
    verify_all_pods_running()

    logger.test_step("Backup current cluster configuration")
    backup = create_cluster_backup()

    logger.test_step("Update operator subscription channel")
    update_subscription(channel="stable-4.16")

    logger.test_step("Monitor operator upgrade progress")
    wait_for_operator_upgrade(timeout=1800)

    logger.test_step("Wait for all pods to be updated")
    wait_for_pod_rollout_complete(timeout=1800)

    logger.test_step("Verify post-upgrade cluster health")
    verify_cluster_health()
    verify_storage_functionality()

    logger.test_step("Run post-upgrade validation suite")
    run_smoke_tests()
```

**Output:**
```
TEST_STEP 1 - Create source PVC and write test data
TEST_STEP 2 - Create volume snapshot
TEST_STEP 3 - Restore snapshot to new PVC
TEST_STEP 4 - Verify data integrity in restored PVC
TEST_STEP 5 - Cleanup test resources
```

---

### INFO (20) - General Progress

**When to use:**
- Successful operations
- High-level progress milestones (starting, completion, major checkpoints)
- State changes
- Resource creation/deletion (successful)
- Configuration details
- Most day-to-day logging

**Note:** Use DEBUG for iteration-level details within loops. INFO should mark the start, completion, or major milestones of an operation, not each iteration.

**Examples:**

```python
# Resource operations
def create_pvc(name, size, sc_name):
    logger.info(
        f"Creating PVC '{name}': size={size}, storage_class={sc_name}"
    )
    pvc = PVC(name=name, size=size, storage_class=sc_name)
    pvc.create()

    logger.info(f"PVC '{name}' created successfully")
    return pvc

# High-level progress milestones
def wait_for_pods_ready(namespace, timeout=300):
    logger.info(
        f"Waiting for all pods in namespace '{namespace}' to be ready "
        f"(timeout: {timeout}s)"
    )

    start_time = time.time()
    while time.time() - start_time < timeout:
        pods = get_pods(namespace)
        ready_count = sum(1 for p in pods if p.is_ready())

        # Use DEBUG for iteration details
        logger.debug(
            f"Pod readiness check: {ready_count}/{len(pods)} pods ready"
        )

        if ready_count == len(pods):
            logger.info(f"All {len(pods)} pods ready")
            return True

        time.sleep(10)

    logger.warning(
        f"Timeout waiting for pods in '{namespace}': "
        f"{ready_count}/{len(pods)} ready after {timeout}s"
    )
    return False

# Configuration
def setup_cluster_monitoring(config):
    logger.info("Configuring cluster monitoring")
    logger.info(f"Retention period: {config['retention_days']} days")
    logger.info(f"Storage size: {config['storage_size']}")
    logger.info(f"Storage class: {config['storage_class']}")

    apply_monitoring_config(config)
    logger.info("Cluster monitoring configured successfully")

# State changes
def scale_deployment(deployment_name, replicas):
    current_replicas = get_deployment_replicas(deployment_name)
    logger.info(
        f"Scaling deployment '{deployment_name}': "
        f"{current_replicas} -> {replicas} replicas"
    )

    scale(deployment_name, replicas)
    logger.info(f"Deployment '{deployment_name}' scaled successfully")
```

**Output:**
```
INFO - Creating PVC 'test-pvc-rbd': size=10Gi, storage_class=ocs-storagecluster-ceph-rbd
INFO - PVC 'test-pvc-rbd' created successfully
INFO - Waiting for all pods in namespace 'openshift-storage' to be ready (timeout: 300s)
INFO - All 12 pods ready
INFO - Configuring cluster monitoring
INFO - Retention period: 7 days
INFO - Storage size: 100Gi
INFO - Storage class: ocs-storagecluster-ceph-rbd
INFO - Cluster monitoring configured successfully
INFO - Scaling deployment 'rook-ceph-mon': 3 -> 5 replicas
INFO - Deployment 'rook-ceph-mon' scaled successfully
```

---

### ASSERTION (15) - Test Validations

**When to use:**
- Before/after Python assertions
- Test checkpoint validations
- Expected vs actual comparisons
- Pass/fail validations

**Examples:**

```python
# Basic assertion logging
def test_pvc_creation(self):
    pvc = create_pvc(size="10Gi")

    logger.assertion(
        f"PVC status: expected='Bound', actual='{pvc.status}'"
    )
    assert pvc.status == "Bound", f"PVC not bound: {pvc.status}"

# Numeric comparisons
def test_performance_threshold(self):
    iops = measure_iops()
    min_iops = 1000

    logger.assertion(
        f"IOPS check: measured={iops}, minimum={min_iops}, "
        f"passed={iops >= min_iops}"
    )
    assert iops >= min_iops, f"IOPS below threshold: {iops} < {min_iops}"

# Multiple conditions
def test_cluster_health(self):
    health_checks = {
        "operators_ready": check_operators(),
        "storage_cluster_healthy": check_storage_cluster(),
        "ceph_healthy": check_ceph_health(),
        "all_pods_running": check_all_pods_ready(),
    }

    for check_name, result in health_checks.items():
        logger.assertion(f"{check_name}: {result}")
        assert result, f"Health check failed: {check_name}"

# List/collection validations
def test_pod_count(self):
    pods = get_pods_in_namespace("openshift-storage")
    expected_count = 12

    logger.assertion(
        f"Pod count: expected={expected_count}, actual={len(pods)}, "
        f"match={len(pods) == expected_count}"
    )
    assert len(pods) == expected_count, \
        f"Expected {expected_count} pods, found {len(pods)}"

# Validation with tolerance
def test_capacity_usage(self):
    usage_pct = get_cluster_capacity_usage()
    expected = 75.0
    tolerance = 5.0

    within_tolerance = abs(usage_pct - expected) <= tolerance
    logger.assertion(
        f"Capacity usage: measured={usage_pct}%, expected={expected}%, "
        f"tolerance=±{tolerance}%, passed={within_tolerance}"
    )
    assert within_tolerance, \
        f"Usage {usage_pct}% outside expected range {expected}±{tolerance}%"
```

**Output:**
```
ASSERTION - PVC status: expected='Bound', actual='Bound'
ASSERTION - IOPS check: measured=1250, minimum=1000, passed=True
ASSERTION - operators_ready: True
ASSERTION - storage_cluster_healthy: True
ASSERTION - Pod count: expected=12, actual=12, match=True
ASSERTION - Capacity usage: measured=73.5%, expected=75%, tolerance=±5%, passed=True
```

---

### DEBUG (10) - Detailed Diagnostics

**When to use:**
- Internal function flow
- Variable values during debugging
- API request/response details
- Detailed resource states
- Iteration-level details within loops (not completion/start)
- Helper function internals

**Note:** Use DEBUG for per-iteration updates in loops. Use INFO to mark when the overall operation starts or completes.

**Examples:**

```python
# Function flow
def create_and_attach_pvc(pod_name, pvc_name, size):
    logger.debug(f"Entering create_and_attach_pvc: pod={pod_name}, pvc={pvc_name}")

    logger.debug(f"Creating PVC with size={size}")
    pvc = create_pvc(pvc_name, size)

    logger.debug(f"PVC object created: {pvc.to_dict()}")

    logger.debug(f"Attaching PVC to pod {pod_name}")
    attach_pvc_to_pod(pod_name, pvc)

    logger.debug("Attachment successful")
    return pvc

# API interactions
def get_storage_cluster():
    logger.debug("Fetching StorageCluster resource")
    logger.debug(f"API endpoint: {API_BASE}/odf.openshift.io/v1/storageclusters")

    response = api_client.get(
        "/apis/odf.openshift.io/v1/storageclusters",
        namespace="openshift-storage"
    )

    logger.debug(f"API response status: {response.status_code}")
    logger.debug(f"API response body: {response.json()}")

    return response.json()

# Variable inspection
def calculate_required_nodes(capacity_gb, node_size_gb):
    logger.debug(f"Calculating required nodes: capacity={capacity_gb}GB, node_size={node_size_gb}GB")

    nodes_needed = math.ceil(capacity_gb / node_size_gb)
    logger.debug(f"Raw calculation: {capacity_gb} / {node_size_gb} = {nodes_needed}")

    # Add replica overhead
    with_replica = nodes_needed * 3
    logger.debug(f"With 3x replica: {with_replica}")

    # Add headroom
    final_count = math.ceil(with_replica * 1.2)
    logger.debug(f"With 20% headroom: {final_count}")

    return final_count

# Iteration-level details (use INFO for start/completion, DEBUG for iterations)
def wait_for_resource(resource_name, desired_state, timeout=300):
    logger.info(
        f"Waiting for resource '{resource_name}' to reach '{desired_state}' "
        f"(timeout: {timeout}s)"
    )

    start_time = time.time()
    iteration = 0

    while time.time() - start_time < timeout:
        iteration += 1
        current_state = get_resource_state(resource_name)

        # DEBUG for each iteration
        logger.debug(
            f"Wait iteration {iteration}: current_state={current_state}, "
            f"elapsed={int(time.time() - start_time)}s"
        )

        if current_state == desired_state:
            logger.info(
                f"Resource '{resource_name}' reached '{desired_state}' "
                f"after {int(time.time() - start_time)}s"
            )
            return True

        time.sleep(5)

    logger.warning(
        f"Timeout waiting for resource '{resource_name}': "
        f"expected='{desired_state}', actual='{current_state}', timeout={timeout}s"
    )
    return False

# Detailed resource state
def verify_storage_cluster():
    sc = get_storage_cluster()

    logger.debug(f"StorageCluster phase: {sc.status.phase}")
    logger.debug(f"StorageCluster conditions: {sc.status.conditions}")
    logger.debug(f"OSD count: {len(sc.status.osd_deployment_status)}")

    for osd in sc.status.osd_deployment_status:
        logger.debug(f"OSD {osd.name}: state={osd.state}, node={osd.node}")
```

**Output:**
```
DEBUG - Entering create_and_attach_pvc: pod=test-pod, pvc=test-pvc
DEBUG - Creating PVC with size=10Gi
DEBUG - PVC object created: {'kind': 'PersistentVolumeClaim', 'metadata': {...}}
DEBUG - Fetching StorageCluster resource
DEBUG - API response status: 200
DEBUG - Wait iteration 5: current_state=Pending, elapsed=25s
```

---

### AI_DATA (5) - ML/AI Information

**When to use:**
- Machine learning predictions
- AI-generated insights
- Model inference details
- Anomaly detection results
- Performance predictions
- Training/inference metrics

**Note:** This level is below DEBUG and requires explicit enabling. Only use for AI/ML features.

**Examples:**

```python
# Failure prediction
def predict_disk_failure(disk_id):
    features = extract_disk_features(disk_id)

    logger.ai_data(f"Model input features: {features}")

    prediction = ml_model.predict(features)
    probability = prediction['failure_probability']

    logger.ai_data(
        f"Disk failure prediction: disk={disk_id}, "
        f"probability={probability:.2%}, model=disk_health_v2"
    )

    if probability > 0.8:
        logger.warning(
            f"High disk failure probability detected: {disk_id} ({probability:.2%})"
        )

    return prediction

# Anomaly detection
def detect_performance_anomaly(metrics):
    baseline = get_baseline_metrics()
    z_score = calculate_z_score(metrics, baseline)

    logger.ai_data(
        f"Performance metrics: current={metrics}, baseline={baseline}, "
        f"z_score={z_score:.2f}"
    )

    if z_score > 2.0:
        logger.ai_data(
            f"Anomaly detected: z_score={z_score:.2f} exceeds threshold=2.0"
        )
        logger.warning(
            f"Performance anomaly: latency {z_score:.1f}σ above baseline"
        )

# Cluster scaling recommendation
def recommend_cluster_scale():
    current_utilization = get_cluster_utilization()
    predicted_peak = predict_peak_utilization()

    logger.ai_data(
        f"Utilization analysis: current={current_utilization:.1%}, "
        f"predicted_peak={predicted_peak:.1%}"
    )

    if predicted_peak > 0.9:
        recommendation = calculate_node_recommendation()
        logger.ai_data(
            f"Scale recommendation: add {recommendation['nodes']} nodes "
            f"(confidence={recommendation['confidence']:.2%})"
        )
        logger.info(
            f"Cluster scale recommended: add {recommendation['nodes']} nodes "
            f"to handle predicted load"
        )

# Test runtime prediction
def predict_test_runtime(test_name):
    historical_data = get_test_history(test_name)

    logger.ai_data(f"Historical runtimes: {historical_data}")

    prediction = time_series_model.predict(test_name)

    logger.ai_data(
        f"Test runtime prediction: test={test_name}, "
        f"predicted={prediction['duration']}min, "
        f"confidence_interval=±{prediction['std']}min, "
        f"model=runtime_predictor_v1"
    )

    return prediction
```

**Output:**
```
AI_DATA - Disk failure prediction: disk=sda, probability=85.3%, model=disk_health_v2
AI_DATA - Performance metrics: current={'latency': 150}, baseline={'latency': 50}, z_score=3.20
AI_DATA - Anomaly detected: z_score=3.20 exceeds threshold=2.0
AI_DATA - Scale recommendation: add 2 nodes (confidence=94.2%)
```

---

## Usage by Code Type

### Test Files (`tests/`)

Tests should use TEST_STEP and ASSERTION liberally to make test flow clear:

```python
import logging
import pytest
from ocs_ci.framework.testlib import tier1, ManageTest
from ocs_ci.ocs.resources.pvc import create_pvc, delete_pvc

logger = logging.getLogger(__name__)


@tier1
@pytest.mark.polarion_id("OCS-1234")
class TestPVCLifecycle(ManageTest):
    """Test PVC lifecycle operations"""

    def test_pvc_create_delete(self):
        """
        Test PVC creation and deletion

        Steps:
        1. Create PVC
        2. Verify PVC is bound
        3. Delete PVC
        4. Verify PVC is removed
        """
        pvc_name = "test-pvc-lifecycle"

        logger.test_step("Create PVC with RBD storage class")
        pvc = create_pvc(
            name=pvc_name,
            size="10Gi",
            storage_class="ocs-storagecluster-ceph-rbd"
        )

        logger.test_step("Verify PVC reaches Bound state")
        pvc.reload()
        logger.assertion(
            f"PVC status: expected='Bound', actual='{pvc.status}'"
        )
        assert pvc.status == "Bound", f"PVC not bound: {pvc.status}"

        logger.test_step("Delete PVC")
        delete_pvc(pvc)

        logger.test_step("Verify PVC is removed from cluster")
        deleted = verify_pvc_deleted(pvc_name, timeout=60)
        logger.assertion(
            f"PVC deletion: pvc={pvc_name}, deleted={deleted}"
        )
        assert deleted, f"PVC {pvc_name} still exists after deletion"
```

### Deployment Code (`ocs_ci/deployment/`)

Deployment code should use TEST_STEP for major phases, INFO for progress:

```python
import logging
from ocs_ci.deployment.helpers import create_manifest, run_installer

logger = logging.getLogger(__name__)


class OCPDeployment:
    """OpenShift cluster deployment"""

    def deploy(self):
        """Deploy OpenShift cluster"""

        logger.test_step("Validate deployment prerequisites")
        self._validate_prerequisites()

        logger.test_step("Generate installation manifests")
        manifest_dir = self._generate_manifests()
        logger.info(f"Manifests generated in: {manifest_dir}")

        logger.test_step("Deploy infrastructure")
        self._deploy_infrastructure()

        logger.test_step("Bootstrap cluster")
        self._bootstrap_cluster()

        logger.test_step("Complete installation")
        self._complete_installation()

        logger.info("Cluster deployment completed successfully")

    def _validate_prerequisites(self):
        """Validate prerequisites"""
        logger.info("Checking pull secret")
        if not self.pull_secret_exists():
            logger.error("Pull secret not found")
            raise FileNotFoundError("Missing pull secret")

        logger.info("Checking cloud credentials")
        if not self.credentials_valid():
            logger.error("Cloud credentials invalid or missing")
            raise ValueError("Invalid credentials")

        logger.debug(f"Using region: {self.region}")
        logger.debug(f"Using instance type: {self.instance_type}")

    def _deploy_infrastructure(self):
        """Deploy infrastructure resources"""
        logger.info("Creating VPC")
        self.vpc_id = self.cloud_provider.create_vpc()
        logger.info(f"VPC created: {self.vpc_id}")

        logger.info("Creating subnets")
        self.subnets = self.cloud_provider.create_subnets(self.vpc_id)
        logger.info(f"Created {len(self.subnets)} subnets")

        logger.debug(f"Subnet IDs: {[s.id for s in self.subnets]}")
```

### Helper Functions (`ocs_ci/helpers/`)

Helpers should use INFO for operations, DEBUG for details:

```python
import logging
from ocs_ci.ocs.resources.pod import get_pods
from ocs_ci.ocs import constants

logger = logging.getLogger(__name__)


def wait_for_pods_ready(namespace, pod_count=None, timeout=300):
    """
    Wait for pods to be ready in namespace

    Args:
        namespace (str): Namespace name
        pod_count (int): Expected pod count (optional)
        timeout (int): Timeout in seconds

    Returns:
        bool: True if all pods ready, False otherwise
    """
    logger.info(
        f"Waiting for pods in namespace '{namespace}' (timeout: {timeout}s)"
    )

    if pod_count:
        logger.debug(f"Expecting {pod_count} pods")

    start_time = time.time()
    iteration = 0

    while time.time() - start_time < timeout:
        iteration += 1
        pods = get_pods(namespace)

        ready_count = sum(1 for p in pods if p.is_ready())
        logger.debug(
            f"Iteration {iteration}: {ready_count}/{len(pods)} pods ready"
        )

        if pod_count and len(pods) != pod_count:
            logger.debug(
                f"Pod count mismatch: expected={pod_count}, actual={len(pods)}"
            )
            time.sleep(5)
            continue

        if ready_count == len(pods):
            elapsed = int(time.time() - start_time)
            logger.info(f"All {len(pods)} pods ready after {elapsed}s")
            return True

        time.sleep(5)

    logger.warning(
        f"Timeout waiting for pods in '{namespace}': "
        f"{ready_count}/{len(pods)} ready after {timeout}s"
    )
    return False


def get_cluster_capacity():
    """
    Get cluster storage capacity information

    Returns:
        dict: Capacity information
    """
    logger.debug("Fetching cluster capacity")

    storage_cluster = get_storage_cluster()
    logger.debug(f"StorageCluster name: {storage_cluster.name}")

    capacity_info = {
        'total': storage_cluster.status.capacity.total,
        'used': storage_cluster.status.capacity.used,
        'available': storage_cluster.status.capacity.available,
    }

    logger.debug(f"Capacity: {capacity_info}")

    usage_pct = (capacity_info['used'] / capacity_info['total']) * 100
    logger.info(f"Cluster capacity usage: {usage_pct:.1f}%")

    return capacity_info
```

### Utilities (`ocs_ci/utility/`)

Utilities should focus on INFO for user-facing operations:

```python
import logging
import sys
from ocs_ci.framework import config
from ocs_ci.ocs.resources.storage_cluster import get_storage_cluster

logger = logging.getLogger(__name__)


def report_cluster_version():
    """
    Report OCS/ODF cluster version information

    Command-line utility to display cluster versions
    """
    logger.info("Collecting cluster version information...")

    try:
        storage_cluster = get_storage_cluster()
    except Exception as e:
        logger.error(f"Failed to get StorageCluster: {e}")
        sys.exit(1)

    logger.debug(f"StorageCluster object: {storage_cluster.name}")

    version_info = {
        'ODF': storage_cluster.get_odf_version(),
        'Ceph': storage_cluster.get_ceph_version(),
        'NooBaa': storage_cluster.get_noobaa_version(),
    }

    logger.info("=== Cluster Version Information ===")
    for component, version in version_info.items():
        logger.info(f"{component:15s}: {version}")

    logger.debug("Version reporting complete")


def cleanup_test_resources(namespace):
    """
    Cleanup test resources in namespace

    Args:
        namespace (str): Namespace to clean
    """
    logger.info(f"Starting cleanup of namespace: {namespace}")

    logger.info("Deleting PVCs...")
    pvcs = get_all_pvcs(namespace)
    logger.info(f"Found {len(pvcs)} PVCs")

    for pvc in pvcs:
        logger.debug(f"Deleting PVC: {pvc.name}")
        try:
            delete_pvc(pvc)
        except Exception as e:
            logger.warning(f"Failed to delete PVC {pvc.name}: {e}")

    logger.info("Deleting pods...")
    pods = get_all_pods(namespace)
    logger.info(f"Found {len(pods)} pods")

    for pod in pods:
        logger.debug(f"Deleting pod: {pod.name}")
        try:
            delete_pod(pod)
        except Exception as e:
            logger.warning(f"Failed to delete pod {pod.name}: {e}")

    logger.info(f"Cleanup of namespace '{namespace}' completed")
```

### Resource Classes (`ocs_ci/ocs/resources/`)

Resource classes should use DEBUG extensively, INFO for state changes:

```python
import logging
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)


class PVC:
    """Represents a PersistentVolumeClaim"""

    def __init__(self, name, namespace, size, storage_class):
        self.name = name
        self.namespace = namespace
        self.size = size
        self.storage_class = storage_class

        logger.debug(
            f"PVC object initialized: name={name}, namespace={namespace}, "
            f"size={size}, sc={storage_class}"
        )

    def create(self):
        """Create the PVC"""
        logger.info(
            f"Creating PVC '{self.name}' in namespace '{self.namespace}'"
        )

        manifest = self._generate_manifest()
        logger.debug(f"PVC manifest: {manifest}")

        ocp = OCP(kind=constants.PVC, namespace=self.namespace)
        ocp.create(manifest)

        logger.info(f"PVC '{self.name}' created")

    def reload(self):
        """Reload PVC state from cluster"""
        logger.debug(f"Reloading PVC state: {self.name}")

        ocp = OCP(kind=constants.PVC, namespace=self.namespace)
        data = ocp.get(resource_name=self.name)

        self.status = data['status']['phase']
        logger.debug(f"PVC {self.name} status: {self.status}")

    def wait_for_bound(self, timeout=300):
        """Wait for PVC to reach Bound state"""
        logger.info(
            f"Waiting for PVC '{self.name}' to be bound (timeout: {timeout}s)"
        )

        start_time = time.time()
        while time.time() - start_time < timeout:
            self.reload()
            logger.debug(f"Current status: {self.status}")

            if self.status == constants.STATUS_BOUND:
                elapsed = int(time.time() - start_time)
                logger.info(
                    f"PVC '{self.name}' bound after {elapsed}s"
                )
                return True

            time.sleep(5)

        logger.error(
            f"PVC '{self.name}' did not reach Bound state after {timeout}s"
        )
        return False
```

---

## Common Patterns

### Pattern: Progressive Logging

Use different levels as operations progress:

```python
def complex_operation():
    """Example of progressive logging"""

    # High-level phase
    logger.test_step("Execute complex operation")

    # Operation details
    logger.info("Starting sub-operation A")
    logger.debug("Sub-operation A parameters: {...}")

    result_a = sub_operation_a()

    # Validation
    logger.assertion(f"Sub-operation A result: {result_a}")
    assert result_a == expected, "Operation A failed"

    # Continue
    logger.info("Starting sub-operation B")
    result_b = sub_operation_b()

    logger.info("Complex operation completed successfully")
```

### Pattern: Retry Logging

Log retries clearly:

```python
def retry_operation(max_attempts=3):
    """Operation with retry logic"""

    for attempt in range(1, max_attempts + 1):
        logger.debug(f"Attempt {attempt}/{max_attempts}")

        try:
            result = perform_operation()
            logger.info(f"Operation succeeded on attempt {attempt}")
            return result

        except TemporaryError as e:
            if attempt < max_attempts:
                logger.warning(
                    f"Operation failed (attempt {attempt}/{max_attempts}): {e}. "
                    f"Retrying in 5s..."
                )
                time.sleep(5)
            else:
                logger.error(
                    f"Operation failed after {max_attempts} attempts: {e}"
                )
                raise
```

### Pattern: Iteration Logging

Use INFO for start/completion, DEBUG for iteration details:

```python
def wait_for_condition(condition_func, timeout=300):
    """Wait for condition with proper iteration logging"""

    logger.info(f"Waiting for condition (timeout: {timeout}s)")

    start_time = time.time()
    iteration = 0

    while time.time() - start_time < timeout:
        iteration += 1

        # DEBUG for each iteration
        logger.debug(f"Checking condition (iteration {iteration})")

        if condition_func():
            elapsed = int(time.time() - start_time)
            logger.info(f"Condition met after {elapsed}s")
            return True

        time.sleep(5)

    logger.warning(f"Condition not met after {timeout}s")
    return False
```

### Pattern: TimeoutSampler Logging

Use TimeoutSampler for polling operations while avoiding repetitive logs:

```python
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.resources.pod import get_all_pods
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import TimeoutExpiredError

def wait_for_pods_ready(namespace, timeout=300):
    """
    Wait for all pods to be ready using TimeoutSampler.

    Demonstrates how to limit repetitive logs during polling operations.
    """
    logger.info(f"Waiting for pods in '{namespace}' to be ready (timeout: {timeout}s)")

    try:
        # TimeoutSampler automatically rate-limits exception logs to once per minute
        # Log only state changes, not every iteration
        last_pod_count = None
        last_ready_count = None

        for pod_list in TimeoutSampler(
            timeout=timeout,
            sleep=10,
            func=get_all_pods,
            namespace=namespace
        ):
            total_pods = len(pod_list)
            ready_pods = sum(
                1 for pod in pod_list
                if pod.data["status"]["phase"] == constants.STATUS_RUNNING
            )

            # Log only when counts change (avoid duplicate logs)
            if (total_pods, ready_pods) != (last_pod_count, last_ready_count):
                logger.debug(
                    f"Pod status: {ready_pods}/{total_pods} ready"
                )
                last_pod_count = total_pods
                last_ready_count = ready_pods

            # Check condition
            if total_pods > 0 and ready_pods == total_pods:
                logger.info(f"All {total_pods} pods are ready")
                return True

    except TimeoutExpiredError:
        logger.error(
            f"Timeout waiting for pods in '{namespace}' to be ready after {timeout}s"
        )
        # Log final state for debugging
        logger.error(f"Final status: {last_ready_count}/{last_pod_count} pods ready")
        raise
```

### Pattern: Conditional Logging

Use DEBUG for verbose details, INFO for summaries:

```python
def process_items(items, verbose=False):
    """Process items with optional verbose logging"""

    logger.info(f"Processing {len(items)} items")

    for i, item in enumerate(items):
        logger.debug(f"Processing item {i+1}/{len(items)}: {item.name}")

        result = process_item(item)

        if verbose or result.has_issues():
            logger.info(f"Item {item.name}: {result.status}")

    logger.info("All items processed")
```

### Pattern: Error Context

Provide context in error logs:

```python
def create_resource(config):
    """Create resource with error context"""

    try:
        logger.info(f"Creating resource: {config['name']}")
        resource = Resource(config)
        resource.create()
        logger.info("Resource created successfully")
        return resource

    except APIError as e:
        logger.error(
            f"API error creating resource '{config['name']}': {e}"
        )
        logger.error(f"Config: {config}")
        logger.error(f"API response: {e.response}")
        raise

    except Exception as e:
        logger.error(
            f"Unexpected error creating resource '{config['name']}': {e}",
            exc_info=True  # Include stack trace
        )
        raise
```

### Pattern: Step Numbering in Loops

Reset step counters for independent iterations:

```python
from ocs_ci.framework.custom_logger import reset_step_counts

def test_multiple_iterations(self):
    """Test with multiple independent iterations"""

    for iteration in range(3):
        logger.info(f"=== Iteration {iteration + 1}/3 ===")

        # Reset step counter for clean numbering
        reset_step_counts(__name__)

        logger.test_step("Setup iteration resources")
        setup_resources()

        logger.test_step("Run iteration workload")
        run_workload()

        logger.test_step("Validate iteration results")
        validate_results()

        logger.test_step("Cleanup iteration resources")
        cleanup_resources()
```

---

## Anti-Patterns to Avoid

### ❌ Don't: Excessive TEST_STEP Usage

```python
# TOO GRANULAR - clutters logs
def test_bad_example(self):
    logger.test_step("Import libraries")  # ❌
    logger.test_step("Define variables")  # ❌
    logger.test_step("Create PVC object")  # ❌
    pvc = PVC()
    logger.test_step("Set PVC name")  # ❌
    pvc.name = "test"
```

✅ **Do:** Use steps for meaningful phases

```python
def test_good_example(self):
    logger.test_step("Create and configure PVC")
    pvc = PVC(name="test", size="10Gi")

    logger.test_step("Deploy PVC to cluster")
    pvc.create()
```

### ❌ Don't: Redundant Messages

```python
# Redundant with function name
def deploy_cluster(self):
    logger.test_step("Deploy cluster")  # ❌ Adds no info
```

✅ **Do:** Add specific details

```python
def deploy_cluster(self):
    logger.test_step("Deploy 3-node cluster with ODF storage")  # ✅
```

### ❌ Don't: Logging Secrets

```python
# NEVER log credentials
logger.info(f"Using password: {password}")  # ❌❌❌
logger.debug(f"API key: {api_key}")  # ❌❌❌
logger.info(f"Config: {config}")  # ❌ May contain secrets
```

✅ **Do:** Sanitize sensitive data

```python
logger.info("Authentication configured")  # ✅
logger.debug(f"API endpoint: {endpoint}")  # ✅
logger.debug(f"Username: {username}")  # ✅ (username OK, not password)
```

### ❌ Don't: String Formatting Waste

```python
# Expensive string formatting even when DEBUG disabled
logger.debug(f"Large object: {expensive_operation()}")  # ❌
```

✅ **Do:** Use lazy formatting for DEBUG

```python
# Only formats if DEBUG enabled
if logger.isEnabledFor(logging.DEBUG):
    logger.debug(f"Large object: {expensive_operation()}")  # ✅
```

### ❌ Don't: Generic Messages

```python
logger.info("Processing")  # ❌ What processing?
logger.error("Failed")  # ❌ What failed?
logger.warning("Issue")  # ❌ What issue?
```

✅ **Do:** Be specific

```python
logger.info("Processing 50 PVC creation requests")  # ✅
logger.error("Failed to create StorageCluster: API timeout")  # ✅
logger.warning("Cluster utilization at 85% - approaching capacity")  # ✅
```

### ❌ Don't: Wrong Log Level

```python
# Using INFO for debug details
logger.info(f"Variable x = {x}, y = {y}, z = {z}")  # ❌

# Using ERROR for warnings
logger.error("Using default configuration")  # ❌

# Using DEBUG for important progress
logger.debug("Cluster deployment completed")  # ❌
```

✅ **Do:** Use appropriate levels

```python
logger.debug(f"Variable x = {x}, y = {y}, z = {z}")  # ✅
logger.warning("Using default configuration")  # ✅
logger.info("Cluster deployment completed")  # ✅
```

---

## Special Topics

### Assertions: Before or After?

**Best practice:** Log assertion BEFORE the assert statement

```python
# ✅ Recommended: Log before assert
logger.assertion(f"PVC status: expected='Bound', actual='{pvc.status}'")
assert pvc.status == "Bound", f"PVC not bound: {pvc.status}"

# ❌ Less useful: Log after assert (might not execute)
assert pvc.status == "Bound", f"PVC not bound: {pvc.status}"
logger.assertion(f"PVC status check passed")  # Never logs if assertion fails
```

**Why?** Logging before ensures the assertion context is captured even if the assert fails.

### Exception Logging

Python's logging module provides `logger.exception()` which automatically logs at ERROR level and includes the full traceback. This is the preferred method for logging exceptions.

#### Using logger.exception()

**Best practice:** Use `logger.exception()` in exception handlers

```python
# ✅ Recommended: Use logger.exception() in except blocks
try:
    risky_operation()
except Exception as e:
    logger.exception(f"Operation failed: {e}")
    raise

# Equivalent to:
try:
    risky_operation()
except Exception as e:
    logger.error(f"Operation failed: {e}", exc_info=True)
    raise
```

**Key points:**
- `logger.exception()` automatically includes the full traceback
- Should **only** be called from exception handlers (inside `except` blocks)
- Logs at ERROR level
- Always includes `exc_info=True` automatically

#### Exception Logging Patterns

**Pattern 1: Simple exception logging**

```python
def create_pvc(name, size):
    """Create PVC with exception handling"""
    try:
        pvc = PVC(name=name, size=size)
        pvc.create()
        logger.info(f"PVC '{name}' created successfully")
        return pvc
    except Exception as e:
        logger.exception(f"Failed to create PVC '{name}'")
        raise
```

**Pattern 2: Specific exception types**

```python
def deploy_cluster():
    """Deploy cluster with specific exception handling"""
    try:
        initialize_cluster()
        deploy_nodes()
        configure_storage()
    except TimeoutError as e:
        logger.exception(f"Timeout during cluster deployment: {e}")
        raise
    except APIError as e:
        logger.exception(f"API error during cluster deployment: {e}")
        logger.error(f"API response: {e.response}")
        raise
    except Exception as e:
        logger.exception(f"Unexpected error during cluster deployment: {e}")
        raise
```

**Pattern 3: Exception with context (no re-raise)**

```python
def cleanup_resources(resource_list):
    """Cleanup resources, continue on individual failures"""
    failed = []

    for resource in resource_list:
        try:
            delete_resource(resource)
            logger.info(f"Deleted resource: {resource.name}")
        except Exception as e:
            # Log exception but don't re-raise - continue cleanup
            logger.exception(
                f"Failed to delete resource '{resource.name}': {e}"
            )
            failed.append(resource.name)

    if failed:
        logger.warning(f"Failed to delete {len(failed)} resources: {failed}")

    return failed
```

**Pattern 4: DEBUG vs ERROR exception logging**

```python
def optional_operation():
    """Operation that may fail without impacting main flow"""
    try:
        perform_optional_task()
    except Exception as e:
        # Use DEBUG for non-critical failures
        logger.debug(f"Optional operation failed: {e}", exc_info=True)
        # Continue execution
```

#### When to Use exc_info=True vs logger.exception()

```python
# ✅ In except block: Use logger.exception()
try:
    operation()
except Exception as e:
    logger.exception("Operation failed")  # Preferred

# ✅ Outside except block: Use exc_info=True with logger.error()
def some_function():
    if error_condition:
        logger.error("Error detected", exc_info=True)

# ✅ For DEBUG level with traceback
try:
    operation()
except Exception as e:
    logger.debug("Operation failed", exc_info=True)  # Can't use .exception() for DEBUG
```

#### Complete Exception Handling Example

```python
def test_cluster_upgrade(self):
    """Test cluster upgrade with comprehensive error handling"""

    logger.test_step("Start cluster upgrade")

    try:
        # Pre-upgrade validation
        logger.info("Running pre-upgrade validation")
        validate_cluster_health()

        # Perform upgrade
        logger.info("Updating operator subscription")
        update_subscription(channel="stable-4.16")

        logger.info("Waiting for upgrade completion")
        wait_for_upgrade_complete(timeout=3600)

        # Post-upgrade validation
        logger.info("Running post-upgrade validation")
        validate_cluster_health()

        logger.info("Cluster upgrade completed successfully")

    except TimeoutError as e:
        logger.exception(f"Upgrade timed out: {e}")
        logger.error("Collecting must-gather for debugging")
        collect_must_gather()
        raise

    except ValidationError as e:
        logger.exception(f"Cluster validation failed: {e}")
        logger.error(f"Cluster state: {get_cluster_state()}")
        raise

    except Exception as e:
        logger.exception(f"Unexpected error during upgrade: {e}")
        logger.error("Attempting to collect diagnostic information")
        try:
            collect_diagnostics()
        except Exception as diag_error:
            logger.warning(f"Could not collect diagnostics: {diag_error}")
        raise
```

**Output:**
```
ERROR - Failed to create PVC 'test-pvc'
Traceback (most recent call last):
  File "test.py", line 123, in create_pvc
    pvc.create()
  File "pvc.py", line 45, in create
    response = api.post(...)
APIError: 500 Internal Server Error
```

### Performance-Sensitive Code

Avoid expensive operations in log messages:

```python
# ❌ Bad: Expensive operation runs even if DEBUG disabled
logger.debug(f"Data: {expensive_serialization(large_object)}")

# ✅ Good: Only runs if DEBUG enabled
if logger.isEnabledFor(logging.DEBUG):
    logger.debug(f"Data: {expensive_serialization(large_object)}")

# ✅ Alternative: Lazy evaluation
logger.debug("Data: %s", lambda: expensive_serialization(large_object))
```

### Multi-line Log Messages

Use proper formatting for readability:

```python
# ✅ Good: Multi-line with clear structure
logger.error(
    "Cluster validation failed:\n"
    f"  Operators ready: {operators_ready}\n"
    f"  Pods running: {pods_running}\n"
    f"  Storage healthy: {storage_healthy}\n"
    f"  Ceph status: {ceph_status}"
)

# ✅ Alternative: Multiple log calls
logger.error("Cluster validation failed:")
logger.error(f"  Operators ready: {operators_ready}")
logger.error(f"  Pods running: {pods_running}")
logger.error(f"  Storage healthy: {storage_healthy}")
```

### Structured Logging Context

Include context for easier log parsing:

```python
# ✅ Good: Structured information
logger.info(
    f"PVC operation: action=create, name={name}, size={size}, "
    f"storage_class={sc}, namespace={namespace}"
)

# Can be parsed/filtered easily:
# grep "action=create" logs.txt
# grep "storage_class=ocs-storagecluster-ceph-rbd" logs.txt
```

### Testing Your Logging

Verify logging in unit tests:

```python
def test_function_logging(caplog):
    """Test that function logs appropriately"""

    with caplog.at_level(logging.INFO):
        result = my_function()

    # Verify expected log messages
    assert "Starting operation" in caplog.text
    assert "Operation completed" in caplog.text

    # Verify log levels
    assert any(
        record.levelname == "TEST_STEP" and "Deploy resources" in record.message
        for record in caplog.records
    )
```

---

## Quick Reference

### When to Use Each Level

| Scenario | Level | Method |
|----------|-------|--------|
| Test phase starts | TEST_STEP | `logger.test_step()` |
| Assertion check | ASSERTION | `logger.assertion()` |
| Resource created successfully | INFO | `logger.info()` |
| Operation progress update | INFO | `logger.info()` |
| API request/response details | DEBUG | `logger.debug()` |
| Variable values during debugging | DEBUG | `logger.debug()` |
| Retry attempt | WARNING | `logger.warning()` |
| Deprecated feature used | WARNING | `logger.warning()` |
| Operation failed | ERROR | `logger.error()` |
| Exception caught (with traceback) | ERROR | `logger.exception()` |
| Test failed | ERROR | `logger.error()` |
| Framework crash | CRITICAL | `logger.critical()` |
| ML prediction made | AI_DATA | `logger.ai_data()` |

### Common Commands

```python
# Standard imports
import logging
logger = logging.getLogger(__name__)

# Custom levels
logger.test_step("Major test phase")
logger.assertion("expected='X', actual='Y'")
logger.ai_data("ML prediction: ...")

# Standard levels
logger.info("Operation progress")
logger.debug("Detailed diagnostics")
logger.warning("Potential issue")
logger.error("Operation failed")
logger.critical("System failure")

# Exception logging (use in except blocks)
try:
    risky_operation()
except Exception as e:
    logger.exception(f"Operation failed: {e}")  # Includes traceback
    raise

# Reset step counter (for loops)
from ocs_ci.framework.custom_logger import reset_step_counts
reset_step_counts(__name__)

# Check if level enabled (performance)
if logger.isEnabledFor(logging.DEBUG):
    logger.debug(f"Expensive: {expensive_call()}")
```

---

## Summary

1. **Use the right level**: TEST_STEP for phases, ASSERTION for validations, INFO for progress, DEBUG for details
2. **Use logger.exception()**: Always use in exception handlers to automatically capture tracebacks
3. **Be specific**: Include context, values, and outcomes in log messages
4. **Think about the reader**: Your logs should tell the story of what happened
5. **Don't over-log**: Too many logs hide important information
6. **Never log secrets**: Sanitize sensitive data
7. **Test your logging**: Verify logs provide useful debugging information

Following these guidelines helps create logs that are:
- **Scannable** - Easy to find important information
- **Debuggable** - Provide enough context to diagnose issues
- **Consistent** - Follow predictable patterns across the codebase
- **Actionable** - Clear what succeeded, failed, or needs attention
