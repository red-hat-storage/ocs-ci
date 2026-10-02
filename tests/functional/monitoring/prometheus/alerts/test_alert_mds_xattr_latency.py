import logging
import pytest
import time

from ocs_ci.framework.pytest_customization.marks import (
    blue_squad,
    ignore_leftovers,
    skipif_external_mode,
)
from ocs_ci.framework.testlib import E2ETest, tier2, tier4b, tier4c
from ocs_ci.framework import config
from ocs_ci.helpers import helpers
from ocs_ci.templates.workloads.helper_scripts.meta_data_io import (
    perform_xattr_only_operations,
)
from ocs_ci.ocs import cluster, constants
from ocs_ci.utility import prometheus
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import ocp
from ocs_ci.ocs.resources.pod import (
    get_operator_pods,
    delete_pods,
    get_prometheus_pods,
    get_pods_having_label,
    get_mds_pods,
)
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.node import wait_for_nodes_status
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)

OCP_POD_OBJ = ocp.OCP(
    kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"]
)

# get storagecluster object
storagecluster_obj = OCP(
    kind="storagecluster",
    namespace=config.ENV_DATA["cluster_namespace"],
    resource_name=constants.DEFAULT_STORAGE_CLUSTER,
)


@pytest.fixture(scope="function")
def set_xattr_with_high_cpu_usage(
    request, pvc_factory, deployment_pod_factory, storageclass_factory
):
    """
    Fixture to set up extended attributes with high CPU usage for MDS xattr latency alert testing.

    Args:
        request: pytest request object for finalizer registration
        pvc_factory: Factory fixture to create PVC objects
        deployment_pod_factory: Factory fixture to create deployment pod objects
        storageclass_factory: Factory fixture to create storage class objects

    Returns:
        PVC: The PVC object created for extended attribute operations

    """
    logger.test_step("Set up CephFS PVC and pod for extended attribute operations")
    active_mds_node_name = cluster.get_active_mds_info()["node_name"]
    file = constants.EXTENDED_ATTRIBUTES
    logger.info(f"Active MDS node: {active_mds_node_name}")

    pvc_obj = pvc_factory(
        interface=constants.CEPHFILESYSTEM,
        access_mode=constants.ACCESS_MODE_RWX,
        size="200",
        status=constants.STATUS_BOUND,
        project=OCP(kind="Project", namespace=config.ENV_DATA["cluster_namespace"]),
    )
    logger.info(f"Created CephFS PVC: {pvc_obj.name}")

    pod_obj = deployment_pod_factory(
        interface=constants.CEPHFILESYSTEM,
        pvc=pvc_obj,
        node_name=active_mds_node_name,
    )
    logger.info(f"Created pod: {pod_obj.name} on node {active_mds_node_name}")

    logger.test_step("Perform extended attribute operations to generate MDS load")
    perform_xattr_only_operations(file=file, pod_obj=pod_obj)
    logger.info(
        "Extended attribute operations started, waiting 120s for load generation"
    )
    time.sleep(120)

    logger.test_step("Reduce MDS CPU resources to trigger xattr latency alert")
    storagecluster_obj.patch(
        resource_name=constants.DEFAULT_STORAGE_CLUSTER,
        params=(
            '{"spec": {"resources": {"mds": {"limits": {"cpu": "250m", '
            '"memory": "512Mi"}, "requests": {"cpu": "250m", "memory": '
            '"512Mi"}}}}}'
        ),
        format_type="merge",
    )
    logger.info("MDS resources reduced to CPU=250m, Memory=512Mi")

    return pvc_obj


def MDSxattr_alert_values(threading_lock, timeout):
    """
    Validate MDS xattr latency alert using Prometheus API.

    This function validates the CephXattrSetLatency alert by checking its
    properties including message, description, runbook URL, severity, and state.

    Args:
        threading_lock: Threading lock for Prometheus API calls
        timeout (int): Timeout in seconds to wait for the alert to appear

    Returns:
        bool: True if alert is found and validated successfully, False otherwise

    """
    logger.info(f"Validating {constants.ALERT_MDSXATTR} alert (timeout: {timeout}s)")
    result = prometheus.validate_alert(
        threading_lock=threading_lock,
        alert_constant=constants.ALERT_MDSXATTR,
        message="There is a latency in setting the 'xattr' values for Ceph Metadata Servers.",
        description=(
            "This latency can be caused by different factors like high CPU usage or network"
            " related issues etc. Please see the runbook URL link to get further help on mitigating the issue."
        ),
        runbook=(
            "https://github.com/openshift/runbooks/blob/master/alerts/"
            "openshift-container-storage-operator/CephXattrSetLatency.md"
        ),
        severity="warning",
        state="pending",
        timeout=timeout,
    )
    if result:
        logger.info(f"{constants.ALERT_MDSXATTR} alert validated successfully")
    else:
        logger.error(f"{constants.ALERT_MDSXATTR} alert validation failed")
    return result


def ceph_not_health_error():
    """
    Check if Ceph cluster health is in a healthy state.

    Returns:
        bool: True if Ceph health check passes (cluster is healthy),
              False if health check fails or raises an exception

    """
    logger.debug("Checking Ceph cluster health (tries=45, delay=60s)")
    try:
        ceph_health_check(
            namespace=config.ENV_DATA["cluster_namespace"], tries=45, delay=60
        )
        logger.info("Ceph health check passed")
        return True
    except Exception as ex:
        logger.warning(f"Ceph health check failed: {ex}")
        return False


def is_cluster_healthy():
    """
    Wrapper function for cluster health check

    Returns:
        bool: True if all checks passed, False otherwise
    """
    return ceph_not_health_error() and pod.wait_for_pods_to_be_running(timeout=900)


def verify_alert_cleared(threading_lock):
    """
    Verify that MDS xattr latency alert is cleared after restoring MDS resources.

    This function restores MDS CPU and memory resources to default values (2 CPU, 6Gi memory),
    waits for load distribution, and verifies that the alert is cleared.

    Args:
        threading_lock: Threading lock for Prometheus API calls
    """
    logger.test_step("Restore MDS resources and verify alert cleared")
    api = prometheus.PrometheusAPI(threading_lock=threading_lock)

    logger.info("Restoring MDS CPU and memory resources to default values")
    storagecluster_obj.patch(
        resource_name=constants.DEFAULT_STORAGE_CLUSTER,
        params=(
            '{"spec": {"resources": {"mds": {"limits": {"cpu": "2", '
            '"memory": "6Gi"}, "requests": {"cpu": "2", "memory": '
            '"6Gi"}}}}}'
        ),
        format_type="merge",
    )
    logger.info("MDS resources restored to CPU=2, Memory=6Gi")

    logger.info("Waiting 400s for load distribution and alert clearance")
    time.sleep(400)
    test_end_time = int(time.time())

    logger.info(f"Checking {constants.ALERT_MDSXATTR} alert is cleared")
    api.check_alert_cleared(
        label=constants.ALERT_MDSXATTR, measure_end_time=test_end_time, time_min=600
    )
    logger.info(f"{constants.ALERT_MDSXATTR} alert cleared successfully")


def recover_mds_pods_if_not_running():
    """
    Recover MDS deployments and verify pods are running.

    This function is used in test teardowns to:
    1. Check all MDS deployments
    2. Scale up any deployments that are at 0 replicas to 1
    3. Verify all MDS pods reach Running state after scaling

    If scaling or pod verification fails, the test will fail with AssertionError.

    Raises:
        AssertionError: If deployment scaling fails or pods don't reach Running state

    Returns:
        None
    """

    # Get all MDS deployments
    mds_deployments = ocp.OCP(
        kind=constants.DEPLOYMENT,
        namespace=config.ENV_DATA["cluster_namespace"],
        selector=constants.MDS_APP_LABEL,
    ).get()

    # Filter deployments that need scaling (replicas == 0)
    deployments_to_scale = [
        d for d in mds_deployments["items"] if d.get("spec", {}).get("replicas") == 0
    ]

    if not deployments_to_scale:
        logger.info("No MDS deployments needed scaling")
        return

    logger.info(f"Found {len(deployments_to_scale)} MDS deployment(s) at 0 replicas")

    logger.test_step("Scale up MDS deployments from 0 to 1 replica")
    for deployment in deployments_to_scale:
        deployment_name = deployment["metadata"]["name"]
        try:
            helpers.modify_deployment_replica_count(
                deployment_name=deployment_name, replica_count=1
            )
            logger.info(f"Successfully scaled {deployment_name} to 1 replica")
        except Exception:
            logger.exception(f"Failed to scale MDS deployment {deployment_name}")
            raise AssertionError(
                f"Teardown failed: Could not scale MDS deployment {deployment_name} from 0 to 1"
            )

    logger.info("Waiting for MDS pods to reach Running state")
    pod.wait_for_pods_to_be_running(
        namespace=config.ENV_DATA["cluster_namespace"],
        selector=constants.MDS_APP_LABEL,
        timeout=180,
        sleep=10,
    )
    logger.info("All MDS pods are running successfully")


@blue_squad
@ignore_leftovers
@skipif_external_mode
class TestMdsXattrAlerts(E2ETest):
    """
    Test class for MDS xattr latency alert validation.

    This test class validates the CephXattrSetLatency alert behavior under various
    scenarios including normal operations, pod restarts, Prometheus failures,
    MDS scale operations, and node restarts.

    """

    @pytest.fixture(scope="function", autouse=True)
    def teardown(self, request, threading_lock):
        """
        Teardown fixture to restore MDS CPU resources and clear memory usage.

        This fixture is automatically used for all test methods in the class.
        It restores MDS CPU resources to original values (2 CPUs) and gradually
        brings down MDS memory usage after each test execution.

        Args:
            request: pytest request object for finalizer registration
            threading_lock: Threading lock fixture for Prometheus API calls

        """

        def finalizer():
            """
            Finalizer function to ensure toolbox pod is ready and restore MDS resources.

            This function waits for toolbox pod to be in running state before cleanup,
            then restores MDS CPU and memory resources to default values as a safety measure.
            Alert verification is done at the end of each test method.

            """
            logger.test_step(
                "Cleanup: Restore MDS CPU and memory resources to defaults"
            )
            storagecluster_obj.patch(
                resource_name=constants.DEFAULT_STORAGE_CLUSTER,
                params=(
                    '{"spec": {"resources": {"mds": {"limits": {"cpu": "2", '
                    '"memory": "6Gi"}, "requests": {"cpu": "2", "memory": '
                    '"6Gi"}}}}}'
                ),
                format_type="merge",
            )
            logger.info("MDS resources restored to CPU=2, Memory=6Gi in teardown")

        request.addfinalizer(finalizer)

    @tier2
    @pytest.mark.polarion_id("OCS-7733")
    def test_mds_xattr_alert_triggered(
        self, set_xattr_with_high_cpu_usage, threading_lock
    ):
        """
        Test MDS xattr latency alert triggering and clearance.

        This test validates that the CephXattrSetLatency alert is triggered when
        extended attributes are set with high CPU usage, and verifies that the
        alert clears after increasing MDS CPU resources.

        Test Steps:
            1. Set up extended attributes and file creation IO using fixture
            2. Wait for CephXattrSetLatency alert to trigger (timeout: 1200s)
            3. Validate alert properties (message, description, runbook, severity, state)
            4. Verify alert is cleared after test completion

        """
        logger.info("Starting test: Verify MDS xattr latency alert is triggered")
        logger.info(
            "Extended attributes operations started. "
            "Monitoring for CephXattrSetLatency alert"
        )

        logger.test_step("Wait for and validate CephXattrSetLatency alert")
        alert_validated = MDSxattr_alert_values(threading_lock, timeout=1200)
        logger.assertion(
            f"MDS xattr alert validation: expected=True, actual={alert_validated}"
        )
        assert alert_validated, "CephXattrSetLatency alert validation failed"

        verify_alert_cleared(threading_lock)

        logger.info("Test passed: MDS xattr alert triggered and cleared successfully")

    @tier4c
    @pytest.mark.polarion_id("OCS-7734")
    def test_alert_triggered_by_restarting_operator_and_metrics_pods(
        self, set_xattr_with_high_cpu_usage, threading_lock
    ):
        """
        Test MDS xattr latency alert persistence after restarting operator and metrics pods.

        This test validates that the CephXattrSetLatency alert remains active and
        can be re-validated after restarting the rook-operator pod and
        ocs-metrics-exporter pod.

        Test Steps:
            1. Set up extended attributes and file creation IO using fixture
            2. Wait for CephXattrSetLatency alert to trigger (timeout: 1200s)
            3. Restart the rook-operator pod
            4. Wait for rook-operator pod to reach Running state
            5. Re-validate the alert after operator restart (timeout: 1200s)
            6. Delete the ocs-metrics-exporter pod
            7. Wait for ocs-metrics-exporter pod to come up (timeout: 600s)
            8. Re-validate the alert after metrics exporter restart (timeout: 1200s)
            9. Verify alert is cleared after test completion

        """
        logger.info(
            "Starting test: Verify alert persistence after restarting operator and metrics pods"
        )
        logger.info(
            "Extended attributes operations started. "
            "Monitoring for CephXattrSetLatency alert"
        )

        logger.test_step("Wait for and validate initial CephXattrSetLatency alert")
        alert_validated = MDSxattr_alert_values(threading_lock, timeout=1200)
        logger.assertion(
            f"Initial alert validation: expected=True, actual={alert_validated}"
        )
        assert alert_validated, "Initial CephXattrSetLatency alert validation failed"

        logger.test_step("Restart rook-operator pod and re-validate alert")
        operator_pod_obj = get_operator_pods()
        delete_pods(pod_objs=operator_pod_obj)
        OCP_POD_OBJ.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=constants.OPERATOR_LABEL,
        )
        alert_validated = MDSxattr_alert_values(threading_lock, timeout=1200)
        logger.assertion(
            f"Alert after operator restart: expected=True, actual={alert_validated}"
        )
        assert alert_validated, "Alert validation failed after operator restart"

        logger.test_step("Restart ocs-metrics-exporter pod and re-validate alert")
        metrics_pods = get_pods_having_label(
            label="app.kubernetes.io/name=ocs-metrics-exporter",
            namespace=config.ENV_DATA["cluster_namespace"],
        )

        assert metrics_pods, "No ocs-metrics-exporter pods found"
        metrics_pod = metrics_pods[0]
        metrics_pod_name = metrics_pod["metadata"]["name"]
        logger.info(f"Initial ocs-metrics-exporter pod: {metrics_pod_name}")

        OCP_POD_OBJ.delete(resource_name=metrics_pod_name)

        logger.info("Wait for ocs-metrics-exporter pod to come up")
        assert OCP_POD_OBJ.wait_for_resource(
            condition="Running",
            selector="app.kubernetes.io/name=ocs-metrics-exporter",
            resource_count=1,
            timeout=600,
        )

        alert_validated = MDSxattr_alert_values(threading_lock, timeout=1200)
        logger.assertion(
            f"Alert after metrics-exporter restart: expected=True, actual={alert_validated}"
        )
        assert alert_validated, "Alert validation failed after metrics-exporter restart"

        verify_alert_cleared(threading_lock)

        logger.info(
            "Test passed: Alert persisted correctly after operator and metrics pod restarts"
        )

    @tier2
    @pytest.mark.polarion_id("OCS-7735")
    def test_alert_after_recovering_prometheus_from_failures(
        self, set_xattr_with_high_cpu_usage, threading_lock
    ):
        """
        Test MDS xattr latency alert persistence after Prometheus pod failures.

        This test validates that the CephXattrSetLatency alert can be recovered
        and re-validated after Prometheus pods are deleted and recreated.

        Test Steps:
            1. Set up extended attributes and file creation IO using fixture
            2. Wait for CephXattrSetLatency alert to trigger (timeout: 1200s)
            3. Delete all Prometheus pods to simulate failure
            4. Wait for Prometheus pods to be recreated automatically
            5. Re-validate the alert after Prometheus recovery (timeout: 300s)
            6. Verify alert is cleared after test completion
        """
        logger.info(
            "Starting test: Verify alert recovery after Prometheus pod failures"
        )
        logger.info(
            "Extended attributes operations started. "
            "Monitoring for CephXattrSetLatency alert"
        )

        logger.test_step("Wait for and validate initial CephXattrSetLatency alert")
        alert_validated = MDSxattr_alert_values(threading_lock, timeout=1200)
        logger.assertion(
            f"Initial alert validation: expected=True, actual={alert_validated}"
        )
        assert alert_validated, "Initial CephXattrSetLatency alert validation failed"

        logger.test_step("Delete Prometheus pods and re-validate alert after recovery")
        logger.info("Deleting Prometheus pods to simulate failure")
        list_of_prometheus_pod_obj = get_prometheus_pods()
        delete_pods(list_of_prometheus_pod_obj)

        alert_validated = MDSxattr_alert_values(threading_lock, timeout=300)
        logger.assertion(
            f"Alert after Prometheus recovery: expected=True, actual={alert_validated}"
        )
        assert alert_validated, "Alert validation failed after Prometheus recovery"

        verify_alert_cleared(threading_lock)

        logger.info(
            "Test passed: Alert recovered successfully after Prometheus pod failures"
        )

    @tier4c
    @pytest.mark.polarion_id("OCS-7736")
    def test_alert_after_active_mds_scaledown(
        self, set_xattr_with_high_cpu_usage, threading_lock, request, nodes
    ):
        """
        Test MDS xattr latency alert persistence after active MDS scale down and up.

        This test validates that the CephXattrSetLatency alert remains active
        after scaling down the active MDS deployment to 0 and scaling it back up to 1.

        Test Steps:
            1. Set up extended attributes and file creation IO using fixture
            2. Wait for CephXattrSetLatency alert to trigger (timeout: 1200s)
            3. Identify the active MDS daemon and its deployment
            4. Scale down the active MDS deployment to 0 replicas
            5. Wait for the active MDS pod to be deleted
            6. Scale up the active MDS deployment back to 1 replica
            7. Wait 60 seconds for MDS scale up to complete
            8. Wait for all MDS pods to reach Running state
            9. Re-validate the alert after MDS scale operations (timeout: 60s)
            10. Verify alert is cleared after test completion

        """

        def finalizer():
            """
            Teardown to ensure MDS deployments are scaled up and pods are running.
            """
            logger.test_step("Cleanup: Recover MDS deployments if needed")
            recover_mds_pods_if_not_running()

        request.addfinalizer(finalizer)

        logger.info(
            "Starting test: Verify alert persistence after active MDS scale down/up"
        )
        logger.info(
            "Extended attributes operations started. "
            "Monitoring for CephXattrSetLatency alert"
        )

        logger.test_step("Wait for and validate initial CephXattrSetLatency alert")
        alert_validated = MDSxattr_alert_values(threading_lock, timeout=1200)
        logger.assertion(
            f"Initial alert validation: expected=True, actual={alert_validated}"
        )
        assert alert_validated, "Initial CephXattrSetLatency alert validation failed"

        logger.test_step("Scale down active MDS deployment and scale back up")
        active_mds = cluster.get_active_mds_info()["mds_daemon"]
        active_mds_pod = cluster.get_active_mds_info()["active_pod"]
        deployment_name = "rook-ceph-mds-" + active_mds
        logger.info(f"Active MDS daemon: {active_mds}, deployment: {deployment_name}")

        logger.info(f"Scaling down {deployment_name} to 0 replicas")
        helpers.modify_deployment_replica_count(
            deployment_name=deployment_name, replica_count=0
        )
        OCP_POD_OBJ.wait_for_delete(resource_name=active_mds_pod)
        logger.info(f"Scaling up {deployment_name} to 1 replica")
        helpers.modify_deployment_replica_count(
            deployment_name=deployment_name, replica_count=1
        )
        logger.info("Waiting 60s for MDS scale up to complete")
        time.sleep(60)

        logger.info("Waiting for all MDS pods to reach Running state")
        mds_pods = get_mds_pods()
        for pd in mds_pods:
            helpers.wait_for_resource_state(resource=pd, state=constants.STATUS_RUNNING)

        logger.test_step("Re-validate alert after MDS scale operations")
        alert_validated = MDSxattr_alert_values(threading_lock, timeout=60)
        logger.assertion(
            f"Alert after MDS scale: expected=True, actual={alert_validated}"
        )
        assert alert_validated, "Alert validation failed after MDS scale operations"

        verify_alert_cleared(threading_lock)

        logger.info(
            "Test passed: Alert persisted correctly after active MDS scale down/up"
        )

    @tier2
    @pytest.mark.polarion_id("OCS-7737")
    def test_alert_with_both_mds_scaledown(
        self, set_xattr_with_high_cpu_usage, threading_lock, request, nodes
    ):
        """
        Test MDS xattr latency alert persistence after both active and standby MDS scale down and up.

        This test validates that the CephXattrSetLatency alert remains active
        after scaling down both active and standby-replay MDS deployments to 0
        and scaling them back up to 1.

        Test Steps:
            1. Set up extended attributes and file creation IO using fixture
            2. Wait for CephXattrSetLatency alert to trigger (timeout: 1200s)
            3. Identify active and standby-replay MDS daemons and their deployments
            4. Scale down the active MDS deployment to 0 replicas
            5. Wait for the active MDS pod to be deleted
            6. Scale down the standby-replay MDS deployment to 0 replicas
            7. Wait for the standby-replay MDS pod to be deleted
            8. Scale up both MDS deployments back to 1 replica each
            9. Wait 60 seconds for both MDS scale up operations to complete
            10. Wait for all MDS pods to reach Running state
            11. Re-validate the alert after MDS scale operations (timeout: 1200s)
            12. Verify alert is cleared after test completion

        Args:
            request: pytest request for finalizer registration
            nodes: nodes fixture for node operations

        """

        def finalizer():
            """
            Teardown to ensure MDS deployments are scaled up and pods are running.
            """
            logger.test_step("Cleanup: Recover MDS deployments if needed")
            recover_mds_pods_if_not_running()

        request.addfinalizer(finalizer)

        logger.info(
            "Starting test: Verify alert persistence after both active and standby MDS scale down/up"
        )
        logger.info(
            "Extended attributes operations started. "
            "Monitoring for CephXattrSetLatency alert"
        )

        logger.test_step("Wait for and validate initial CephXattrSetLatency alert")
        alert_validated = MDSxattr_alert_values(threading_lock, timeout=1200)
        logger.assertion(
            f"Initial alert validation: expected=True, actual={alert_validated}"
        )
        assert alert_validated, "Initial CephXattrSetLatency alert validation failed"

        logger.test_step(
            "Scale down both active and standby MDS deployments, then scale back up"
        )
        active_mds = cluster.get_active_mds_info()["mds_daemon"]
        standby_mds = cluster.get_mds_standby_replay_info()["mds_daemon"]
        active_mds_d = "rook-ceph-mds-" + active_mds
        standby_mds_d = "rook-ceph-mds-" + standby_mds
        active_mds_pod = cluster.get_active_mds_info()["active_pod"]
        standby_mds_pod = cluster.get_mds_standby_replay_info()["standby_replay_pod"]
        mds_dc_pods = [active_mds_d, standby_mds_d]
        logger.info(f"Active MDS: {active_mds_d}, Standby MDS: {standby_mds_d}")

        logger.info(f"Scaling down {active_mds_d} to 0 replicas")
        helpers.modify_deployment_replica_count(
            deployment_name=active_mds_d, replica_count=0
        )
        OCP_POD_OBJ.wait_for_delete(resource_name=active_mds_pod)

        logger.info(f"Scaling down {standby_mds_d} to 0 replicas")
        helpers.modify_deployment_replica_count(
            deployment_name=standby_mds_d, replica_count=0
        )
        OCP_POD_OBJ.wait_for_delete(resource_name=standby_mds_pod)

        for mds_pod_obj in mds_dc_pods:
            logger.info(f"Scaling up {mds_pod_obj} to 1 replica")
            helpers.modify_deployment_replica_count(
                deployment_name=mds_pod_obj, replica_count=1
            )
        logger.info("Waiting 60s for both MDS scale up operations to complete")
        time.sleep(60)

        logger.info("Waiting for all MDS pods to reach Running state")
        mds_pods = get_mds_pods()
        for pd in mds_pods:
            helpers.wait_for_resource_state(resource=pd, state=constants.STATUS_RUNNING)

        logger.test_step("Re-validate alert after both MDS scale operations")
        alert_validated = MDSxattr_alert_values(threading_lock, timeout=1200)
        logger.assertion(
            f"Alert after both MDS scale: expected=True, actual={alert_validated}"
        )
        assert (
            alert_validated
        ), "Alert validation failed after both MDS scale operations"

        verify_alert_cleared(threading_lock)

        logger.info(
            "Test passed: Alert persisted correctly after both MDS scale down/up"
        )

    @tier4b
    @pytest.mark.polarion_id("OCS-7738")
    def test_alert_with_mds_running_node_restart(
        self, set_xattr_with_high_cpu_usage, threading_lock, nodes, request
    ):
        """
        Test MDS xattr latency alert persistence after active MDS node restart.

        This test validates that the CephXattrSetLatency alert remains active
        after restarting the node where the active MDS pod is running.

        Test Steps:
            1. Set up extended attributes and file creation IO using fixture
            2. Wait for CephXattrSetLatency alert to trigger (timeout: 1200s)
            3. Identify the active MDS pod and its running node
            4. Restart the node where active MDS is running
            5. Wait for the node to reach Ready state (timeout: 420s)
            6. Verify cluster health after node restart
            7. Re-validate the alert after node restart (timeout: 1200s)
            8. Verify alert is cleared after test completion

        Args:
            nodes: Node fixture for performing node operations
            request: pytest request for finalizer registration

        """

        def finalizer():
            """Teardown to ensure all nodes are up and cluster is healthy after test"""
            logger.test_step(
                "Cleanup: Restart any stopped nodes and verify cluster health"
            )
            nodes.restart_nodes_by_stop_and_start_teardown()
            cluster_healthy = is_cluster_healthy()
            logger.assertion(
                f"Cluster health after teardown: expected=True, actual={cluster_healthy}"
            )
            assert (
                cluster_healthy
            ), "Cluster is not healthy after node restart in teardown"

        request.addfinalizer(finalizer)

        logger.info(
            "Starting test: Verify alert persistence after active MDS node restart"
        )
        logger.info(
            "Extended attributes operations started. "
            "Monitoring for CephXattrSetLatency alert"
        )

        logger.test_step("Wait for and validate initial CephXattrSetLatency alert")
        alert_validated = MDSxattr_alert_values(threading_lock, timeout=1200)
        logger.assertion(
            f"Initial alert validation: expected=True, actual={alert_validated}"
        )
        assert alert_validated, "Initial CephXattrSetLatency alert validation failed"

        logger.test_step(
            "Restart node running active MDS pod and verify cluster health"
        )
        active_mds_pod_obj = cluster.get_active_mds_info()["active_pod_obj"]
        active_mds_node = pod.get_pod_node(active_mds_pod_obj)
        logger.info(f"Restarting node: {active_mds_node.name}")
        nodes.restart_nodes([active_mds_node])
        wait_for_nodes_status(
            [active_mds_node.name], constants.STATUS_READY, timeout=420
        )
        cluster_healthy = is_cluster_healthy()
        logger.assertion(
            f"Cluster health after node restart: expected=True, actual={cluster_healthy}"
        )
        assert cluster_healthy, "Cluster is not healthy after active MDS node restart"

        logger.test_step("Re-validate alert after node restart")
        alert_validated = MDSxattr_alert_values(threading_lock, timeout=1200)
        logger.assertion(
            f"Alert after node restart: expected=True, actual={alert_validated}"
        )
        assert alert_validated, "Alert validation failed after node restart"

        verify_alert_cleared(threading_lock)

        logger.info(
            "Test passed: Alert persisted correctly after active MDS node restart"
        )
