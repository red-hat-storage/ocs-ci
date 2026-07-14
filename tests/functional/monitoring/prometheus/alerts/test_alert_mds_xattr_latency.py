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

log = logging.getLogger(__name__)

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
    log.info("setting extended attributes value for multiple files in MDS server ")
    active_mds_node_name = cluster.get_active_mds_info()["node_name"]
    file = constants.EXTENDED_ATTRIBUTES

    # Creating PVC to attach POD to it
    pvc_obj = pvc_factory(
        interface=constants.CEPHFILESYSTEM,
        access_mode=constants.ACCESS_MODE_RWX,
        size="200",
        status=constants.STATUS_BOUND,
        project=OCP(kind="Project", namespace=config.ENV_DATA["cluster_namespace"]),
    )

    pod_obj = deployment_pod_factory(
        interface=constants.CEPHFILESYSTEM,
        pvc=pvc_obj,
        node_name=active_mds_node_name,
    )

    perform_xattr_only_operations(file=file, pod_obj=pod_obj)

    time.sleep(120)

    log.info("Reducing MDS CPU resources to trigger MDS xattr latency alert")
    storagecluster_obj.patch(
        resource_name=constants.DEFAULT_STORAGE_CLUSTER,
        params=(
            '{"spec": {"resources": {"mds": {"limits": {"cpu": "250m", '
            '"memory": "512Mi"}, "requests": {"cpu": "250m", "memory": '
            '"512Mi"}}}}}'
        ),
        format_type="merge",
    )

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
    return prometheus.validate_alert(
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


def ceph_not_health_error():
    """
    Check if Ceph cluster health is in a healthy state.

    Returns:
        bool: True if Ceph health check passes (cluster is healthy),
              False if health check fails or raises an exception

    """
    try:
        ceph_health_check(
            namespace=config.ENV_DATA["cluster_namespace"], tries=45, delay=60
        )
        return True
    except Exception as ex:
        log.warning(f"Ceph health check failed: {ex}")
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
    api = prometheus.PrometheusAPI(threading_lock=threading_lock)

    log.info("Restoring MDS CPU and memory resources to default values to clear alert")
    storagecluster_obj.patch(
        resource_name=constants.DEFAULT_STORAGE_CLUSTER,
        params=(
            '{"spec": {"resources": {"mds": {"limits": {"cpu": "2", '
            '"memory": "6Gi"}, "requests": {"cpu": "2", "memory": '
            '"6Gi"}}}}}'
        ),
        format_type="merge",
    )

    # waiting for sometime for load distribution
    time.sleep(400)
    test_end_time = int(time.time())
    api.check_alert_cleared(
        label=constants.ALERT_MDSXATTR, measure_end_time=test_end_time, time_min=600
    )


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
        log.info("No MDS deployments needed scaling")
        return

    log.info(f"Found {len(deployments_to_scale)} MDS deployment(s) at 0 replicas")

    # Scale up each deployment from 0 to 1
    for deployment in deployments_to_scale:
        deployment_name = deployment["metadata"]["name"]
        try:
            helpers.modify_deployment_replica_count(
                deployment_name=deployment_name, replica_count=1
            )
            log.info(f"Successfully scaled {deployment_name} to 1 replica")
        except Exception as e:
            log.error(f"Failed to scale MDS deployment {deployment_name}: {e}")
            raise AssertionError(
                f"Teardown failed: Could not scale MDS deployment {deployment_name} from 0 to 1"
            )

    pod.wait_for_pods_to_be_running(
        namespace=config.ENV_DATA["cluster_namespace"],
        selector=constants.MDS_APP_LABEL,
        timeout=180,
        sleep=10,
    )
    log.info("All MDS pods are running successfully")


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

            log.info(
                "Restoring MDS CPU and memory resources to default values in teardown"
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
        log.info(
            "Setting extended attributes and file creation IO started in the background."
            " Script will look for CephXattrSetLatency  alert"
        )
        assert MDSxattr_alert_values(threading_lock, timeout=1200)

        verify_alert_cleared(threading_lock)

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
        log.info(
            "Setting extended attributes and file creation IO started in the background."
            " Script will look for CephXattrSetLatency  alert"
        )
        assert MDSxattr_alert_values(threading_lock, timeout=1200)

        log.info("Restart the rook-operator pod")
        operator_pod_obj = get_operator_pods()
        delete_pods(pod_objs=operator_pod_obj)
        OCP_POD_OBJ.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=constants.OPERATOR_LABEL,
        )
        log.info("Validating the alert after the rook-operator pod restart")
        assert MDSxattr_alert_values(threading_lock, timeout=1200)

        log.info("Respin the ocs-metrics-exporter pod")
        metrics_pods = get_pods_having_label(
            label="app.kubernetes.io/name=ocs-metrics-exporter",
            namespace=config.ENV_DATA["cluster_namespace"],
        )

        assert metrics_pods, "No ocs-metrics-exporter pods found"
        metrics_pod = metrics_pods[0]
        metrics_pod_name = metrics_pod["metadata"]["name"]
        log.info(f"Initial ocs-metrics-exporter pod: {metrics_pod_name}")

        OCP_POD_OBJ.delete(resource_name=metrics_pod_name)

        log.info("Wait for ocs-metrics-exporter pod to come up")
        assert OCP_POD_OBJ.wait_for_resource(
            condition="Running",
            selector="app.kubernetes.io/name=ocs-metrics-exporter",
            resource_count=1,
            timeout=600,
        )

        log.info("Validating the alert after ocs-metrics-exporter pod restart")
        assert MDSxattr_alert_values(threading_lock, timeout=1200)

        verify_alert_cleared(threading_lock)

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

        log.info(
            "Setting extended attributes and file creation IO started in the background."
            " Script will look for CephXattrSetLatency  alert"
        )
        assert MDSxattr_alert_values(threading_lock, timeout=1200)

        log.info("Bring down the prometheus")
        list_of_prometheus_pod_obj = get_prometheus_pods()
        delete_pods(list_of_prometheus_pod_obj)

        assert MDSxattr_alert_values(threading_lock, timeout=300)

        verify_alert_cleared(threading_lock)

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
            recover_mds_pods_if_not_running()

        request.addfinalizer(finalizer)

        log.info(
            "Setting extended attributes and file creation IO started in the background."
            " Script will look for CephXattrSetLatency  alert"
        )
        assert MDSxattr_alert_values(threading_lock, timeout=1200)

        active_mds = cluster.get_active_mds_info()["mds_daemon"]
        active_mds_pod = cluster.get_active_mds_info()["active_pod"]
        deployment_name = "rook-ceph-mds-" + active_mds

        log.info(f"Scale down {deployment_name} to 0")
        helpers.modify_deployment_replica_count(
            deployment_name=deployment_name, replica_count=0
        )
        OCP_POD_OBJ.wait_for_delete(resource_name=active_mds_pod)
        log.info(f"Scale up {deployment_name} to 1")
        helpers.modify_deployment_replica_count(
            deployment_name=deployment_name, replica_count=1
        )
        log.info(
            " Script will be in sleep for 60 seconds to make sure mds scale up completed."
        )
        time.sleep(60)
        mds_pods = get_mds_pods()
        for pd in mds_pods:
            helpers.wait_for_resource_state(resource=pd, state=constants.STATUS_RUNNING)

        assert MDSxattr_alert_values(threading_lock, timeout=60)

        verify_alert_cleared(threading_lock)

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
            recover_mds_pods_if_not_running()

        request.addfinalizer(finalizer)

        log.info(
            "Setting extended attributes and file creation IO started in the background."
            " Script will look for CephXattrSetLatency  alert"
        )
        assert MDSxattr_alert_values(threading_lock, timeout=1200)

        active_mds = cluster.get_active_mds_info()["mds_daemon"]
        standby_mds = cluster.get_mds_standby_replay_info()["mds_daemon"]
        active_mds_d = "rook-ceph-mds-" + active_mds
        standby_mds_d = "rook-ceph-mds-" + standby_mds
        active_mds_pod = cluster.get_active_mds_info()["active_pod"]
        standby_mds_pod = cluster.get_mds_standby_replay_info()["standby_replay_pod"]
        mds_dc_pods = [active_mds_d, standby_mds_d]

        log.info(f"Scale down {active_mds_d} to 0")
        helpers.modify_deployment_replica_count(
            deployment_name=active_mds_d, replica_count=0
        )
        OCP_POD_OBJ.wait_for_delete(resource_name=active_mds_pod)

        log.info(f"Scale down {standby_mds_d} to 0")
        helpers.modify_deployment_replica_count(
            deployment_name=standby_mds_d, replica_count=0
        )
        OCP_POD_OBJ.wait_for_delete(resource_name=standby_mds_pod)

        for mds_pod_obj in mds_dc_pods:
            log.info(f"Scale up {mds_pod_obj} to 1")
            helpers.modify_deployment_replica_count(
                deployment_name=mds_pod_obj, replica_count=1
            )
        log.info(
            " Script will be in sleep for 60 seconds to make sure both mds scale up completed."
        )
        time.sleep(60)

        mds_pods = get_mds_pods()
        for pd in mds_pods:
            helpers.wait_for_resource_state(resource=pd, state=constants.STATUS_RUNNING)

        assert MDSxattr_alert_values(threading_lock, timeout=1200)

        verify_alert_cleared(threading_lock)

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
            nodes.restart_nodes_by_stop_and_start_teardown()
            assert (
                is_cluster_healthy()
            ), "Cluster is not healthy after node restart in teardown"

        request.addfinalizer(finalizer)

        log.info(
            "Setting extended attributes and file creation IO started in the background."
            " Script will look for CephXattrSetLatency  alert"
        )
        assert MDSxattr_alert_values(threading_lock, timeout=1200)

        active_mds_pod_obj = cluster.get_active_mds_info()["active_pod_obj"]
        log.info("Restart active mds running node")
        active_mds_node = pod.get_pod_node(active_mds_pod_obj)
        nodes.restart_nodes([active_mds_node])
        wait_for_nodes_status(
            [active_mds_node.name], constants.STATUS_READY, timeout=420
        )
        assert (
            is_cluster_healthy()
        ), "Cluster is not healthy after active MDS node restart"

        assert MDSxattr_alert_values(threading_lock, timeout=1200)

        verify_alert_cleared(threading_lock)
