import logging
import pytest
import time

from concurrent.futures import ThreadPoolExecutor
from ocs_ci.framework.pytest_customization.marks import blue_squad
from ocs_ci.framework.testlib import E2ETest, tier2, ignore_leftovers, jira
from ocs_ci.framework import config
from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import cluster
from ocs_ci.ocs.node import (
    unschedule_nodes,
    drain_nodes,
    schedule_nodes,
    get_worker_nodes,
)
from ocs_ci.ocs.resources.pod import (
    get_mon_pods,
    get_operator_pods,
    get_osd_pods,
    delete_pods,
    get_prometheus_pods,
)
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.utility import prometheus

logger = logging.getLogger(__name__)


# sleep timer (in seconds) for scale up, resource deletion & alert verification
timer = 60
POD_OBJ = OCP(kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"])
state = constants.STATUS_RUNNING


@pytest.fixture(scope="function")
def run_metadata_io_with_cephfs(deployment_pod_factory):
    """
    This function facilitates
    1. Create PVC with Cephfs, access mode RWX
    2. Create dc pod with Fedora image
    3. Copy helper_scripts/meta_data_io.py to Fedora dc pod
    4. Run meta_data_io.py on fedora pod

    """
    logger.test_step("Identify worker nodes excluding MDS nodes")
    access_mode = constants.ACCESS_MODE_RWX
    file = constants.METAIO
    interface = constants.CEPHFILESYSTEM
    active_mds_node = cluster.get_active_mds_info()["node_name"]
    sr_mds_node = cluster.get_mds_standby_replay_info()["node_name"]
    worker_nodes = get_worker_nodes()
    logger.info(f"Active MDS node: {active_mds_node}")
    logger.info(f"Standby-replay MDS node: {sr_mds_node}")

    target_node = []
    logger.info("Verifying Ceph cluster health")
    ceph_health_check()

    for node in worker_nodes:
        if (node != active_mds_node) and (node != sr_mds_node):
            target_node.append(node)
    logger.info(f"Selected target node for pods: {target_node[0]}")

    logger.test_step("Create 3 CephFS pods and start metadata IO workload")
    for dc_pod in range(3):
        logger.info(f"Creating pod {dc_pod + 1}/3 with CephFS PVC")
        pod_obj = deployment_pod_factory(
            size="30",
            access_mode=access_mode,
            interface=interface,
            node_name=target_node[0],
        )
        logger.debug(f"Pod created: {pod_obj.name} in namespace {pod_obj.namespace}")

        logger.debug(f"Copying {file} to pod {pod_obj.name}")
        cmd = f"oc cp {file} {pod_obj.namespace}/{pod_obj.name}:/"
        helpers.run_cmd(cmd=cmd)
        logger.debug("meta_data_io.py copied successfully")

        logger.debug(f"Starting metadata IO on pod {pod_obj.name}")
        metaio_executor = ThreadPoolExecutor(max_workers=1)
        metaio_executor.submit(
            pod_obj.exec_sh_cmd_on_pod, command="python3 meta_data_io.py"
        )

    logger.info("All 3 pods created and metadata IO started in background")


@tier2
@blue_squad
@ignore_leftovers
@jira("DFBUGS-368")
class TestMdsMemoryAlerts(E2ETest):
    @pytest.fixture(scope="function", autouse=True)
    def teardown(self, request):
        def finalizer():
            """
            This function will call a function to clear the mds memory usage gradually

            """
            logger.test_step("Cleanup: Bring down MDS memory usage gradually")
            cluster.bring_down_mds_memory_usage_gradually()
            logger.info("MDS memory usage cleared successfully")

        request.addfinalizer(finalizer)

    def active_mds_alert_values(self, threading_lock):
        """
        This function verifies the prometheus alerts and compare details with the given alert values.
        If given alert values matched with the pulled alert values in prometheus alerts then it returns True.

        Returns:
            True: (bool) True --> if alert verified successfully.

        """
        cache_alert = constants.ALERT_MDSCACHEUSAGEHIGH
        logger.info(f"Validating {cache_alert} alert")

        api = prometheus.PrometheusAPI(threading_lock=threading_lock)
        logger.info(f"Waiting for {cache_alert} alert to be triggered (sleep={timer}s)")
        alerts = api.wait_for_alert(name=cache_alert, state="firing", sleep=timer)
        logger.info(f"Alert detected: {len(alerts) if alerts else 0} instances")

        active_mds = cluster.get_active_mds_info()["mds_daemon"]
        logger.info(f"Active MDS daemon: {active_mds}")

        message = f"High MDS cache usage for the daemon mds.{active_mds}."
        description = (
            f"MDS cache usage for the daemon mds.{active_mds} has exceeded above 95% of the requested value."
            f" Increase the memory request for mds.{active_mds} pod."
        )
        runbook = (
            "https://github.com/openshift/runbooks/blob/master/alerts/"
            "openshift-container-storage-operator/CephMdsCacheUsageHigh.md"
        )
        state = ["firing"]
        severity = "error"

        logger.debug(f"Expected message: {message}")
        logger.debug(f"Expected severity: {severity}")
        logger.debug(f"Expected state: {state}")

        prometheus.check_alert_list(
            label=cache_alert,
            msg=message,
            description=description,
            runbook=runbook,
            states=state,
            severity=severity,
            alerts=alerts,
        )
        logger.info(f"Alert {cache_alert} verified successfully")
        return True

    @pytest.mark.polarion_id("OCS-5570")
    def deprecated_test_mds_cache_alert_triggered(
        self, run_metadata_io_with_cephfs, threading_lock
    ):
        """
        This function verifies the mds cache alert triggered or not.

        """
        logger.info("Starting test: Verify MDS cache high usage alert is triggered")
        logger.info(
            "Metadata IO started in the background. Monitoring for MDS cache alert"
        )

        logger.test_step("Validate MDS cache high usage alert is triggered")
        alert_validated = self.active_mds_alert_values(threading_lock)
        logger.assertion(
            f"MDS cache alert validation: expected=True, actual={alert_validated}"
        )
        assert alert_validated, "MDS cache high usage alert validation failed"

        logger.info("Test passed: MDS cache alert triggered successfully")

    @pytest.mark.polarion_id("OCS-5571")
    def deprecated_test_mds_cache_alert_with_active_node_drain(
        self, run_metadata_io_with_cephfs, threading_lock
    ):
        """
        This function verifies the mds cache alert when the active mds running node drained.

        """
        logger.info(
            "Starting test: Verify MDS cache alert persists after draining active MDS node"
        )
        logger.info(
            "Metadata IO started in the background. Monitoring for MDS cache alert"
        )

        logger.test_step("Validate initial MDS cache high usage alert")
        alert_validated = self.active_mds_alert_values(threading_lock)
        logger.assertion(
            f"Initial alert validation: expected=True, actual={alert_validated}"
        )
        assert alert_validated, "Initial MDS cache alert validation failed"

        logger.test_step("Drain active MDS node and verify alert persists")
        node_name = cluster.get_active_mds_info()["node_name"]
        logger.info(f"Active MDS running on node: {node_name}")

        logger.info(f"Unscheduling node {node_name}")
        unschedule_nodes([node_name])
        logger.info(f"Node {node_name} unscheduled successfully")

        logger.info(f"Draining node {node_name}")
        drain_nodes([node_name])
        logger.info(f"Node {node_name} drained successfully")

        logger.info(f"Making node {node_name} schedulable again")
        schedule_nodes([node_name])
        logger.info(f"Node {node_name} scheduled successfully")

        logger.info(f"Waiting {timer}s before re-validating alert")
        time.sleep(timer)

        logger.test_step("Re-validate MDS cache alert after node drain")
        alert_validated = self.active_mds_alert_values(threading_lock)
        logger.assertion(
            f"Alert after node drain: expected=True, actual={alert_validated}"
        )
        assert alert_validated, "Alert validation failed after node drain"

        logger.info("Test passed: MDS cache alert persisted correctly after node drain")

    @pytest.mark.polarion_id("OCS-5572")
    def deprecated_test_alert_by_restarting_operator_and_ceph_pods(
        self, run_metadata_io_with_cephfs, threading_lock
    ):
        """
        This test function verifies the mds cache alert by
        1. Restarting the rook operator
        2. Deleting the mon pod running on the active mds node
        3. Deleting the OSD pod running on the active mds node

        """
        logger.info(
            "Metadata IO started in the background. Script will look for the MDS alert now."
        )
        assert self.active_mds_alert_values(threading_lock)

        active_mds_node_name = cluster.get_active_mds_info()["node_name"]
        logger.info("Restart the rook-operator pod")
        operator_pod_obj = get_operator_pods()
        delete_pods(pod_objs=operator_pod_obj)
        POD_OBJ.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=constants.OPERATOR_LABEL,
        )
        logger.info("Validating the alert after the rook-operator pod restart")
        assert self.active_mds_alert_values(threading_lock)

        logger.info("Find mon pod on active mds running node and delete it.")
        mon_pod_objs = get_mon_pods()
        for pod_obj in mon_pod_objs:
            mon_pod_running_node_name = pod_obj.data["spec"].get("nodeName")
            if mon_pod_running_node_name == active_mds_node_name:
                delete_pods([pod_obj])
        logger.info("Validating the alert after the mon pod restart")
        assert self.active_mds_alert_values(threading_lock)

        logger.info("Find OSD pod on active mds running node and delete it.")
        osd_pod_objs = get_osd_pods()
        for pod_obj in osd_pod_objs:
            osd_pod_running_node_name = pod_obj.data["spec"].get("nodeName")
            if osd_pod_running_node_name == active_mds_node_name:
                delete_pods([pod_obj])
        logger.info("Validating the alert after the OSD pod restart")
        assert self.active_mds_alert_values(threading_lock)

    @pytest.mark.polarion_id("OCS-5576")
    def deprecated_test_mds_cache_alert_after_recovering_prometheus_from_failures(
        self, run_metadata_io_with_cephfs, threading_lock
    ):
        """
        This test function verifies the mds cache alert and fails the prometheus.
        It also verifies the alert after recovering prometheus from failures.

        """
        assert self.active_mds_alert_values(threading_lock)
        logger.info("Bring down the prometheus")
        list_of_prometheus_pod_obj = get_prometheus_pods()
        delete_pods(list_of_prometheus_pod_obj)
        assert self.active_mds_alert_values(threading_lock)

    @pytest.mark.polarion_id("OCS-5577")
    def deprecated_test_mds_cache_alert_with_active_node_scaledown(
        self, run_metadata_io_with_cephfs, threading_lock
    ):
        """
        This test function verifies the mds cache alert with active mds scale down and up

        """
        logger.info(
            "Starting test: Verify MDS cache alert persists after active MDS scale down/up"
        )
        logger.info(
            "Metadata IO started in the background. Monitoring for MDS cache alert"
        )

        logger.test_step("Validate initial MDS cache high usage alert")
        alert_validated = self.active_mds_alert_values(threading_lock)
        logger.assertion(
            f"Initial alert validation: expected=True, actual={alert_validated}"
        )
        assert alert_validated, "Initial MDS cache alert validation failed"

        logger.test_step("Scale down active MDS deployment and scale back up")
        active_mds = cluster.get_active_mds_info()["mds_daemon"]
        active_mds_pod = cluster.get_active_mds_info()["active_pod"]
        deployment_name = "rook-ceph-mds-" + active_mds
        logger.info(f"Active MDS daemon: {active_mds}, deployment: {deployment_name}")

        logger.info(f"Scaling down {deployment_name} to 0 replicas")
        helpers.modify_deployment_replica_count(
            deployment_name=deployment_name, replica_count=0
        )
        POD_OBJ.wait_for_delete(resource_name=active_mds_pod)

        logger.info(f"Scaling up {deployment_name} to 1 replica")
        helpers.modify_deployment_replica_count(
            deployment_name=deployment_name, replica_count=1
        )
        logger.info(f"Waiting {timer}s for MDS scale up to complete")
        time.sleep(timer)

        logger.info("Waiting for all MDS pods to reach Running state")
        mds_pods = cluster.get_mds_pods()
        for pod in mds_pods:
            helpers.wait_for_resource_state(resource=pod, state=state)

        logger.test_step("Re-validate MDS cache alert after scale operations")
        alert_validated = self.active_mds_alert_values(threading_lock)
        logger.assertion(
            f"Alert after MDS scale: expected=True, actual={alert_validated}"
        )
        assert alert_validated, "Alert validation failed after MDS scale operations"

        logger.info(
            "Test passed: MDS cache alert persisted correctly after scale down/up"
        )

    @pytest.mark.polarion_id("OCS-5578")
    def deprecated_test_mds_cache_alert_with_sr_node_scaledown(
        self, run_metadata_io_with_cephfs, threading_lock
    ):
        """
        This test function verifies the mds cache alert with standby-replay mds scale down and up

        """
        logger.info(
            "Metadata IO started in the background. Script will look for the MDS alert now."
        )
        assert self.active_mds_alert_values(threading_lock)

        sr_mds = cluster.get_mds_standby_replay_info()["mds_daemon"]
        deployment_name = "rook-ceph-mds-" + sr_mds
        sr_mds_pod = cluster.get_mds_standby_replay_info()["standby_replay_pod"]
        helpers.modify_deployment_replica_count(
            deployment_name=deployment_name, replica_count=0
        )
        POD_OBJ.wait_for_delete(resource_name=sr_mds_pod)
        helpers.modify_deployment_replica_count(
            deployment_name=deployment_name, replica_count=1
        )
        time.sleep(timer)
        mds_pods = cluster.get_mds_pods()
        for pod in mds_pods:
            helpers.wait_for_resource_state(resource=pod, state=state)

        assert self.active_mds_alert_values(threading_lock)

    @pytest.mark.polarion_id("OCS-5579")
    def deprecated_test_mds_cache_alert_with_all_mds_node_scaledown(
        self, run_metadata_io_with_cephfs, threading_lock
    ):
        """
        This test function verifies the mds cache alert with both active and standby-replay mds scale down and up

        """
        logger.info(
            "Metadata IO started in the background. Script will look for the MDS alert now."
        )
        assert self.active_mds_alert_values(threading_lock)

        active_mds = cluster.get_active_mds_info()["mds_daemon"]
        sr_mds = cluster.get_mds_standby_replay_info()["mds_daemon"]
        active_mds_dc = "rook-ceph-mds-" + active_mds
        sr_mds_dc = "rook-ceph-mds-" + sr_mds
        active_mds_pod = cluster.get_active_mds_info()["active_pod"]
        sr_mds_pod = cluster.get_mds_standby_replay_info()["standby_replay_pod"]
        mds_dc_pods = [active_mds_dc, sr_mds_dc]

        logger.info(f"Scale down {active_mds_dc} to 0")
        helpers.modify_deployment_replica_count(
            deployment_name=active_mds_dc, replica_count=0
        )
        POD_OBJ.wait_for_delete(resource_name=active_mds_pod)

        logger.info(f"Scale down {sr_mds_dc} to 0")
        helpers.modify_deployment_replica_count(
            deployment_name=sr_mds_dc, replica_count=0
        )
        POD_OBJ.wait_for_delete(resource_name=sr_mds_pod)

        for mds_pod_obj in mds_dc_pods:
            logger.info(f"Scale up {mds_pod_obj} to 1")
            helpers.modify_deployment_replica_count(
                deployment_name=mds_pod_obj, replica_count=1
            )
        logger.info(
            f" Script will be in sleep for {timer} seconds to make sure both mds scale up completed."
        )
        time.sleep(timer)

        mds_pods = cluster.get_mds_pods()
        for pod in mds_pods:
            helpers.wait_for_resource_state(resource=pod, state=state)

        assert self.active_mds_alert_values(threading_lock)
