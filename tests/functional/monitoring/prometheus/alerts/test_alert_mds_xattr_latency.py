import logging
import pytest
import time

from ocs_ci.framework.pytest_customization.marks import blue_squad
from ocs_ci.framework.testlib import E2ETest, tier2
from ocs_ci.framework import config
from ocs_ci.helpers import helpers
from ocs_ci.ocs import cluster, constants
from ocs_ci.utility import prometheus
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import ocp
from ocs_ci.ocs.resources.pod import (
    get_operator_pods,
    delete_pods,
    get_prometheus_pods,
    get_pods_having_label,
)
from ocs_ci.helpers.cephfs_stress_helpers import CephFSStressTestManager
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.pv import delete_released_pvs_in_sc
from ocs_ci.ocs.node import wait_for_nodes_status
from ocs_ci.ocs.exceptions import CommandFailed

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

    This fixture performs the following operations:
    1. Creates a PVC with CephFS interface and RWX access mode for setting extended attributes
       on multiple files in the MDS server
    2. Creates a deployment pod on the active MDS node with service account and SCC policy
    3. Copies helper_scripts/check_xattr.py to the deployment pod
    4. Executes the check_xattr.py script to set extended attributes on multiple directories
    5. Creates a storage class with SELinux security context for CephFS stress testing
    6. Creates additional PVC and CephFS stress job to increase CPU utilization in the cluster
    7. Submits the CephFS stress job with specified parallelism and file creation parameters

    Args:
        request: pytest request object for finalizer registration
        pvc_factory: Factory fixture to create PVC objects
        deployment_pod_factory: Factory fixture to create deployment pod objects
        storageclass_factory: Factory fixture to create storage class objects

    Yields:
        None: This fixture sets up the environment and cleans up resources in finalizer

    """
    log.info("setting extented attributes value for multiple files in MDS server ")
    active_mds_node_name = cluster.get_active_mds_info()["node_name"]
    file = constants.EXTENDED_ATTRIBUTES
    stress_mgr = CephFSStressTestManager(namespace=constants.DEFAULT_NAMESPACE)
    m_factor = "1,2,3,4"
    parallelism = 5
    completions = 5

    # Creating PVC to attach POD to it
    pvc_obj = pvc_factory(
        interface=constants.CEPHFILESYSTEM,
        access_mode=constants.ACCESS_MODE_RWX,
        size="200",
        status=constants.STATUS_BOUND,
        project=OCP(kind="Project", namespace=config.ENV_DATA["cluster_namespace"]),
    )
    # Create service_account to get privilege for deployment pods
    sa_obj = helpers.create_serviceaccount(pvc_obj.project.namespace)

    helpers.add_scc_policy(sa_name=sa_obj.name, namespace=pvc_obj.project.namespace)

    pod_obj = deployment_pod_factory(
        interface=constants.CEPHFILESYSTEM,
        pvc=pvc_obj,
        node_name=active_mds_node_name,
        sa_obj=sa_obj,
    )

    log.info("Copying check_xattr.py to fedora pod ")
    cmd = f"oc cp {file} {pod_obj.namespace}/{pod_obj.name}:/mnt/"
    helpers.run_cmd(cmd=cmd)
    log.info("check_xattr.py copied successfully ")
    log.info("Setting extended attributed from fedora pod ")
    cmd = (
        "bash -c 'cd /mnt; "
        "for i in {1..6}; do "
        'dir="my_test_dir${i}"; '
        'python3 check_xattr.py "$dir" 10000 100 > "${dir}.log" 2>&1 & '
        "sleep 5; "
        "done'"
    )
    pod_obj.exec_sh_cmd_on_pod(cmd)

    log.info(
        "Setting up cephfs stress job for increasing CPU utilization in the cluster"
    )

    # Create storageclass with security context
    sc_name = "ocs-storagecluster-cephfs-selinux-relabel"
    try:
        storage_class = storageclass_factory(
            sc_name=sc_name,
            interface=constants.CEPHFILESYSTEM,
            kernelMountOptions='context="system_u:object_r:container_file_t:s0"',
        )
        log.info(f"Storage class {sc_name} created successfully !")

    except CommandFailed as ecf:
        assert "AlreadyExists" in str(ecf)
        log.info(
            f"Cannot create two StorageClasses with same name !"
            f" Error message:  \n"
            f"{ecf}"
        )

    pvc_obj1 = pvc_factory(
        access_mode=constants.ACCESS_MODE_RWX,
        status=constants.STATUS_BOUND,
        project=OCP(kind="Project", namespace=constants.DEFAULT_NAMESPACE),
        storageclass=storage_class,
        size="200",
    )
    cephfs_stress_job_obj = stress_mgr.create_cephfs_stress_job(
        pvc_name=pvc_obj1.name,
        multiplication_factors=m_factor,
        parallelism=parallelism,
        completions=completions,
        base_file_count=7000,
        files_size=6,
        threads=16,
    )
    log.info(f"The CephFS-stress Job {cephfs_stress_job_obj.name} has been submitted")

    def finalizer():

        # delete_deployment_pods(pod_obj)

        job_obj = OCP(
            kind="Job",
            namespace=constants.DEFAULT_NAMESPACE,
        )
        job_obj.delete(resource_name="cephfs-stress-job")

        pvc_obj1.delete()
        pvc_obj1.ocp.wait_for_delete(resource_name=pvc_obj1.name)
        delete_released_pvs_in_sc(sc_name)
        log.info("All resources cleaned up successully")

    request.addfinalizer(finalizer)


def MDSxattr_alert_values(threading_lock, timeout):
    """
    Validate MDS xattr latency alert using Prometheus API.

    This function checks for the CephXattrSetLatency alert in Prometheus and validates
    its properties including message, description, runbook URL, severity, and state.

    Args:
        threading_lock: Threading lock object for thread-safe Prometheus API operations
        timeout (int): Timeout in seconds to wait for the alert to appear

    Returns:
        bool: True if alert is validated successfully with all expected properties,
              False if validation fails or alert is not found

    """
    MDSxattr_alert = constants.ALERT_MDSXATTR

    api = prometheus.PrometheusAPI(threading_lock=threading_lock)
    alert = api.wait_for_alert(name=MDSxattr_alert, state="pending", timeout=timeout)
    message = (
        "There is a latency in setting the 'xattr' values for Ceph Metadata Servers."
    )
    description = (
        "This latency can be caused by different factors like high CPU usage or network"
        " related issues etc. Please see the runbook URL link to get further help on mitigating the issue."
    )
    runbook = (
        "https://github.com/openshift/runbooks/blob/master/alerts/"
        "openshift-container-storage-operator/CephXattrSetLatency.md"
    )
    severity = "warning"
    state = ["pending"]
    try:
        prometheus.check_alert_list(
            label=MDSxattr_alert,
            msg=message,
            description=description,
            runbook=runbook,
            states=state,
            severity=severity,
            alerts=alert,
        )
        log.info("Alert verified successfully")
        return True
    except Exception:
        return False


def initiate_alert_clearance():
    """
    Initiate clearance of MDS xattr latency alert by increasing MDS CPU resources.

    This function patches the storage cluster to increase MDS CPU limits and requests
    from default values to 16 CPUs, which helps clear the CephXattrSetLatency alert
    by providing more resources to handle the xattr operations.

    Returns:
        None

    """
    log.info("Increase MDS CPU Resources")

    storagecluster_obj.patch(
        resource_name=constants.DEFAULT_STORAGE_CLUSTER,
        params='{"spec": {"resources": {"mds": {"limits": {"cpu": "16"}, "requests": {"cpu": "16"}}}}}',
        format_type="merge",
    )


@blue_squad
@tier2
class TestMdsXattrAlerts(E2ETest):
    """
    Test class for MDS xattr latency alert validation.

    This test class validates the CephXattrSetLatency alert behavior under various
    scenarios including normal operations, pod restarts, Prometheus failures,
    MDS scale operations, and node restarts.

    """

    @pytest.fixture(scope="function", autouse=True)
    def teardown(self, request):
        """
        Teardown fixture to restore MDS CPU resources and clear memory usage.

        This fixture is automatically used for all test methods in the class.
        It restores MDS CPU resources to original values (2 CPUs) and gradually
        brings down MDS memory usage after each test execution.

        Args:
            request: pytest request object for finalizer registration

        Returns:
            None

        """

        def finalizer():
            """
            Finalizer function to restore MDS resources and clear memory usage.

            This function:
            1. Patches the storage cluster to restore MDS CPU limits and requests to 2 CPUs
            2. Calls cluster function to gradually bring down MDS memory usage

            """
            log.info("Setting MDS CPU Resources back to original values")

            storagecluster_obj.patch(
                resource_name=constants.DEFAULT_STORAGE_CLUSTER,
                params='{"spec": {"resources": {"mds": {"limits": {"cpu": "2"}, "requests": {"cpu": "2"}}}}}',
                format_type="merge",
            )
            cluster.bring_down_mds_memory_usage_gradually()

        request.addfinalizer(finalizer)

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
            4. Initiate alert clearance by increasing MDS CPU resources to 16 CPUs
            5. Wait for 600 seconds for load distribution
            6. Verify alert is cleared within 300 seconds

        """
        api = prometheus.PrometheusAPI(threading_lock=threading_lock)

        log.info(
            "Setting extended attributes and file creation IO started in the background."
            " Script will look for CephXattrSetLatency  alert"
        )
        assert MDSxattr_alert_values(threading_lock, timeout=1200)

        log.info("Checking for clearance of alert")
        initiate_alert_clearance()
        # waiting for sometime for load distribution
        time.sleep(600)
        api.check_alert_cleared(
            label=constants.ALERT_MDSXATTR, measure_end_time=600, time_min=30
        )

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

    @pytest.mark.polarion_id("OCS-7736")
    def test_alert_after_active_mds_scaledown(
        self, set_xattr_with_high_cpu_usage, threading_lock
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

        """

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
        mds_pods = cluster.get_mds_pods()
        for pd in mds_pods:
            helpers.wait_for_resource_state(resource=pd, state=constants.STATUS_RUNNING)

        assert MDSxattr_alert_values(threading_lock, timeout=60)

    @pytest.mark.polarion_id("OCS-7737")
    def test_alert_with_both_mds_scaledown(
        self, set_xattr_with_high_cpu_usage, threading_lock
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

        """
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

        mds_pods = cluster.get_mds_pods()
        for pd in mds_pods:
            helpers.wait_for_resource_state(resource=pd, state=constants.STATUS_RUNNING)

        assert MDSxattr_alert_values(threading_lock, timeout=1200)

    @pytest.mark.polarion_id("OCS-7738")
    def test_alert_with_mds_running_node_restart(
        self, set_xattr_with_high_cpu_usage, threading_lock, nodes
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
            6. Wait for MDS pods to be rescheduled and reach Running state
            7. Re-validate the alert after node restart (timeout: 1200s)

        Args:
            nodes: Node fixture for performing node operations

        """
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

        assert MDSxattr_alert_values(threading_lock, timeout=1200)
