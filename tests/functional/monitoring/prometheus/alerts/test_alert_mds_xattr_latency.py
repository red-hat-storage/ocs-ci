import logging
import pytest
import time

from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import E2ETest, tier2
from ocs_ci.framework import config
from ocs_ci.helpers import helpers
from ocs_ci.ocs import cluster, constants
from ocs_ci.utility import prometheus
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import (
    delete_deployment_pods,
    get_operator_pods,
    delete_pods,
    get_prometheus_pods,
    get_pods_having_label,
)
from ocs_ci.helpers.cephfs_stress_helpers import CephFSStressTestManager
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.node import wait_for_nodes_status
from ocs_ci.ocs.exceptions import CommandFailed

log = logging.getLogger(__name__)

OCP_POD_OBJ = OCP(kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"])

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
    This function facilitates
    1. Create Pod and PVC with Cephfs, access mode RWX for setting extended atrributed
       for multiple files in MDS server
    2. Copy helper_scripts/check_xattr.py to deployment pod
    3. Create pvc's and deployment pod's with Fedora image for running file creator IO for
       increasing CPU utilization in the cluster.
    4. Copy helper_scripts/file_creator_io.py to Fedora pods
    5. Run file_creator_io.py on fedora pods

    """
    log.info("setting extented attributes value for multiple files in MDS server ")
    active_mds_node_name = cluster.get_active_mds_info()["node_name"]
    file = constants.EXTENTDED_ATTRIBUTES
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
    sa_name = helpers.create_serviceaccount(pvc_obj.project.namespace)

    helpers.add_scc_policy(sa_name=sa_name.name, namespace=pvc_obj.project.namespace)
    pod_obj = helpers.create_pod(
        interface_type=constants.CEPHFILESYSTEM,
        pvc_name=pvc_obj.name,
        namespace=pvc_obj.project.namespace,
        sa_name=sa_name.name,
        node_name=active_mds_node_name,
        deployment=True,
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

        delete_deployment_pods(pod_obj)
        job_obj = OCP(
            kind="Job",
            namespace=constants.DEFAULT_NAMESPACE,
        )
        job_obj.delete(resource_name="cephfs-stress-job")

    request.addfinalizer(finalizer)


def MDSxattr_alert_values(threading_lock, timeout):
    """
    This function validates the mds alert using prometheus api
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


def initiate_alert_clearanace():
    """
    This function initiates the clerance of mds alert
    """
    log.info("Increase MDS CPU Resources")

    storagecluster_obj.patch(
        resource_name=constants.DEFAULT_STORAGE_CLUSTER,
        params='{"spec": {"resources": {"mds": {"limits": {"cpu": "16"}, "requests": {"cpu": "16"}}}}}',
        format_type="merge",
    )


@magenta_squad
@tier2
class TestMdsXattrAlerts(E2ETest):
    @pytest.fixture(scope="function", autouse=True)
    def teardown(self, request):
        def finalizer():
            """
            This function will call a function to clear the mds memory usage gradually

            """
            log.info("Setting MDS CPU Resources back to original values")

            storagecluster_obj.patch(
                resource_name=constants.DEFAULT_STORAGE_CLUSTER,
                params='{"spec": {"resources": {"mds": {"limits": {"cpu": "2"}, "requests": {"cpu": "2"}}}}}',
                format_type="merge",
            )
            cluster.bring_down_mds_memory_usage_gradually()

        request.addfinalizer(finalizer)

    def test_mds_xattr_alert_triggered(
        self, set_xattr_with_high_cpu_usage, threading_lock
    ):
        log.info(
            "Setting extended attributes and file creation IO started in the background."
            " Script will look for CephXattrSetLatency  alert"
        )
        assert MDSxattr_alert_values(threading_lock, timeout=1200)

        log.info("Checking for clearance of alert")
        initiate_alert_clearanace()
        # waiting for sometime for load distribution
        time.sleep(600)
        assert MDSxattr_alert_values(threading_lock, timeout=30) is False

    def test_alert_triggered_by_restarting_operator_and_metrics_pods(
        self, set_xattr_with_high_cpu_usage, threading_lock
    ):
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
        # metrics_pods = OCP_POD_OBJ.get(selector="app.kubernetes.io/name=ocs-metrics-exporter")[
        #     "items"
        # ]
        # metrics_pods.delete(force=True)
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

    def test_alert_after_recovering_prometheus_from_failures(
        self, set_xattr_with_high_cpu_usage, threading_lock
    ):
        """
        This test function verifies the mds cache alert and fails the prometheus.
        It also verifies the alert after recovering prometheus from failures.

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

    def test_alert_after_active_mds_scaledown(
        self, set_xattr_with_high_cpu_usage, threading_lock
    ):
        """
        This test function verifies the mds alert with active mds scale down and up
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

    def test_alert_with_both_mds_scaledown(
        self, set_xattr_with_high_cpu_usage, threading_lock
    ):
        """
        This test function verifies the mds alert with both active and standby mds scale down and up
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

    def test_alert_with_mds_running_node_restart(
        self, set_xattr_with_high_cpu_usage, threading_lock, nodes
    ):
        """
        This test function verifies the mds alert when the active mds running node is restart.
        #"""
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
