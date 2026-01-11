import logging
import pytest

from concurrent.futures import ThreadPoolExecutor
from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import E2ETest, tier2
from ocs_ci.framework import config
from ocs_ci.helpers import helpers
from ocs_ci.ocs import cluster, constants
from ocs_ci.utility import prometheus
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import delete_deployment_pods
from ocs_ci.utility.utils import ceph_health_check_base

log = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def set_xattr_with_high_cpu_usage(request, pvc_factory, deployment_pod_factory):
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

    file1 = constants.FILE_CREATOR_IO
    log.info("Checking for Ceph Health OK")
    ceph_health_check_base()

    # increasing cpu usage in cluster
    for dc_pod in range(10):
        log.info("Creating fedora dc pod")
        pod_obj1 = deployment_pod_factory(
            size="15",
            access_mode=constants.ACCESS_MODE_RWX,
            interface=constants.CEPHFILESYSTEM,
        )
        log.info("Copying file_creator_io.py to fedora pod ")
        cmd1 = f"oc cp {file1} {pod_obj1.namespace}/{pod_obj1.name}:/"
        helpers.run_cmd(cmd=cmd1)
        log.info("file_creator_io.py copied successfully ")
        log.info("Running file creator IO on fedora pod ")
        metaio_executor = ThreadPoolExecutor(max_workers=1)
        metaio_executor.submit(
            pod_obj1.exec_sh_cmd_on_pod, command="python3 file_creator_io.py"
        )

    def finalizer():
        delete_deployment_pods(pod_obj)

    request.addfinalizer(finalizer)


def MDSxattr_alert_values(threading_lock):
    """
    This function validates the mds alert using prometheus api
    """
    MDSxattr_alert = constants.ALERT_MDSXATTR

    api = prometheus.PrometheusAPI(threading_lock=threading_lock)
    alert = api.wait_for_alert(name=MDSxattr_alert, state="firing")
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
    state = ["firing"]

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


@magenta_squad
@tier2
class TestMdsXattrAlerts(E2ETest):
    @pytest.fixture(scope="function", autouse=True)
    def teardown(self, request):
        def finalizer():
            """
            This function will call a function to clear the mds memory usage gradually

            """
            cluster.bring_down_mds_memory_usage_gradually()

        request.addfinalizer(finalizer)

    def test_mds_xattr_alert_triggered(
        self, set_xattr_with_high_cpu_usage, threading_lock
    ):
        log.info(
            "Setting extended attributes and file creation IO started in the background."
            " Script will look for CephXattrSetLatency  alert"
        )
        assert MDSxattr_alert_values(threading_lock)
