import logging
import pytest

from concurrent.futures import ThreadPoolExecutor
from ocs_ci.framework.pytest_customization.marks import (
    blue_squad,
    skipif_ocs_version,
    aws_platform_required,
    baremetal_deployment_required,
)
from ocs_ci.framework.testlib import E2ETest, tier2
from ocs_ci.helpers import helpers
from ocs_ci.ocs import cluster, constants
from ocs_ci.utility import prometheus
from ocs_ci.utility.utils import ceph_health_check_base

log = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def run_file_creator_io_with_cephfs(dc_pod_factory):
    """
    This function facilitates
    1. Create PVC with Cephfs, access mode RWX
    2. Create dc pod with Fedora image
    3. Copy helper_scripts/file_creator_io.py to Fedora dc pod
    4. Run file_creator_io.py on fedora pod

    """
    access_mode = constants.ACCESS_MODE_RWX
    file = constants.FILE_CREATOR_IO
    interface = constants.CEPHFILESYSTEM
    log.info("Checking for Ceph Health OK")
    ceph_health_check_base()

    for dc_pod in range(10):
        log.info(f"Creating {interface} based PVC")
        log.info("Creating fedora dc pod")
        pod_obj = dc_pod_factory(
            size="15", access_mode=access_mode, interface=interface
        )
        log.info("Copying file_creator_io.py to fedora pod ")
        cmd = f"oc cp {file} {pod_obj.namespace}/{pod_obj.name}:/"
        helpers.run_cmd(cmd=cmd)
        log.info("file_creator_io.py copied successfully ")
        log.info("Running file creator IO on fedora pod ")
        metaio_executor = ThreadPoolExecutor(max_workers=1)
        metaio_executor.submit(
            pod_obj.exec_sh_cmd_on_pod, command="python3 file_creator_io.py"
        )


def active_mds_alert_values(threading_lock):
    """
    This function validates the mds alerts using prometheus api

    """
    active_mds_pod = cluster.get_active_mds_info()["active_pod"]
    cpu_alert = constants.ALERT_MDSCPUUSAGEHIGH

    api = prometheus.PrometheusAPI(threading_lock=threading_lock)
    alert_list = api.wait_for_alert(name=cpu_alert, state="pending")
    message = f"Ceph metadata server pod ({active_mds_pod}) has high cpu usage"
    description = (
        f"Ceph metadata server pod ({active_mds_pod}) has high cpu usage"
        f"\n. Please consider Vertical"
        f"\nscaling, by adding more resources to the existing MDS pod."
        f"\nPlease see 'runbook_url' for more details."
    )
    runbook = (
        "https://github.com/openshift/runbooks/blob/master/alerts/"
        "openshift-container-storage-operator/CephMdsCPUUsageHighNeedsVerticalScaling.md "
    )
    severity = "warning"
    state = ["pending"]

    prometheus.check_alert_list(
        label=cpu_alert,
        msg=message,
        description=description,
        runbook=runbook,
        states=state,
        severity=severity,
        alerts=alert_list,
    )
    log.info("Alert verified successfully")
    return True


@tier2
@blue_squad
@skipif_ocs_version("<4.15")
@aws_platform_required
@baremetal_deployment_required
class TestMdsCpuAlerts(E2ETest):
    @pytest.fixture(scope="function", autouse=True)
    def teardown(self, request):
        def finalizer():
            """
            This function will call a function to clear the mds memory usage gradually

            """
            cluster.bring_down_mds_memory_usage_gradually()

        request.addfinalizer(finalizer)

    @pytest.mark.polarion_id("OCS-5581")
    def test_mds_cpu_alert_triggered(
        self, run_file_creator_io_with_cephfs, threading_lock
    ):
        """
        This test case is to verify the alert for MDS cpu high usage for only vertical scaling,
        alert for Horizontal scaling is skipped as it is not easy to achieve the rate(ceph_mds_request)>=1000.

        Args:
        run_file_creator_io_with_cephfs: function to generate load on mds cpu to achieve "cpu utilisation >67%"
        threading_lock: to pass the threading lock in alert validation function

        """
        log.info(
            "File creation IO started in the background."
            " Script will look for MDSCPUUsageHigh  alert"
        )
        assert active_mds_alert_values(threading_lock)
