import loggerging
import pytest

from concurrent.futures import ThreadPoolExecutor
from ocs_ci.framework.pytest_customization.marks import (
    blue_squad,
    skipif_ocs_version,
    skipif_vsphere_platform,
    skipif_disconnected_cluster,
)
from ocs_ci.framework.testlib import E2ETest, tier2
from ocs_ci.helpers import helpers
from ocs_ci.ocs import cluster, constants
from ocs_ci.utility import prometheus
from ocs_ci.utility.utils import ceph_health_check_base

logger = loggerging.getLogger(__name__)


@pytest.fixture(scope="function")
def run_file_creator_io_with_cephfs(deployment_pod_factory):
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

    logger.test_step("Verify Ceph cluster health before starting IO workload")
    ceph_health_check_base()
    logger.info("Ceph cluster health verified as OK")

    logger.test_step("Create 10 CephFS pods and start file creator IO workload")
    for dc_pod in range(10):
        logger.info(f"Creating pod {dc_pod + 1}/10 with {interface} PVC")
        pod_obj = deployment_pod_factory(
            size="15", access_mode=access_mode, interface=interface
        )
        logger.debug(f"Pod created: {pod_obj.name} in namespace {pod_obj.namespace}")

        logger.debug(f"Copying {file} to pod {pod_obj.name}")
        cmd = f"oc cp {file} {pod_obj.namespace}/{pod_obj.name}:/"
        helpers.run_cmd(cmd=cmd)
        logger.debug("file_creator_io.py copied successfully")

        logger.debug(f"Starting file creator IO on pod {pod_obj.name}")
        metaio_executor = ThreadPoolExecutor(max_workers=1)
        metaio_executor.submit(
            pod_obj.exec_sh_cmd_on_pod, command="python3 file_creator_io.py"
        )

    logger.info("All 10 pods created and file creator IO started in background")


def active_mds_alert_values(threading_lock):
    """
    This function validates the mds alerts using prometheus api

    """
    logger.test_step("Get active MDS pod information")
    active_mds_pod = cluster.get_active_mds_info()["active_pod"]
    cpu_alert = constants.ALERT_MDSCPUUSAGEHIGH
    logger.info(f"Active MDS pod: {active_mds_pod}")
    logger.info(f"Monitoring for alert: {cpu_alert}")

    logger.test_step("Wait for MDS CPU high usage alert to trigger")
    api = prometheus.PrometheusAPI(threading_lock=threading_lock)
    alert_list = api.wait_for_alert(name=cpu_alert, state="pending")
    logger.info(f"Alert detected: {len(alert_list) if alert_list else 0} instances")

    logger.test_step("Validate alert details match expected values")
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

    logger.debug(f"Expected message: {message}")
    logger.debug(f"Expected severity: {severity}")
    logger.debug(f"Expected state: {state}")

    prometheus.check_alert_list(
        label=cpu_alert,
        msg=message,
        description=description,
        runbook=runbook,
        states=state,
        severity=severity,
        alerts=alert_list,
    )
    logger.assertion(
        f"Alert validation: alert={cpu_alert}, severity={severity}, "
        f"state={state}, validation=passed"
    )
    logger.info("Alert verified successfully")
    return True


@tier2
@blue_squad
@skipif_ocs_version("<4.15")
@skipif_vsphere_platform
@skipif_disconnected_cluster
class TestMdsCpuAlerts(E2ETest):
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
        logger.info("Starting test: Verify MDS CPU high usage alert is triggered")
        logger.info(
            "File creation IO started in the background. "
            "Monitoring for MDSCPUUsageHigh alert"
        )

        logger.test_step("Validate MDSCPUUsageHigh alert is triggered and correct")
        alert_validated = active_mds_alert_values(threading_lock)
        logger.assertion(
            f"MDS CPU alert validation: expected=True, actual={alert_validated}"
        )
        assert alert_validated, "MDS CPU high usage alert validation failed"

        logger.info(
            "Test passed: MDS CPU high usage alert triggered and validated successfully"
        )
