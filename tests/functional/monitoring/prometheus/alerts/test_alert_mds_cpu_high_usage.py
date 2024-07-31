import time
import logging
import pytest

from ocs_ci.framework.testlib import tier2
from ocs_ci.ocs import constants
from concurrent.futures import ThreadPoolExecutor
from ocs_ci.helpers import helpers
from ocs_ci.ocs import cluster
from ocs_ci.utility import prometheus
from ocs_ci.framework.pytest_customization.marks import blue_squad
from ocs_ci.utility.utils import ceph_health_check_base

log = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def run_file_creator_io_with_cephfs(pvc_factory, dc_pod_factory):
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

    for i in range(6):
        # Creating PVC with cephfs as interface
        log.info(f"Creating {interface} based PVC")
        pvc_obj = pvc_factory(interface=interface, access_mode=access_mode, size="30")
        # Creating a Fedora dc pod
        log.info("Creating fedora dc pod")
        pod_obj = dc_pod_factory(
            pvc=pvc_obj, access_mode=access_mode, interface=interface
        )
        # Copy file_creator_io.py to fedora pod
        log.info("Copying file_creator_io.py to fedora pod ")
        cmd = f"oc cp {file} {pvc_obj.namespace}/{pod_obj.name}:/"
        helpers.run_cmd(cmd=cmd)
        log.info("file_creator_io.py copied successfully ")

        # Run file_creator_io.py on fedora pod
        log.info("Running file creator IO on fedora pod ")
        metaio_executor = ThreadPoolExecutor(max_workers=1)
        metaio_executor.submit(
            pod_obj.exec_sh_cmd_on_pod, command="python3 file_creator_io.py"
        )


def active_mds_alert_values(threading_lock):
    # This function validates the mds alerts using prometheus api
    active_mds = cluster.get_active_mds_info()["mds_daemon"]
    sr_mds = cluster.get_mds_standby_replay_info()["mds_daemon"]
    active_mds_pod = cluster.get_active_mds_info()["active_pod"]
    cache_alert = constants.ALERT_MDSCPUUSAGEHIGH
    message = f"Ceph metadata server pod ({active_mds_pod}) has high cpu usage"
    description = (
        f"Ceph metadata server pod ({active_mds_pod}) has high cpu usage."
        f" Please consider increasing the CPU request for the {active_mds_pod} pod as described in the runbook."
    )
    runbook = (
        "https://github.com/openshift/runbooks/blob/master/alerts/"
        "openshift-container-storage-operator/CephMdsCpuUsageHigh.md"
    )
    state = "firing"
    severity = "warning"

    api = prometheus.PrometheusAPI(threading_lock=threading_lock)
    alerts_response = api.get("alerts", payload={"silenced": False, "inhibited": False})
    prometheus_alerts = alerts_response.json()["data"]["alerts"]

    prometheus.verify_mds_alerts(
        alert_name=cache_alert,
        msg=message,
        description=description,
        runbook=runbook,
        state=state,
        severity=severity,
        alerts=prometheus_alerts,
        active_mds=active_mds,
        standby_mds=sr_mds,
    )
    log.info("Alert verified successfully")
    return True


@tier2
@blue_squad
class TestMdsCpuAlerts:
    @pytest.mark.polarion_id("OCS-5581")
    def test_alert_triggered(self, run_file_creator_io_with_cephfs, threading_lock):
        log.info(
            "File creation IO started in the background."
            " Script will sleep for 15 minutes before validating the MDS alert"
        )
        time.sleep(900)
        log.info("Validating the alert now")
        assert active_mds_alert_values(threading_lock)
