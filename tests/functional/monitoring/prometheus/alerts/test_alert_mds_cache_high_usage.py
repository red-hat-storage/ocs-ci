import time
import logging
import pytest

from ocs_ci.ocs import constants
from concurrent.futures import ThreadPoolExecutor
from ocs_ci.helpers import helpers
from ocs_ci.ocs import cluster
from ocs_ci.utility import prometheus
from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.utility.utils import ceph_health_check_base
from ocs_ci.ocs.node import (
    unschedule_nodes,
    drain_nodes,
    schedule_nodes,
)

log = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def run_metadata_io_with_cephfs(pvc_factory, dc_pod_factory):
    """
    This function facilitates
    1. Create PVC with Cephfs, access mode RWX
    2. Create dc pod with Fedora image
    3. Copy helper_scripts/meta_data_io.py to Fedora dc pod
    4. Run meta_data_io.py on fedora pod
    """
    access_mode = constants.ACCESS_MODE_RWX
    file = constants.METAIO
    interface = constants.CEPHFILESYSTEM
    log.info("Checking for Ceph Health OK")
    ceph_health_check_base()

    for i in range(3):
        # Creating PVC with cephfs as interface
        log.info(f"Creating {interface} based PVC")
        pvc_obj = pvc_factory(interface=interface, access_mode=access_mode, size="30")
        # Creating a Fedora dc pod
        log.info("Creating fedora dc pod")
        pod_obj = dc_pod_factory(
            pvc=pvc_obj, access_mode=access_mode, interface=interface
        )
        # Copy chunk.py to fedora pod
        log.info("Copying meta_data_io.py to fedora pod ")
        cmd = f"oc cp {file} {pvc_obj.namespace}/{pod_obj.name}:/"
        helpers.run_cmd(cmd=cmd)
        log.info("meta_data_io.py copied successfully ")

        # Run meta_data_io.py on fedora pod
        log.info("Running meta data IO on fedora pod ")
        metaio_executor = ThreadPoolExecutor(max_workers=1)
        # self.metaio_thread = metaio_executor.submit(
        metaio_executor.submit(
            pod_obj.exec_sh_cmd_on_pod, command="python3 meta_data_io.py"
        )


def active_mds_alert_values(threading_lock):
    active_mds = cluster.get_active_mds_info()["mds_daemon"]
    sr_mds = cluster.get_mds_standby_replay_info()["mds_daemon"]
    cache_alert = constants.ALERT_MDSCACHEUSAGEHIGH
    message = f"High MDS cache usage for the daemon mds.{active_mds}."
    description = (
        f"MDS cache usage for the daemon mds.{active_mds} has exceeded above 95% of the requested value."
        f" Increase the memory request for mds.{active_mds} pod."
    )
    runbook = (
        "https://github.com/openshift/runbooks/blob/master/alerts/"
        "openshift-container-storage-operator/CephMdsCacheUsageHigh.md"
    )
    state = "firing"
    severity = "critical"

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
    # ignore_more_occurences = True
    log.info("Alert verified successfully")
    return True


@magenta_squad
class TestMdsMemoryAlerts:
    def test_alert_triggered(self, run_metadata_io_with_cephfs, threading_lock):
        log.info(
            "Metadata IO started in the background. Script will sleep for 15 minutes before validating the MDS alert"
        )
        time.sleep(900)
        log.info("Validating the alert now")
        assert active_mds_alert_values(threading_lock)

    def test_mds_cache_alert_with_active_node_drain(
        self, run_metadata_io_with_cephfs, threading_lock
    ):

        log.info(
            "Metadata IO started in the background. Lets wait for 15 minutes before validating the MDS alert"
        )
        time.sleep(900)
        log.info("Validating the alert now")
        assert active_mds_alert_values(threading_lock)

        node_name = cluster.get_active_mds_info()["node_name"]

        # Unschedule active mds running node.
        unschedule_nodes([node_name])
        log.info(f"node {node_name} unscheduled successfully")

        # Drain node operation
        drain_nodes([node_name])
        log.info(f"node {node_name} drained successfully")

        # Make the node schedule-able
        schedule_nodes([node_name])
        log.info(f"Scheduled the node {node_name}")
        log.info("Script will sleep for 10 minutes before validating the alert")
        time.sleep(600)
        assert active_mds_alert_values(threading_lock)
