import logging
import time

from ocs_ci.helpers import helpers
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.ocs.benchmark_operator_fio import get_file_size
from ocs_ci.utility.prometheus import PrometheusAPI
from ocs_ci.ocs import constants

log = logging.getLogger(__name__)


class TestFullClusterMonitoring(E2ETest):
    def test_full_cluster_monitoring(
        self, benchmark_fio_factory_fixture, teardown_project_factory
    ):
        size = get_file_size(50)
        benchmark_fio_factory_fixture(total_size=size)
        prometheus = PrometheusAPI()
        log.info("Logging of all prometheus alerts started")
        alerts_response = prometheus.get(
            "alerts", payload={"silenced": False, "inhibited": False}
        )
        actual_alerts = list()
        for alert in alerts_response.json().get("data").get("alerts"):
            actual_alerts.append(alert.get("labels").get("alertname"))
        expected_alerts = ["CephClusterCriticallyFull", "CephOSDNearFull"]
        for expected_alert in expected_alerts:
            assert (
                expected_alert in actual_alerts
            ), f"Alert {expected_alert} not found!!"
        project_name = "test750"
        project_obj = helpers.create_project(project_name=project_name)
        teardown_project_factory(project_obj)
        pvc_obj_blk = helpers.create_pvc(
            sc_name=constants.CEPHBLOCKPOOL_SC,
            namespace=project_name,
            size="1Gi",
            do_reload=False,
            access_mode=constants.ACCESS_MODE_RWO,
        )
        pvc_obj_fs = helpers.create_pvc(
            sc_name=constants.CEPHFILESYSTEM_SC,
            namespace=project_name,
            size="1Gi",
            do_reload=False,
            access_mode=constants.ACCESS_MODE_RWO,
        )
        time.sleep(20)
        helpers.wait_for_resource_state(
            resource=pvc_obj_blk, state=constants.STATUS_PENDING
        )
        helpers.wait_for_resource_state(
            resource=pvc_obj_fs, state=constants.STATUS_PENDING
        )
