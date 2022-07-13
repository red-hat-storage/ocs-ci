import logging
import time
import pytest

from ocs_ci.helpers import helpers
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.ocs.benchmark_operator_fio import get_file_size
from ocs_ci.utility.prometheus import PrometheusAPI
from ocs_ci.ocs import constants
from ocs_ci.ocs.benchmark_operator_fio import BenchmarkOperatorFIO
from ocs_ci.ocs.cluster import change_ceph_backfillfull_ratio
from ocs_ci.ocs.resources import pvc

log = logging.getLogger(__name__)


class TestFullClusterMonitoring(E2ETest):
    @pytest.fixture()
    def monitor_teardown(self, request):
        def teardown():
            self.benchmark_obj.cleanup()

        request.addfinalizer(teardown)

    def test_full_cluster_monitoring(self, teardown_project_factory):
        project_name = "test750"
        project_obj = helpers.create_project(project_name=project_name)
        teardown_project_factory(project_obj)

        pvc_obj_blk1 = helpers.create_pvc(
            sc_name=constants.CEPHBLOCKPOOL_SC,
            namespace=project_name,
            size="1Gi",
            do_reload=False,
            access_mode=constants.ACCESS_MODE_RWO,
        )
        pvc_obj_fs1 = helpers.create_pvc(
            sc_name=constants.CEPHFILESYSTEM_SC,
            namespace=project_name,
            size="1Gi",
            do_reload=False,
            access_mode=constants.ACCESS_MODE_RWO,
        )
        helpers.wait_for_resource_state(
            resource=pvc_obj_blk1, state=constants.STATUS_BOUND
        )
        helpers.wait_for_resource_state(
            resource=pvc_obj_fs1, state=constants.STATUS_BOUND
        )

        log.info("Full fill the cluster")
        size = get_file_size(100)
        self.benchmark_obj = BenchmarkOperatorFIO()
        self.benchmark_obj.setup_benchmark_fio(total_size=size)
        self.benchmark_obj.run_fio_benchmark_operator()
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
        pvc_obj_blk2 = helpers.create_pvc(
            sc_name=constants.CEPHBLOCKPOOL_SC,
            namespace=project_name,
            size="1Gi",
            do_reload=False,
            access_mode=constants.ACCESS_MODE_RWO,
        )
        pvc_obj_fs2 = helpers.create_pvc(
            sc_name=constants.CEPHFILESYSTEM_SC,
            namespace=project_name,
            size="1Gi",
            do_reload=False,
            access_mode=constants.ACCESS_MODE_RWO,
        )
        time.sleep(20)
        helpers.wait_for_resource_state(
            resource=pvc_obj_blk2, state=constants.STATUS_PENDING
        )
        helpers.wait_for_resource_state(
            resource=pvc_obj_fs2, state=constants.STATUS_PENDING
        )

        change_ceph_backfillfull_ratio(95)

        self.benchmark_obj.cleanup()

        change_ceph_backfillfull_ratio(80)

        helpers.wait_for_resource_state(
            resource=pvc_obj_blk2, state=constants.STATUS_RUNNING
        )
        helpers.wait_for_resource_state(
            resource=pvc_obj_fs2, state=constants.STATUS_RUNNING
        )

        snap_blk_obj = pvc.create_pvc_snapshot(
            pvc_name=pvc_obj_blk1.name,
            snap_yaml=constants.CSI_RBD_SNAPSHOT_YAML,
            snap_name=f"snap-{pvc_obj_blk1.name}",
            namespace=pvc_obj_blk1.namespace,
            sc_name=constants.CEPHBLOCKPOOL_SC,
            wait=False,
        )

        snap_fs_obj = pvc.create_pvc_snapshot(
            pvc_name=pvc_obj_fs1.name,
            snap_yaml=constants.CSI_CEPHFS_SNAPSHOT_YAML,
            snap_name=f"snap-{pvc_obj_fs1.name}",
            namespace=pvc_obj_fs1.namespace,
            sc_name=constants.CEPHFILESYSTEM_SC,
            wait=False,
        )

        time.sleep(20)

        snap_blk_obj.ocp.wait_for_resource(
            condition="false",
            resource_name=snap_blk_obj.name,
            column=constants.STATUS_READYTOUSE,
            timeout=60,
        )

        snap_blk_obj.ocp.wait_for_resource(
            condition="false",
            resource_name=snap_fs_obj.name,
            column=constants.STATUS_READYTOUSE,
            timeout=60,
        )
