import logging
import time

from ocs_ci.helpers import helpers
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.ocs.benchmark_operator_fio import get_file_size
from ocs_ci.utility.prometheus import PrometheusAPI
from ocs_ci.utility.utils import TimeoutSampler, ceph_health_check
from ocs_ci.ocs import constants
from ocs_ci.ocs.benchmark_operator_fio import BenchmarkOperatorFIO
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.cluster import (
    change_ceph_backfillfull_ratio,
    change_ceph_full_ratio,
    get_percent_used_capacity,
)

log = logging.getLogger(__name__)


class TestClusterFullAndRecovery(E2ETest):
    """
    Test Cluster Full And Recovery

    """

    def teardown(self):
        if self.banchmark_operator_teardown:
            change_ceph_full_ratio(95)
            self.benchmark_obj.cleanup()
            change_ceph_backfillfull_ratio(95)
            ceph_health_check(tries=30, delay=60)
        change_ceph_backfillfull_ratio(80)
        change_ceph_full_ratio(85)

    def test_cluster_full_and_recovery(
        self, teardown_project_factory, snapshot_factory, pvc_factory
    ):
        """
        1.Create PVC1 [FS + RBD]
        2.Verify new PVC1 [FS + RBD] on Bound state
        3.Fill the cluster to “Full ratio” (usually 85%) with benchmark-operator
        4.Verify Alerts are seen ["CephClusterCriticallyFull", "CephOSDNearFull"]
        5.Create PVC2 [FS + RBD]
        6.Verify PVC2 [FS + RBD] are in Pending state
        7.Create snapshot from PVC1
        8.Verify snapshots on false state
        9.Change Ceph full_ratiofrom from 85% to 95%
        10.Delete  benchmark-operator PVCs
        11.Change Ceph backfillfull_ratio from 80% to 95%
        12.Verify PVC2 [FS + RBD]  are moved to Bound state
        13.Verify snapshots moved from false state to true state

        """
        self.banchmark_operator_teardown = False
        project_name = "test774"
        project_obj = helpers.create_project(project_name=project_name)
        teardown_project_factory(project_obj)

        log.info("Create PVC1 [FS + RBD]")
        pvc_obj_blk1 = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            project=project_obj,
            size=2,
            status=constants.STATUS_BOUND,
        )
        pvc_obj_fs1 = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            project=project_obj,
            size=2,
            status=constants.STATUS_BOUND,
        )

        log.info(
            "Fill the cluster to “Full ratio” (usually 85%) with benchmark-operator"
        )
        size = get_file_size(92)
        self.benchmark_obj = BenchmarkOperatorFIO()
        self.benchmark_obj.setup_benchmark_fio(total_size=size)
        self.benchmark_obj.run_fio_benchmark_operator(is_completed=False)
        self.banchmark_operator_teardown = True

        log.info("Verify used capacity bigger than 85%")
        sample = TimeoutSampler(
            timeout=1800,
            sleep=40,
            func=self.verify_used_capacity_greater_than_expected,
            expected_used_capacity=85.0,
        )
        if not sample.wait_for_func_status(result=True):
            log.error("The after 18000 seconds the used capacity smaller than 85%")
            raise TimeoutExpiredError

        log.info(
            "Verify Alerts are seen 'CephClusterCriticallyFull' and 'CephOSDNearFull'"
        )
        log.info("Verify used capacity bigger than 85%")
        expected_alerts = ["CephClusterCriticallyFull", "CephOSDNearFull"]
        sample = TimeoutSampler(
            timeout=600,
            sleep=50,
            func=self.verify_alerts_via_prometheus,
            expected_alerts=expected_alerts,
        )
        if not sample.wait_for_func_status(result=True):
            log.error(f"The alerts {expected_alerts} do not exist after 600 sec")
            raise TimeoutExpiredError

        log.info("Verify PVC2 [FS + RBD] are in Pending state")
        pvc_obj_blk2 = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            project=project_obj,
            size=2,
            status=constants.STATUS_PENDING,
        )
        pvc_obj_fs2 = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            project=project_obj,
            size=2,
            status=constants.STATUS_PENDING,
        )

        log.info("Waiting 20 sec to verify PVC2 [FS + RBD] are in Pending state.")
        time.sleep(20)
        helpers.wait_for_resource_state(
            resource=pvc_obj_blk2, state=constants.STATUS_PENDING
        )
        helpers.wait_for_resource_state(
            resource=pvc_obj_fs2, state=constants.STATUS_PENDING
        )

        log.info("Create snapshot from PVC1 and verify snapshots on false state")
        snap_blk1_obj = snapshot_factory(pvc_obj_blk1, wait=False)
        snap_blk1_obj.ocp.wait_for_resource(
            condition="false",
            resource_name=snap_blk1_obj.name,
            column=constants.STATUS_READYTOUSE,
            timeout=90,
        )
        snap_fs1_obj = snapshot_factory(pvc_obj_fs1, wait=False)
        snap_fs1_obj.ocp.wait_for_resource(
            condition="false",
            resource_name=snap_fs1_obj.name,
            column=constants.STATUS_READYTOUSE,
            timeout=90,
        )

        log.info("Verify Snapshots stack on False state")
        time.sleep(20)
        snap_blk1_obj.ocp.wait_for_resource(
            condition="false",
            resource_name=snap_blk1_obj.name,
            column=constants.STATUS_READYTOUSE,
            timeout=60,
        )
        snap_blk1_obj.ocp.wait_for_resource(
            condition="false",
            resource_name=snap_blk1_obj.name,
            column=constants.STATUS_READYTOUSE,
            timeout=60,
        )

        log.info("Change Ceph full_ratiofrom from 85% to 95%")
        change_ceph_full_ratio(95)

        log.info("Delete  benchmark-operator PVCs")
        self.benchmark_obj.cleanup()
        self.banchmark_operator_teardown = False

        log.info("Change Ceph full_ratio from from 85% to 95%")
        change_ceph_backfillfull_ratio(95)

        log.info("Verify PVC2 [FS + RBD]  are moved to Bound state")
        helpers.wait_for_resource_state(
            resource=pvc_obj_blk2, state=constants.STATUS_BOUND, timeout=600
        )
        helpers.wait_for_resource_state(
            resource=pvc_obj_fs2, state=constants.STATUS_BOUND, timeout=600
        )

        log.info("Verify snapshots moved from false state to true state")
        snap_fs1_obj.ocp.wait_for_resource(
            condition="true",
            resource_name=snap_blk1_obj.name,
            column=constants.STATUS_READYTOUSE,
            timeout=300,
        )
        snap_blk1_obj.ocp.wait_for_resource(
            condition="true",
            resource_name=snap_blk1_obj.name,
            column=constants.STATUS_READYTOUSE,
            timeout=300,
        )

    def verify_used_capacity_greater_than_expected(self, expected_used_capacity):
        """
        Verify cluster percent used capacity

        Args:
            expected_used_capacity (float): expected used capacity

        Returns:
             bool: True if used_capacity greater than expected_used_capacity, False otherwise

        """
        used_capacity = get_percent_used_capacity()
        return used_capacity > expected_used_capacity

    def verify_alerts_via_prometheus(self, expected_alerts):
        """
        Verify Alerts on prometheus

        Args:
            expected_alerts (list): list of alert names

        Returns:
            bool: True if expected_alerts exist, False otherwise

        """
        prometheus = PrometheusAPI()
        log.info("Logging of all prometheus alerts started")
        alerts_response = prometheus.get(
            "alerts", payload={"silenced": False, "inhibited": False}
        )
        actual_alerts = list()
        for alert in alerts_response.json().get("data").get("alerts"):
            actual_alerts.append(alert.get("labels").get("alertname"))
        for expected_alert in expected_alerts:
            if expected_alert not in actual_alerts:
                log.error(f"{expected_alert} alert does not exist in alerts list")
                return False
        return True
