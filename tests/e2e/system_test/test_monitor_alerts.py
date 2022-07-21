import logging
import time

from ocs_ci.helpers import helpers
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.ocs.benchmark_operator_fio import get_file_size
from ocs_ci.utility.prometheus import PrometheusAPI
from ocs_ci.utility.utils import TimeoutSampler, ceph_health_check
from ocs_ci.ocs import constants
from ocs_ci.ocs.benchmark_operator_fio import BenchmarkOperatorFIO
from ocs_ci.ocs.resources import pvc
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

    def test_cluster_full_and_recovery(self, teardown_project_factory):
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
        project_name = "test755"
        project_obj = helpers.create_project(project_name=project_name)
        teardown_project_factory(project_obj)

        log.info("Create PVC1 [FS + RBD]")
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

        log.info("Verify new PVC1 [FS + RBD] on Bound state")
        helpers.wait_for_resource_state(
            resource=pvc_obj_blk1, state=constants.STATUS_BOUND
        )
        helpers.wait_for_resource_state(
            resource=pvc_obj_fs1, state=constants.STATUS_BOUND
        )

        log.info(
            "Fill the cluster to “Full ratio” (usually 85%) with benchmark-operator"
        )
        size = get_file_size(88)
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
            log.error("The after 900 seconds")
            raise TimeoutExpiredError

        log.info(
            "Verify Alerts are seen 'CephClusterCriticallyFull' and 'CephOSDNearFull'"
        )
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

        log.info("Verify PVC2 [FS + RBD] are in Pending state")
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
        log.info("Waiting 20 sec to verify PVC2 [FS + RBD] are in Pending state.")
        time.sleep(20)
        helpers.wait_for_resource_state(
            resource=pvc_obj_blk2, state=constants.STATUS_PENDING
        )
        helpers.wait_for_resource_state(
            resource=pvc_obj_fs2, state=constants.STATUS_PENDING
        )

        log.info("Create snapshot from PVC1 and verify snapshots on false state")
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

        log.info("Change Ceph full_ratiofrom from 85% to 95%")
        change_ceph_full_ratio(95)

        log.info("Delete  benchmark-operator PVCs")
        self.benchmark_obj.cleanup()
        self.banchmark_operator_teardown = False

        log.info("Change Ceph full_ratio from from 85% to 95%")
        change_ceph_backfillfull_ratio(95)

        log.info("Verify PVC2 [FS + RBD]  are moved to Bound state")
        helpers.wait_for_resource_state(
            resource=pvc_obj_blk2, state=constants.STATUS_BOUND
        )
        helpers.wait_for_resource_state(
            resource=pvc_obj_fs2, state=constants.STATUS_BOUND
        )

        log.info("Verify snapshots moved from false state to true state")
        snap_blk_obj.ocp.wait_for_resource(
            condition="true",
            resource_name=snap_blk_obj.name,
            column=constants.STATUS_READYTOUSE,
            timeout=60,
        )

        snap_blk_obj.ocp.wait_for_resource(
            condition="true",
            resource_name=snap_fs_obj.name,
            column=constants.STATUS_READYTOUSE,
            timeout=60,
        )
        a = 1

    def verify_used_capacity_greater_than_expected(self, expected_used_capacity):
        """
        Verify cluster percent used capacity

        Args:
            expected_used_capacity (int): expected used capacity

        Returns:
             bool: True if used_capacity greater than expected_used_capacity, False otherwise

        """
        used_capacity = get_percent_used_capacity()
        return used_capacity > expected_used_capacity
