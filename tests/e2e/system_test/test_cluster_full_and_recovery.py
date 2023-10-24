import logging
import time

from ocs_ci.helpers import helpers
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.ocs.benchmark_operator_fio import get_file_size
from ocs_ci.framework.pytest_customization.marks import (
    system_test,
    polarion_id,
    magenta_squad,
)
from ocs_ci.utility.prometheus import PrometheusAPI
from ocs_ci.utility.utils import TimeoutSampler, ceph_health_check
from ocs_ci.ocs import constants
from ocs_ci.ocs.benchmark_operator_fio import BenchmarkOperatorFIO
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.resources.pod import cal_md5sum, wait_for_storage_pods
from ocs_ci.helpers import disruption_helpers
from ocs_ci.ocs.cluster import (
    change_ceph_full_ratio,
    get_percent_used_capacity,
    get_osd_utilization,
    get_ceph_df_detail,
)

log = logging.getLogger(__name__)


@magenta_squad
@system_test
@polarion_id("OCS-4621")
class TestClusterFullAndRecovery(E2ETest):
    """
    Test Cluster Full And Recovery

    """

    def teardown(self):
        if self.benchmark_operator_teardown:
            change_ceph_full_ratio(95)
            self.benchmark_obj.cleanup()
            ceph_health_check(tries=30, delay=60)
        change_ceph_full_ratio(85)

    def test_cluster_full_and_recovery(
        self,
        teardown_project_factory,
        snapshot_restore_factory,
        snapshot_factory,
        pvc_factory,
        pod_factory,
        project_factory,
        threading_lock,
    ):
        """
        1.Create PVC1 [FS + RBD]
        2.Verify new PVC1 [FS + RBD] on Bound state
        3.Run FIO on PVC1_FS + PVC1_RBD
        4.Calculate Checksum PVC1_FS + PVC1_RBD
        5.Fill the cluster to “Full ratio” (usually 85%) with benchmark-operator
        6.Verify Alerts are seen ["CephClusterCriticallyFull", "CephOSDNearFull"]
        7.Respin 'osd' ,'mgr', 'mon' pods
        8.Create PVC2 [FS + RBD]
        9.Verify PVC2 [FS + RBD] are in Pending state
        10.Create snapshot from PVC1 [FS+RBD]
        11.Verify snapshots on false state
        12.Change Ceph full_ratiofrom from 85% to 95%
        13.Delete  benchmark-operator PVCs
        14.Verify PVC2 [FS + RBD]  are moved to Bound state
        15.Verify snapshots moved from false state to true state
        16.Restore new pvc from snapshot pvc [RBD + FS]
        17.Verify checksum PVC1 equal to PVC1_RESTORE
        18.Change Ceph full_ratiofrom from 95% to 85%

        """
        self.benchmark_operator_teardown = False
        project_name = "system-test-fullcluster"
        self.project_obj = helpers.create_project(project_name=project_name)
        teardown_project_factory(self.project_obj)

        log.info("Create PVC1 CEPH-RBD, Run FIO and get checksum")
        pvc_obj_rbd1 = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            project=self.project_obj,
            size=2,
            status=constants.STATUS_BOUND,
        )
        pod_rbd1_obj = pod_factory(
            interface=constants.CEPHFILESYSTEM,
            pvc=pvc_obj_rbd1,
            status=constants.STATUS_RUNNING,
        )
        pod_rbd1_obj.run_io(
            storage_type="fs",
            size="1G",
            io_direction="write",
            runtime=60,
        )
        pod_rbd1_obj.get_fio_results()
        log.info(f"IO finished on pod {pod_rbd1_obj.name}")
        pod_rbd1_obj.md5 = cal_md5sum(
            pod_obj=pod_rbd1_obj,
            file_name="fio-rand-write",
            block=False,
        )

        log.info("Create PVC1 CEPH-FS, Run FIO and get checksum")
        pvc_obj_fs1 = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            project=self.project_obj,
            size=2,
            status=constants.STATUS_BOUND,
        )
        pod_fs1_obj = pod_factory(
            interface=constants.CEPHFILESYSTEM,
            pvc=pvc_obj_fs1,
            status=constants.STATUS_RUNNING,
        )
        pod_fs1_obj.run_io(
            storage_type="fs",
            size="1G",
            io_direction="write",
            runtime=60,
        )
        pod_fs1_obj.get_fio_results()
        log.info(f"IO finished on pod {pod_fs1_obj.name}")
        pod_fs1_obj.md5 = cal_md5sum(
            pod_obj=pod_fs1_obj,
            file_name="fio-rand-write",
            block=False,
        )

        log.info(
            "Fill the cluster to “Full ratio” (usually 85%) with benchmark-operator"
        )
        size = get_file_size(100)
        self.benchmark_obj = BenchmarkOperatorFIO()
        self.benchmark_obj.setup_benchmark_fio(total_size=size)
        self.benchmark_obj.run_fio_benchmark_operator(is_completed=False)
        self.benchmark_operator_teardown = True

        log.info("Verify used capacity bigger than 85%")
        sample = TimeoutSampler(
            timeout=2500,
            sleep=40,
            func=self.verify_osd_used_capacity_greater_than_expected,
            expected_used_capacity=85.0,
        )
        if not sample.wait_for_func_status(result=True):
            log.error("The after 1800 seconds the used capacity smaller than 85%")
            raise TimeoutExpiredError

        log.info(
            "Verify Alerts are seen 'CephClusterCriticallyFull' and 'CephOSDNearFull'"
        )
        log.info("Verify used capacity bigger than 85%")
        expected_alerts = ["CephOSDCriticallyFull", "CephOSDNearFull"]
        sample = TimeoutSampler(
            timeout=600,
            sleep=50,
            func=self.verify_alerts_via_prometheus,
            expected_alerts=expected_alerts,
            threading_lock=threading_lock,
        )
        if not sample.wait_for_func_status(result=True):
            log.error(f"The alerts {expected_alerts} do not exist after 600 sec")
            raise TimeoutExpiredError

        for pod_name in ("mon", "mgr", "osd"):
            log.info(f"Respin pod {pod_name}")
            disruption = disruption_helpers.Disruptions()
            disruption.set_resource(resource=f"{pod_name}")
            disruption.delete_resource()

        log.info("Verify all storage pods are running")
        wait_for_storage_pods()

        log.info("Verify PVC2 [CEPH-FS + CEPH-RBD] are in Pending state")
        pvc_obj_rbd2 = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            project=self.project_obj,
            size=2,
            status=constants.STATUS_PENDING,
        )
        pvc_obj_fs2 = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            project=self.project_obj,
            size=2,
            status=constants.STATUS_PENDING,
        )
        log.info(
            "Waiting 20 sec to verify PVC2 [CEPH-FS + CEPH-RBD] are in Pending state."
        )
        time.sleep(20)
        helpers.wait_for_resource_state(
            resource=pvc_obj_rbd2, state=constants.STATUS_PENDING
        )
        helpers.wait_for_resource_state(
            resource=pvc_obj_fs2, state=constants.STATUS_PENDING
        )

        log.info("Create snapshot from PVC1 and verify snapshots on false state")
        snap_rbd1_obj = snapshot_factory(pvc_obj_rbd1, wait=False)
        snap_fs1_obj = snapshot_factory(pvc_obj_fs1, wait=False)
        time.sleep(20)
        snap_rbd1_obj.ocp.wait_for_resource(
            condition="false",
            resource_name=snap_rbd1_obj.name,
            column=constants.STATUS_READYTOUSE,
            timeout=60,
        )
        snap_fs1_obj.ocp.wait_for_resource(
            condition="false",
            resource_name=snap_fs1_obj.name,
            column=constants.STATUS_READYTOUSE,
            timeout=60,
        )

        log.info("Change Ceph full_ratio from from 85% to 95%")
        log.info(
            "Based on doc we need to change the ceph_full_ratio to 88%, but we run "
            "many fio pods therefore, it may not be enough to increase by only 3%"
        )
        change_ceph_full_ratio(95)

        log.info("Delete  benchmark-operator PVCs")
        self.benchmark_obj.cleanup()
        self.benchmark_operator_teardown = False

        log.info("Verify PVC2 [CEPH-FS + CEPH-RBD]  are moved to Bound state")
        helpers.wait_for_resource_state(
            resource=pvc_obj_rbd2, state=constants.STATUS_BOUND, timeout=600
        )
        helpers.wait_for_resource_state(
            resource=pvc_obj_fs2, state=constants.STATUS_BOUND, timeout=600
        )

        log.info("Verify snapshots moved from false state to true state")
        snap_fs1_obj.ocp.wait_for_resource(
            condition="true",
            resource_name=snap_rbd1_obj.name,
            column=constants.STATUS_READYTOUSE,
            timeout=300,
        )
        snap_rbd1_obj.ocp.wait_for_resource(
            condition="true",
            resource_name=snap_rbd1_obj.name,
            column=constants.STATUS_READYTOUSE,
            timeout=300,
        )

        log.info(f"Creating a PVC from snapshot [restore] {snap_rbd1_obj.name}")
        restore_pvc_rbd1_obj = snapshot_restore_factory(
            snapshot_obj=snap_rbd1_obj,
            size="2Gi",
            volume_mode=snap_rbd1_obj.parent_volume_mode,
            access_mode=snap_rbd1_obj.parent_access_mode,
            status="",
        )
        pod_restore_rbd1_obj = pod_factory(
            interface=constants.CEPHFILESYSTEM,
            pvc=restore_pvc_rbd1_obj,
            status=constants.STATUS_RUNNING,
        )
        pod_restore_rbd1_obj.md5 = cal_md5sum(
            pod_obj=pod_restore_rbd1_obj,
            file_name="fio-rand-write",
            block=False,
        )

        log.info(f"Creating a PVC from snapshot [restore] {snap_fs1_obj.name}")
        restore_pvc_fs1_obj = snapshot_restore_factory(
            snapshot_obj=snap_fs1_obj,
            size="2Gi",
            volume_mode=snap_fs1_obj.parent_volume_mode,
            access_mode=snap_fs1_obj.parent_access_mode,
            status="",
        )
        pod_restore_fs1_obj = pod_factory(
            interface=constants.CEPHFILESYSTEM,
            pvc=restore_pvc_fs1_obj,
            status=constants.STATUS_RUNNING,
        )
        pod_restore_fs1_obj.md5 = cal_md5sum(
            pod_obj=pod_restore_fs1_obj,
            file_name="fio-rand-write",
            block=False,
        )

        assert pod_restore_fs1_obj.md5 == pod_fs1_obj.md5, (
            f"md5sum of restore_fs1 {pod_restore_fs1_obj.md5} is not equal "
            f"to pod_fs1_obj {pod_fs1_obj.md5}"
        )
        assert pod_restore_rbd1_obj.md5 == pod_rbd1_obj.md5, (
            f"md5sum of restore_rbd1 {pod_restore_rbd1_obj.md5} is not equal "
            f"to pod_rbd1_obj {pod_rbd1_obj.md5}"
        )

    def verify_osd_used_capacity_greater_than_expected(self, expected_used_capacity):
        """
        Verify OSD percent used capacity greate than ceph_full_ratio

        Args:
            expected_used_capacity (float): expected used capacity

        Returns:
             bool: True if used_capacity greater than expected_used_capacity, False otherwise

        """
        used_capacity = get_percent_used_capacity()
        log.info(f"Used Capacity is {used_capacity}%")
        ceph_df_detail = get_ceph_df_detail()
        log.info(f"ceph df detail: {ceph_df_detail}")
        osds_utilization = get_osd_utilization()
        log.info(f"osd utilization: {osds_utilization}")
        for osd_id, osd_utilization in osds_utilization.items():
            if osd_utilization > expected_used_capacity:
                log.info(f"OSD ID:{osd_id}:{osd_utilization} greater than 85%")
                return True
        return False

    def verify_alerts_via_prometheus(self, expected_alerts, threading_lock):
        """
        Verify Alerts on prometheus

        Args:
            expected_alerts (list): list of alert names
            threading_lock (threading.Rlock): Lock object to prevent simultaneous calls to 'oc'

        Returns:
            bool: True if expected_alerts exist, False otherwise

        """
        prometheus = PrometheusAPI(threading_lock=threading_lock)
        log.info("Logging of all prometheus alerts started")
        alerts_response = prometheus.get(
            "alerts", payload={"silenced": False, "inhibited": False}
        )
        actual_alerts = list()
        for alert in alerts_response.json().get("data").get("alerts"):
            actual_alerts.append(alert.get("labels").get("alertname"))
        for expected_alert in expected_alerts:
            if expected_alert not in actual_alerts:
                log.error(
                    f"{expected_alert} alert does not exist in alerts list."
                    f"The actaul alerts: {actual_alerts}"
                )
                return False
        return True
