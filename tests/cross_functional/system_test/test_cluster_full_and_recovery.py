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
)
from ocs_ci.helpers.managed_services import (
    verify_osd_used_capacity_greater_than_expected,
)

logger = logging.getLogger(__name__)


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
        5.Fill the cluster to "Full ratio" (usually 85%) with benchmark-operator
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

        logger.test_step("Create RBD PVC, run IO and calculate checksum")
        logger.info("Creating CEPH-RBD PVC with 2GB size")
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
        logger.info(f"IO finished on pod {pod_rbd1_obj.name}")
        pod_rbd1_obj.md5 = cal_md5sum(
            pod_obj=pod_rbd1_obj,
            file_name="fio-rand-write",
            block=False,
        )
        logger.info("RBD PVC created, IO completed, and checksum calculated")

        logger.test_step("Create CephFS PVC, run IO and calculate checksum")
        logger.info("Creating CEPH-FS PVC with 2GB size")
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
        logger.info(f"IO finished on pod {pod_fs1_obj.name}")
        pod_fs1_obj.md5 = cal_md5sum(
            pod_obj=pod_fs1_obj,
            file_name="fio-rand-write",
            block=False,
        )
        logger.info("CephFS PVC created, IO completed, and checksum calculated")

        logger.test_step("Fill cluster to 85% using benchmark operator")
        logger.info("Starting benchmark operator to fill cluster to full ratio")
        size = get_file_size(100)
        self.benchmark_obj = BenchmarkOperatorFIO()
        self.benchmark_obj.setup_benchmark_fio(total_size=size)
        self.benchmark_obj.run_fio_benchmark_operator(is_completed=False)
        self.benchmark_operator_teardown = True

        logger.test_step("Verify cluster capacity reaches 85%")
        logger.info("Verifying used capacity is greater than 85%")
        sample = TimeoutSampler(
            timeout=2500,
            sleep=40,
            func=verify_osd_used_capacity_greater_than_expected,
            expected_used_capacity=85.0,
        )
        if not sample.wait_for_func_status(result=True):
            logger.error(
                "After 2500 seconds the used capacity is still smaller than 85%"
            )
            raise TimeoutExpiredError
        logger.info("Cluster capacity successfully reached 85%")

        logger.test_step("Verify cluster full alerts are triggered")
        logger.info(
            "Checking for 'CephClusterCriticallyFull' and 'CephOSDNearFull' alerts"
        )
        expected_alerts = ["CephOSDCriticallyFull", "CephOSDNearFull"]
        prometheus = PrometheusAPI(threading_lock=threading_lock)
        sample = TimeoutSampler(
            timeout=600,
            sleep=50,
            func=prometheus.verify_alerts_via_prometheus,
            expected_alerts=expected_alerts,
            threading_lock=threading_lock,
        )
        if not sample.wait_for_func_status(result=True):
            logger.error(f"The alerts {expected_alerts} do not exist after 600 seconds")
            raise TimeoutExpiredError
        logger.info("Cluster full alerts verified successfully")

        logger.test_step("Respin mon, mgr, and osd pods")
        for pod_name in ("mon", "mgr", "osd"):
            logger.info(f"Respinning {pod_name} pods")
            disruption = disruption_helpers.Disruptions()
            disruption.set_resource(resource=f"{pod_name}")
            disruption.delete_resource()
        logger.info("All storage pods respun successfully")

        logger.test_step("Validate all storage pods are running")
        wait_for_storage_pods()
        logger.info("All storage pods are running")

        logger.test_step("Create PVC2 and verify they remain in Pending state")
        logger.info("Creating PVC2 [CEPH-FS + CEPH-RBD] and expecting Pending state")
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
        logger.info(
            "Waiting 20 sec to verify PVC2 [CEPH-FS + CEPH-RBD] are in Pending state."
        )
        time.sleep(20)
        helpers.wait_for_resource_state(
            resource=pvc_obj_rbd2, state=constants.STATUS_PENDING
        )
        helpers.wait_for_resource_state(
            resource=pvc_obj_fs2, state=constants.STATUS_PENDING
        )
        logger.info("PVC2 [CEPH-FS + CEPH-RBD] verified in Pending state")

        logger.test_step("Create snapshots from PVC1 and verify readyToUse is false")
        logger.info("Creating snapshots from PVC1")
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
        logger.info("Snapshots verified in false readyToUse state")

        logger.test_step("Increase Ceph full ratio from 85% to 95%")
        logger.info("Changing Ceph full_ratio from 85% to 95%")
        logger.info(
            "Based on doc we need to change the ceph_full_ratio to 88%, but we run "
            "many fio pods therefore, it may not be enough to increase by only 3%"
        )
        change_ceph_full_ratio(95)
        logger.info("Ceph full_ratio increased to 95%")

        logger.test_step("Delete benchmark operator PVCs to free up space")
        logger.info("Cleaning up benchmark-operator PVCs")
        self.benchmark_obj.cleanup()
        self.benchmark_operator_teardown = False
        logger.info("Benchmark operator PVCs deleted successfully")

        logger.test_step("Verify PVC2 [CEPH-FS + CEPH-RBD] move to Bound state")
        logger.info("Waiting for PVC2 to reach Bound state")
        helpers.wait_for_resource_state(
            resource=pvc_obj_rbd2, state=constants.STATUS_BOUND, timeout=600
        )
        helpers.wait_for_resource_state(
            resource=pvc_obj_fs2, state=constants.STATUS_BOUND, timeout=600
        )
        logger.info("PVC2 [CEPH-FS + CEPH-RBD] successfully moved to Bound state")

        logger.test_step("Verify snapshots move from false to true readyToUse state")
        logger.info("Checking snapshot readyToUse status")
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
        logger.info("Snapshots successfully moved to true readyToUse state")

        logger.test_step("Restore PVCs from snapshots and verify data integrity")
        logger.info(f"Creating RBD PVC from snapshot {snap_rbd1_obj.name}")
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

        logger.info(f"Creating a PVC from snapshot [restore] {snap_fs1_obj.name}")
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

        logger.test_step("Verify restored data integrity matches original")
        logger.assertion(
            f"CephFS restore md5: expected={pod_fs1_obj.md5}, actual={pod_restore_fs1_obj.md5}, "
            f"match={pod_restore_fs1_obj.md5 == pod_fs1_obj.md5}"
        )
        assert pod_restore_fs1_obj.md5 == pod_fs1_obj.md5, (
            f"md5sum of restore_fs1 {pod_restore_fs1_obj.md5} is not equal "
            f"to pod_fs1_obj {pod_fs1_obj.md5}"
        )
        logger.assertion(
            f"RBD restore md5: expected={pod_rbd1_obj.md5}, actual={pod_restore_rbd1_obj.md5}, "
            f"match={pod_restore_rbd1_obj.md5 == pod_rbd1_obj.md5}"
        )
        assert pod_restore_rbd1_obj.md5 == pod_rbd1_obj.md5, (
            f"md5sum of restore_rbd1 {pod_restore_rbd1_obj.md5} is not equal "
            f"to pod_rbd1_obj {pod_rbd1_obj.md5}"
        )
        logger.info("Data integrity verified successfully for all restored PVCs")
