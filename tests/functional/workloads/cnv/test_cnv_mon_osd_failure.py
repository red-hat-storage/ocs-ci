import random
import time
import pytest
import logging


from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import (
    polarion_id,
    magenta_squad,
    skipif_external_mode,
)
from ocs_ci.helpers.helpers import modify_deployment_replica_count
from ocs_ci.ocs.resources.pod import get_deployment_name, wait_for_pods_by_label_count
from ocs_ci.helpers.cnv_helpers import cal_md5sum_vm, run_dd_io
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.resources.pod import get_osd_pods
from ocs_ci.utility import utils

logger = logging.getLogger(__name__)


@pytest.fixture(scope="class")
def setup_cnv_workload(
    request, setup_cnv, project_factory_class, multi_cnv_workload_class
):
    """
    Set up CNV workload and create initial data.
    """
    logger.test_step("Set up CNV workload and create initial data")
    proj_obj = project_factory_class()
    file_paths = ["/source_file.txt", "/new_file.txt"]
    (
        vm_objs_def,
        vm_objs_aggr,
        _,
        _,
    ) = multi_cnv_workload_class(namespace=proj_obj.namespace)
    all_vms = vm_objs_def + vm_objs_aggr
    logger.info(f"Created {len(all_vms)} VMs for mon/OSD failure testing")
    source_csums = {
        vm_obj.name: run_dd_io(vm_obj, file_path=file_paths[0], verify=True)
        for vm_obj in all_vms
    }
    return all_vms, source_csums, file_paths


@magenta_squad
@skipif_external_mode
@pytest.mark.usefixtures("setup_cnv_workload")
class TestMonAndOSDFailures:
    """
    Tests Mon and OSD failures while CNV workloads are running.
    """

    @polarion_id("OCS-6609")
    @pytest.mark.parametrize("mon_count", [1, 2])
    def test_mon_failures(self, mon_count, request, setup_cnv_workload):
        """
        Test mon failure with VM workloads running in the background

        """
        ceph_obj = CephCluster()
        logger.test_step(f"Simulate {mon_count} mon failure(s)")

        mons = ceph_obj.get_mons_from_cluster()[:mon_count]

        def teardown():
            logger.info("Restoring mons back to 1 replica each")
            errors = []
            for mon in mons:
                try:
                    modify_deployment_replica_count(mon, 1)
                except Exception as e:
                    logger.exception(f"Failed to restore mon '{mon}': {e}")
                    errors.append(str(e))
            if errors:
                raise RuntimeError(
                    f"MON restoration failed for {len(errors)} deployment(s): {errors}"
                )

        request.addfinalizer(teardown)

        for mon in mons:
            logger.info(f"Scaling down mon deployment '{mon}' to 0 replicas")
            modify_deployment_replica_count(mon, 0)

        logger.info(
            f"Waiting 300 seconds to simulate {mon_count} mon(s) being inaccessible"
        )
        time.sleep(300)

        logger.test_step("Restore mons and verify Ceph health")
        for mon in mons:
            logger.info(f"Scaling mon deployment '{mon}' back to 1 replica")
            modify_deployment_replica_count(mon, 1)

        wait_for_pods_by_label_count(
            label=constants.MON_APP_LABEL, expected_count=3, timeout=300
        )
        logger.info("Running Ceph health check")
        utils.ceph_health_check(tries=20)

        logger.test_step("Verify data integrity and run I/O on all VMs")
        all_vms, source_csums, file_paths = setup_cnv_workload
        for vm_obj in all_vms:
            vm_obj.wait_for_ssh_connectivity()
            md5sum_after = cal_md5sum_vm(vm_obj, file_path=file_paths[0])
            logger.assertion(
                f"Data integrity: vm='{vm_obj.name}', "
                f"expected='{source_csums[vm_obj.name]}', actual='{md5sum_after}', "
                f"match={source_csums[vm_obj.name] == md5sum_after}"
            )
            assert (
                source_csums[vm_obj.name] == md5sum_after
            ), f"MD5 mismatch for VM '{vm_obj.name}' after mon failure"
            run_dd_io(vm_obj=vm_obj, file_path=file_paths[1])
        logger.info("Data integrity verified and I/O completed on all VMs")

    @polarion_id("OCS-6608")
    def test_single_osd_failure(self, request, setup_cnv_workload):
        """
        Test single osd failure with VM workloads running in the background

        """
        logger.test_step("Simulate single OSD failure")

        osd_pods = get_osd_pods()
        osd_pod_to_fail = random.choice(osd_pods).name
        osd_dep = get_deployment_name(osd_pod_to_fail)
        logger.info(
            f"Selected OSD pod '{osd_pod_to_fail}' (deployment: '{osd_dep}') for failure"
        )

        def teardown():
            logger.info(f"Restoring OSD deployment '{osd_dep}' to 1 replica")
            try:
                modify_deployment_replica_count(osd_dep, 1)
            except Exception as e:
                logger.exception(f"Failed to restore OSD deployment '{osd_dep}': {e}")
                raise

        request.addfinalizer(teardown)

        logger.info(f"Scaling down OSD deployment '{osd_dep}' to 0 replicas")
        if modify_deployment_replica_count(osd_dep, 0):
            logger.info("Waiting 600 seconds to simulate OSD being down")
            time.sleep(600)

        logger.test_step("Restore OSD and verify pod recovery")
        logger.info(f"Scaling OSD deployment '{osd_dep}' back to 1 replica")
        modify_deployment_replica_count(osd_dep, 1)

        wait_for_pods_by_label_count(
            label=constants.OSD_APP_LABEL, expected_count=3, timeout=300
        )

        logger.test_step("Verify data integrity and run I/O on all VMs")
        all_vms, source_csums, file_paths = setup_cnv_workload
        for vm_obj in all_vms:
            vm_obj.wait_for_ssh_connectivity()
            md5sum_after = cal_md5sum_vm(vm_obj, file_path=file_paths[0])
            logger.assertion(
                f"Data integrity: vm='{vm_obj.name}', "
                f"expected='{source_csums[vm_obj.name]}', actual='{md5sum_after}', "
                f"match={source_csums[vm_obj.name] == md5sum_after}"
            )
            assert (
                source_csums[vm_obj.name] == md5sum_after
            ), f"MD5 mismatch for VM '{vm_obj.name}' after OSD failure"
            run_dd_io(vm_obj=vm_obj, file_path=file_paths[1])
        logger.info("Data integrity verified and I/O completed on all VMs")
