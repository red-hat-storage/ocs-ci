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
    logger.info("Setting up CNV workload and creating initial data")
    proj_obj = project_factory_class()
    file_paths = ["/source_file.txt", "/new_file.txt"]
    (
        vm_objs_def,
        vm_objs_aggr,
        _,
        _,
    ) = multi_cnv_workload_class(namespace=proj_obj.namespace)
    all_vms = vm_objs_def + vm_objs_aggr
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
        logger.info(f"testing mon failures scenario with {mon_count} mon")

        mons = ceph_obj.get_mons_from_cluster()[:mon_count]

        def teardown():
            logger.info("[TEARDOWN] Restoring mons back to 1 replica each")
            for mon in mons:
                try:
                    modify_deployment_replica_count(mon, 1)
                except Exception as e:
                    logger.error(f"Failed to restore mon {mon}: {e}")

        request.addfinalizer(teardown)

        for mon in mons:
            logger.info(f"Scaling down mon deployment {mon} to 0 replicas")
            modify_deployment_replica_count(mon, 0)

        logger.info(
            "Sleeping for 300 seconds to emulate a condition where the 2 mons is inaccessibe  for 10 seconds."
        )
        time.sleep(300)
        for mon in mons:
            logger.info(f"Scaling mon deployment {mon} back to 1 replica")
            modify_deployment_replica_count(mon, 1)

        wait_for_pods_by_label_count(
            label=constants.MON_APP_LABEL, expected_count=3, timeout=300
        )
        # Check ceph health status
        utils.ceph_health_check(tries=20)
        # Data integrity validation here
        all_vms, source_csums, file_paths = setup_cnv_workload
        for vm_obj in all_vms:
            vm_obj.wait_for_ssh_connectivity()
            md5sum_after = cal_md5sum_vm(vm_obj, file_path=file_paths[0])
            assert (
                source_csums[vm_obj.name] == md5sum_after
            ), f"Data integrity failed for VM {vm_obj.name}"
            logger.info(f"Data integrity maintained for VM {vm_obj.name}")
            run_dd_io(vm_obj=vm_obj, file_path=file_paths[1])

    @polarion_id("OCS-6608")
    def test_single_osd_failure(self, request, setup_cnv_workload):
        """
        Test single osd failure with VM workloads running in the background

        """
        logger.info("testing single osd failure scenarios")

        osd_pods = get_osd_pods()
        osd_pod_to_fail = random.choice(osd_pods).name
        osd_dep = get_deployment_name(osd_pod_to_fail)

        def teardown():
            logger.info(f"[TEARDOWN] Restoring OSD deployment {osd_dep} to 1 replica")
            try:
                modify_deployment_replica_count(osd_dep, 1)
            except Exception as e:
                logger.error(f"Failed to restore osd {osd_dep}: {e}")

        request.addfinalizer(teardown)

        # scale down the osd deployment to 0
        logger.info(f"Failing osd by scaling down osd deployment {osd_dep}")
        if modify_deployment_replica_count(osd_dep, 0):
            time.sleep(600)

        # scale the deployment back to 1
        logger.info(f"Recovering osd by scaling up osd deployment {osd_dep}")
        modify_deployment_replica_count(osd_dep, 1)

        wait_for_pods_by_label_count(
            label=constants.OSD_APP_LABEL, expected_count=3, timeout=300
        )

        # Data integrity validation here
        all_vms, source_csums, file_paths = setup_cnv_workload
        for vm_obj in all_vms:
            vm_obj.wait_for_ssh_connectivity()
            md5sum_after = cal_md5sum_vm(vm_obj, file_path=file_paths[0])
            assert (
                source_csums[vm_obj.name] == md5sum_after
            ), f"Data integrity failed for VM {vm_obj.name}"
            logger.info(f"Data integrity maintained for VM {vm_obj.name}")
            run_dd_io(vm_obj=vm_obj, file_path=file_paths[1])
