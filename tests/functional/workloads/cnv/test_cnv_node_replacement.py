import logging
import pytest
import random

from ocs_ci.framework.testlib import E2ETest
from ocs_ci.framework.pytest_customization.marks import (
    workloads,
    magenta_squad,
    ignore_leftovers,
    skipif_external_mode,
    skipif_bm,
)
from ocs_ci.helpers.cnv_helpers import cal_md5sum_vm, run_dd_io
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import CephCluster
from tests.functional.z_cluster.nodes.test_node_replacement_proactive import (
    delete_and_create_osd_node,
)
from ocs_ci.helpers.helpers import (
    verify_storagecluster_nodetopology,
)
from ocs_ci.helpers.sanity_helpers import Sanity

logger = logging.getLogger(__name__)


@magenta_squad
@workloads
@ignore_leftovers
@skipif_bm
@skipif_external_mode
class TestCnvNodeReplace(E2ETest):
    """
    Node replacement proactive

    """

    @pytest.fixture(autouse=True)
    def setup(self, request, project_factory, multi_cnv_workload):
        """
        Setting up VMs for tests

        """

        logger.test_step("Create project and deploy CNV workload VMs")
        proj_obj = project_factory()
        (
            self.vm_objs_def,
            self.vm_objs_aggr,
            self.sc_obj_def_compr,
            self.sc_obj_aggressive,
        ) = multi_cnv_workload(namespace=proj_obj.namespace)
        logger.info(
            f"Created {len(self.vm_objs_def + self.vm_objs_aggr)} VMs successfully"
        )

    def test_vms_with_node_replacement(
        self,
        setup_cnv,
        setup,
        vm_clone_fixture,
        vm_snapshot_restore_fixture,
        admin_client,
    ):
        """
        Node Replacement proactive
        """
        logger.test_step("Write initial data and create clone/snapshot VMs")
        all_vms = self.vm_objs_def + self.vm_objs_aggr
        file_paths = ["/source_file.txt", "/new_file.txt"]
        source_csums = {}
        for vm_obj in all_vms:
            source_csum = run_dd_io(vm_obj=vm_obj, file_path=file_paths[0], verify=True)
            source_csums[vm_obj.name] = source_csum

        eligible_vms = [vm for vm in all_vms if vm.pvc_access_mode != "ReadWriteOnce"]

        self.vm_obj_on_replacing_node = random.choice(eligible_vms)

        remaining_vms = [
            vm
            for vm in all_vms
            if vm != self.vm_obj_on_replacing_node
            and vm.pvc_access_mode != "ReadWriteOnce"
        ]
        self.vm_for_clone, self.vm_for_stop, self.vm_for_snap = random.sample(
            remaining_vms, 3
        )
        logger.info(
            f"Selected VMs: on_replacing_node='{self.vm_obj_on_replacing_node.name}', "
            f"clone='{self.vm_for_clone.name}', stop='{self.vm_for_stop.name}', "
            f"snapshot='{self.vm_for_snap.name}'"
        )

        for vm in [self.vm_for_clone, self.vm_for_snap]:
            op = "clone" if vm == self.vm_for_clone else "snapshot"
            logger.info(f"Creating {op} of VM '{vm.name}'")
            vm_obj = (
                vm_clone_fixture(vm, admin_client)
                if vm == self.vm_for_clone
                else vm_snapshot_restore_fixture(vm, admin_client)
            )

            source_csums[vm_obj.name] = cal_md5sum_vm(vm_obj, file_paths[0])
            if vm == self.vm_for_clone:
                all_vms.append(vm_obj)

        logger.test_step("Set VM states and perform node replacement")
        node_name = self.vm_obj_on_replacing_node.get_vmi_instance().node()
        logger.info(f"Target node for replacement: '{node_name}'")

        stopped_rwo_vm = None
        for vm_rwo in all_vms:
            if (
                vm_rwo != self.vm_obj_on_replacing_node
                and vm_rwo.get_vmi_instance().node() == node_name
                and vm_rwo.pvc_access_mode == "ReadWriteOnce"
                and vm_rwo.ready()
            ):
                logger.info(
                    f"Stopping RWO VM '{vm_rwo.name}' on target node to avoid drain stuck"
                )
                vm_rwo.stop()
                stopped_rwo_vm = vm_rwo
                break

        logger.info(
            f"Stopping VM '{self.vm_for_stop.name}' and pausing VM '{self.vm_for_snap.name}'"
        )
        self.vm_for_stop.stop()
        self.vm_for_snap.pause()

        logger.info(f"Replacing node '{node_name}'")
        delete_and_create_osd_node(node_name)

        logger.test_step("Verify cluster health and Ceph rebalance")
        self.sanity_helpers = Sanity()
        self.sanity_helpers.health_check(tries=120)

        ceph_cluster_obj = CephCluster()
        logger.assertion("Ceph data rebalance completion")
        assert ceph_cluster_obj.wait_for_rebalance(
            timeout=1800
        ), "Data re-balance failed to complete"

        logger.assertion("StorageCluster node topology validity")
        assert (
            verify_storagecluster_nodetopology()
        ), "StorageCluster node topology contains non-OCS node entries"

        logger.test_step("Verify VM state after node replacement")
        vm_status = self.vm_obj_on_replacing_node.printableStatus()
        logger.assertion(
            f"VM running state: vm='{self.vm_obj_on_replacing_node.name}', "
            f"expected='{constants.VM_RUNNING}', actual='{vm_status}', "
            f"match={vm_status == constants.VM_RUNNING}"
        )
        assert (
            vm_status == constants.VM_RUNNING
        ), f"VM '{self.vm_obj_on_replacing_node.name}' not running after node replacement, status: '{vm_status}'"

        logger.info(
            f"Starting VM '{self.vm_for_stop.name}' and unpausing VM '{self.vm_for_snap.name}'"
        )
        self.vm_for_stop.start()
        if stopped_rwo_vm and not stopped_rwo_vm.ready():
            stopped_rwo_vm.start()
        if self.vm_for_snap.printableStatus() == constants.VM_PAUSED:
            self.vm_for_snap.unpause()

        logger.test_step("Verify data integrity on all VMs after node replacement")
        for vm_obj in all_vms:
            new_csum = cal_md5sum_vm(vm_obj=vm_obj, file_path=file_paths[0])
            logger.assertion(
                f"Data integrity: vm='{vm_obj.name}', "
                f"expected='{source_csums[vm_obj.name]}', actual='{new_csum}', "
                f"match={source_csums[vm_obj.name] == new_csum}"
            )
            assert (
                source_csums[vm_obj.name] == new_csum
            ), f"MD5 mismatch for VM '{vm_obj.name}' after node replacement"
        logger.info("Data integrity verified on all VMs")
