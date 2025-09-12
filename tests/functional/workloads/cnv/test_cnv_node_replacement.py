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

        # Create a project
        proj_obj = project_factory()
        (
            self.vm_objs_def,
            self.vm_objs_aggr,
            self.sc_obj_def_compr,
            self.sc_obj_aggressive,
        ) = multi_cnv_workload(namespace=proj_obj.namespace)

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
        all_vms = self.vm_objs_def + self.vm_objs_aggr
        logger.info(f"list of all vms: {all_vms}")
        file_paths = ["/source_file.txt", "/new_file.txt"]
        source_csums = {}
        for vm_obj in all_vms:
            source_csum = run_dd_io(vm_obj=vm_obj, file_path=file_paths[0], verify=True)
            source_csums[vm_obj.name] = source_csum

        # Filter out VMs that do not have ReadWriteOnce access mode
        eligible_vms = [vm for vm in all_vms if vm.pvc_access_mode != "ReadWriteOnce"]

        # Pick one VM for replacing_node from eligible set
        self.vm_obj_on_replacing_node = random.choice(eligible_vms)

        # Pick 3 random VMs from the remaining pool
        remaining_vms = [
            vm
            for vm in all_vms
            if vm != self.vm_obj_on_replacing_node
            and vm.pvc_access_mode != "ReadWriteOnce"
        ]
        self.vm_for_clone, self.vm_for_stop, self.vm_for_snap = random.sample(
            remaining_vms, 3
        )

        for vm in [self.vm_for_clone, self.vm_for_snap]:
            vm_obj = (
                vm_clone_fixture(vm, admin_client)
                if vm == self.vm_for_clone
                else vm_snapshot_restore_fixture(vm, admin_client)
            )

            # Use cal_md5sum_vm here
            source_csums[vm_obj.name] = cal_md5sum_vm(vm_obj, file_paths[0])
            if vm == self.vm_for_clone:
                all_vms.append(vm_obj)

        # Find node where VM is running
        node_name = self.vm_obj_on_replacing_node.get_vmi_instance().node()

        # Stop VM if its not live migratable to avoid drain stuck issue.
        for vm_rwo in all_vms:
            if (
                vm_rwo != self.vm_obj_on_replacing_node
                and vm_rwo.get_vmi_instance().node() == node_name
            ):
                if vm_rwo.pvc_access_mode == "ReadWriteOnce" and vm_rwo.ready():
                    vm_rwo.stop()
                    break

        # Keep vms in different states(paused, stoped)
        self.vm_for_stop.stop()
        self.vm_for_snap.pause()

        # Replace Node
        delete_and_create_osd_node(node_name)

        logger.info("Verifying All resources are Running and matches expected result")
        self.sanity_helpers = Sanity()
        self.sanity_helpers.health_check(tries=120)

        ceph_cluster_obj = CephCluster()
        assert ceph_cluster_obj.wait_for_rebalance(
            timeout=1800
        ), "Data re-balance failed to complete"

        assert (
            verify_storagecluster_nodetopology
        ), "Storagecluster node topology is having an entry of non ocs node(s) - Not expected"

        # Check VM status
        assert (
            self.vm_obj_on_replacing_node.printableStatus() == constants.VM_RUNNING
        ), "VM is not in ruuning state after node replacement."
        logger.info("After Node replacement vm is running.")

        logger.info("Starting vms")
        self.vm_for_stop.start()
        vm_rwo.start()
        if self.vm_for_snap.printableStatus() == constants.VM_PAUSED:
            self.vm_for_snap.unpause()

        # Perform post node replacement data integrity check
        for vm_obj in all_vms:
            new_csum = cal_md5sum_vm(vm_obj=vm_obj, file_path=file_paths[0])
            assert source_csums[vm_obj.name] == new_csum, (
                f"ERROR: Failed data integrity before replacing node and after replacing the node "
                f"for VM '{vm_obj.name}'."
            )
