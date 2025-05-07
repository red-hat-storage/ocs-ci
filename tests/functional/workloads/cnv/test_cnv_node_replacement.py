import logging
import pytest
import random

from ocs_ci.framework import config
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.framework.pytest_customization.marks import workloads, magenta_squad
from ocs_ci.helpers.cnv_helpers import cal_md5sum_vm, run_dd_io
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.resources.storage_cluster import osd_encryption_verification
from tests.functional.z_cluster.nodes.test_node_replacement_proactive import (
    delete_and_create_osd_node,
    select_osd_node_name,
)
from ocs_ci.helpers.helpers import (
    verify_storagecluster_nodetopology,
)
from ocs_ci.helpers.sanity_helpers import Sanity

logger = logging.getLogger(__name__)


@magenta_squad
@workloads
class TestCnvNodeReplace(E2ETest):
    """
    Node replacement proactive

    """

    @pytest.fixture(autouse=True)
    def setup(self, project_factory, multi_cnv_workload):
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

        logger.info("All vms created successfully")

    def test_vms_with_node_replacement(
        self,
        setup_cnv,
        setup,
    ):
        """
        Node Replacement proactive
        """
        all_vms = self.vm_objs_def + self.vm_objs_aggr

        file_paths = ["/source_file.txt", "/new_file.txt"]
        source_csums = {}
        for vm_obj in all_vms:
            source_csum = run_dd_io(vm_obj=vm_obj, file_path=file_paths[0], verify=True)
            source_csums[vm_obj.name] = source_csum

        # Choose VMs randomaly
        vm_for_clone, vm_for_stop, vm_for_snap = random.sample(all_vms, 3)

        # Uncomment code ones 11088 merged.
        """
        # Create Clone of VM
        cloned_vm = clone_or_snapshot_vm(
            "clone",
            vm_for_clone,
            admin_client=admin_client,
            all_vms=all_vms,
            file_path=file_paths[0],
        )
        csum = cal_md5sum_vm(vm_obj=cloned_vm, file_path=file_paths[0])
        source_csums[cloned_vm.name] = csum
        # Create a snapshot
        vm_for_snap = clone_or_snapshot_vm(
            "snapshot", vm_for_snap, admin_client=admin_client, file_path=file_paths[0]
        )
        csum = cal_md5sum_vm(vm_obj=vm_for_snap, file_path=file_paths[0])
        source_csums[vm_for_snap.name] = csum
        """

        # Keep vms in different states (power on, paused, stoped)
        vm_for_stop.stop()
        vm_for_snap.pause()

        # Replace Node
        osd_node_name = select_osd_node_name()
        delete_and_create_osd_node(osd_node_name)

        # Verify everything running fine
        logger.info("Verifying All resources are Running and matches expected result")
        self.sanity_helpers = Sanity()
        self.sanity_helpers.health_check(tries=120)

        # Verify OSD encrypted
        if config.ENV_DATA.get("encryption_at_rest"):
            osd_encryption_verification()

        ceph_cluster_obj = CephCluster()
        assert ceph_cluster_obj.wait_for_rebalance(
            timeout=1800
        ), "Data re-balance failed to complete"

        assert (
            verify_storagecluster_nodetopology
        ), "Storagecluster node topology is having an entry of non ocs node(s) - Not expected"

        # Check VMs status
        assert (
            vm_for_stop.printableStatus() == constants.CNV_VM_STOPPED
        ), "VM did not stop with preserved state after device replacement."
        logger.info("After device replacement, stopped VM preserved state.")

        assert (
            vm_for_snap.printableStatus() == constants.VM_PAUSED
        ), "VM did not pause with preserved state after device replacement."
        logger.info("After device replacement, paused VM preserved state.")

        logger.info("Starting vms")
        vm_for_stop.start()
        vm_for_clone.start()
        vm_for_snap.unpause()

        # Perform post node replacement data integrity check
        for vm_obj in all_vms:
            new_csum = cal_md5sum_vm(vm_obj=vm_obj, file_path=file_paths[0])
            assert source_csums[vm_obj.name] == new_csum, (
                f"ERROR: Failed data integrity before replacing device and after replacing the device "
                f"for VM '{vm_obj.name}'."
            )

        for vm_obj in all_vms:
            vm_obj.stop()
