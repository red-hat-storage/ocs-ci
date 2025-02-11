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
    clear_crash_warning_and_osd_removal_leftovers,
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
        clone_vm_workload,
        snapshot_factory,
        snapshot_restore_factory,
        cnv_workload,
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

        # Create VM using cloned pvc of source VM PVC
        vm_for_clone.stop()
        clone_obj = clone_vm_workload(
            vm_obj=vm_for_clone,
            volume_interface=vm_for_clone.volume_interface,
            namespace=vm_for_clone.namespace,
        )
        all_vms.append(clone_obj)
        csum = cal_md5sum_vm(vm_obj=clone_obj, file_path=file_paths[0])
        source_csums[clone_obj.name] = csum

        # Create a snapshot
        # Taking Snapshot of PVC
        pvc_obj = vm_for_snap.get_vm_pvc_obj()
        snap_obj = snapshot_factory(pvc_obj)

        # Restore the snapshot
        res_snap_obj = snapshot_restore_factory(
            snapshot_obj=snap_obj,
            storageclass=vm_for_snap.sc_name,
            volume_mode=snap_obj.parent_volume_mode,
            access_mode=vm_for_snap.pvc_access_mode,
            status=constants.STATUS_BOUND,
            timeout=300,
        )

        # Create new VM using the restored PVC
        res_vm_obj = cnv_workload(
            source_url=constants.CNV_FEDORA_SOURCE,
            storageclass=vm_for_snap.sc_name,
            existing_pvc_obj=res_snap_obj,
            namespace=vm_obj.namespace,
        )
        all_vms.append(res_vm_obj)
        csum = cal_md5sum_vm(vm_obj=res_vm_obj, file_path=file_paths[0])
        source_csums[res_vm_obj.name] = csum

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

        # ToDo: check VMs status

        # Perform post device replacement data integrity check
        for vm_obj in all_vms:
            new_csum = cal_md5sum_vm(vm_obj=vm_obj, file_path=file_paths[0])
            assert source_csums[vm_obj.name] == new_csum, (
                f"ERROR: Failed data integrity before replacing device and after replacing the device "
                f"for VM '{vm_obj.name}'."
            )

        logger.info("Clear crash warnings and osd removal leftovers")
        clear_crash_warning_and_osd_removal_leftovers()

        for vm_obj in all_vms:
            vm_obj.stop()
