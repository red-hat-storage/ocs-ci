import logging
import random
import pytest
from ocs_ci.framework.pytest_customization.marks import (
    magenta_squad,
    workloads,
    ignore_leftovers,
)
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.helpers.cnv_helpers import (
    all_nodes_ready,
    cal_md5sum_vm,
    setup_kms_and_storageclass,
    create_and_clone_vms,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility.utils import TimeoutSampler
from tests.functional.z_cluster.cluster_expansion.test_add_capacity import (
    add_capacity_test,
)

logger = logging.getLogger(__name__)


@pytest.mark.polarion_id("OCS-")
@ignore_leftovers
@magenta_squad
@workloads
class TestVmStorageCapacity(E2ETest):
    """
    Perform add capacity operation while the VMs are in different states
    and in the presence of snapshots and clones of the VMs.
    """

    def test_vm_storage_capacity(
        self,
        setup_cnv,
        pv_encryption_kms_setup_factory,
        storageclass_factory,
        project_factory,
        cnv_workload,
        clone_vm_workload,
        snapshot_factory,
    ):
        """
        Test steps:
        1. Keep IO operations going on VMs, with snapshots and clones present.
        2. Keep VMs in different states (running, paused, stopped).
        3. Perform add capacity using official docs.
        4. Verify Cluster Stability and Data Integrity.
        5. Ensure the additional storage has been added.
        6. Verify VMs, snapshots and clones have preserved their states and
        data integrity.
        """
        file_paths = ["/source_file.txt", "/new_file.txt"]
        # Setup csi-kms-connection-details configmap and project
        proj_obj, kms, sc_obj_def = setup_kms_and_storageclass(
            pv_encryption_kms_setup_factory, storageclass_factory, project_factory
        )
        vm_list, vm_list_clone, source_csum, res_csum = create_and_clone_vms(
            cnv_workload=cnv_workload,
            clone_vm_workload=clone_vm_workload,
            proj_obj=proj_obj,
            sc_obj_def=sc_obj_def,
            file_paths=file_paths,
            number_of_vm=3,
        )

        logger.info("Stopping VMs in random order...")
        vm_stopped = random.sample(vm_list, 1)
        for vm_obj in vm_stopped:
            logger.info(f"Stopping VM: {vm_obj.name}")
            vm_obj.stop()
            snapshot_factory(vm_obj.get_vm_pvc_obj())
            vm_list.remove(vm_obj)

        logger.info("Pausing VMs in random order...")
        vm_pause = random.sample(vm_list, 1)
        for vm in vm_pause:
            logger.info(f"Pausing VM: {vm.name}")
            vm.pause()
            vm_list.remove(vm)

        initial_vm_states = {
            vm_obj.name: vm_obj.printableStatus()
            for vm_obj in vm_list + vm_list_clone + vm_stopped + vm_pause
        }
        logger.info(f"Initial VM states: {initial_vm_states}")

        logger.info("Adding storage capacity...")
        add_capacity_test()
        logger.info("Added storage capacity!")

        logger.info("Verifying cluster stability after capacity addition...")
        assert all_nodes_ready(), "Some nodes are not ready!"

        sample = TimeoutSampler(
            timeout=600,
            sleep=10,
            func=wait_for_pods_to_be_running,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        assert sample.wait_for_func_status(result=True), (
            "Not all pods are running after capacity "
            f"addition in {constants.OPENSHIFT_STORAGE_NAMESPACE}"
        )

        final_vm_states = {
            vm_obj.name: vm_obj.printableStatus()
            for vm_obj in vm_list + vm_list_clone + vm_stopped + vm_pause
        }
        logger.info(f"Final VM states: {final_vm_states}")
        for vm_name in initial_vm_states:
            assert initial_vm_states[vm_name] == final_vm_states[vm_name], (
                f"VM state mismatch for {vm_name}: "
                f"initial state was {initial_vm_states[vm_name]}, "
                f"but final state is {final_vm_states[vm_name]}"
            )

        for vm_obj in vm_list_clone:
            result_checksum = res_csum.get(vm_obj.name)
            assert (
                cal_md5sum_vm(vm_obj=vm_obj, file_path=file_paths[0]) == result_checksum
            ), f"Failed: MD5 comparison between source {vm_obj.name} and its cloned VMs"

        logger.info("Stopping VMs...")
        for vm_obj in vm_list_clone + vm_list:
            logger.info(f"Stopping VM: {vm_obj.name}")
            vm_obj.stop()
