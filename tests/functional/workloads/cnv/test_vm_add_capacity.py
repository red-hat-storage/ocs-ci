import logging
import random
import pytest
from ocs_ci.framework.pytest_customization.marks import (
    magenta_squad,
    workloads,
    ignore_leftovers,
    skipif_external_mode,
)
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.helpers.cnv_helpers import (
    cal_md5sum_vm,
    setup_kms_and_storageclass,
    create_and_clone_vms,
    run_dd_io,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility.utils import TimeoutSampler
from tests.functional.z_cluster.cluster_expansion.test_add_capacity import (
    add_capacity_test,
)

logger = logging.getLogger(__name__)


@pytest.mark.polarion_id("OCS-6395")
@skipif_external_mode
@ignore_leftovers
@magenta_squad
@workloads
class TestVmAddCapacity(E2ETest):
    """
    Perform add capacity operation while the VMs are in different states
    and in the presence of snapshots and clones of the VMs.
    """

    @pytest.fixture()
    def setup(
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
        Sets up the test environment:
        - Creates KMS and StorageClass.
        - Creates initial VMs and clones.
        - Stops and pauses a subset of VMs.
        """
        self.file_paths = ["/source_file.txt", "/new_file.txt"]
        self.proj_obj, self.kms, self.sc_obj_def = setup_kms_and_storageclass(
            pv_encryption_kms_setup_factory, storageclass_factory, project_factory
        )
        (
            self.vm_list,
            self.vm_list_clone,
            self.init_csum,
        ) = create_and_clone_vms(
            cnv_workload=cnv_workload,
            clone_vm_workload=clone_vm_workload,
            proj_obj=self.proj_obj,
            sc_obj_def=self.sc_obj_def,
            file_paths=self.file_paths,
            number_of_vm=3,
        )

        self.vm_stopped = random.sample(self.vm_list, 1)
        logger.info(f"Stopping VM: {self.vm_stopped[0].name}")
        self.vm_stopped[0].stop()
        snapshot_factory(self.vm_stopped[0].get_vm_pvc_obj())
        self.vm_list.remove(self.vm_stopped[0])

        self.vm_pause = random.sample(self.vm_list, 1)
        logger.info(f"Pausing VM: {self.vm_pause[0].name}")
        self.vm_pause[0].pause()
        self.vm_list.remove(self.vm_pause[0])

        self.initial_vm_states = {
            vm_obj.name: vm_obj.printableStatus()
            for vm_obj in self.vm_list
            + self.vm_list_clone
            + self.vm_stopped
            + self.vm_pause
        }
        logger.info(f"Initial VM states: {self.initial_vm_states}")

    def test_vm_add_capacity(self, setup):
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
        logger.info("Adding storage capacity...")
        add_capacity_test()
        logger.info("Added storage capacity!")

        logger.info(
            f"Waiting for pods in {constants.OPENSHIFT_STORAGE_NAMESPACE} to be running"
        )
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

        logger.info("Getting final VM states...")
        final_vm_states = {
            vm_obj.name: vm_obj.printableStatus()
            for vm_obj in self.vm_list
            + self.vm_list_clone
            + self.vm_stopped
            + self.vm_pause
        }
        logger.info(f"Final VM states: {final_vm_states}")

        logger.info("Verifying VM states after capacity addition...")
        for vm_name, initial_state in self.initial_vm_states.items():
            assert (
                initial_state == final_vm_states[vm_name]
            ), f"VM {vm_name} state changed: Initial={initial_state}, Final={final_vm_states[vm_name]}"

        logger.info("Starting the VMs which were stopped and paused")
        self.vm_stopped[0].start()
        self.vm_pause[0].unpause()

        logger.info("Verifying data integrity for VMs")
        vms_to_verify = (
            self.vm_list_clone + self.vm_stopped + self.vm_pause + self.vm_list
        )

        for vm_obj in vms_to_verify:
            expected_checksum = self.init_csum.get(vm_obj.name)
            logger.info(f"Calculating checksum for VM {vm_obj.name}")
            actual_checksum = cal_md5sum_vm(vm_obj=vm_obj, file_path=self.file_paths[0])
            assert actual_checksum == expected_checksum, (
                f"Data integrity failed for VM {vm_obj.name}: "
                f"Expected {expected_checksum}, Got {actual_checksum}"
            )
            run_dd_io(vm_obj=vm_obj, file_path=self.file_paths[1])

        for vm_obj in vms_to_verify:
            logger.info(f"Stopping VM: {vm_obj.name}")
            vm_obj.stop()
