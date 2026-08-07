import logging

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
    run_dd_io,
)
from ocs_ci.helpers.keyrotation_helper import PVKeyrotation
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
    ):
        """
        Sets up the test environment:
        - Creates KMS and StorageClass.
        - Creates initial VMs and clones.
        - Stops and pauses a subset of VMs.
        """
        self.file_paths = ["/source_file.txt", "/new_file.txt"]

        logger.test_step("Set up KMS, StorageClass, and project")
        kms = pv_encryption_kms_setup_factory(kv_version="v2")
        logger.info("KMS setup successful")

        sc_obj_def = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
            encrypted=True,
            encryption_kms_id=kms.kmsid,
            new_rbd_pool=True,
            mapOptions="krbd:rxbounce",
            mounter="rbd",
        )

        proj_obj = project_factory()
        kms.vault_path_token = kms.generate_vault_token()
        kms.create_vault_csi_kms_token(namespace=proj_obj.namespace)
        pvk_obj = PVKeyrotation(sc_obj_def)
        pvk_obj.annotate_storageclass_key_rotation(schedule="*/3 * * * *")

        logger.test_step("Create VMs and write initial test data")
        self.vm_list = []
        self.source_csum = {}
        self.final_csum = {}
        for vm_index in range(3):
            vm_obj = cnv_workload(
                storageclass=sc_obj_def.name,
                namespace=proj_obj.namespace,
                volume_interface=constants.VM_VOLUME_PVC,
            )
            self.vm_list.append(vm_obj)
            self.source_csum[vm_obj.name] = run_dd_io(
                vm_obj=vm_obj, file_path=self.file_paths[0], verify=True
            )
        logger.info(f"Created {len(self.vm_list)} VMs with initial checksums")

        logger.test_step("Set VMs to different states")
        self.vm_stopped = self.vm_list[0]
        logger.info(f"Stopping VM '{self.vm_stopped.name}'")
        self.vm_stopped.stop()

        self.vm_paused = self.vm_list[1]
        logger.info(f"Pausing VM '{self.vm_paused.name}'")
        self.vm_paused.pause()

    def test_vm_add_capacity(self, setup):
        """
        Test steps:
        1. Keep IO operations going on VMs
        2. Keep VMs in different states (running, paused, stopped).
        3. Perform add capacity using official docs.
        4. Verify Cluster Stability and Data Integrity.
        5. Ensure the additional storage has been added.
        6. Verify VMs have preserved their states and data integrity.
        """
        initial_vm_states = {
            vm_obj.name: vm_obj.printableStatus() for vm_obj in self.vm_list
        }
        logger.info(f"Initial VM states: {initial_vm_states}")

        logger.test_step("Add storage capacity")
        add_capacity_test()
        logger.info("Storage capacity added successfully")

        logger.info(
            f"Waiting for pods in '{constants.OPENSHIFT_STORAGE_NAMESPACE}' to be running"
        )
        sample = TimeoutSampler(
            timeout=600,
            sleep=10,
            func=wait_for_pods_to_be_running,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        logger.assertion(
            f"Pods running in '{constants.OPENSHIFT_STORAGE_NAMESPACE}' after capacity addition"
        )
        assert sample.wait_for_func_status(result=True), (
            "Not all pods are running after capacity "
            f"addition in {constants.OPENSHIFT_STORAGE_NAMESPACE}"
        )

        logger.test_step("Verify VM state preservation after capacity addition")
        final_vm_states = {
            vm_obj.name: vm_obj.printableStatus() for vm_obj in self.vm_list
        }
        for vm_name in initial_vm_states:
            logger.assertion(
                f"VM state: name='{vm_name}', "
                f"expected='{initial_vm_states[vm_name]}', "
                f"actual='{final_vm_states[vm_name]}'"
            )
            assert initial_vm_states[vm_name] == final_vm_states[vm_name], (
                f"VM state mismatch for {vm_name}: "
                f"initial state was {initial_vm_states[vm_name]}, "
                f"but final state is {final_vm_states[vm_name]}"
            )

        logger.info(
            f"Resuming VMs: starting '{self.vm_stopped.name}', "
            f"unpausing '{self.vm_paused.name}'"
        )
        self.vm_stopped.start()
        self.vm_paused.unpause()

        logger.test_step("Verify data integrity and run I/O on all VMs")
        for vm_obj in self.vm_list:
            new_csum = cal_md5sum_vm(vm_obj=vm_obj, file_path=self.file_paths[0])
            logger.assertion(
                f"Data integrity: vm='{vm_obj.name}', "
                f"expected='{self.source_csum[vm_obj.name]}', "
                f"actual='{new_csum}', "
                f"match={self.source_csum[vm_obj.name] == new_csum}"
            )
            assert (
                self.source_csum[vm_obj.name] == new_csum
            ), f"Data integrity failed for VM '{vm_obj.name}': checksum mismatch"
            run_dd_io(vm_obj=vm_obj, file_path=self.file_paths[1])
        logger.info("All VMs passed data integrity check")

        logger.test_step("Stop all VMs")
        for vm_obj in self.vm_list:
            logger.info(f"Stopping VM '{vm_obj.name}'")
            vm_obj.stop()
