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
from ocs_ci.helpers.keyrotation_helper import PVKeyRotation
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

        logger.info("Setting up csi-kms-connection-details configmap")
        kms = pv_encryption_kms_setup_factory(kv_version="v2")
        logger.info("csi-kms-connection-details setup successful")

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
        pvk_obj = PVKeyRotation(sc_obj_def)
        pvk_obj.annotate_storageclass_key_rotation(schedule="*/3 * * * *")

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

        logger.info(f"Stopping VM: {self.vm_list[0].name}")
        self.vm_stopped = self.vm_list[0]
        self.vm_stopped.stop()

        logger.info(f"Pausing VM: {self.vm_list[1].name}")
        self.vm_paused = self.vm_list[1]
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

        final_vm_states = {
            vm_obj.name: vm_obj.printableStatus() for vm_obj in self.vm_list
        }
        logger.info(f"Final VM states: {final_vm_states}")
        for vm_name in initial_vm_states:
            assert initial_vm_states[vm_name] == final_vm_states[vm_name], (
                f"VM state mismatch for {vm_name}: "
                f"initial state was {initial_vm_states[vm_name]}, "
                f"but final state is {final_vm_states[vm_name]}"
            )

        self.vm_stopped.start()
        self.vm_paused.unpause()

        logger.info("Verifying data integrity for VMs")
        for vm_obj in self.vm_list:
            logger.info(f"Calculating checksum for VM {vm_obj.name}")
            assert self.source_csum[vm_obj.name] == cal_md5sum_vm(
                vm_obj=vm_obj, file_path=self.file_paths[0]
            ), f"Data integrity failed for VM {vm_obj.name}: checksum mismatch"
            run_dd_io(vm_obj=vm_obj, file_path=self.file_paths[1])

        for vm_obj in self.vm_list:
            logger.info(f"Stopping VM: {vm_obj.name}")
            vm_obj.stop()
