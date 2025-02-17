import logging
import random
import pytest
from ocs_ci.framework.pytest_customization.marks import magenta_squad, workloads
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.helpers.cnv_helpers import (
    run_dd_io,
    all_nodes_ready,
    cal_md5sum_vm,
    get_vm_status,
)
from ocs_ci.helpers.keyrotation_helper import PVKeyrotation
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility.utils import TimeoutSampler
from tests.functional.z_cluster.cluster_expansion.test_add_capacity import (
    add_capacity_test,
)

logger = logging.getLogger(__name__)


@magenta_squad
@workloads
@pytest.mark.polarion_id("OCS-")
class TestVmStorageCapacity(E2ETest):
    """
    Perform add capacity operation while the VMs are in different states
    and in the presence of snapshots and clones of the VMs.
    """

    def test_vm_storage_capacity(
        self,
        # setup_cnv,
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
        source_csum = {}
        res_csum = {}
        vm_list = []
        vm_list_clone = []
        i = 3
        # Create ceph-csi-kms-token in the tenant namespace
        proj_obj = project_factory()
        file_paths = ["/source_file.txt", "/new_file.txt"]

        # Setup csi-kms-connection-details configmap
        logger.info("Setting up csi-kms-connection-details configmap")
        kms = pv_encryption_kms_setup_factory(kv_version="v2")
        logger.info("csi-kms-connection-details setup successful")

        # Create an encryption enabled storageclass for RBD
        sc_obj_def = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
            encrypted=True,
            encryption_kms_id=kms.kmsid,
            new_rbd_pool=True,
            mapOptions="krbd:rxbounce",
            mounter="rbd",
        )

        kms.vault_path_token = kms.generate_vault_token()
        kms.create_vault_csi_kms_token(namespace=proj_obj.namespace)

        pvk_obj = PVKeyrotation(sc_obj_def)
        pvk_obj.annotate_storageclass_key_rotation(schedule="*/3 * * * *")

        # Create a PVC-based VM (VM1)
        while i > 0:
            vm_obj = cnv_workload(
                storageclass=sc_obj_def.name,
                namespace=proj_obj.namespace,
                volume_interface=constants.VM_VOLUME_PVC,
            )
            vm_list.append(vm_obj)
            source_csum[f"{vm_obj.name}"] = run_dd_io(
                vm_obj=vm_obj, file_path=file_paths[0], verify=True
            )
            logger.info(f" before cloning source csum: {source_csum[f'{vm_obj.name}']}")
            clone_vm_obj = clone_vm_workload(vm_obj, namespace=vm_obj.namespace)
            vm_list_clone.append(clone_vm_obj)
            res_csum[f"{clone_vm_obj.name}"] = cal_md5sum_vm(
                vm_obj=clone_vm_obj, file_path=file_paths[0]
            )
            assert res_csum[f"{clone_vm_obj.name}"] == source_csum[f"{vm_obj.name}"], (
                f"Failed: MD5 comparison between source {vm_obj.name} and "
                f"its cloned VMs"
            )
            i -= 1

        # Stop and pause VMs in random order
        logger.info("Stopping and pausing VMs in random order...")
        vm_stopped = random.sample(vm_list, 1)
        for vm_obj in vm_stopped:
            logger.info(f"Stopping VM: {vm_obj.name}")
            vm_obj.stop()
            snapshot_factory(vm_obj.get_vm_pvc_obj())
            vm_list.remove(vm_obj)

        vm_pause = random.sample(vm_list, 1)
        for vm in vm_pause:
            logger.info(f"Pausing VM: {vm.name}")
            vm.pause()
            vm_list.remove(vm)

        logger.info("Verifying cluster stability before capacity addition...")
        assert all_nodes_ready(), "Some nodes are not ready!"

        sample = TimeoutSampler(
            timeout=600,
            sleep=10,
            func=wait_for_pods_to_be_running,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        assert sample.wait_for_func_status(
            result=True
        ), "Not all OCS pods are running before capacity addition."

        # Save initial states of VMs
        initial_vm_states = {}
        for vm_obj in vm_list + vm_list_clone:
            initial_vm_states[vm_obj.name] = get_vm_status(vm_obj)
            logger.info(
                f"Initial status of VM {vm_obj.name}: {initial_vm_states[vm_obj.name]}"
            )

        logger.info("Adding storage capacity...")
        add_capacity_test()
        logger.info("Added storage capacity!")

        # Verify cluster stability after capacity addition
        logger.info("Verifying cluster stability after capacity addition...")
        assert all_nodes_ready(), "Some nodes are not ready!"

        sample = TimeoutSampler(
            timeout=600,
            sleep=10,
            func=wait_for_pods_to_be_running,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        assert sample.wait_for_func_status(
            result=True
        ), "Not all pods are running after capacity addition."

        vm_list.append(vm_pause)
        for vm_obj in vm_list_clone + vm_list:
            final_vm_status = get_vm_status(vm_obj)
            logger.info(f"Final status of VM {vm_obj.name}: {final_vm_status}")

            if vm_obj.name in initial_vm_states:
                assert (
                    final_vm_status == initial_vm_states[vm_obj.name]
                ), f"VM {vm_obj.name} state has changed after add capacity."
                logger.info(
                    f"VM {vm_obj.name} state is consistent "
                    f"before and after add capacity."
                )
            else:
                logger.warning(f"Initial state of VM {vm_obj.name} not found.")

        # Data integrity check
        for vm_obj in vm_list_clone:
            result_checksum = res_csum.get(vm_obj.name)
            assert (
                cal_md5sum_vm(vm_obj=vm_obj, file_path=file_paths[0]) == result_checksum
            ), f"Failed: MD5 comparison between source {vm_obj.name} and its cloned VMs"

        # Cleanup VMs
        logger.info("Stopping VMs...")
        for vm_obj in vm_list_clone + vm_list:
            logger.info(f"Stopping VM: {vm_obj.name}")
            vm_obj.stop()
