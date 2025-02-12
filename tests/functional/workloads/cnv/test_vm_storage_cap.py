import logging
import random
import pytest
from ocs_ci.framework.pytest_customization.marks import magenta_squad, workloads
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.helpers.cnv_helpers import (
    run_dd_io,
    all_nodes_ready,
    cal_md5sum_vm,
)
from ocs_ci.helpers.keyrotation_helper import PVKeyrotation
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import storage_cluster
from ocs_ci.ocs.resources.pod import get_pod_restarts_count, wait_for_pods_to_be_running
from ocs_ci.utility.utils import TimeoutSampler

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
        6. Verify snapshots and clones have preserved data integrity.
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
            i -= 1
            source_csum[f"{vm_obj.name}"] = run_dd_io(
                vm_obj=vm_obj, file_path=file_paths[0], verify=True
            )
            clone_vm_obj = clone_vm_workload(vm_obj, namespace=vm_obj.namespace)
            source_csum[f"{clone_vm_obj.name}"] = run_dd_io(
                vm_obj=vm_obj, file_path=file_paths[0], verify=True
            )
            vm_list_clone.append(clone_vm_obj)

        # Stop and pause VMs in random order
        vm_stopped = random.sample(vm_list, 1)
        for vm_obj in vm_stopped:
            logger.info(f"VM Name{vm_obj.name}")
            vm_obj.stop()
            snapshot_factory(vm_obj.get_vm_pvc_obj())
            vm_list.remove(vm_obj)

        vm_pause = random.sample(vm_list, 1)
        for vm in vm_pause:
            vm.pause()

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

        osd_pods_restart_count_before = get_pod_restarts_count(
            label=constants.OSD_APP_LABEL
        )

        # Perform add capacity operation
        osd_size = storage_cluster.get_osd_size()
        logger.info(f"Adding {osd_size} to existing storageclass capacity")
        storage_cluster.add_capacity(osd_size)
        logger.info("Successfully added capacity")

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

        osd_pods_restart_count_after = get_pod_restarts_count(
            label=constants.OSD_APP_LABEL
        )

        assert sum(osd_pods_restart_count_before.values()) == sum(
            osd_pods_restart_count_after.values()
        ), "Some of the osd pods have restarted during the add capacity"
        logger.info("OSD pod restart counts are the same before and after.")

        for vm_obj in vm_list + vm_list_clone:
            res_csum[f"{vm_obj.name}"] = cal_md5sum_vm(
                vm_obj=vm_obj, file_path=file_paths[0]
            )
            source_checksum = source_csum.get(vm_obj.name)
            result_checksum = res_csum.get(vm_obj.name)
            assert (
                source_checksum == result_checksum
            ), f"Failed: MD5 comparison between source {vm_obj.name} and its cloned VMs"
            vm_obj.stop()
