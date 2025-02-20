import logging
import random
import pytest
from concurrent.futures import ThreadPoolExecutor, as_completed

from ocs_ci.framework.pytest_customization.marks import magenta_squad, workloads
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.helpers.keyrotation_helper import PVKeyrotation
from ocs_ci.ocs import constants

logger = logging.getLogger(__name__)


@magenta_squad
class TestOcvUpgrade(E2ETest):
    """
    Test OCV upgrade while VMs are in different states,
    snapshots and clone exists and while performing
    various operation during operator upgrade
    """

    @workloads
    @pytest.mark.polarion_id("OCS-")
    def test_ocv_upgrd(
        self,
        setup_cnv,
        pv_encryption_kms_setup_factory,
        storageclass_factory,
        project_factory,
        cnv_workload,
        snapshot_factory,
        clone_vm_workload,
    ):
        """ """
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

        # Create ceph-csi-kms-token in the tenant namespace
        proj_obj = project_factory()
        kms.vault_path_token = kms.generate_vault_token()
        kms.create_vault_csi_kms_token(namespace=proj_obj.namespace)
        pvk_obj = PVKeyrotation(sc_obj_def)
        pvk_obj.annotate_storageclass_key_rotation(schedule="*/3 * * * *")

        # file_paths = ["/source_file.txt", "/new_file.txt"]

        # prerequisites

        vm_list_all = []
        vm_list_stop = []
        vm_list_pause = []
        vm_list_running = vm_list_all
        i = 9

        # Create VMs concurrently
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(
                    cnv_workload,
                    storageclass=sc_obj_def.name,
                    namespace=proj_obj.namespace,
                    volume_interface=constants.VM_VOLUME_PVC,
                )
                for _ in range(i)
            ]
            for future in as_completed(futures):
                vm_list_all.append(future.result())

        # 3 stop vms concurrently
        with ThreadPoolExecutor() as executor:
            stop_futures = [
                executor.submit(vm_obj.stop)
                for vm_obj in random.sample(vm_list_running, 3)
            ]
            for future in as_completed(stop_futures):
                vm_list_stop.append(future.result())
                vm_list_running.remove(future.result())

        # 3 paused vms concurrently
        with ThreadPoolExecutor() as executor:
            pause_futures = [
                executor.submit(vm_obj.pause)
                for vm_obj in random.sample(vm_list_running, 3)
            ]
            for future in as_completed(pause_futures):
                vm_list_pause.append(future.result())
                vm_list_running.remove(future.result())

        vm_snap_list = []

        # snapshot of 2 running, 1 paused, and 1 stopped vm concurrently
        with ThreadPoolExecutor() as executor:
            snap_futures = [
                executor.submit(snapshot_factory, vm_obj.get_vm_pvc_obj())
                for vm_obj in random.sample(vm_list_running, 2)
            ]
            snap_futures.append(
                executor.submit(
                    snapshot_factory, random.sample(vm_list_stop, 1)[0].get_vm_pvc_obj()
                )
            )
            snap_futures.append(
                executor.submit(
                    snapshot_factory,
                    random.sample(vm_list_pause, 1)[0].get_vm_pvc_obj(),
                )
            )

            for future in as_completed(snap_futures):
                vm_snap_list.append(future.result())

        vm_clone_list = []

        # clones of 1 vm running, 1 paused, and 1 stopped vm concurrently
        with ThreadPoolExecutor() as executor:
            clone_futures = [
                executor.submit(clone_vm_workload, vm_obj, vm_obj.namespace)
                for vm_obj in random.sample(vm_list_running, 1)
            ]
            clone_futures.append(
                executor.submit(
                    clone_vm_workload,
                    random.sample(vm_list_stop, 1)[0],
                    random.sample(vm_list_stop, 1)[0].namespace,
                )
            )
            clone_futures.append(
                executor.submit(
                    clone_vm_workload,
                    random.sample(vm_list_pause, 1)[0],
                    random.sample(vm_list_pause, 1)[0].namespace,
                )
            )

            for future in as_completed(clone_futures):
                vm_clone_list.append(future.result())

        # execution
        """Upgrade process ocp and odf"""

        """ Taking snapshots of newly created vm, restore a snapshot to a VM"""
        """ Clone of newly created snapshots """

        # post execution validation

        """Ensuring all the vms are in their expected state"""
        """Start previously shudown VMs and check if they start successfully"""
        """verify all OCV pods are running"""
        """verify all nodes are running"""
        """VM ssh is possible"""
        """all snapshots are intact"""
        """clone are working properly"""
        """VM migrate to other host/node is proper"""
        """data integrity should be maintained"""
