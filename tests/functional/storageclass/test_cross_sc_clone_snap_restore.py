import pytest
import logging
from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    ManageTest,
    tier2,
    polarion_id,
    post_upgrade,
    skipif_managed_service,
    skipif_mcg_only,
    skipif_hci_provider_and_client,
    skipif_external_mode,
)
from ocs_ci.ocs.resources.pod import get_fio_rw_iops
from tests.fixtures import create_project
from ocs_ci.ocs.resources import pvc
from ocs_ci.utility.utils import run_cmd

logger = logging.getLogger(__name__)


@green_squad
@tier2
@post_upgrade
@skipif_managed_service
@skipif_hci_provider_and_client
@pytest.mark.usefixtures(
    create_project.__name__,
)
class TestCrossScCloneSnapRestore(ManageTest):
    """
    The test verifies creation of clones and restore of snapshots to other than pvc storage class
    """

    @pytest.mark.parametrize(
        argnames="interface_type",
        argvalues=[
            pytest.param(*[constants.CEPHBLOCKPOOL]),
            pytest.param(*[constants.CEPHFILESYSTEM]),
        ],
    )
    @skipif_mcg_only
    @polarion_id("OCS-5872")
    @polarion_id("OCS-5873")
    @skipif_external_mode
    def test_cross_class_same_pool_clone_snap_restore(
        self,
        interface_type,
        storageclass_factory,
        pvc_factory,
        pod_factory,
        pvc_clone_factory,
        snapshot_factory,
    ):
        """
        Test function which validates operations accross different storage classes.
        1. Create two storage classes in the same pool
        2. Create a pvc in the first sc
        3. Create a clone1 on the same sc
        4. Create a clone2 on another sc
        5. Create a clone of clone2 on the first sc
        6. Create pvc's shapshot
        7. Restore the snapshot on the same sc
        8. Restore the snapshot on another sc
        9. Take snapshot of the pvs from step 9.
        10. Restore pvc from the snapshot of step 9 to the first sc
        """

        logger.test_step(f"Create first {interface_type} StorageClass")
        sc_obj1 = storageclass_factory(interface=interface_type)
        logger.info(
            f"{interface_type} StorageClass: {sc_obj1.name} created successfully"
        )

        logger.test_step(f"Create PVC and run FIO on StorageClass {sc_obj1.name}")
        pvc_obj = self.create_pvc_and_run_fio(
            pvc_factory, pod_factory, interface_type, sc_obj1
        )

        logger.test_step("Clone PVC on the same StorageClass")
        clone_same_sc_pvc = pvc_clone_factory(
            pvc_obj,
            clone_name=f"pvc-{interface_type.lower()}-clone-test-cross-same-sc",
            storageclass=pvc_obj.backed_sc,
        )
        logger.info(f"Same SC clone {clone_same_sc_pvc.name} successfully created")

        logger.test_step(
            f"Create second {interface_type} StorageClass and clone PVC to it"
        )
        sc_obj2 = storageclass_factory(interface=interface_type)
        logger.info(
            f"{interface_type} StorageClass: {sc_obj2.name} created successfully"
        )
        clone_sc2_pvc = pvc_clone_factory(
            pvc_obj,
            clone_name=f"pvc-{interface_type.lower()}-clone-test-cross-other-sc",
            storageclass=sc_obj2.name,
        )
        logger.info(f"Other SC clone {clone_sc2_pvc.name} successfully created")

        clone_sc1_pvc = pvc_clone_factory(
            clone_sc2_pvc,
            clone_name=f"pvc-{interface_type.lower()}-clone-test-cross-first-sc",
            storageclass=sc_obj1.name,
        )
        logger.info(f"First SC clone {clone_sc1_pvc.name} successfully created")

        logger.test_step(
            "Create snapshot and restore on same and different StorageClasses"
        )
        snap_name1 = f"pvc-{interface_type.lower()}-snapshot-test-cross"
        snap_obj1 = snapshot_factory(pvc_obj, snap_name1)
        logger.info(f"Snapshot {snap_name1} successfully created")

        restore_pvc_yaml = constants.CSI_RBD_PVC_RESTORE_YAML
        if interface_type == constants.CEPHFILESYSTEM:
            restore_pvc_yaml = constants.CSI_CEPHFS_PVC_RESTORE_YAML

        restore_pvc_name1 = f"{pvc_obj.name}-restored-same-sc"
        logger.info(f"Restoring PVC from snapshot on the same SC ({sc_obj1.name})")
        restore_pvc_obj1 = pvc.create_restore_pvc(
            sc_name=f"{sc_obj1.name}",
            snap_name=snap_obj1.name,
            namespace=snap_obj1.namespace,
            size=f"{pvc_obj.size}Gi",
            pvc_name=restore_pvc_name1,
            restore_pvc_yaml=restore_pvc_yaml,
        )
        helpers.wait_for_resource_state(
            restore_pvc_obj1, constants.STATUS_BOUND, timeout=600
        )
        restore_pvc_obj1.reload()
        logger.info(f"PVC {restore_pvc_name1} restored from snapshot on the same SC")
        restore_pvc_obj1.delete()

        restore_pvc_name2 = f"{pvc_obj.name}-restored-other-sc"
        logger.info(f"Restoring PVC from snapshot on another SC ({sc_obj2.name})")
        restore_pvc_obj2 = pvc.create_restore_pvc(
            sc_name=f"{sc_obj2.name}",
            snap_name=snap_obj1.name,
            namespace=snap_obj1.namespace,
            size=f"{pvc_obj.size}Gi",
            pvc_name=restore_pvc_name2,
            restore_pvc_yaml=restore_pvc_yaml,
        )
        helpers.wait_for_resource_state(
            restore_pvc_obj2, constants.STATUS_BOUND, timeout=600
        )
        restore_pvc_obj2.reload()
        logger.info(f"PVC {restore_pvc_name2} restored from snapshot on another SC")

        logger.test_step("Snapshot restored PVC and restore back to first StorageClass")
        snap_name2 = f"pvc-{interface_type.lower()}-snapshot-test-cross-back1"
        snap_obj2 = snapshot_factory(restore_pvc_obj2, snap_name2)
        logger.info(f"Snapshot {snap_name2} successfully created")

        restore_pvc_obj2.delete()

        restore_pvc_sc1_name = f"{pvc_obj.name}-restored-from-other-sc"
        logger.info(f"Restoring PVC from snapshot on the first SC ({sc_obj1.name})")
        restore_pvc_obj3 = pvc.create_restore_pvc(
            sc_name=f"{sc_obj1.name}",
            snap_name=snap_obj2.name,
            namespace=snap_obj1.namespace,
            size=f"{pvc_obj.size}Gi",
            pvc_name=restore_pvc_sc1_name,
            restore_pvc_yaml=restore_pvc_yaml,
        )
        helpers.wait_for_resource_state(
            restore_pvc_obj3, constants.STATUS_BOUND, timeout=600
        )
        restore_pvc_obj3.reload()
        logger.info(
            f"PVC {restore_pvc_sc1_name} restored on the first SC from snapshot on another SC"
        )
        restore_pvc_obj3.delete()

    @pytest.mark.parametrize(
        argnames=["interface_type", "sc1_replica", "sc_replica2", "sc2_compression"],
        argvalues=[
            pytest.param(*[constants.CEPHBLOCKPOOL], "", "", False),
            pytest.param(*[constants.CEPHFILESYSTEM], "", "", False),
            pytest.param(*[constants.CEPHBLOCKPOOL], "", "", True),
            pytest.param(*[constants.CEPHFILESYSTEM], "", "", True),
            pytest.param(*[constants.CEPHBLOCKPOOL], 3, 2, False),
            pytest.param(*[constants.CEPHFILESYSTEM], 3, 2, False),
            pytest.param(*[constants.CEPHBLOCKPOOL], 2, 3, False),
            pytest.param(*[constants.CEPHFILESYSTEM], 2, 3, False),
        ],
    )
    @skipif_mcg_only
    @skipif_external_mode
    @polarion_id("OCS-5871")
    @polarion_id("OCS-5874")
    @polarion_id("OCS-5875")
    @polarion_id("OCS-5876")
    @polarion_id("OCS-5881")
    @polarion_id("OCS-5882")
    @polarion_id("OCS-5884")
    def test_cross_class_different_pool_clone_snap_restore(
        self,
        interface_type,
        storageclass_factory,
        sc1_replica,
        sc_replica2,
        sc2_compression,
        pvc_factory,
        pod_factory,
        pvc_clone_factory,
        snapshot_factory,
    ):
        """
        Test function which validates operations across different storage classes created on different pools
        1. Create first storage class on default pool
        2. Create a pvc in the first storage class
        3. Create second storage class on another pool
        4. Clone pvc created on the first storage class to the second storage class
        5. Clone the clone created on the step 4 back to the first storage class
        6. Create pvc's shapshot
        7. Restore the snapshot to a pvc on the second storage class
        8. Take snapshot of the pvc restored on the step 7 and restore it back to the first storage class

        Args:
            sc1_replica (str/int): Number of replica for the first sc object. If is empty string, use default
            sc2_replica (str/int): Number of replica for the second sc object. If is empty string, use default
            sc2_compression (bool) If true, sc2 is created with compression = True, otherwise use default
        """

        logger.test_step(
            f"Create first {interface_type} StorageClass "
            f"(replica={sc1_replica or 'default'})"
        )
        if sc1_replica == "":
            sc_obj1 = storageclass_factory(interface=interface_type)
        else:
            sc_obj1 = storageclass_factory(
                interface=interface_type, replica=sc1_replica
            )
        pool_name1 = run_cmd(
            f"oc get sc {sc_obj1.name} -o jsonpath={{'.parameters.pool'}}"
        )
        logger.info(
            f"{interface_type} StorageClass: {sc_obj1.name} on pool {pool_name1} created successfully"
        )

        logger.test_step(f"Create PVC and run FIO on StorageClass {sc_obj1.name}")
        pvc_obj = self.create_pvc_and_run_fio(
            pvc_factory, pod_factory, interface_type, sc_obj1
        )

        logger.test_step(
            f"Create second {interface_type} StorageClass on a different pool "
            f"(replica={sc_replica2 or 'default'}, compression={sc2_compression})"
        )
        if sc_replica2 == "":
            if sc2_compression:
                sc_obj2 = storageclass_factory(
                    interface=interface_type,
                    new_rbd_pool=True,
                    compression="aggressive",
                )
            else:
                sc_obj2 = storageclass_factory(
                    interface=interface_type, new_rbd_pool=True
                )
        else:
            sc_obj2 = storageclass_factory(
                interface=interface_type, replica=sc_replica2, new_rbd_pool=True
            )
        pool_name2 = run_cmd(
            f"oc get sc {sc_obj2.name} -o jsonpath={{'.parameters.pool'}}"
        )
        logger.info(
            f"{interface_type} StorageClass: {sc_obj2.name} on pool {pool_name2} created successfully"
        )

        logger.test_step("Clone PVC across storage classes on different pools")
        clone_pvc_sc2 = pvc_clone_factory(
            pvc_obj,
            clone_name=f"pvc-{interface_type.lower()}-clone-test-cross-other-sc",
            storageclass=sc_obj2.name,
        )
        logger.info(
            f"Clone {clone_pvc_sc2.name} on SC {sc_obj2.name} (pool {pool_name2}) created successfully"
        )
        clone_pvc_sc1 = pvc_clone_factory(
            clone_pvc_sc2,
            clone_name=f"pvc-{interface_type.lower()}-clone-test-cross-back-sc1",
            storageclass=sc_obj1.name,
        )
        logger.info(
            f"Clone {clone_pvc_sc1.name} on SC {sc_obj1.name} (pool {pool_name1}) created successfully"
        )

        logger.test_step(
            "Snapshot PVC and restore across storage classes on different pools"
        )
        snap_name_sc1 = f"pvc-{interface_type.lower()}-snapshot-test-cross-sc1"
        snap_obj1 = snapshot_factory(pvc_obj, snap_name_sc1)
        logger.info(f"Snapshot {snap_name_sc1} successfully created")

        restore_pvc_yaml = constants.CSI_RBD_PVC_RESTORE_YAML
        if interface_type == constants.CEPHFILESYSTEM:
            restore_pvc_yaml = constants.CSI_CEPHFS_PVC_RESTORE_YAML

        restore_pvc_sc2_name = f"{pvc_obj.name}-restored-sc-another-pool"
        logger.info(
            f"Restoring PVC from snapshot to SC {sc_obj2.name} (pool {pool_name2})"
        )
        restore_pvc_sc2_obj = pvc.create_restore_pvc(
            sc_name=f"{sc_obj2.name}",
            snap_name=snap_obj1.name,
            namespace=snap_obj1.namespace,
            size=f"{pvc_obj.size}Gi",
            pvc_name=restore_pvc_sc2_name,
            restore_pvc_yaml=restore_pvc_yaml,
        )
        helpers.wait_for_resource_state(
            restore_pvc_sc2_obj, constants.STATUS_BOUND, timeout=600
        )
        restore_pvc_sc2_obj.reload()
        logger.info(f"PVC {restore_pvc_sc2_name} restored on SC on another pool")

        logger.test_step("Snapshot restored PVC and restore back to first StorageClass")
        snap_name_sc2 = f"pvc-{interface_type.lower()}-snapshot-test-cross-sc1"
        snap_obj2 = snapshot_factory(restore_pvc_sc2_obj, snap_name_sc2)
        logger.info(f"Snapshot {snap_name_sc2} successfully created")

        restore_pvc_sc2_obj.delete()

        restore_pvc_sc1_name = f"{restore_pvc_sc2_obj.name}-restored-sc-same-pool"
        logger.info(
            f"Restoring PVC from snapshot back to SC {sc_obj1.name} (pool {pool_name1})"
        )
        restore_pvc_sc1_obj = pvc.create_restore_pvc(
            sc_name=f"{sc_obj1.name}",
            snap_name=snap_obj2.name,
            namespace=snap_obj1.namespace,
            size=f"{pvc_obj.size}Gi",
            pvc_name=restore_pvc_sc1_name,
            restore_pvc_yaml=restore_pvc_yaml,
        )
        helpers.wait_for_resource_state(
            restore_pvc_sc1_obj, constants.STATUS_BOUND, timeout=600
        )
        restore_pvc_sc1_obj.reload()
        logger.info(
            f"PVC {restore_pvc_sc1_name} restored on the first SC from snapshot on another SC"
        )

        restore_pvc_sc1_obj.delete()

    def create_pvc_and_run_fio(
        self,
        pvc_factory,
        pod_factory,
        interface_type,
        sc_obj,
    ):
        """
        Creates pvc and pod ; then runs fio on the pod

        Args:
            interface_type (str) Interface type of PVC to e created
            sc_obj (obj): storage class object on which the PVC should be created

        Returns:
            pvc (obj): PVC object created
        """
        logger.info(f"Creating a PVC using {sc_obj.name}")
        pvc_obj = pvc_factory(interface=interface_type, storageclass=sc_obj)
        logger.info(f"PVC: {pvc_obj.name} created successfully using {sc_obj.name}")

        logger.info(f"Creating an app pod and mounting {pvc_obj.name}")
        pod_obj = pod_factory(interface=interface_type)
        logger.info(f"{pod_obj.name} created successfully and mounted {pvc_obj.name}")

        logger.info(f"Running FIO on {pod_obj.name}")
        pod_obj.run_io("fs", size="500M")
        get_fio_rw_iops(pod_obj)

        return pvc_obj
