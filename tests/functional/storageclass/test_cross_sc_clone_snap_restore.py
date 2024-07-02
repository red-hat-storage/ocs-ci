import pytest
import logging
from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    ManageTest,
    tier2,
    skipif_managed_service,
    skipif_hci_provider_and_client,
)
from ocs_ci.ocs.resources.pod import get_fio_rw_iops
from tests.fixtures import create_project
from ocs_ci.ocs.resources import pvc
from ocs_ci.utility.utils import run_cmd


log = logging.getLogger(__name__)


@green_squad
@tier2
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
        3. Create a clone on the same sc
        4. Create a clone on another sc
        5. Create pvc's shapshot
        6. Restore the snapshot on the same sc
        7. Restore the snapshot on another sc
        """

        # Create a Storage Class
        sc_obj1 = storageclass_factory(interface=interface_type)
        log.info(
            f"{interface_type}StorageClass: {sc_obj1.name} " f"created successfully"
        )

        pvc_obj = self.create_pvc_and_run_fio(
            pvc_factory, pod_factory, interface_type, sc_obj1
        )

        clone_pvc = pvc_clone_factory(
            pvc_obj,
            clone_name=f"pvc-{interface_type.lower()}-clone-test-cross-same-sc",
            storageclass=pvc_obj.backed_sc,
        )
        log.info(f"Same SC clone {clone_pvc.name} successfully created")

        sc_obj2 = storageclass_factory(interface=interface_type)
        log.info(
            f"{interface_type}StorageClass: {sc_obj2.name} " f"created successfully"
        )
        clone_pvc = pvc_clone_factory(
            pvc_obj,
            clone_name=f"pvc-{interface_type.lower()}-clone-test-cross-other-sc",
            storageclass=sc_obj2.name,
        )
        log.info(f"Other SC clone {clone_pvc.name} successfully created")

        snap_name = f"pvc-{interface_type.lower()}-snapshot-test-cross"
        snap_obj = snapshot_factory(pvc_obj, snap_name)
        log.info(f"Snapshot {snap_name} successfully created")

        restore_pvc_yaml = constants.CSI_RBD_PVC_RESTORE_YAML
        if interface_type == constants.CEPHFILESYSTEM:
            restore_pvc_yaml = constants.CSI_CEPHFS_PVC_RESTORE_YAML

        restore_pvc_name1 = f"{pvc_obj.name}-restored-same-sc"
        log.info("Restoring the PVC from snapshot on the same SC")
        restore_pvc_obj1 = pvc.create_restore_pvc(
            sc_name=f"{sc_obj1.name}",
            snap_name=snap_obj.name,
            namespace=snap_obj.namespace,
            size=f"{pvc_obj.size}Gi",
            pvc_name=restore_pvc_name1,
            restore_pvc_yaml=restore_pvc_yaml,
        )
        helpers.wait_for_resource_state(
            restore_pvc_obj1, constants.STATUS_BOUND, timeout=600
        )
        restore_pvc_obj1.reload()
        log.info("PVC was restored from the snapshot on the same SC")
        restore_pvc_obj1.delete()

        restore_pvc_name2 = f"{pvc_obj.name}-restored-other-sc"
        log.info("Restoring the PVC from Snapshot")
        restore_pvc_obj2 = pvc.create_restore_pvc(
            sc_name=f"{sc_obj2.name}",
            snap_name=snap_obj.name,
            namespace=snap_obj.namespace,
            size=f"{pvc_obj.size}Gi",
            pvc_name=restore_pvc_name2,
            restore_pvc_yaml=restore_pvc_yaml,
        )
        helpers.wait_for_resource_state(
            restore_pvc_obj2, constants.STATUS_BOUND, timeout=600
        )
        restore_pvc_obj2.reload()
        log.info("PVC was restored from the snapshot on another SC")
        restore_pvc_obj2.delete()

    @pytest.mark.parametrize(
        argnames=["interface_type", "sc1_replica", "sc_replica2"],
        argvalues=[
            pytest.param(*[constants.CEPHBLOCKPOOL], "", ""),
            pytest.param(*[constants.CEPHFILESYSTEM], "", ""),
            pytest.param(*[constants.CEPHBLOCKPOOL], 3, 2),
            pytest.param(*[constants.CEPHFILESYSTEM], 3, 2),
            pytest.param(*[constants.CEPHBLOCKPOOL], 2, 3),
            pytest.param(*[constants.CEPHFILESYSTEM], 2, 3),
        ],
    )
    def test_cross_class_different_pool_clone_snap_restore(
        self,
        interface_type,
        sc1_replica,
        sc_replica2,
        storageclass_factory,
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
        5. Create pvc's shapshot
        6. Restore the snapshot to a pvc on the second storage class

        Args:
            sc1_replica (str/int): Number of replica for the first sc object. If is empty string, use default
            sc2_replica (str/int): Number of replica for the second sc object. If is empty string, use default
        """

        # Create a Storage Class on default pool
        if sc1_replica == "":
            sc_obj1 = storageclass_factory(interface=interface_type)
        else:
            sc_obj1 = storageclass_factory(
                interface=interface_type, replica=sc1_replica
            )
        pool_name1 = run_cmd(
            f"oc get sc {sc_obj1.name} -o jsonpath={{'.parameters.pool'}}"
        )
        log.info(
            f"{interface_type}StorageClass: {sc_obj1.name} on pool {pool_name1} created successfully"
        )
        pvc_obj = self.create_pvc_and_run_fio(
            pvc_factory, pod_factory, interface_type, sc_obj1
        )

        # Create a Storage Class on another pool
        if sc_replica2 == "":
            sc_obj2 = storageclass_factory(interface=interface_type, new_rbd_pool=True)
        else:
            sc_obj2 = storageclass_factory(
                interface=interface_type, replica=sc_replica2, new_rbd_pool=True
            )
        pool_name2 = run_cmd(
            f"oc get sc {sc_obj2.name} -o jsonpath={{'.parameters.pool'}}"
        )
        log.info(
            f"{interface_type}StorageClass: {sc_obj2.name} on pool {pool_name2} created successfully"
        )

        clone_pvc = pvc_clone_factory(
            pvc_obj,
            clone_name=f"pvc-{interface_type.lower()}-clone-test-cross-other-sc",
            storageclass=sc_obj2.name,
        )
        log.info(
            f"SC clone {clone_pvc.name}  on storage class on another pool successfully created"
        )

        snap_name = f"pvc-{interface_type.lower()}-snapshot-test-cross"
        snap_obj = snapshot_factory(pvc_obj, snap_name)
        log.info(f"Snapshot {snap_name} successfully created")

        restore_pvc_yaml = constants.CSI_RBD_PVC_RESTORE_YAML
        if interface_type == constants.CEPHFILESYSTEM:
            restore_pvc_yaml = constants.CSI_CEPHFS_PVC_RESTORE_YAML

        restore_pvc_name = f"{pvc_obj.name}-restored-sc-another-pool"
        log.info("Restoring the PVC from snapshot")
        restore_pvc_obj = pvc.create_restore_pvc(
            sc_name=f"{sc_obj2.name}",
            snap_name=snap_obj.name,
            namespace=snap_obj.namespace,
            size=f"{pvc_obj.size}Gi",
            pvc_name=restore_pvc_name,
            restore_pvc_yaml=restore_pvc_yaml,
        )
        helpers.wait_for_resource_state(
            restore_pvc_obj, constants.STATUS_BOUND, timeout=600
        )
        restore_pvc_obj.reload()
        log.info("PVC was restored from the snapshot on SC on another pool")
        restore_pvc_obj.delete()

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
        # Create a PVC using the created StorageClass
        log.info(f"Creating a PVC using {sc_obj.name}")
        pvc_obj = pvc_factory(interface=interface_type, storageclass=sc_obj)
        log.info(f"PVC: {pvc_obj.name} created successfully using " f"{sc_obj.name}")

        # Create app pod and mount each PVC
        log.info(f"Creating an app pod and mount {pvc_obj.name}")
        pod_obj = pod_factory(interface=interface_type)
        log.info(f"{pod_obj.name} created successfully and mounted {pvc_obj.name}")

        # Run IO on each app pod for sometime
        log.info(f"Running FIO on {pod_obj.name}")
        pod_obj.run_io("fs", size="500M")
        get_fio_rw_iops(pod_obj)

        return pvc_obj
