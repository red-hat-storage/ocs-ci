import pytest
import logging
from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import ManageTest, tier2
from ocs_ci.ocs.resources.pod import get_fio_rw_iops
from tests.fixtures import create_project
from ocs_ci.ocs.resources import pvc

log = logging.getLogger(__name__)


@green_squad
@tier2
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
        sc_obj = storageclass_factory(interface=interface_type)
        log.info(
            f"{interface_type}StorageClass: {sc_obj.name} " f"created successfully"
        )

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

        restore_pvc_name = f"{pvc_obj.name}-restored-same-sc"
        log.info("Restoring the PVC from snapshot on the same SC")
        restore_pvc_obj = pvc.create_restore_pvc(
            sc_name=f"{sc_obj.name}",
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
        log.info("PVC was restored from the snapshot on the same SC")
        restore_pvc_obj.delete()

        restore_pvc_name = f"{pvc_obj.name}-restored-other-sc"
        log.info("Restoring the PVC from Snapshot")
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
        log.info("PVC was restored from the snapshot on another SC")
        restore_pvc_obj.delete()
