import logging
import pytest
from ocs_ci.framework import config

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import (
    ManageTest,
)
from ocs_ci.framework.pytest_customization.marks import (
    pre_ocs_upgrade,
    post_ocs_upgrade,
    green_squad,
)
from ocs_ci.ocs.resources.pod import (
    wait_for_storage_pods,
    get_ocs_operator_pod,
    get_pod_logs,
)
from ocs_ci.utility import version

logger = logging.getLogger(__name__)
upgrade_ocs_version = config.UPGRADE.get("upgrade_ocs_version")


@pytest.mark.polarion_id("OCS-4689")
@pytest.mark.skipif(
    not (
        upgrade_ocs_version
        and version.get_semantic_ocs_version_from_config() == version.VERSION_4_11
        and version.get_semantic_version(upgrade_ocs_version, True)
        == version.VERSION_4_12
    ),
    reason=(
        "The fix is present in 4.12. bug related only to upgrade from 4.11 to 4.12."
        "no need to check for upcoming releases. "
    ),
)
@green_squad
class TestUpgrade(ManageTest):
    """
    Tests to check upgrade of OCS when we set without expansion secret and allowExpansion to false
    """

    all_sc_obj = []

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        def finalizer():
            self.storageclass_obj_cleanup()

        request.addfinalizer(finalizer)

    def storageclass_obj_cleanup(self):
        """
        Delete storageclass
        """
        logger.info(f"Teardown: deleting {len(self.all_sc_obj)} custom storage classes")
        for instance in self.all_sc_obj:
            instance.delete(wait=True)

    @pre_ocs_upgrade
    def test_ocs_upgrade_with_allowexpansion_false(
        self, project_factory, storageclass_factory, multi_pvc_factory
    ):
        """
        1. Create Storage class for rbd and cephfs
        2. Create custom rbd and cephfs sc without expansion secret and allowExpansion set to false.
        3. Created few cephfs and rbd PVCs
        4. Create new pvc's from custom sc
        5. Update ocs-operator to 4.12.0
        """
        size_list = ["1", "3", "5"]

        access_modes_cephfs = [constants.ACCESS_MODE_RWO, constants.ACCESS_MODE_RWX]
        access_modes_rbd = [
            f"{constants.ACCESS_MODE_RWO}-Block",
            f"{constants.ACCESS_MODE_RWX}-Block",
        ]

        logger.test_step(
            "Create custom storage classes without expansion and allowExpansion=false"
        )
        custom_cephfs_sc = storageclass_factory(
            interface=constants.CEPHFILESYSTEM, allow_volume_expansion=False
        )
        custom_rbd_sc = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL, allow_volume_expansion=False
        )

        # Appending all the pvc obj to base case param for cleanup and evaluation
        self.all_sc_obj.append(custom_cephfs_sc)
        self.all_sc_obj.append(custom_rbd_sc)

        logger.test_step(
            f"Create PVCs for custom and default storage classes with sizes {size_list}"
        )
        project_obj = project_factory()
        for size in size_list:
            rbd_pvcs = multi_pvc_factory(
                interface=constants.CEPHBLOCKPOOL,
                access_modes=access_modes_rbd,
                project=project_obj,
                size=size,
                num_of_pvc=2,
            )
            logger.debug(f"RBD PVCs created for size {size}")
            logger.assertion(
                f"RBD PVCs of size {size}: expected=truthy, "
                f"actual={bool(rbd_pvcs)} (count={len(rbd_pvcs) if rbd_pvcs else 0})"
            )
            assert rbd_pvcs, f"Failed to create rbd_pvcs of size {size}"

            cephfs_pvcs = multi_pvc_factory(
                interface=constants.CEPHFILESYSTEM,
                project=project_obj,
                access_modes=access_modes_cephfs,
                size=size,
                num_of_pvc=2,
            )
            logger.assertion(
                f"CephFS PVCs of size {size}: expected=truthy, "
                f"actual={bool(cephfs_pvcs)} (count={len(cephfs_pvcs) if cephfs_pvcs else 0})"
            )
            assert cephfs_pvcs, "Failed to create cephfs_pvcs PVC"

            custom_rbd_pvcs = multi_pvc_factory(
                interface=constants.CEPHBLOCKPOOL,
                project=project_obj,
                access_modes=access_modes_rbd,
                storageclass=custom_rbd_sc,
                size=size,
                num_of_pvc=2,
            )
            logger.assertion(
                f"Custom RBD PVCs of size {size}: expected=truthy, "
                f"actual={bool(custom_rbd_pvcs)} "
                f"(count={len(custom_rbd_pvcs) if custom_rbd_pvcs else 0})"
            )
            assert custom_rbd_pvcs, "Failed to create custom_rbd_pvcs PVC"

            custom_cephfs_pvcs = multi_pvc_factory(
                interface=constants.CEPHFILESYSTEM,
                project=project_obj,
                access_modes=access_modes_cephfs,
                storageclass=custom_cephfs_sc,
                size=size,
                num_of_pvc=2,
            )
            logger.assertion(
                f"Custom CephFS PVCs of size {size}: expected=truthy, "
                f"actual={bool(custom_cephfs_pvcs)} "
                f"(count={len(custom_cephfs_pvcs) if custom_cephfs_pvcs else 0})"
            )
            assert custom_cephfs_pvcs, "Failed to create custom_cephfs_pvcs PVC"
        logger.info(f"All PVCs created successfully for sizes {size_list}")

    @post_ocs_upgrade
    def test_logs_pod_status_after_upgrade(self):
        """
        1. Verify All pods are restarted and in running state after upgrade.
        2. Verify that logs should not found related to ocs operator trying to patch non expandable PVC.
        """

        logger.test_step("Verify all storage pods are running after upgrade")
        wait_for_storage_pods(timeout=10), "Some pods were not in expected state"

        logger.test_step(
            "Verify OCS operator logs do not contain expansion secret errors"
        )
        pod_name = get_ocs_operator_pod().name
        unexpected_log_after_upgrade = (
            "spec.csi.controllerExpandSecretRef.name: Required value,"
            " spec.csi.controllerExpandSecretRef.namespace: Required value"
        )
        pod_logs = get_pod_logs(pod_name=pod_name, all_containers=True)
        has_unexpected_log = unexpected_log_after_upgrade in pod_logs
        logger.assertion(
            f"Unexpected expansion log in pod {pod_name}: expected=False, actual={has_unexpected_log}"
        )
        assert (
            not has_unexpected_log
        ), f"The unexpected log after upgrade exist on pod {pod_name}"
