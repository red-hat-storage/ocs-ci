import logging
import pytest
from ocs_ci.framework import config

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import (
    ManageTest,
    bugzilla,
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

log = logging.getLogger(__name__)
upgrade_ocs_version = config.UPGRADE.get("upgrade_ocs_version")


@bugzilla("2125815")
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
        log.info("Teardown for custome sc")
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

        # Create custom storage class

        custom_cephfs_sc = storageclass_factory(
            interface=constants.CEPHFILESYSTEM, allow_volume_expansion=False
        )
        custom_rbd_sc = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL, allow_volume_expansion=False
        )

        # Appending all the pvc obj to base case param for cleanup and evaluation
        self.all_sc_obj.append(custom_cephfs_sc)
        self.all_sc_obj.append(custom_rbd_sc)

        log.info("Create pvcs for custom sc as well as for default sc")
        project_obj = project_factory()
        for size in size_list:
            rbd_pvcs = multi_pvc_factory(
                interface=constants.CEPHBLOCKPOOL,
                access_modes=access_modes_rbd,
                project=project_obj,
                size=size,
                num_of_pvc=2,
            )
            log.info(f"rbd_pvc created for size {size}")
            assert rbd_pvcs, f"Failed to create rbd_pvcs of size {size}"

            cephfs_pvcs = multi_pvc_factory(
                interface=constants.CEPHFILESYSTEM,
                project=project_obj,
                access_modes=access_modes_cephfs,
                size=size,
                num_of_pvc=2,
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
            assert custom_rbd_pvcs, "Failed to create custom_rbd_pvcs PVC"

            custom_cephfs_pvcs = multi_pvc_factory(
                interface=constants.CEPHFILESYSTEM,
                project=project_obj,
                access_modes=access_modes_cephfs,
                storageclass=custom_cephfs_sc,
                size=size,
                num_of_pvc=2,
            )
            assert custom_cephfs_pvcs, "Failed to create custom_cephfs_pvcs PVC"

    @post_ocs_upgrade
    def test_logs_pod_status_after_upgrade(self):
        """
        1. Verify All pods are restarted and in running state after upgrade.
        2. Verify that logs should not found related to ocs operator trying to patch non expandable PVC.
        """

        wait_for_storage_pods(timeout=10), "Some pods were not in expected state"
        pod_name = get_ocs_operator_pod().name
        unexpected_log_after_upgrade = (
            "spec.csi.controllerExpandSecretRef.name: Required value,"
            " spec.csi.controllerExpandSecretRef.namespace: Required value"
        )
        pod_logs = get_pod_logs(pod_name=pod_name, all_containers=True)
        assert not (
            unexpected_log_after_upgrade in pod_logs
        ), f"The unexpected log after upgrade exist on pod {pod_name}"
