import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import (
    ManageTest,
    tier1,
    bugzilla,
    skipif_ocs_version,
)
from ocs_ci.framework.pytest_customization.marks import (
    pre_ocs_upgrade,
    post_ocs_upgrade,
)
from ocs_ci.ocs.resources.pod import (
    wait_for_storage_pods,
    get_ocs_operator_pod,
    get_pod_logs,
)
from ocs_ci.utility import templating
from ocs_ci.helpers import helpers

log = logging.getLogger(__name__)


@tier1
@bugzilla("2125815")
@skipif_ocs_version("<4.11")
class TestUpgrade(ManageTest):
    """
    Tests to check upgrade of OCS when we set without expansion secret and allowExpansion set to false
    """

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
        6. Verify All pods are restarted and in running state after upgrade.
        7. Verify that logs should not found related to ocs operator trying to patch non expandable PVC.
            e.g "msg":"Error patching PersistentVolume."
        """
        size_list = ["1", "3", "5"]
        self.all_sc_obj = []

        access_modes_cephfs = [constants.ACCESS_MODE_RWO, constants.ACCESS_MODE_RWX]
        access_modes_rbd = [
            constants.ACCESS_MODE_RWO,
            f"{constants.ACCESS_MODE_RWO}-Block",
            f"{constants.ACCESS_MODE_RWX}-Block",
        ]

        # Create custom storage class
        custom_cephfs_sc_no_expnasion_data = templating.load_yaml(
            constants.CUSTOM_CEPHFS_SC_NO_EXPANSION_YAML
        )
        custom_rbd_sc_no_expnasion_data = templating.load_yaml(
            constants.CUSTOM_RBD_SC_NO_EXPANSION_YAML
        )

        custom_cephfs_sc = helpers.create_resource(**custom_cephfs_sc_no_expnasion_data)
        custom_rbd_sc = helpers.create_resource(**custom_rbd_sc_no_expnasion_data)

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
                num_of_pvc=3,
            )
            assert rbd_pvcs, "Failed to create rbd_pvcs PVC"

            cephfs_pvcs = multi_pvc_factory(
                interface=constants.CEPHFILESYSTEM,
                project=project_obj,
                access_modes=access_modes_cephfs,
                size=size,
                num_of_pvc=1,
            )
            assert cephfs_pvcs, "Failed to create cephfs_pvcs PVC"

            custom_rbd_pvcs = multi_pvc_factory(
                interface=constants.CEPHBLOCKPOOL,
                project=project_obj,
                access_modes=access_modes_rbd,
                storageclass=custom_rbd_sc,
                size=size,
                num_of_pvc=1,
            )
            assert custom_rbd_pvcs, "Failed to create custom_rbd_pvcs PVC"

            custom_cephfs_pvcs = multi_pvc_factory(
                interface=constants.CEPHFILESYSTEM,
                project=project_obj,
                access_modes=access_modes_cephfs,
                storageclass=custom_cephfs_sc,
                size=size,
                num_of_pvc=1,
            )
            assert custom_cephfs_pvcs, "Failed to create custom_cephfs_pvcs PVC"

    @post_ocs_upgrade
    def test_logs_pod_status_after_upgrade(self):

        wait_for_storage_pods(timeout=10), "Some pods were not in expected state"
        pod_objs = get_ocs_operator_pod()
        pod_name = pod_objs.name
        expected_log_after_upgrade = '"Error patching PersistentVolume"'
        pod_logs = get_pod_logs(pod_name=pod_name, all_containers=True)
        assert expected_log_after_upgrade not in pod_logs, (
            f"The expected log after upgrade '{expected_log_after_upgrade}'exist"
            f" on pod {pod_name}"
        )
