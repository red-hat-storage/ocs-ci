import logging

import pytest

from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    ManageTest,
    tier3,
    skipif_ocs_version,
    skipif_ocp_version,
    skipif_managed_service,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import pvc, ocs
from ocs_ci.helpers import helpers
from ocs_ci.utility import templating

logger = logging.getLogger(__name__)


@green_squad
@tier3
@skipif_managed_service
@skipif_ocs_version("<4.7")
@skipif_ocp_version("<4.7")
@pytest.mark.polarion_id("OCS-2638")
class TestPvcSnapshotWhenSnapshotClassDeleted(ManageTest):
    """
    Tests to verify PVC snapshot feature when snapshotclass deleted
    """

    @pytest.fixture(autouse=True)
    def base_setup(self, interface_iterate, pvc_factory, pod_factory):
        """
        Create resources for the test

        Args:
            interface_iterate: A fixture to iterate over interfaces
            pvc_factory: A fixture to create new pvc
            pod_factory: A fixture to create new pod

        """

        # Create resources for the test
        self.interface = interface_iterate

        self.pvc_obj = pvc_factory(
            interface=self.interface, size=5, status=constants.STATUS_BOUND
        )

        self.pod_object = pod_factory(
            interface=self.interface, pvc=self.pvc_obj, status=constants.STATUS_RUNNING
        )

    def create_snapshotclass(self, interface):
        """
        Creates own volumesnapshotclass

        Args:
            interface (str): Interface type used

        Returns:
            ocs_obj (obj): Snapshotclass obj instances

        """
        if interface == constants.CEPHFILESYSTEM:
            snapshotclass_data = templating.load_yaml(
                constants.CSI_CEPHFS_SNAPSHOTCLASS_YAML
            )
            snapclass_name = "cephfssnapshotclass"
        else:
            snapshotclass_data = templating.load_yaml(
                constants.CSI_RBD_SNAPSHOTCLASS_YAML
            )
            snapclass_name = "rbdsnapshotclass"
        snapshotclass_data["metadata"]["name"] = snapclass_name
        ocs_obj = ocs.OCS(**snapshotclass_data)
        created_snapclass = ocs_obj.create(do_reload=True)
        logger.assertion(
            f"Snapshot class creation: expected=True, actual={bool(created_snapclass)}"
        )
        assert created_snapclass, f"Failed to create snapshot class {snapclass_name}"
        return ocs_obj

    def test_pvc_snapshot(self, interface_iterate, teardown_factory):
        """
        1. Create PVC, POD's
        2. Create own volumesnapshotclass
        3. Take a snapshot of PVC from created snapshotclass
        4. Delete the volumesnapshotclass used to create the above volume snapshot
        5. Check the status of volume snapshot

        """

        logger.test_step(
            f"Create custom VolumeSnapshotClass for interface {self.interface}"
        )
        snapclass_obj = self.create_snapshotclass(interface=self.interface)
        teardown_factory(snapclass_obj)

        logger.test_step(
            f"Take a snapshot of PVC {self.pvc_obj.name} using custom snapshotclass"
        )
        snap_yaml = (
            constants.CSI_CEPHFS_SNAPSHOT_YAML
            if self.interface == constants.CEPHFILESYSTEM
            else constants.CSI_RBD_SNAPSHOT_YAML
        )
        snap_name = helpers.create_unique_resource_name("test", "snapshot")
        snap_obj = pvc.create_pvc_snapshot(
            self.pvc_obj.name,
            snap_yaml,
            snap_name,
            self.pvc_obj.namespace,
            snapclass_obj.name,
        )
        snap_obj.ocp.wait_for_resource(
            condition="true",
            resource_name=snap_obj.name,
            column=constants.STATUS_READYTOUSE,
            timeout=60,
        )
        teardown_factory(snap_obj)
        snap_obj_get = snap_obj.get()

        logger.test_step(f"Delete VolumeSnapshotClass {snapclass_obj.name}")
        snapclass_obj.delete()

        # Verify volumesnapshotclass deleted
        try:
            snapclass_obj.get()
        except CommandFailed as ex:
            if (
                f'volumesnapshotclasses.snapshot.storage.k8s.io "{snapclass_obj.name}" not found'
                not in str(ex)
            ):
                logger.warning(
                    f"VolumeSnapshotClass {snapclass_obj.name} not deleted as expected"
                )
                raise ex
            logger.info(
                f"VolumeSnapshotClass {snapclass_obj.name} deleted successfully"
            )

        logger.test_step(
            "Verify snapshot remains in Ready state after snapshotclass deletion"
        )
        snap_obj.ocp.wait_for_resource(
            condition="true",
            resource_name=snap_obj.name,
            column=constants.STATUS_READYTOUSE,
            timeout=60,
        )

        logger.test_step("Verify snapshot content remains in Ready state")
        snapshotcontent_name = snap_obj_get["status"]["boundVolumeSnapshotContentName"]
        snapshotcontent_obj = OCP(
            kind=constants.VOLUMESNAPSHOTCONTENT, namespace=snap_obj.namespace
        )
        snapshotcontent_obj.wait_for_resource(
            condition="true",
            resource_name=snapshotcontent_name,
            column=constants.STATUS_READYTOUSE,
            timeout=60,
        )
