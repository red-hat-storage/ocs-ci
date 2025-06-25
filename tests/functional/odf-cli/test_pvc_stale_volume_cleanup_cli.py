import logging
import pytest

from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier1,
    tier2,
    green_squad,
)
from ocs_ci.ocs import constants
from ocs_ci.helpers import helpers

from ocs_ci.utility.utils import run_cmd
from ocs_ci.framework.testlib import ignore_leftovers

logger = logging.getLogger(__name__)


@ignore_leftovers
@green_squad
class TestSubvolumesCommand(ManageTest):
    @skipif_ocs_version("<4.15")
    @pytest.mark.polarion_id("OCS-5794")
    @tier1
    def test_pvc_stale_volume_cleanup_cli(self, storageclass_factory, pvc_factory):
        """
        1. Create a new PVC with Retain strategy.
        2. Delete the PVC
        3. Check for stale volumes
        4. Run the odf cli.
        5. Check for stale volumes
        6. No stale volumes should be present of the deleted PVC.
        """
        from pathlib import Path

        if not Path(constants.CLI_TOOL_LOCAL_PATH).exists():
            helpers.retrieve_cli_binary(cli_type="odf")
        output = run_cmd(cmd="odf-cli subvolume ls")
        inital_subvolume_list = self.parse_subvolume_ls_output(output)
        logger.info(f"{inital_subvolume_list=}")
        cephfs_sc_obj = storageclass_factory(
            interface=constants.CEPHFILESYSTEM,
            reclaim_policy=constants.RECLAIM_POLICY_RETAIN,
        )
        pvc_obj = pvc_factory(
            storageclass=cephfs_sc_obj,
            interface=constants.CEPHFILESYSTEM,
            size=3,
            access_mode=constants.ACCESS_MODE_RWX,
            status=constants.STATUS_BOUND,
        )
        output = run_cmd(cmd="odf-cli subvolume ls")
        later_subvolume_list = self.parse_subvolume_ls_output(output)
        old = set(inital_subvolume_list)
        new = set(later_subvolume_list)
        new_pvc = list(new.difference(old))[0]
        logger.info(f"{new_pvc=}")

        # Deleteting PVC and SC
        cephfs_sc_obj.delete()
        pvc_obj.delete()

        # Deleteing stale subvolume
        run_cmd(cmd=f"odf-cli subvolume delete {new_pvc[0]} {new_pvc[1]} {new_pvc[2]}")

        # Checking for stale volumes
        output = run_cmd(cmd="odf-cli subvolume ls --stale")
        stale_volumes = self.parse_subvolume_ls_output(output)
        assert len(stale_volumes) == 0  # No stale volumes available

    def parse_subvolume_ls_output(self, output):
        subvolumes = []
        subvolumes_list = output.strip().split("\n")[1:]
        for item in subvolumes_list:
            fs, sv, svg, status = item.split(" ")
            subvolumes.append((fs, sv, svg, status))
        return subvolumes

    @tier2
    @skipif_ocs_version("<4.17")
    @pytest.mark.polarion_id("OCS-6194")
    def test_rox_pvc_stale_volume_cleanup_cli(
        self,
        storageclass_factory,
        pvc_factory,
        snapshot_factory,
        snapshot_restore_factory,
    ):
        """
        1. Create a pvc with retain strategy
        2. Create a snapshot
        3. Create a ROX pvc with source as the above snapshot
        2. Delete the source PVC and PV and snapshot and rox pvc
        3. Check for stale volumes
        4. Run the script.
        5. Check for stale volumes
        6. No stale volumes should be present of the deleted PVC.
        """
        from pathlib import Path

        if not Path(constants.CLI_TOOL_LOCAL_PATH).exists():
            helpers.retrieve_cli_binary(cli_type="odf")
        output = run_cmd(cmd="odf-cli subvolume ls")
        inital_subvolume_list = self.parse_subvolume_ls_output(output)
        logger.info(f"{inital_subvolume_list=}")
        cephfs_sc_obj = storageclass_factory(
            interface=constants.CEPHFILESYSTEM,
            reclaim_policy=constants.RECLAIM_POLICY_RETAIN,
        )

        pvc_obj = pvc_factory(
            storageclass=cephfs_sc_obj,
            interface=constants.CEPHFILESYSTEM,
            size=1,
            access_mode=constants.ACCESS_MODE_RWX,
            status=constants.STATUS_BOUND,
        )

        # Taking snapshot of pvc
        logger.info("Taking Snapshot of the PVC")
        snapshot_obj = snapshot_factory(pvc_obj, wait=False)
        logger.info("Verify snapshots moved from false state to true state")

        # Restoring pvc snapshot to pvc
        logger.info(f"Creating a PVC from snapshot [restore] {snapshot_obj.name}")
        restore_snapshot_obj = snapshot_restore_factory(
            snapshot_obj=snapshot_obj,
            size="1Gi",
            volume_mode=snapshot_obj.parent_volume_mode,
            access_mode=constants.ACCESS_MODE_ROX,
            status=constants.STATUS_BOUND,
            timeout=300,
        )

        output = run_cmd(cmd="odf-cli subvolume ls")
        later_subvolume_list = self.parse_subvolume_ls_output(output)
        old = set(inital_subvolume_list)
        new = set(later_subvolume_list)
        new_pvc = list(new.difference(old))[0]
        logger.info(f"{new_pvc=}")

        # Deleteting original PVC, SC, snapshot created by pvc, pv created by pvc and ROX PVC
        snapshot_obj.delete(wait=True)
        pv_created_by_original_pvc = pvc_obj.backed_pv_obj
        pvc_obj.delete(wait=True)
        cephfs_sc_obj.delete(wait=True)
        restore_snapshot_obj.delete(wait=True)
        helpers.wait_for_resource_state(
            pv_created_by_original_pvc, constants.STATUS_RELEASED
        )
        pv_created_by_original_pvc.delete(wait=True)

        # Checking for stale volumes
        output = run_cmd(cmd="odf-cli subvolume ls --stale")

        # Deleteing stale subvolume
        run_cmd(cmd=f"odf-cli subvolume delete {new_pvc[0]} {new_pvc[1]} {new_pvc[2]}")

        # Checking for stale volumes
        output = run_cmd(cmd="odf-cli subvolume ls --stale")
        stale_volumes = self.parse_subvolume_ls_output(output)
        assert len(stale_volumes) == 0  # No stale volumes available

    @skipif_ocs_version("<4.17")
    @pytest.mark.polarion_id("OCS-6195")
    @tier2
    def test_stale_volume_snapshot_cleanup_cli(
        self,
        storageclass_factory,
        pvc_factory,
        snapshot_factory,
        snapshot_restore_factory,
    ):
        """
        1. Create a pvc with retain strategy
        2. Create a snapshot
        3. Delete the source PVC and PV
        4. Check for stale volumes will show stale-with-snapshot
        5. Delete the snapshot
        6. Check for stale volumes
        7. Run script
        8. No stale volumes should be present of the deleted PVC and its snapshot.
        """
        from pathlib import Path

        if not Path(constants.CLI_TOOL_LOCAL_PATH).exists():
            helpers.retrieve_cli_binary(cli_type="odf")
        output = run_cmd(cmd="odf-cli subvolume ls")
        inital_subvolume_list = self.parse_subvolume_ls_output(output)
        logger.info(f"{inital_subvolume_list=}")
        cephfs_sc_obj = storageclass_factory(
            interface=constants.CEPHFILESYSTEM,
            reclaim_policy=constants.RECLAIM_POLICY_RETAIN,
        )

        pvc_obj = pvc_factory(
            storageclass=cephfs_sc_obj,
            interface=constants.CEPHFILESYSTEM,
            size=1,
            access_mode=constants.ACCESS_MODE_RWX,
            status=constants.STATUS_BOUND,
        )

        # Taking snapshot of pvc
        logger.info("Taking Snapshot of the PVC")
        snapshot_obj = snapshot_factory(pvc_obj, wait=False)
        logger.info("Verify snapshots moved from false state to true state")

        output = run_cmd(cmd="odf-cli subvolume ls")
        later_subvolume_list = self.parse_subvolume_ls_output(output)
        old = set(inital_subvolume_list)
        new = set(later_subvolume_list)
        new_pvc = list(new.difference(old))[0]
        logger.info(f"{new_pvc=}")

        # Deleteting original PVC, SC, snapshot created by pvc, pv created by pvc and ROX PVC
        pv_created_by_original_pvc = pvc_obj.backed_pv_obj
        pvc_obj.delete(wait=True)
        cephfs_sc_obj.delete(wait=True)
        helpers.wait_for_resource_state(
            pv_created_by_original_pvc, constants.STATUS_RELEASED
        )
        pv_created_by_original_pvc.delete(wait=True)

        # Checking for stale volumes
        output = run_cmd(cmd="odf-cli subvolume ls --stale")
        stale_with_snapshot_subvolume = self.parse_subvolume_ls_output(output)[0]
        logger.info(f"{stale_with_snapshot_subvolume=}")
        assert stale_with_snapshot_subvolume[3] == "stale-with-snapshot"

        # Delete Snapshot
        snapshot_obj.delete(wait=True)

        # Deleteing stale subvolume
        run_cmd(cmd=f"odf-cli subvolume delete {new_pvc[0]} {new_pvc[1]} {new_pvc[2]}")

        # Checking for stale volumes
        output = run_cmd(cmd="odf-cli subvolume ls --stale")
        stale_volumes = self.parse_subvolume_ls_output(output)
        assert len(stale_volumes) == 0  # No stale volumes available
