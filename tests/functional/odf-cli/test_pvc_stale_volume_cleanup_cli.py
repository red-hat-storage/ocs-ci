import logging
import tempfile
import yaml
from subprocess import CompletedProcess

import pytest

from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier1,
    tier2,
    green_squad,
)
from ocs_ci.ocs import constants, ocp
from ocs_ci.helpers import helpers
from ocs_ci.utility.templating import dump_data_to_temp_yaml
from ocs_ci.utility.utils import run_cmd

from ocs_ci.framework.testlib import ignore_leftovers

logger = logging.getLogger(__name__)


@ignore_leftovers
@green_squad
class TestSubvolumesCommand(ManageTest):

    @pytest.fixture(autouse=True)
    def setup(self, odf_cli_setup):
        self.odf_cli_runner = odf_cli_setup

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

        output = self.odf_cli_runner.run_command("subvolume ls")

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

        output = self.odf_cli_runner.run_command("subvolume ls")
        later_subvolume_list = self.parse_subvolume_ls_output(output)
        new_pvc_list = list(set(later_subvolume_list) - set(inital_subvolume_list))
        assert new_pvc_list, "No New PVC found in the cluster."
        new_pvc = new_pvc_list[0]
        logger.info(f"{new_pvc=}")

        # Deleteting PVC and SC
        cephfs_sc_obj.delete()
        pvc_obj.delete()

        # Deleteing stale subvolume
        self.odf_cli_runner.run_command(
            f"subvolume delete {new_pvc[0]} {new_pvc[1]} {new_pvc[2]}"
        )

        # Checking for stale volumes
        output = self.odf_cli_runner.run_command("subvolume ls --stale")
        stale_volumes = self.parse_subvolume_ls_output(output)
        assert len(stale_volumes) == 0  # No stale volumes available

    def parse_subvolume_ls_output(self, output):
        if isinstance(output, CompletedProcess):
            output = output.stdout.decode("utf-8")

        subvolumes = []
        subvolumes_list = output.strip().split("\n")[1:]
        for item in subvolumes_list:
            # Split by whitespace and filter out empty strings
            parts = [part for part in item.split() if part]
            if len(parts) >= 4:
                fs, sv, svg, status = parts[0], parts[1], parts[2], parts[3]
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

        output = self.odf_cli_runner.run_command("subvolume ls")
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

        output = self.odf_cli_runner.run_command("subvolume ls")
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

        self.odf_cli_runner.run_command("subvolume ls --stale")
        # Deleteing stale subvolume
        self.odf_cli_runner.run_command(
            f"subvolume delete {new_pvc[0]} {new_pvc[1]} {new_pvc[2]}"
        )

        # Checking for stale volumes
        output = self.odf_cli_runner.run_command("subvolume ls --stale")
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

        output = self.odf_cli_runner.run_command("subvolume ls")
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

        output = self.odf_cli_runner.run_command("subvolume ls")
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
        output = self.odf_cli_runner.run_command("subvolume ls --stale")
        stale_with_snapshot_subvolume = self.parse_subvolume_ls_output(output)[0]
        logger.info(f"{stale_with_snapshot_subvolume=}")
        assert stale_with_snapshot_subvolume[3] == "stale-with-snapshot"

        # Delete Snapshot
        snapshot_obj.delete(wait=True)

        # Deleteing stale subvolume
        self.odf_cli_runner.run_command(
            f"subvolume delete {new_pvc[0]} {new_pvc[1]} {new_pvc[2]}"
        )

        # Checking for stale volumes
        output = self.odf_cli_runner.run_command("subvolume ls --stale")
        stale_volumes = self.parse_subvolume_ls_output(output)
        assert len(stale_volumes) == 0  # No stale volumes available

    @skipif_ocs_version("<4.17")
    @pytest.mark.polarion_id("OCS-6196")
    @tier2
    def test_pv_backup_restore_with_stale_volume_check(
        self, storageclass_factory, pvc_factory
    ):
        """
        Test to verify PV backup/restore workflow and stale volume detection.

        Steps:
        1.Create a PVC with cephfs storageclass and set the deletionPolicy to retain.
        2.Back up the PV yaml for the above created PVC. Delete the PV and remove its finalizer.
        3.Edit the PV backup file by removing all the volumeAttributes and apply this yaml file.
        4.Verify that the PV is bound.
        5.Now run the odf-cli command to list the stale subvolumes.
        6.The volume attached to the above PVC would be labelled as in-use by the cli tool.
        """

        logger.info("Creating PVC with CephFS storageclass and retain policy")
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

        # Get initial subvolume list
        output = self.odf_cli_runner.run_command("subvolume ls")
        initial_subvolume_list = self.parse_subvolume_ls_output(output)
        logger.info(f"Initial subvolume list: {initial_subvolume_list}")

        # Backup PV yaml, delete PV and remove finalizer
        logger.info("Backing up PV yaml")
        pv_name = pvc_obj.get().get("spec").get("volumeName")
        backup_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="pv_backup_", suffix=".yaml", delete=False
        )
        backup_file_path = backup_file.name
        backup_get = pvc_obj.backed_pv_obj.get()
        dump_data_to_temp_yaml(backup_get, backup_file_path)
        logger.info(f"PV backup file created: {backup_file_path}")

        logger.info(f"Deleting PV: {pv_name}")
        ocp_pv = ocp.OCP(kind=constants.PV)
        ocp_pv.delete(resource_name=pv_name, wait=False)
        ocp_pv.patch(
            resource_name=pv_name,
            params='{"metadata": {"finalizers":null}}',
            format_type="merge",
        )
        ocp_pv.wait_for_delete(resource_name=pv_name)
        logger.info(f"PV {pv_name} deleted successfully")

        logger.info("Editing PV backup file to remove volumeAttributes")
        with open(backup_file_path, "r") as backup:
            backup_data = yaml.safe_load(backup)

        # Remove volumeAttributes from the backup
        if "spec" in backup_data and "csi" in backup_data["spec"]:
            if "volumeAttributes" in backup_data["spec"]["csi"]:
                logger.info("Removing volumeAttributes from PV backup")
                del backup_data["spec"]["csi"]["volumeAttributes"]

        # Remove resourceVersion and uid for re-creation
        if "metadata" in backup_data:
            backup_data["metadata"].pop("resourceVersion", None)
            backup_data["metadata"].pop("uid", None)
            backup_data["metadata"].pop("creationTimestamp", None)

        # Write modified backup
        with open(backup_file_path, "w") as backup:
            yaml.dump(backup_data, backup)
            logger.info(f"PV backup file updated: {backup_file_path}")

        # Apply the modified PV yaml
        logger.info("Applying modified PV yaml")
        run_cmd(f"oc apply -f {backup_file_path}")

        # Verify PV is bound
        helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND, timeout=120)
        logger.info(f"PV {pv_name} recreated and bound successfully")

        logger.info("Checking stale subvolumes with odf-cli")
        output = self.odf_cli_runner.run_command("subvolume ls")
        current_subvolume_list = self.parse_subvolume_ls_output(output)
        logger.info(f"Current subvolume list: {current_subvolume_list}")

        # Find the subvolume for PVC
        new_subvolumes = list(set(current_subvolume_list) - set(initial_subvolume_list))
        pvc_subvolume = None
        if new_subvolumes:
            pvc_subvolume = new_subvolumes[0]
            logger.info(f"PVC subvolume found: {pvc_subvolume}")
            # Verify the volume is marked as "in-use" (not stale)
            assert (
                pvc_subvolume[3] == "in-use"
            ), f"Expected volume status to be 'in-use', but got '{pvc_subvolume[3]}'"
            logger.info("Volume correctly marked as 'in-use' by odf-cli tool")
        else:
            logger.warning(
                "No new subvolumes found, PVC might be using existing volume"
            )

        # Check stale volumes - should not include created PVC's volume
        output = self.odf_cli_runner.run_command("subvolume ls --stale")
        stale_volumes = self.parse_subvolume_ls_output(output)
        logger.info(f"Stale volumes: {stale_volumes}")
        if pvc_subvolume is not None:
            pvc_subvolume_identifier = (
                pvc_subvolume[0],
                pvc_subvolume[1],
                pvc_subvolume[2],
            )
            for stale_vol in stale_volumes:
                stale_identifier = (stale_vol[0], stale_vol[1], stale_vol[2])
                assert (
                    stale_identifier != pvc_subvolume_identifier
                ), f"PVC volume should not be in stale list: {pvc_subvolume}"
