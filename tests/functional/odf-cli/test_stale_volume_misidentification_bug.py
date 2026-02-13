import logging
from subprocess import CompletedProcess

import pytest

from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier2,
    green_squad,
    jira,
)
from ocs_ci.ocs import constants
from ocs_ci.helpers import helpers
from ocs_ci.framework.testlib import ignore_leftovers

logger = logging.getLogger(__name__)


@ignore_leftovers
@green_squad
class TestStaleVolumeMisidentification(ManageTest):
    """
    Test class to verify the bug fix for ODF-CLI misidentifying active CephFS volumes as stale.

    Bug Description:
    The ODF-CLI tool was misidentifying active cephfs volumes (like image registry volumes)
    as stale when the PV had missing volumeAttributes. This happened when:
    1. A PVC was created with cephfs storageclass and deletionPolicy set to retain
    2. The PV was backed up, deleted (with finalizer removed)
    3. The PV backup was edited to remove all volumeAttributes and reapplied
    4. The PV was bound again
    5. The odf-cli would incorrectly label this volume as stale
    """

    @pytest.fixture(autouse=True)
    def setup(self, odf_cli_setup):
        """
        Setup fixture to initialize ODF CLI runner.

        Args:
            odf_cli_setup: Fixture that provides ODFCliRunner instance
        """
        self.odf_cli_runner = odf_cli_setup

    @skipif_ocs_version("<4.17")
    @pytest.mark.polarion_id("OCS-XXXX")  # TODO: Update with actual Polarion ID
    @jira("DFBUGS-3778")
    @tier2
    def test_stale_volume_misidentification_with_missing_volume_attributes(
        self, storageclass_factory, pvc_factory
    ):
        """
        Test to verify that ODF-CLI does not misidentify active CephFS volumes as stale
        when volumeAttributes are missing from the PV.

        This test reproduces the bug scenario where:
        1. Create a PVC with cephfs storageclass and retain reclaim policy
        2. Back up the PV yaml
        3. Delete the PV and remove its finalizer
        4. Edit the PV backup by removing all volumeAttributes
        5. Apply the edited PV yaml
        6. Verify that the PV is bound
        7. Run odf-cli to list stale subvolumes
        8. Verify that the volume is NOT labeled as stale (bug fix verification)

        Expected Result:
        Without the fix: The volume would be incorrectly labeled as stale
        With the fix: The volume should be labeled as in-use

        Args:
            storageclass_factory: Factory fixture to create storage classes
            pvc_factory: Factory fixture to create PVCs
        """
        # Get initial list of subvolumes
        logger.info("Getting initial list of subvolumes")
        output = self.odf_cli_runner.run_command("subvolume ls")
        initial_subvolume_list = self.parse_subvolume_ls_output(output)
        logger.info(f"Initial subvolumes: {initial_subvolume_list}")

        # Step 1: Create a PVC with CephFS storageclass and retain reclaim policy
        logger.info("Creating CephFS storageclass with retain reclaim policy")
        cephfs_sc_obj = storageclass_factory(
            interface=constants.CEPHFILESYSTEM,
            reclaim_policy=constants.RECLAIM_POLICY_RETAIN,
        )

        logger.info("Creating PVC with CephFS storageclass")
        pvc_obj = pvc_factory(
            storageclass=cephfs_sc_obj,
            interface=constants.CEPHFILESYSTEM,
            size=3,
            access_mode=constants.ACCESS_MODE_RWX,
            status=constants.STATUS_BOUND,
        )

        # Get the PV object and verify it's bound
        pv_obj = pvc_obj.backed_pv_obj
        logger.info(f"PVC {pvc_obj.name} is bound to PV {pv_obj.name}")

        # Verify the new subvolume appears in the list
        output = self.odf_cli_runner.run_command("subvolume ls")
        current_subvolume_list = self.parse_subvolume_ls_output(output)
        new_subvolumes = list(set(current_subvolume_list) - set(initial_subvolume_list))
        assert new_subvolumes, "No new subvolume found after PVC creation"
        new_subvolume = new_subvolumes[0]
        logger.info(f"New subvolume created: {new_subvolume}")

        # Step 2: Back up the PV yaml
        logger.info("Backing up PV yaml")
        pv_yaml_backup = pv_obj.get()

        # Step 3: Delete the PVC (this will not delete the PV due to retain policy)
        logger.info("Deleting PVC")
        pvc_obj.delete()
        pvc_obj.ocp.wait_for_delete(resource_name=pvc_obj.name, timeout=120)

        # Wait for PV to be in Released state
        logger.info("Waiting for PV to be in Released state")
        helpers.wait_for_resource_state(pv_obj, constants.STATUS_RELEASED, timeout=120)

        # Remove PV finalizers to allow deletion
        logger.info("Removing PV finalizers")
        pv_obj.ocp.patch(
            resource_name=pv_obj.name,
            params='{"metadata":{"finalizers":null}}',
            format_type="merge",
        )

        # Step 4: Delete the PV
        logger.info("Deleting PV")
        pv_obj.delete()
        pv_obj.ocp.wait_for_delete(resource_name=pv_obj.name, timeout=120)

        # Step 5: Edit the PV backup by removing volumeAttributes
        logger.info("Editing PV yaml to remove volumeAttributes")
        pv_yaml_modified = pv_yaml_backup.copy()

        # Remove volumeAttributes from the CSI section
        if "spec" in pv_yaml_modified and "csi" in pv_yaml_modified["spec"]:
            if "volumeAttributes" in pv_yaml_modified["spec"]["csi"]:
                logger.info("Removing volumeAttributes from PV spec")
                del pv_yaml_modified["spec"]["csi"]["volumeAttributes"]

        # Remove claimRef to make PV available for binding
        if "spec" in pv_yaml_modified and "claimRef" in pv_yaml_modified["spec"]:
            logger.info("Removing claimRef from PV spec")
            del pv_yaml_modified["spec"]["claimRef"]

        # Reset status
        if "status" in pv_yaml_modified:
            pv_yaml_modified["status"] = {"phase": "Available"}

        # Step 6: Apply the edited PV yaml
        logger.info("Applying edited PV yaml")
        from ocs_ci.ocs.resources.ocs import OCS

        pv_obj_new = OCS(**pv_yaml_modified)
        pv_obj_new.create()

        # Wait for PV to be available
        logger.info("Waiting for PV to be Available")
        helpers.wait_for_resource_state(
            pv_obj_new, constants.STATUS_AVAILABLE, timeout=120
        )

        # Create a new PVC to bind to this PV
        logger.info("Creating new PVC to bind to the modified PV")
        pvc_obj_new = pvc_factory(
            storageclass=cephfs_sc_obj,
            interface=constants.CEPHFILESYSTEM,
            size=3,
            access_mode=constants.ACCESS_MODE_RWX,
            status=constants.STATUS_BOUND,
            pv_name=pv_obj_new.name,
        )

        # Verify the PVC is bound to the PV
        logger.info(f"New PVC {pvc_obj_new.name} is bound to PV {pv_obj_new.name}")
        assert pvc_obj_new.backed_pv == pv_obj_new.name, (
            f"PVC is not bound to the expected PV. "
            f"Expected: {pv_obj_new.name}, Got: {pvc_obj_new.backed_pv}"
        )

        # Step 7: Run odf-cli to list stale subvolumes
        logger.info("Checking for stale subvolumes")
        output = self.odf_cli_runner.run_command("subvolume ls --stale")
        stale_subvolumes = self.parse_subvolume_ls_output(output)
        logger.info(f"Stale subvolumes found: {stale_subvolumes}")

        # Step 8: Verify that the volume is NOT labeled as stale
        # The subvolume should not appear in the stale list
        stale_subvolume_names = [
            sv[1] for sv in stale_subvolumes
        ]  # Extract subvolume names

        logger.info(f"Verifying that subvolume {new_subvolume[1]} is not in stale list")
        assert new_subvolume[1] not in stale_subvolume_names, (
            f"BUG: Active volume {new_subvolume[1]} is incorrectly identified as stale. "
            f"This indicates the bug is not fixed. Stale volumes: {stale_subvolumes}"
        )

        logger.info(
            "SUCCESS: Volume with missing volumeAttributes is correctly identified as in-use, not stale"
        )

        # Cleanup
        logger.info("Cleaning up test resources")
        pvc_obj_new.delete()
        cephfs_sc_obj.delete()
        # Note: PV will be deleted automatically due to retain policy when PVC is deleted

    def parse_subvolume_ls_output(self, output):
        """
        Parse the output of 'odf subvolume ls' command.

        Args:
            output: Command output (CompletedProcess or string)

        Returns:
            list: List of tuples containing (filesystem, subvolume, subvolumegroup, status)
        """
        if isinstance(output, CompletedProcess):
            output = output.stdout.decode("utf-8")

        subvolumes = []
        subvolumes_list = output.strip().split("\n")[1:]  # Skip header line
        for item in subvolumes_list:
            if item.strip():  # Skip empty lines
                parts = item.split()
                if len(parts) >= 4:
                    fs, sv, svg, status = parts[0], parts[1], parts[2], parts[3]
                    subvolumes.append((fs, sv, svg, status))
        return subvolumes


# Made with Bob
