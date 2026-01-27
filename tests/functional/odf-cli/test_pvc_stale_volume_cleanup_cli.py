import logging
import pytest

from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier1,
    green_squad,
)
from ocs_ci.ocs import constants
from ocs_ci.helpers.helpers import retrieve_cli_binary
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
            retrieve_cli_binary(cli_type="odf")
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
        new_pvc_list = list(set(later_subvolume_list) - set(inital_subvolume_list))
        assert new_pvc_list, "No New PVC found in the cluster."
        new_pvc = new_pvc_list[0]
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
