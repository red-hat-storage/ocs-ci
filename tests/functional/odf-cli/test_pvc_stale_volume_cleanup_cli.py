import logging

from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier1,
    acceptance,
)
from ocs_ci.helpers.helpers import retrieve_cli_binary
from ocs_ci.utility.utils import run_cmd

logger = logging.getLogger(__name__)


@tier1
@acceptance
class TestSubvolumesCommand(ManageTest):
    @skipif_ocs_version("<4.15")
    def test_pvc_stale_volume_cleanup_cli(self):
        """
        1. Create a new PVC with Retain strategy.
        2. Delete the PVC
        3. Check for stale volumes
        4. Run the odf cli.
        5. Check for stale volumes
        6. No stale volumes should be present of the deleted PVC.
        """
        retrieve_cli_binary(cli_type="odf")
        inital_subvolume_list = run_cmd(
            cmd="odf subvolume ls",
        )
        logger.info(f"{inital_subvolume_list=}")
        logger.info("I am here")
