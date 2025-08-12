import logging

from ocs_ci.framework import config
from ocs_ci.framework.testlib import ManageTest, tier2, skipif_external_mode
from ocs_ci.framework.pytest_customization.marks import green_squad, jira, polarion_id
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)


@green_squad
@skipif_external_mode
@tier2
class TestCSISubvolumeGroup(ManageTest):
    @jira("DFBUGS-2759")
    @polarion_id("OCS-5740")
    def test_subvolume_group_pinning(self):
        """
        Test that verifies that the pinning value of CephFilesystemSubVolumeGroup is 1
        """

        subvolumegroup = OCP(
            kind=constants.CEPHFILESYSTEMSUBVOLUMEGROUP,
            namespace=config.ENV_DATA["cluster_namespace"],
        ).get()
        for subvolumegroup_info in subvolumegroup["items"]:
            logger.info(
                f"Checking {constants.CEPHFILESYSTEMSUBVOLUMEGROUP} {subvolumegroup_info['metadata']['name']}"
            )
            try:
                pinning = subvolumegroup_info["status"]["info"]["pinning"]
                pinning_key, pinning_val = pinning.strip().split("=")
                assert pinning_key == "distributed", "Pinning must be 'distributed'"
                pinning_val = int(pinning_val)
            except KeyError as e:
                err_msg = 'Pinning property not found, missing key "%s"' % str(e)
                logger.error(err_msg)
                raise Exception(err_msg)

            if pinning_val != 1:
                err_msg = f"Expected pinning value 1, got {pinning_val} instead"
                logger.error(err_msg)
                raise Exception(err_msg)

            logger.info("Pinning value found and it is 1, as expected")
