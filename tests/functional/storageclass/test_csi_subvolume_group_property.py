import logging
import yaml

from ocs_ci.framework.testlib import ManageTest, tier2, skipif_external_mode
from ocs_ci.framework.pytest_customization.marks import green_squad, jira, polarion_id
from ocs_ci.helpers.performance_lib import run_oc_command

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

        result = run_oc_command("get CephFilesystemSubVolumeGroup -o yaml")
        yaml_dict = yaml.safe_load("\n".join(result))
        try:
            pinning = yaml_dict["items"][0]["status"]["pinning"]
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
