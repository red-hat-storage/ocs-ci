import logging
import yaml

from ocs_ci.framework.testlib import ManageTest
from ocs_ci.helpers.performance_lib import run_oc_command

logger = logging.getLogger(__name__)


class TestCSISubvolumeGroup(ManageTest):
    def test_network_fence_not_editable(self):
        """
        Test that verifies that the pinning value of CephFilesystemSubVolumeGroup is 1
        """

        result = run_oc_command("get CephFilesystemSubVolumeGroup -o yaml")
        yaml_dict = yaml.safe_load("\n".join(result))
        try:
            pinning_val = yaml_dict["items"][0]["spec"]["pinning"]["distributed"]
        except KeyError as e:
            err_msg = 'Pinning property not found, missing key "%s"' % str(e)
            logger.error(err_msg)
            raise Exception(err_msg)

        if pinning_val != 1:
            err_msg = f"Expected pinning value 1, got {pinning_val} instead"
            logger.error(err_msg)
            raise Exception(err_msg)

        logger.info("Pinning value found and it is 1, ax expected")
