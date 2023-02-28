import logging
import os
import pytest

from ocs_ci.framework.testlib import E2ETest
from ocs_ci.helpers.performance_lib import run_oc_command
from ocs_ci.ocs import constants

logger = logging.getLogger(__name__)


def create_check_not_editable():
    ERRMSG = "Error in command"

    network_fence_yaml = os.path.join(constants.TEMPLATE_CSI_ADDONS_DIR, "NetworkFence.yaml")

    res = run_oc_command(cmd=f"create -f {network_fence_yaml}")
    if ERRMSG in res[0]:
        err_msg = f"Failed to create resource from yaml file : {network_fence_yaml}, got result {res}"
        logger.error(err_msg)
        raise Exception(err_msg)

    logger.info("Network fence created")
    logger.info(res)


class TestCRRsourcesValidation(E2ETest):
    """

    """

    def test_resources(
        self,
    ):
        create_check_not_editable()
