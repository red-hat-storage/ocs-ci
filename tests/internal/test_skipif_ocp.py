import logging

from ocs_ci.framework.pytest_customization.marks import libtest
from ocs_ci.framework.testlib import BaseTest, skipif_ocp_version

log = logging.getLogger(__name__)


@libtest
class TestSkipifOCP(BaseTest):
    """
    Tests to check the skipif_ocp marker
    """

    @skipif_ocp_version("<4.7")
    def test_skipif_ocp(self):
        """
        Simple test to verify that skipif_ocp marker is working
        """

        log.error("Test did not skipped")

    @skipif_ocp_version("<4.5")
    def test_skipif_ocp_need2run(self):
        """
        Simple test to verify that skipif_ocp marker is working
        """

        log.info("Test did not skipped and is running")
