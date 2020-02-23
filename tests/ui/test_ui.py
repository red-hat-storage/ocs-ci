"""
UI tests
"""

import logging

from ocs_ci.framework.testlib import ignore_leftovers, ManageTest, ui
from ocs_ci.ocs import constants
from ocs_ci.utility.openshift_console import OpenshiftConsole


logger = logging.getLogger(__name__)


@ui
@ignore_leftovers
class TestUI(ManageTest):
    """
    UI tests
    """

    def test_ui_chrome(self):
        logger.info(
            "Running ceph-storage-tests suite with openshift-console with "
            "chrome browser"
        )
        ocp_console = OpenshiftConsole(constants.CHROME_BROWSER)
        ocp_console.run_openshift_console(
            suite="ceph-storage-tests", log_suffix="ui-tests-chrome",
            timeout=3600,
        )
