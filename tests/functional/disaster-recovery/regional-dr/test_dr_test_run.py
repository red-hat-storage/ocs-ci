import logging

from ocs_ci.framework.testlib import (
    rdr,
    tier1,
    turquoise_squad,
)
from ocs_ci.helpers import dr_helpers

logger = logging.getLogger(__name__)


@rdr
@tier1
@turquoise_squad
class TestDRTestRun:
    """
    Test ODF DR test run command

    """

    def test_dr_test_run(self):
        """
        Run ``odf dr test run`` and store the output in the log directory
        following the must-gather path convention.

        """
        dr_helpers.run_odf_dr_test()
