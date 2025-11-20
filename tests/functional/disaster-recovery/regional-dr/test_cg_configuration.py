import logging

import pytest

from ocs_ci.helpers import dr_helpers

logger = logging.getLogger(__name__)


# @rdr
# @tier1
# @acceptance
# @post_ocs_upgrade
# @turquoise_squad
# Test disabled until CG support is added in a future release. Related bug: DFBUGS-4556
@pytest.mark.skip(
    reason="Test disabled until CG support is added in a future release. Related bug: DFBUGS-4556"
)
class TestCGConfiguration:
    """
    Test for validating CG behavior for ODF version >= 4.20

    """

    def test_drpolicy_grouping(self):
        """
        Validate DRPolicy has grouping=true for every storageClass
        """
        dr_helpers.validate_drpolicy_grouping()

    def test_vgrc_count(self):
        """
        Validate VGRC exists on each managed cluster per scheduling interval
        """
        dr_helpers.validate_vgrc_count()
