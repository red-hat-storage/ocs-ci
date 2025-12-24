import logging

from ocs_ci.framework.testlib import (
    acceptance,
    post_ocs_upgrade,
    rdr,
    skipif_ocs_version,
    tier1,
    turquoise_squad,
)
from ocs_ci.helpers import dr_helpers

logger = logging.getLogger(__name__)


@rdr
@tier1
@acceptance
@turquoise_squad
@post_ocs_upgrade
@skipif_ocs_version("<=4.20")
class TestCGConfiguration:
    """
    Test for validating CG behavior for ODF version >= 4.21

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
