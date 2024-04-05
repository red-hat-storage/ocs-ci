import pytest
import logging

from ocs_ci.helpers.helpers import (
    odf_cli_set_recover_profile,
    get_ceph_recovery_profile,
)
from ocs_ci.framework.pytest_customization.marks import brown_squad
from ocs_ci.framework.testlib import tier1

log = logging.getLogger(__name__)


@brown_squad
@tier1
class TestRecoveryProfileInCeph:
    @pytest.mark.polarion_id("OCS-XXXX")
    @pytest.mark.parametrize(
        argnames=["recovery_profile"],
        argvalues=[
            pytest.param("balanced"),
            pytest.param("high_client_ops"),
            pytest.param("high_recovery_ops"),
        ],
    )
    def test_set_recovery_profile_odfcli(self, recovery_profile):
        """
        Test setting the recovery profile by ODF CLI.
        Steps:
            1. Set recovery-profile using ODF cli tool
            2. Verify recovery profile from the ceph toolbox pod
        """

        # Setting up and verifying the recovery profile value with the odf CLI tool

        assert odf_cli_set_recover_profile(recovery_profile)
        log.info("Fetching ceph osd_mclock_profile/recovery profile using odf-cli tool.")
        a = get_ceph_recovery_profile()
        log.info (f"Applied recovery profile on ceph cluster is {a}")
        assert (
            recovery_profile == get_ceph_recovery_profile()
        ), f"Recovery profile set by ODF CLI ({recovery_profile}) does not match with the value reported by Ceph"
