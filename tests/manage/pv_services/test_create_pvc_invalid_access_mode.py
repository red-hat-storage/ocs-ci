import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import ManageTest, tier3, ignore_data_rebalance
from ocs_ci.ocs.exceptions import CommandFailed

log = logging.getLogger(__name__)


@tier3
@ignore_data_rebalance
@pytest.mark.parametrize(
    argnames="interface",
    argvalues=[
        pytest.param(
            *[constants.CEPHBLOCKPOOL], marks=pytest.mark.polarion_id("OCS-746")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM], marks=pytest.mark.polarion_id("OCS-747")
        ),
    ],
)
class TestCreatePvcInvalidAccessMode(ManageTest):
    """
    This test class consists of tests to verify that PVC creation will not
    succeed with invalid access mode value
    """

    def test_verify_pvc_invalid_access_mode(self, interface, pvc_factory):
        """
        This test case verifies that PVC creation will not succeed with
        invalid access mode value
        """
        expected_err = 'invalid: spec.accessModes: Unsupported value: "RWO"'

        # Try to create a PVC by providing invalid value of access mode
        try:
            self.pvc_obj = pvc_factory(
                interface=interface,
                project=None,
                storageclass=None,
                size=3,
                access_mode="RWO",
                custom_data=None,
                status=constants.STATUS_BOUND,
            )
            assert not self.pvc_obj, "Unexpected: PVC creation hasn't failed."
        except CommandFailed as err:
            assert expected_err in str(
                err
            ), f"Couldn't verify PVC creation. Unexpected error {str(err)}"
            log.info("PVC creation failed as expected.")
