import logging
import pytest

from ocs_ci.ocs import constants, node
from ocs_ci.framework.testlib import (
    ManageTest,
    skipif_ocs_version,
    bugzilla
)

log = logging.getLogger(__name__)


@pytest.mark.parametrize(
    argnames="interface",
    argvalues=[
        pytest.param(*[constants.CEPHBLOCKPOOL]),
        pytest.param(*[constants.CEPHFILESYSTEM]),
    ],
)
@skipif_ocs_version("<4.16")
class TestChangePvcAccessMode(ManageTest):
    """
    Tests changing access modes of the created pvc to ReadWriteOncePod access mode
    """

    def verify_access_mode(self, access_mode):
        assert (self.pvc_obj.get_pvc_access_mode == access_mode), \
            f"PVC object {self.pvc_obj.name} has access mode {self.pvc_obj.get_pvc_access_mode}, expected {access_mode}"

    @bugzilla("OCPBUGS-36618")
    def test_change_rwo_access_mode(self, pvc_factory, interface):
        """
        Tests that changing access mode from ReadWriteOnce to ReadWriteOncePod is successful

        """
        self.pvc_obj = pvc_factory(interface=interface, access_mode=constants.ACCESS_MODE_RWO)
        self.verify_access_mode(constants.ACCESS_MODE_RWO)

        self.pvc_obj.access_mode = constants.ACCESS_MODE_RWOP
        # verify that the mode changed successfully
        self.verify_access_mode(constants.ACCESS_MODE_RWOP)

    def test_change_rox_access_mode(self, pvc_factory, interface):
        """
        Tests that it is not possible to change access mode from ReadOnlyMany to ReadWriteOncePod

        """
        self.pvc_obj = pvc_factory(interface=interface, access_mode=constants.ACCESS_MODE_ROX)
        self.verify_access_mode(constants.ACCESS_MODE_ROX)

        self.pvc_obj.access_mode = constants.ACCESS_MODE_RWOP
        # verify that the mode was not changed
        self.verify_access_mode(constants.ACCESS_MODE_ROX)

    def test_change_rwx_access_mode(self, pvc_factory, interface):
        """
        Tests that it is not possible to change access mode from ReadWriteMany to ReadWriteOncePod

        """
        self.pvc_obj = pvc_factory(interface=interface,access_mode=constants.ACCESS_MODE_RWX)
        self.verify_access_mode(constants.ACCESS_MODE_RWX)

        self.pvc_obj.access_mode = constants.ACCESS_MODE_RWOP
        # verify that the mode was not changed
        self.verify_access_mode(constants.ACCESS_MODE_RWX)
