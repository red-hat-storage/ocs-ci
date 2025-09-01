import logging
import pytest
from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier2,
    # polarion_id,
    skipif_ocp_version,
)
from ocs_ci.ocs.resources import pvc
from ocs_ci.helpers import helpers

log = logging.getLogger(__name__)


@green_squad
@tier2
@skipif_ocs_version("<4.20")
@skipif_ocp_version("<4.20")
class TestAlertWhenTooManyClonesCreated(ManageTest):
    """
    Tests for alerts when too many clones are created
    """

    @pytest.fixture(scope="class")
    def setup(self, pvc_factory):
        """
        Create a PVC and 199 clones

        Args:
            pvc_factory: A fixture to create new pvc

        """
        self.pvc_obj = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            size=1,
            status=constants.STATUS_BOUND,
            access_mode=constants.ACCESS_MODE_RWO,
        )

        # create 199 clones
        sc_name = self.pvc_obj.backed_sc
        parent_pvc = self.pvc_obj.name
        clone_yaml = constants.CSI_RBD_PVC_CLONE_YAML
        namespace = self.pvc_obj.namespace
        for _ in range(199):
            cloned_pvc_obj = pvc.create_pvc_clone(
                sc_name, parent_pvc, clone_yaml, namespace
            )
            helpers.wait_for_resource_state(
                cloned_pvc_obj, constants.STATUS_BOUND, timeout=300
            )
