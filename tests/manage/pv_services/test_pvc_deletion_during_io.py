import pytest
import logging

from ocs_ci.ocs import exceptions, constants
from ocs_ci.ocs.resources import pod
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import ManageTest, tier2


logger = logging.getLogger(__name__)


@green_squad
@tier2
@pytest.mark.parametrize(
    argnames=["interface"],
    argvalues=[
        pytest.param(constants.CEPHBLOCKPOOL, marks=pytest.mark.polarion_id("OCS-371")),
        pytest.param(
            constants.CEPHFILESYSTEM, marks=pytest.mark.polarion_id("OCS-1318")
        ),
    ],
)
class TestDeletePVCWhileRunningIO(ManageTest):
    """
    Delete PVC while IO is in progress

    """

    pvc_obj = None
    pod_obj = None

    @pytest.fixture(autouse=True)
    def test_setup(self, interface, pvc_factory, pod_factory):
        """
        Create resources for the test

        """
        self.pvc_obj = pvc_factory(interface=interface)
        self.pod_obj = pod_factory(pvc=self.pvc_obj)

    def test_run_io_and_delete_pvc(self):
        """
        Delete PVC while IO is in progress

        """
        thread = pod.run_io_in_bg(self.pod_obj, expect_to_fail=True)
        self.pvc_obj.delete(wait=False)
        self.pvc_obj.ocp.wait_for_resource(
            condition=constants.STATUS_TERMINATING, resource_name=self.pvc_obj.name
        )
        thread.join(timeout=15)

        self.pod_obj.delete()

        # The PVC will no longer exist because the pod got deleted while it was
        # in Terminating status. Hence, catching this exception
        try:
            self.pvc_obj.get(out_yaml_format=False)
        except exceptions.CommandFailed as ex:
            if "NotFound" in str(ex):
                pass
