import logging
import pytest

from ocs_ci.framework.testlib import ManageTest, tier4, tier4a
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pod
from ocs_ci.helpers import disruption_helpers


log = logging.getLogger(__name__)

DISRUPTION_OPS = disruption_helpers.Disruptions()


@tier4
@tier4a
@pytest.mark.parametrize(
    argnames=["interface", "resource_to_delete"],
    argvalues=[
        pytest.param(
            *[constants.CEPHFILESYSTEM, "cephfsplugin"],
            marks=pytest.mark.polarion_id(""),
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "rbdplugin"],
            marks=pytest.mark.polarion_id(""),
        ),
    ],
)
class TestDeletePluginPod(ManageTest):
    """
    Test cases to verify the impact of plugin pod deletion on app pod
    """

    @pytest.fixture(autouse=True)
    def setup(self, interface, pvc_factory, pod_factory):
        """
        Create PVC and pod

        """
        self.pvc_obj = pvc_factory(
            interface=interface, size=3, status=constants.STATUS_BOUND
        )
        self.pod_obj = pod_factory(
            interface=interface, pvc=self.pvc_obj, status=constants.STATUS_RUNNING
        )

    def test_delete_plugin_pod(self, resource_to_delete):
        """
        Test case to verify the impact of plugin pod deletion on app pod

        """
        DISRUPTION_OPS.set_resource(resource=resource_to_delete)
        log.info(f"Deleting a {resource_to_delete} pod")
        DISRUPTION_OPS.delete_resource()
        log.info(
            f"Deleted {resource_to_delete} pod and new {resource_to_delete} reached Running state"
        )

        self.pod_obj.run_io(storage_type="fs", size="1G", runtime=20)
        log.info("FIO started on pod")
        log.info("Waiting for fio result")
        pod.get_fio_rw_iops(self.pod_obj)
        log.info("Fio completed")
