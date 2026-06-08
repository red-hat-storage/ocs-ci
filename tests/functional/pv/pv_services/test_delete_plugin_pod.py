import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import ManageTest, tier4c
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pod
from ocs_ci.helpers import disruption_helpers


logger = logging.getLogger(__name__)

DISRUPTION_OPS = disruption_helpers.Disruptions()


@green_squad
@tier4c
@pytest.mark.parametrize(
    argnames=["interface", "resource_to_delete"],
    argvalues=[
        pytest.param(
            *[constants.CEPHFILESYSTEM, "cephfsplugin"],
            marks=pytest.mark.polarion_id("OCS-2550"),
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "rbdplugin"],
            marks=pytest.mark.polarion_id("OCS-2551"),
        ),
    ],
)
class TestDeletePluginPod(ManageTest):
    """
    Test cases to verify the impact of plugin pod deletion on app pod
    """

    @pytest.fixture(autouse=True)
    def setup(self, interface, pod_factory):
        """
        Create PVC and pod

        """
        self.pod_obj = pod_factory(
            interface=interface,
            pvc=None,
            status=constants.STATUS_RUNNING,
        )

    def test_delete_plugin_pod(self, resource_to_delete):
        """
        Test case to verify the impact of plugin pod deletion on app pod.
        Verifies bug 1970352.
        """
        logger.test_step(
            f"Select {resource_to_delete} pod running on same node as app pod"
        )
        resource_id = None

        DISRUPTION_OPS.set_resource(resource=resource_to_delete)
        pod_node = self.pod_obj.get_node()

        logger.info(
            f"Selecting {resource_to_delete} pod which is running on the same "
            f"node as that of the app pod"
        )
        for index, res_obj in enumerate(DISRUPTION_OPS.resource_obj):
            if res_obj.get_node() == pod_node:
                resource_id = index
                logger.info(f"Selected the pod {res_obj.name}")
                break
        logger.assertion(
            f"{resource_to_delete} pod found on node {pod_node}: expected=not None, actual={resource_id}"
        )
        assert (
            resource_id is not None
        ), f"No {resource_to_delete} pod is running on the node {pod_node}"

        logger.test_step(f"Delete {resource_to_delete} plugin pod and verify recovery")
        logger.info(
            f"Deleting the pod {DISRUPTION_OPS.resource_obj[resource_id].name}"
            f" which is running on the node {pod_node}"
        )
        DISRUPTION_OPS.delete_resource(resource_id=resource_id)
        logger.info(
            f"Deleted {DISRUPTION_OPS.resource_obj[resource_id].name} pod and "
            f"new {resource_to_delete} pod reached Running state"
        )

        logger.test_step(
            "Run IO on app pod to verify it is unaffected by plugin pod deletion"
        )
        # Run IO
        self.pod_obj.run_io(storage_type="fs", size="1G", runtime=20)
        logger.info("FIO started on pod")
        logger.info("Waiting for FIO result")
        pod.get_fio_rw_iops(self.pod_obj)
        logger.info("Fio completed on pod")
