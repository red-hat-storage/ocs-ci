import logging
from itertools import cycle
import pytest

from ocs_ci.framework.testlib import ManageTest, tier4, tier4a
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import get_worker_nodes
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
    def setup(self, interface, multi_pvc_factory, pod_factory):
        """
        Create PVC and pod

        """
        self.pvc_objs = multi_pvc_factory(
            interface=interface, size=3, status=constants.STATUS_BOUND, num_of_pvc=3
        )

        nodes_iter = cycle(get_worker_nodes())
        self.pod_objs = []
        for pvc_obj in self.pvc_objs:
            pod_obj = pod_factory(
                interface=interface,
                pvc=pvc_obj,
                node_name=next(nodes_iter),
                status=constants.STATUS_RUNNING,
            )
            self.pod_objs.append(pod_obj)

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

        for pod_obj in self.pod_objs:
            pod_obj.run_io(storage_type="fs", size="1G", runtime=20)
        log.info("FIO started on all pod")
        log.info("Waiting for fio results")
        for pod_obj in self.pod_objs:
            pod.get_fio_rw_iops(pod_obj)
        log.info("Fio completed")
