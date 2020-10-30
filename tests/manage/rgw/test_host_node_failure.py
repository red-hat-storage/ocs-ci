import logging
import pytest

from ocs_ci.ocs.resources.pod import (
    get_rgw_pods, get_pod_node, get_noobaa_pods, wait_for_storage_pods
)
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework.testlib import (
    ManageTest, tier4a
)
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.helpers.helpers import wait_for_resource_state
from ocs_ci.ocs.node import get_node_objs

log = logging.getLogger(__name__)


@tier4a
@pytest.mark.polarion_id("OCS-2374")
@pytest.mark.bugzilla('1852983')
class TestRGWAndNoobaaDBHostNodeFailure(ManageTest):
    """
    Test to verify fail node hosting
    RGW pods and Noobaa-db pods and its impact

    """

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    @pytest.fixture(autouse=True)
    def teardown(self, request, nodes):
        """
        Make sure all nodes are up again

        """

        def finalizer():
            nodes.restart_nodes_by_stop_and_start_teardown()

        request.addfinalizer(finalizer)

    def test_rgw_host_node_failure(self, nodes):
        """
        Test case to fail node where RGW and Noobaa-db-0 hosting
        and verify new pod spuns on healthy node

        """
        # Get rgw pods
        rgw_pod_obj = get_rgw_pods()

        # Get nooba pods
        noobaa_pod_obj = get_noobaa_pods()

        # Get the node where noobaa-db hosted
        for noobaa_pod in noobaa_pod_obj:
            if noobaa_pod.name == 'noobaa-db-0':
                noobaa_pod_node = get_pod_node(noobaa_pod)

        for rgw_pod in rgw_pod_obj:
            pod_node = rgw_pod.get().get('spec').get('nodeName')
            if pod_node == noobaa_pod_node.name:
                # Stop the node
                log.info(f"Stopping node {pod_node} where"
                         f" rgw pod {rgw_pod.name} and noobaa-db-0 hosted")
                node_obj = get_node_objs(node_names=[pod_node])
                nodes.stop_nodes(node_obj)

                # Validate old rgw pod went terminating state
                wait_for_resource_state(
                    resource=rgw_pod, state=constants.STATUS_TERMINATING,
                    timeout=720
                )

                # Validate new rgw pod spun
                ocp_obj = OCP(kind=constants.POD, namespace=defaults.ROOK_CLUSTER_NAMESPACE)
                ocp_obj.wait_for_resource(
                    condition=constants.STATUS_RUNNING,
                    resource_count=len(rgw_pod_obj),
                    selector=constants.RGW_APP_LABEL
                )

                # Start the node
                nodes.start_nodes(node_obj)

        # Verify cluster health
        self.sanity_helpers.health_check()

        # Verify all storage pods are running
        wait_for_storage_pods()
