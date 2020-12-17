import logging
import pytest
from ocs_ci.framework.testlib import (
    tier4a,
    E2ETest,
)
from ocs_ci.ocs import defaults
from ocs_ci.ocs.resources.pod import (
    get_all_pods,
    check_toleration_on_pods,
    delete_pods,
    validate_pods_are_respinned_and_running_state,
    wait_for_pods_to_be_running,
)
from ocs_ci.ocs.node import taint_nodes, untaint_nodes


logger = logging.getLogger(__name__)


@tier4a
@pytest.mark.polarion_id("OCS-2450")
class TestTaintAndTolerations(E2ETest):
    """
    Test to test taints and toleration
    """

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Make sure all nodes are untainted again

        """

        def finalizer():
            untaint_nodes()

        request.addfinalizer(finalizer)

    def test_toleration(self):
        """
        1. Check if nodes are tainted
        2. Taint ocs nodes if not tainted
        3. Check for tolerations on all pod
        4. Respin all ocs pods and check if it runs on ocs nodes
        5. Untaint nodes

        """
        # taint nodes if not already tainted
        taint_nodes()

        # Check tolerations on pods under openshift-storage
        check_toleration_on_pods()

        # Respin all pods and check it if is still running
        pod_list = get_all_pods(namespace=defaults.ROOK_CLUSTER_NAMESPACE)
        assert delete_pods(pod_list), "Failed to delete pods"
        assert wait_for_pods_to_be_running()
        assert validate_pods_are_respinned_and_running_state(
            pod_list
        ), "Pod not respinned"
