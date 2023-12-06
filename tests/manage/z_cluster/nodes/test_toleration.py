import logging
import pytest
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import brown_squad
from ocs_ci.framework.testlib import (
    tier4c,
    E2ETest,
    ignore_leftovers,
    skipif_managed_service,
    skipif_hci_provider_and_client,
)
from ocs_ci.ocs.resources.pod import (
    get_all_pods,
    check_toleration_on_pods,
    wait_for_pods_to_be_running,
)
from ocs_ci.ocs.node import (
    get_ocs_nodes,
    taint_nodes,
    untaint_nodes,
)


logger = logging.getLogger(__name__)


@brown_squad
@tier4c
@ignore_leftovers
@skipif_managed_service
@skipif_hci_provider_and_client
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
            assert untaint_nodes(), "Failed to untaint"

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
        nodes = get_ocs_nodes()
        for node in nodes:
            taint_nodes([node.name])

        # Check tolerations on pods under openshift-storage
        check_toleration_on_pods()

        # Respin all pods and check it if is still running
        pod_list = get_all_pods(namespace=config.ENV_DATA["cluster_namespace"])
        for pod in pod_list:
            if "s3cli" in pod.name:
                continue
            else:
                pod.delete(wait=False)
        assert wait_for_pods_to_be_running(timeout=360)
