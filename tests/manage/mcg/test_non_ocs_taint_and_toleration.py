import logging
import pytest
import time

from ocs_ci.ocs import ocp, constants, defaults

from ocs_ci.framework.testlib import (
    E2ETest,
    ignore_leftovers,
    skipif_tainted_nodes,
    skipif_managed_service,
)
from ocs_ci.ocs.resources.pod import (
    get_all_pods,
    wait_for_pods_to_be_running,
    check_toleration_on_pods,
)
from ocs_ci.ocs.node import (
    taint_nodes,
    untaint_nodes,
    get_worker_nodes,
)
from ocs_ci.framework.pytest_customization.marks import bugzilla
from ocs_ci.helpers.sanity_helpers import Sanity

logger = logging.getLogger(__name__)


@ignore_leftovers
@skipif_tainted_nodes
@skipif_managed_service
@bugzilla("2115613")
class TestNonOCSTaintAndTolerations(E2ETest):
    """
    Test to test non ocs taints on ocs nodes
    and toleration
    """

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Make sure all nodes are untainted

        """

        def finalizer():
            assert untaint_nodes(
                taint_label="xyz=true:NoSchedule",
            ), "Failed to untaint"

        request.addfinalizer(finalizer)

    def test_non_ocs_taint_and_tolerations(self):
        """
        Test runs the following steps
        1. Taint ocs nodes with non-ocs taint
        2. Set tolerations on storagecluster, subscription.
        3. Force delete all pods
        4. Check toleration on all ocs pods
        """

        # Taint all nodes with non-ocs taint
        ocs_nodes = get_worker_nodes()
        taint_nodes(nodes=ocs_nodes, taint_label="xyz=true:NoSchedule")

        # Add tolerations to the storagecluster
        storagecluster_obj = ocp.OCP(
            resource_name=constants.DEFAULT_CLUSTERNAME,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            kind=constants.STORAGECLUSTER,
        )
        param = (
            '{"spec": {"placement": {"noobaa-standalone": {"tolerations": '
            '[{"effect": "NoSchedule", "key": "xyz", "operator": "Equal", '
            '"value": "true"}]}}}}'
        )
        storagecluster_obj.patch(params=param, format_type="merge")
        logger.info(f"Successfully added toleration to {storagecluster_obj.kind}")

        # Add tolerations to the subscription
        sub_list = ocp.get_all_resource_names_of_a_kind(kind=constants.SUBSCRIPTION)
        param = (
            '{"spec": {"config":  {"tolerations": '
            '[{"effect": "NoSchedule", "key": "xyz", "operator": "Equal", '
            '"value": "true"}]}}}'
        )
        for sub in sub_list:
            sub_obj = ocp.OCP(
                resource_name=sub,
                namespace=defaults.ROOK_CLUSTER_NAMESPACE,
                kind=constants.SUBSCRIPTION,
            )
            sub_obj.patch(params=param, format_type="merge")
            logger.info(f"Successfully added toleration to {sub}")

        # Wait some time after adding toleration.
        waiting_time = 60
        logger.info(f"Waiting {waiting_time} seconds...")
        time.sleep(waiting_time)

        # After edit noticed few pod remain in pending state.Force delete all pods.
        pod_list = get_all_pods(
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            exclude_selector=True,
        )
        for pod in pod_list:
            pod.delete(wait=False)

        assert wait_for_pods_to_be_running(
            timeout=600, sleep=15
        ), "Pod didn't reach to running state"

        # Check non ocs toleration on all pods under openshift-storage
        check_toleration_on_pods(toleration_key="xyz")
        self.sanity_helpers.health_check()
