import logging
import pytest
import random
import time


from ocs_ci.framework.pytest_customization.marks import brown_squad
from ocs_ci.framework.testlib import (
    tier4a,
    tier4b,
    ignore_leftovers,
    ManageTest,
    provider_client_platform_required,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.constants import HCI_PROVIDER
from ocs_ci.ocs.node import (
    get_node_objs,
    recover_node_to_ready_state,
    wait_for_nodes_status,
    get_nodes,
    drain_nodes,
    schedule_nodes,
)
from ocs_ci.helpers.sanity_helpers import SanityProviderMode
from ocs_ci.ocs.cluster import (
    ceph_health_check,
)
from ocs_ci.framework import config
from ocs_ci.utility.utils import switch_to_correct_cluster_at_setup

logger = logging.getLogger(__name__)


def check_drain_and_unschedule_node(ocp_node):
    """
    Drain and unschedule a node

    Args:
        ocp_node (OCS): The node object

    Raises:
        ResourceWrongStatusException: In case the node didn't reach the desired state

    """
    drain_nodes([ocp_node.name])
    # Wait for the node to be unschedule
    wait_for_nodes_status(
        node_names=[ocp_node.name],
        status=constants.NODE_READY_SCHEDULING_DISABLED,
        timeout=120,
        sleep=5,
    )

    wait_time_before_reschedule = 30
    logger.info(
        f"Wait {wait_time_before_reschedule} seconds before rescheduling the node"
    )
    time.sleep(wait_time_before_reschedule)

    schedule_nodes([ocp_node.name])
    wait_for_nodes_status(
        node_names=[ocp_node.name],
        status=constants.NODE_READY,
        timeout=120,
        sleep=5,
    )
    logger.info("Checking that the Ceph health is OK")
    ceph_health_check()


@brown_squad
@ignore_leftovers
@provider_client_platform_required
class TestNodesMaintenanceProviderMode(ManageTest):
    """
    Test nodes maintenance scenarios when using a Provider mode
    """

    @pytest.fixture(autouse=True)
    def setup(self, request, create_scale_pods_and_pvcs_using_kube_job_on_hci_clients):
        """
        1. Save the original index
        2. Switch to the correct cluster index
        3. Initialize the Sanity instance

        """
        self.orig_index = config.cur_index
        switch_to_correct_cluster_at_setup(request)
        self.sanity_helpers = SanityProviderMode(
            create_scale_pods_and_pvcs_using_kube_job_on_hci_clients
        )

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        1. Make sure all nodes are up again
        2. Switch to the original cluster index
        3. Check the Ceph health

        """

        def finalizer():
            ocp_nodes = get_node_objs()
            for n in ocp_nodes:
                recover_node_to_ready_state(n)

            logger.info("Switch to the original cluster index")
            config.switch_ctx(self.orig_index)
            ceph_health_check()

        request.addfinalizer(finalizer)

    @tier4a
    @pytest.mark.parametrize(
        argnames=["cluster_type", "node_type"],
        argvalues=[
            pytest.param(
                *[HCI_PROVIDER, constants.WORKER_MACHINE],
                marks=pytest.mark.polarion_id("OCS-5461"),
            ),
            pytest.param(
                *[HCI_PROVIDER, constants.MASTER_MACHINE],
                marks=pytest.mark.polarion_id("OCS-5462"),
            ),
        ],
    )
    def test_node_maintenance(self, cluster_type, node_type):
        """
        Test node maintenance

        """
        ocp_nodes = get_nodes(node_type=node_type)
        ocp_node = random.choice(ocp_nodes)
        check_drain_and_unschedule_node(ocp_node)
        logger.info(
            "Check basic cluster functionality by creating resources, run IO, "
            "and deleting the resources"
        )
        self.sanity_helpers.create_resources_on_clients()
        self.sanity_helpers.delete_resources()
        logger.info("Check the cluster health")
        self.sanity_helpers.health_check_provider_mode()

    @tier4b
    @pytest.mark.polarion_id("OCS-5466")
    @pytest.mark.parametrize(
        argnames=["cluster_type", "node_type"],
        argvalues=[
            pytest.param(*[HCI_PROVIDER, constants.WORKER_MACHINE]),
        ],
    )
    def test_rolling_nodes_maintenance(self, cluster_type, node_type):
        """
        Test maintenance nodes one after the other and check health status in between

        """
        ocp_nodes = get_nodes(node_type=node_type)
        for ocp_node in ocp_nodes:
            check_drain_and_unschedule_node(ocp_node)

        logger.info(
            "Check basic cluster functionality by creating resources, run IO, "
            "and deleting the resources"
        )
        self.sanity_helpers.create_resources_on_clients()
        self.sanity_helpers.delete_resources()
        logger.info("Check the cluster health")
        self.sanity_helpers.health_check_provider_mode()
