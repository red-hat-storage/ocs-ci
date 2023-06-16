import pytest
import logging

from ocs_ci.framework.testlib import (
    E2ETest,
    workloads,
    ignore_leftovers,
    skipif_ibm_cloud,
)
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed, ResourceWrongStatusException
from ocs_ci.ocs.node import wait_for_nodes_status, get_nodes
from ocs_ci.ocs.resources.pod import wait_for_storage_pods
from ocs_ci.ocs.registry import (
    validate_registry_pod_status,
    image_pull_and_push,
    validate_image_exists,
)
from ocs_ci.utility.retry import retry

log = logging.getLogger(__name__)


@workloads
@ignore_leftovers
class TestRegistryShutdownAndRecoveryNode(E2ETest):
    """
    Test to shutdown and recovery node and
    its impact on registry
    """

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    @pytest.fixture(autouse=True)
    def setup(self, project_factory, node_restart_teardown):
        """
        Setup and clean up the namespace
        """

        self.project_name = "test"
        project_factory(project_name=self.project_name)

    @pytest.mark.polarion_id("OCS-1800")
    @skipif_ibm_cloud
    def test_registry_shutdown_and_recovery_node(self, nodes):
        """
        Test registry workload when backed by OCS and
        its impact when node is shutdown and recovered

        """

        # Pull and push images to registries
        log.info("Pull and push images to registries")
        image_pull_and_push(project_name=self.project_name)

        # Get the node list
        node_list = get_nodes(node_type="worker")

        for node in node_list:

            # Stop node
            nodes.stop_nodes(nodes=[node])

            # Validate node reached NotReady state
            wait_for_nodes_status(
                node_names=[node.name], status=constants.NODE_NOT_READY
            )

            # Start node
            nodes.start_nodes(nodes=[node])

            # Validate all nodes are in READY state and up
            retry(
                (
                    CommandFailed,
                    TimeoutError,
                    AssertionError,
                    ResourceWrongStatusException,
                ),
                tries=60,
                delay=15,
            )(wait_for_nodes_status)(timeout=900)

        # Validate all storage pods are running
        retry(CommandFailed)(wait_for_storage_pods)(timeout=900)

        # Validate cluster health ok and all pods are running
        self.sanity_helpers.health_check(tries=40)

        # Validate image registry pods
        validate_registry_pod_status()

        # Validate image exists in registries path
        validate_image_exists()
