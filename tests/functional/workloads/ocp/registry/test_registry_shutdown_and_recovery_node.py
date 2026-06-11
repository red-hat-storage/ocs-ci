import pytest
import logging

from ocs_ci.framework.pytest_customization.marks import magenta_squad
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

logger = logging.getLogger(__name__)


@magenta_squad
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
        logger.info("Setting up test environment")
        self.project_name = "test"
        project_factory(project_name=self.project_name)
        logger.info(f"Created test project: {self.project_name}")

    @pytest.mark.polarion_id("OCS-1800")
    @skipif_ibm_cloud
    def test_registry_shutdown_and_recovery_node(self, nodes):
        """
        Test registry workload when backed by OCS and
        its impact when node is shutdown and recovered

        """
        logger.test_step("Pull and push images to registry")
        logger.info(f"Pulling and pushing images to project: {self.project_name}")
        image_pull_and_push(project_name=self.project_name)
        logger.info("Images pulled and pushed successfully")

        logger.test_step("Get all worker nodes for shutdown and recovery")
        node_list = get_nodes(node_type="worker")
        logger.info(
            f"Selected {len(node_list)} worker node(s) for shutdown and recovery: {[n.name for n in node_list]}"
        )

        logger.test_step(
            f"Perform shutdown and recovery of {len(node_list)} worker node(s)"
        )
        for idx, node in enumerate(node_list, 1):
            logger.info(f"Shutting down node {idx}/{len(node_list)}: {node.name}")
            nodes.stop_nodes(nodes=[node])

            logger.debug(f"Waiting for node {node.name} to reach NotReady state")
            wait_for_nodes_status(
                node_names=[node.name], status=constants.NODE_NOT_READY
            )
            logger.info(f"Node {node.name} reached NotReady state")

            logger.info(f"Starting node {node.name}")
            nodes.start_nodes(nodes=[node])

            logger.debug(
                f"Waiting for all nodes to be Ready after recovering {node.name}"
            )
            retry(
                (
                    CommandFailed,
                    TimeoutError,
                    AssertionError,
                    ResourceWrongStatusException,
                ),
                tries=28,
                delay=15,
            )(wait_for_nodes_status)(timeout=900)
            logger.info(f"Node {node.name} recovered successfully, all nodes are Ready")

        logger.info(
            f"Completed shutdown and recovery of all {len(node_list)} worker node(s)"
        )

        logger.test_step("Wait for all storage pods to be running")
        retry(CommandFailed)(wait_for_storage_pods)(timeout=900)
        logger.info("All storage pods are running")

        logger.test_step("Verify cluster and Ceph health")
        self.sanity_helpers.health_check(tries=40)
        logger.info("Cluster and Ceph health checks passed")

        logger.test_step("Validate registry pods are running")
        validate_registry_pod_status()
        logger.info("All registry pods are in Running state")

        logger.test_step("Validate images exist in registry after node recovery")
        validate_image_exists()
        logger.info(
            "Images validated successfully in registry after shutdown and recovery"
        )
