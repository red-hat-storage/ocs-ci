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

log = logging.getLogger(__name__)


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
        log.debug("Initializing Sanity instance for registry shutdown/recovery test")
        self.sanity_helpers = Sanity()
        log.debug("Sanity instance initialized successfully")

    @pytest.fixture(autouse=True)
    def setup(self, project_factory, node_restart_teardown):
        """
        Setup and clean up the namespace
        """
        self.project_name = "test"
        log.info(f"Setting up test project: '{self.project_name}'")
        project_factory(project_name=self.project_name)
        log.info(f"Test project '{self.project_name}' created successfully")

    @pytest.mark.polarion_id("OCS-1800")
    @skipif_ibm_cloud
    def test_registry_shutdown_and_recovery_node(self, nodes):
        """
        Test registry workload when backed by OCS and
        its impact when node is shutdown and recovered

        """
        log.info(
            "Starting test: OCS-1800 — registry workload resilience during "
            "node shutdown and recovery"
        )

        # Pull and push images to registries
        log.info(
            f"Step 1: Pulling and pushing images to registry "
            f"in project '{self.project_name}'"
        )
        image_pull_and_push(project_name=self.project_name)
        log.info("Image pull and push to registry completed successfully")

        # Get the node list
        log.info("Step 2: Retrieving worker node list for shutdown/recovery cycle")
        node_list = get_nodes(node_type="worker")
        node_names = [n.name for n in node_list]
        log.info(f"Found {len(node_list)} worker node(s) to cycle: {node_names}")

        for index, node in enumerate(node_list, start=1):
            log.info(f"--- Node cycle {index}/{len(node_list)}: '{node.name}' ---")

            # Stop node
            log.info(f"Stopping node '{node.name}'")
            nodes.stop_nodes(nodes=[node])
            log.info(f"Stop command issued for node '{node.name}'")

            # Validate node reached NotReady state
            log.info(
                f"Waiting for node '{node.name}' to reach "
                f"'{constants.NODE_NOT_READY}' state"
            )
            wait_for_nodes_status(
                node_names=[node.name], status=constants.NODE_NOT_READY
            )
            log.info(
                f"Node '{node.name}' confirmed in '{constants.NODE_NOT_READY}' state"
            )

            # Start node
            log.info(f"Starting node '{node.name}'")
            nodes.start_nodes(nodes=[node], wait_time=300)
            log.info(f"Start command issued for node '{node.name}'")

            # Validate all nodes are in READY state and up
            log.info(
                f"Waiting for all nodes to return to '{constants.NODE_READY}' "
                f"state after recovering '{node.name}' "
                f"(up to 28 retries, 15s delay, 900s timeout)"
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
            log.info(
                f"All nodes are in '{constants.NODE_READY}' state after "
                f"recovering '{node.name}'"
            )

        log.info(
            f"Completed shutdown/recovery cycle for all "
            f"{len(node_list)} worker node(s): {node_names}"
        )

        # Validate all storage pods are running
        log.info(
            "Step 3: Validating all storage pods are running "
            "(up to retry, 900s timeout)"
        )
        retry(CommandFailed)(wait_for_storage_pods)(timeout=900)
        log.info("All storage pods are in Running state")

        # Validate cluster health ok and all pods are running
        log.info("Step 4: Running cluster health check (tries=40)")
        self.sanity_helpers.health_check(tries=40)
        log.info("Cluster health check passed — cluster is healthy")

        # Validate image registry pods
        log.info("Step 5: Validating image registry pod status")
        validate_registry_pod_status()
        log.info("Image registry pods are in expected Running state")

        # Validate image exists in registries path
        log.info("Step 6: Validating previously pushed image exists in registry path")
        validate_image_exists()
        log.info(
            "Image verified in registry path — test OCS-1800 completed successfully"
        )
