import time
import pytest
import logging

from ocs_ci.ocs.constants import (
    MASTER_MACHINE,
    WORKER_MACHINE,
)
from ocs_ci.ocs.ocp import wait_for_cluster_connectivity
from ocs_ci.ocs.registry import (
    validate_registry_pod_status,
    image_pull_and_push,
    validate_image_exists,
)
from ocs_ci.ocs.node import wait_for_nodes_status, get_nodes
from ocs_ci.ocs.resources.pod import wait_for_storage_pods
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import CommandFailed, ResourceWrongStatusException
from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import E2ETest, workloads, ignore_leftovers
from ocs_ci.helpers.sanity_helpers import Sanity

logger = logging.getLogger(__name__)


@magenta_squad
@workloads
@ignore_leftovers
class TestRegistryRebootNode(E2ETest):
    """
    Test to run svt workload for pushing
    images to registry when node is rebooted
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
        Setup and clean up
        """

        self.project_name = "test"
        project_factory(project_name=self.project_name)

    @pytest.mark.parametrize(
        argnames=["node_type"],
        argvalues=[
            pytest.param(*[MASTER_MACHINE], marks=pytest.mark.polarion_id("OCS-1803")),
            pytest.param(*[WORKER_MACHINE], marks=pytest.mark.polarion_id("OCS-1795")),
        ],
    )
    def test_registry_reboot_node(self, node_type, nodes):
        """
        Test registry workload when backed by OCS and reboot node
        """

        logger.test_step(f"Get {node_type} node for reboot")
        node = get_nodes(node_type, num_of_nodes=1)
        logger.info(f"Selected node: {node[0].name}")

        logger.test_step("Pull and push images to registries")
        image_pull_and_push(project_name=self.project_name)

        logger.test_step("Validate image exists in registry before reboot")
        validate_image_exists()

        logger.test_step(f"Reboot {node_type} node {node[0].name}")
        nodes.restart_nodes(node, wait=False)

        logger.test_step("Wait for cluster connectivity and node readiness")
        retry(
            (CommandFailed, TimeoutError, AssertionError, ResourceWrongStatusException),
            tries=28,
            delay=15,
            backoff=1,
        )(wait_for_cluster_connectivity)(tries=400)

        node_ready_timeout = 1800 if node_type == MASTER_MACHINE else 900
        logger.info(f"Waiting for nodes to be ready (timeout: {node_ready_timeout}s)")
        wait_for_nodes_status(timeout=node_ready_timeout)

        logger.test_step("Validate cluster health and storage pods")
        self.sanity_helpers.health_check(tries=40)
        wait_for_storage_pods()

        logger.test_step("Validate registry pods and image persistence after reboot")
        validate_registry_pod_status()
        validate_image_exists()
        logger.info("Registry image persisted after node reboot")

    @pytest.mark.parametrize(
        argnames=["node_type"],
        argvalues=[
            pytest.param(*[MASTER_MACHINE], marks=pytest.mark.polarion_id("OCS-1802")),
            pytest.param(*[WORKER_MACHINE], marks=pytest.mark.polarion_id("OCS-1804")),
        ],
    )
    def test_registry_rolling_reboot_node(self, node_type, nodes):
        """
        Test registry workload when backed by OCS and reboot node one by one
        """

        logger.test_step(f"Get all {node_type} nodes for rolling reboot")
        node_list = get_nodes(node_type)
        logger.info(f"Found {len(node_list)} {node_type} nodes")

        logger.test_step("Pull and push images to registries")
        image_pull_and_push(project_name=self.project_name)

        logger.test_step("Validate image exists in registry before rolling reboot")
        validate_image_exists()

        logger.test_step(f"Rolling reboot of {len(node_list)} {node_type} nodes")
        for i, node in enumerate(node_list, 1):
            logger.info(f"Rebooting node {i}/{len(node_list)}: {node.name}")
            nodes.restart_nodes([node], wait=False)

            waiting_time = 40
            logger.debug(f"Waiting {waiting_time}s after reboot of {node.name}")
            time.sleep(waiting_time)

            logger.info(
                f"Waiting for cluster connectivity and node readiness "
                f"after rebooting {node.name}"
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
            )(wait_for_cluster_connectivity)(tries=400)
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
            logger.info(f"Node {node.name} recovered successfully")

        logger.test_step("Validate cluster health and storage pods")
        self.sanity_helpers.health_check(tries=40)
        wait_for_storage_pods()

        logger.test_step(
            "Validate registry pods and image persistence after rolling reboot"
        )
        validate_registry_pod_status()
        validate_image_exists()
        logger.info("Registry image persisted after rolling node reboot")
