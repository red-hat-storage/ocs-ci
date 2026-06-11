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
        logger.info("Setting up test environment")
        self.project_name = "test"
        project_factory(project_name=self.project_name)
        logger.info(f"Created test project: {self.project_name}")

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
        logger.test_step(f"Select {node_type} node for reboot")
        node = get_nodes(node_type, num_of_nodes=1)
        logger.info(f"Selected node for reboot: {[n.name for n in node]}")

        logger.test_step("Pull and push images to registry")
        logger.info(f"Pulling and pushing images to project: {self.project_name}")
        image_pull_and_push(project_name=self.project_name)
        logger.info("Images pulled and pushed successfully")

        logger.test_step("Validate images exist in registry before reboot")
        validate_image_exists()
        logger.info("Images validated successfully in registry")

        logger.test_step(f"Reboot {node_type} node")
        logger.info(f"Rebooting node: {[n.name for n in node]}")
        nodes.restart_nodes(node, wait=False)

        logger.test_step("Wait for cluster connectivity and nodes to be Ready")
        retry(
            (CommandFailed, TimeoutError, AssertionError, ResourceWrongStatusException),
            tries=28,
            delay=15,
        )(wait_for_cluster_connectivity)(tries=400)
        logger.info("Cluster connectivity restored")

        retry(
            (CommandFailed, TimeoutError, AssertionError, ResourceWrongStatusException),
            tries=28,
            delay=15,
        )(wait_for_nodes_status)(timeout=900)
        logger.info("All nodes are in Ready state")

        logger.test_step("Verify cluster and Ceph health")
        self.sanity_helpers.health_check(tries=40)
        logger.info("Cluster and Ceph health checks passed")

        logger.test_step("Validate storage and registry pods are running")
        wait_for_storage_pods()
        logger.info("All storage pods are running")

        validate_registry_pod_status()
        logger.info("All registry pods are in Running state")

        logger.test_step("Validate images still exist in registry after reboot")
        validate_image_exists()
        logger.info("Images validated successfully in registry after node reboot")

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
        logger.info(
            f"Selected {len(node_list)} {node_type} node(s) for rolling reboot: {[n.name for n in node_list]}"
        )

        logger.test_step("Pull and push images to registry")
        logger.info(f"Pulling and pushing images to project: {self.project_name}")
        image_pull_and_push(project_name=self.project_name)
        logger.info("Images pulled and pushed successfully")

        logger.test_step("Validate images exist in registry before rolling reboot")
        validate_image_exists()
        logger.info("Images validated successfully in registry")

        logger.test_step(
            f"Perform rolling reboot of {len(node_list)} {node_type} node(s)"
        )
        for idx, node in enumerate(node_list, 1):
            logger.info(f"Rebooting node {idx}/{len(node_list)}: {node.name}")
            nodes.restart_nodes([node], wait=False)

            waiting_time = 40
            logger.debug(f"Waiting {waiting_time} seconds after rebooting {node.name}")
            time.sleep(waiting_time)

            logger.debug(
                f"Waiting for cluster connectivity after rebooting {node.name}"
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

            logger.debug(
                f"Waiting for all nodes to be Ready after rebooting {node.name}"
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
            logger.info(f"Node {node.name} rebooted successfully, cluster stabilized")

        logger.info(
            f"Completed rolling reboot of all {len(node_list)} {node_type} node(s)"
        )

        logger.test_step("Verify cluster and Ceph health after rolling reboot")
        self.sanity_helpers.health_check(tries=40)
        logger.info("Cluster and Ceph health checks passed")

        logger.test_step("Validate storage and registry pods are running")
        wait_for_storage_pods()
        logger.info("All storage pods are running")

        validate_registry_pod_status()
        logger.info("All registry pods are in Running state")

        logger.test_step("Validate images still exist in registry after rolling reboot")
        validate_image_exists()
        logger.info("Images validated successfully in registry after rolling reboot")
