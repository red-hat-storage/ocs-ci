import pytest
import logging

from ocs_ci.ocs import constants
from ocs_ci.ocs.registry import (
    validate_registry_pod_status, image_pull_and_push,
    validate_image_exists
)
from ocs_ci.framework.testlib import E2ETest, workloads, ignore_leftovers
from ocs_ci.ocs.node import (
    wait_for_nodes_status, get_typed_nodes
)
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import CommandFailed, ResourceWrongStatusException
from tests.sanity_helpers import Sanity

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
    def setup(self, request, project_factory_class, nodes):
        """
        Setup and clean up the namespace
        """

        self.project_name = 'test'
        project_factory_class(project_name=self.project_name)

        def finalizer():
            log.info("Validate all nodes are in Ready state, if not restart nodes")
            nodes.restart_nodes_by_stop_and_start_teardown()

        request.addfinalizer(finalizer)

    @pytest.mark.polarion_id("OCS-1800")
    def test_registry_shutdown_and_recovery_node(self, nodes):
        """
        Test registry workload when backed by OCS and
        its impact when node is shutdown and recovered

        """

        # Pull and push images to registries
        log.info("Pull and push images to registries")
        image_pull_and_push(
            project_name=self.project_name, template='eap-cd-basic-s2i',
            image='registry.redhat.io/jboss-eap-7-tech-preview/eap-cd-openshift-rhel8:latest',
            pattern='eap-app'
        )

        # Get the node list
        node_list = get_typed_nodes(node_type='worker')

        for node in node_list:

            # Stop node
            nodes.stop_nodes(nodes=[node])

            # Validate node reached NotReady state
            wait_for_nodes_status(node_names=[node.name], status=constants.NODE_NOT_READY)

            # Validate image exists in registries path
            validate_image_exists(namespace=self.project_name)

            # Start node
            nodes.start_nodes(nodes=[node])

            # Validate all nodes are in READY state and up
            retry(
                (CommandFailed, TimeoutError, AssertionError, ResourceWrongStatusException),
                tries=30,
                delay=15)(
                wait_for_nodes_status(timeout=900)
            )

            # Validate image exists in registries path
            validate_image_exists(namespace=self.project_name)

        # Validate image registry pods
        validate_registry_pod_status()

        # Validate cluster health ok and all pods are running
        self.sanity_helpers.health_check()
