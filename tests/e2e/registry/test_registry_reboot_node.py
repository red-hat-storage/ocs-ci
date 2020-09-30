import time
import pytest
import logging

from ocs_ci.ocs.constants import (
    MASTER_MACHINE, WORKER_MACHINE,
)
from ocs_ci.ocs.ocp import wait_for_cluster_connectivity
from ocs_ci.ocs.registry import (
    validate_registry_pod_status, image_pull_and_push,
    validate_image_exists
)
from ocs_ci.ocs.node import (
    wait_for_nodes_status, get_typed_nodes
)
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import CommandFailed, ResourceWrongStatusException
from ocs_ci.framework.testlib import E2ETest, workloads, ignore_leftovers
from tests.sanity_helpers import Sanity

log = logging.getLogger(__name__)


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

    @pytest.mark.parametrize(
        argnames=['node_type'],
        argvalues=[
            pytest.param(
                *[MASTER_MACHINE], marks=pytest.mark.polarion_id("OCS-1803")
            ),
            pytest.param(
                *[WORKER_MACHINE], marks=pytest.mark.polarion_id("OCS-1795")
            ),
        ]
    )
    def test_registry_reboot_node(self, node_type, nodes):
        """
        Test registry workload when backed by OCS and reboot node
        """

        # Get the node list
        node = get_typed_nodes(node_type, num_of_nodes=1)

        # Pull and push images to registries
        log.info("Pull and push images to registries")
        image_pull_and_push(
            project_name=self.project_name, template='eap-cd-basic-s2i',
            image='registry.redhat.io/jboss-eap-7-tech-preview/eap-cd-openshift-rhel8:latest',
            pattern='eap-app'
        )

        # Validate image exists in registries path
        validate_image_exists(namespace=self.project_name)

        # Reboot one node
        nodes.restart_nodes(node, wait=False)

        # Validate all nodes and services are in READY state and up
        retry(
            (CommandFailed, TimeoutError, AssertionError, ResourceWrongStatusException),
            tries=60,
            delay=15)(
            wait_for_cluster_connectivity(tries=400)
        )
        retry(
            (CommandFailed, TimeoutError, AssertionError, ResourceWrongStatusException),
            tries=60,
            delay=15)(
            wait_for_nodes_status(timeout=900)
        )

        # Validate image registry pods
        validate_registry_pod_status()

        # Validate image exists in registries path
        validate_image_exists(namespace=self.project_name)

        # Validate cluster health ok and all pods are running
        self.sanity_helpers.health_check()

    @pytest.mark.parametrize(
        argnames=['node_type'],
        argvalues=[
            pytest.param(
                *[MASTER_MACHINE], marks=pytest.mark.polarion_id("OCS-1802")
            ),
            pytest.param(
                *[WORKER_MACHINE], marks=pytest.mark.polarion_id("OCS-1804")
            ),
        ]
    )
    def test_registry_rolling_reboot_node(self, node_type, nodes):
        """
        Test registry workload when backed by OCS and reboot node one by one
        """

        # Get the node list
        node_list = get_typed_nodes(node_type)

        # Pull and push images to registries
        log.info("Pull and push images to registries")
        image_pull_and_push(
            project_name=self.project_name, template='eap-cd-basic-s2i',
            image='registry.redhat.io/jboss-eap-7-tech-preview/eap-cd-openshift-rhel8:latest',
            pattern='eap-app'
        )

        # Validate image exists in registries path
        validate_image_exists(namespace=self.project_name)

        for node in node_list:

            # Reboot node
            log.info(node.name)
            nodes.restart_nodes([node], wait=False)

            # Wait some time after rebooting node
            waiting_time = 40
            log.info(f"Waiting {waiting_time} seconds...")
            time.sleep(waiting_time)

            # Validate all nodes and services are in READY state and up
            retry(
                (CommandFailed, TimeoutError, AssertionError, ResourceWrongStatusException),
                tries=60,
                delay=15)(
                wait_for_cluster_connectivity(tries=400)
            )
            retry(
                (CommandFailed, TimeoutError, AssertionError, ResourceWrongStatusException),
                tries=60,
                delay=15)(
                wait_for_nodes_status(timeout=900)
            )

            # Validate image registry pods
            validate_registry_pod_status()

            # Validate image exists in registries path
            validate_image_exists(namespace=self.project_name)

        # Validate cluster health ok and all pods are running
        self.sanity_helpers.health_check()
