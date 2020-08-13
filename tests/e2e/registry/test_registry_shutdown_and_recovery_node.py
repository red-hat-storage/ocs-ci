import pytest
import logging
import time

from ocs_ci.ocs.constants import OPENSHIFT_IMAGE_REGISTRY_NAMESPACE
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.registry import (
    validate_registry_pod_status,
    image_pull, image_push, image_list_all, image_rm,
    check_image_exists_in_registry
)
from ocs_ci.framework.testlib import E2ETest, workloads, ignore_leftovers
from ocs_ci.utility.svt import svt_setup, svt_cleanup
from ocs_ci.ocs.node import (
    wait_for_nodes_status, get_typed_nodes
)
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import CommandFailed, ResourceWrongStatusException
from tests.sanity_helpers import Sanity

log = logging.getLogger(__name__)
IMAGE_URL = ['docker.io/library/busybox', 'docker.io/library/nginx']


def validate_registry_pod_and_run_workload(iterations, image_url):
    """
    Validates registry pod is running and
    performs pull and push images to registry

    """

    # Validate image registry pods
    validate_registry_pod_status()

    # Start SVT workload for pushing images to registry
    svt_setup(iterations=iterations)

    # Image pull and push to registry
    image_pull(image_url=image_url)
    image_path = image_push(
        image_url=image_url, namespace=OPENSHIFT_IMAGE_REGISTRY_NAMESPACE
    )

    # List the images in registry
    img_list = image_list_all()
    log.info(f"Image list {img_list}")

    # Check either image present in registry or not
    validate = check_image_exists_in_registry(image_url=image_url)
    if not validate:
        raise UnexpectedBehaviour("Image URL not present in registry")

    return image_path


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
    def teardown(self, request, nodes):
        """
        Clean up svt

        """
        self.image_path = []

        def finalizer():

            # Make sure all VMs are up by the end of the test
            nodes.restart_nodes_by_stop_and_start_teardown()

            # Remove images from registry
            log.info("Remove image from registry")
            for img_path, img_url in zip(self.image_path, IMAGE_URL):
                image_rm(registry_path=img_path, image_url=img_url)

            # svt workload cleanup
            log.info("Calling svt cleanup")
            assert svt_cleanup(), "Failed to cleanup svt"

        request.addfinalizer(finalizer)

    @pytest.mark.parametrize(
        argnames=["iterations"],
        argvalues=[
            pytest.param(
                *[5], marks=pytest.mark.polarion_id("OCS-1800")
            )
        ]
    )
    def test_registry_shutdown_and_recovery_node(self, iterations, nodes):
        """
        Test registry workload when backed by OCS and
        its impact when node is shutdown and recovered

        """

        # Get the node list
        node_list = get_typed_nodes(node_type='worker')

        for node in node_list:

            # Stop node
            nodes.stop_nodes(nodes=[node])

            waiting_time = 20
            log.info(f"Waiting for {waiting_time} seconds")
            time.sleep(waiting_time)

            self.image_path.append(
                validate_registry_pod_and_run_workload(iterations, IMAGE_URL[0])
            )

            # Start node
            nodes.start_nodes(nodes=[node])

            # Validate all nodes are in READY state and up
            retry(
                (CommandFailed, TimeoutError, AssertionError, ResourceWrongStatusException),
                tries=30,
                delay=15)(
                wait_for_nodes_status(timeout=1800)
            )

            self.image_path.append(
                validate_registry_pod_and_run_workload(iterations, IMAGE_URL[1])
            )

        # Validate cluster health ok and all pods are running
        self.sanity_helpers.health_check()
