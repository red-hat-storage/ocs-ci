import time
import pytest
import logging

from ocs_ci.ocs.constants import (
    OPENSHIFT_IMAGE_REGISTRY_NAMESPACE, MASTER_MACHINE, WORKER_MACHINE
)
from ocs_ci.ocs.ocp import wait_for_cluster_connectivity
from ocs_ci.ocs.registry import (
    validate_registry_pod_status,
    image_pull, image_push, image_list_all, image_rm,
    check_image_exists_in_registry
)
from ocs_ci.ocs.node import (
    wait_for_nodes_status, get_typed_nodes
)
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import CommandFailed, ResourceWrongStatusException
from ocs_ci.framework.testlib import E2ETest, workloads, ignore_leftovers
from ocs_ci.utility.svt import svt_setup, svt_cleanup
from tests.sanity_helpers import Sanity

log = logging.getLogger(__name__)
IMAGE_URL = 'docker.io/library/busybox'


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
    def teardown(self, request, nodes):
        """
        Remove images and clean up svt

        """
        self.image_path = None

        def finalizer():
            nodes.restart_nodes_by_stop_and_start_teardown()
            log.info("Remove image from registry")
            image_rm(registry_path=self.image_path, image_url=IMAGE_URL)
            log.info("Calling svt cleanup")
            assert svt_cleanup(), "Failed to cleanup svt"
        request.addfinalizer(finalizer)

    @pytest.mark.parametrize(
        argnames=['node_type', 'iterations'],
        argvalues=[
            pytest.param(
                *[MASTER_MACHINE, 5], marks=pytest.mark.polarion_id("OCS-1803")
            ),
            pytest.param(
                *[WORKER_MACHINE, 5], marks=pytest.mark.polarion_id("OCS-1795")
            ),
        ]
    )
    def test_registry_reboot_node(self, node_type, iterations, nodes):
        """
        Test registry workload when backed by OCS and reboot node
        """

        # Get the node list
        node = get_typed_nodes(node_type, num_of_nodes=1)

        # Reboot one node
        nodes.restart_nodes(node)

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

        # Start SVT workload for pushing images to registry
        svt_setup(iterations=iterations)

        # Image pull and push to registry
        image_pull(image_url=IMAGE_URL)
        self.image_path = image_push(
            image_url=IMAGE_URL, namespace=OPENSHIFT_IMAGE_REGISTRY_NAMESPACE
        )

        # List the images in registry
        img_list = image_list_all()
        log.info(f"Image list {img_list}")

        # Check either image present in registry or not
        assert check_image_exists_in_registry(image_url=IMAGE_URL), (
            "Image URL not present in registry"
        )

        # Validate cluster health ok and all pods are running
        self.sanity_helpers.health_check()

    @pytest.mark.parametrize(
        argnames=['node_type', 'iterations'],
        argvalues=[
            pytest.param(
                *[MASTER_MACHINE, 5], marks=pytest.mark.polarion_id("OCS-1802")
            ),
            pytest.param(
                *[WORKER_MACHINE, 5], marks=pytest.mark.polarion_id("OCS-1804")
            ),
        ]
    )
    def test_registry_rolling_reboot_node(self, node_type, iterations, nodes):
        """
        Test registry workload when backed by OCS and reboot node one by one
        """

        # Get the node list
        node_list = get_typed_nodes(node_type)

        for node in node_list:

            # Reboot node
            log.info(node)
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

            # Start SVT workload for pushing images to registry
            svt_setup(iterations=iterations)

            # Image pull and push to registry
            image_pull(image_url=IMAGE_URL)
            self.image_path = image_push(
                image_url=IMAGE_URL, namespace=OPENSHIFT_IMAGE_REGISTRY_NAMESPACE
            )

            # List the images in registry
            img_list = image_list_all()
            log.info(f"Image list {img_list}")

            # Check either image present in registry or not
            assert check_image_exists_in_registry(image_url=IMAGE_URL), (
                "Image URL not present in registry"
            )

        # Validate cluster health ok and all pods are running
        self.sanity_helpers.health_check()
