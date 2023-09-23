import logging
import pytest

from ocs_ci.helpers import sanity_helpers
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs import constants
from ocs_ci.utility.utils import (
    enable_huge_pages,
    disable_huge_pages,
)
from ocs_ci.ocs.node import (
    wait_for_nodes_status,
    get_nodes,
)
from ocs_ci.ocs.resources.pod import (
    wait_for_pods_to_be_running,
)
from ocs_ci.framework.pytest_customization.marks import brown_squad
from ocs_ci.framework.testlib import (
    bugzilla,
    skipif_external_mode,
    skipif_ocs_version,
    ignore_leftovers,
    E2ETest,
    tier2,
)

log = logging.getLogger(__name__)


@brown_squad
@tier2
@skipif_ocs_version("<4.8")
@bugzilla("1995271")
@bugzilla("2001933")
@pytest.mark.polarion_id("OCS-2754")
@ignore_leftovers
class TestHugePages(E2ETest):
    """
    Enable huge pages post ODF installation

    """

    @pytest.fixture(scope="function", autouse=True)
    def huge_pages_setup(self, request):
        """
        Initializes sanity

        """
        self.sanity_helpers = Sanity()

        def finalizer():
            """
            Removes huge pages on worker nodes and verifies all pods are up

            """
            disable_huge_pages()

            wait_for_nodes_status(status=constants.NODE_READY, timeout=600)

            nodes = get_nodes()
            for node in nodes:
                assert (
                    node.get()["status"]["allocatable"]["hugepages-2Mi"] == "0"
                ), f"Huge pages is not applied on {node.name}"

            log.info("Wait for all pods to be in running state")
            wait_for_pods_to_be_running(timeout=600)
            sanity_helpers.ceph_health_check(tries=120)

        request.addfinalizer(finalizer)

    @skipif_external_mode
    def test_hugepages_post_odf_deployment(
        self,
        pvc_factory,
        pod_factory,
        bucket_factory,
        rgw_bucket_factory,
        node_restart_teardown,
    ):
        """
        Test to verify that after enabling huge pages the nodes come up with
        higher page size and all odf cluster pods come back up.

        """
        # Applies huge pages on the cluster nodes
        enable_huge_pages()

        log.info("Wait for all worker node to be READY state")
        wait_for_nodes_status(status=constants.NODE_READY, timeout=600)

        nodes = get_nodes()
        for node in nodes:
            assert (
                node.get()["status"]["allocatable"]["hugepages-2Mi"] == "64Mi"
            ), f"Huge pages is not applied on {node.name}"

        log.info("Wait for all storage cluster pods to be in running state")
        wait_for_pods_to_be_running(timeout=600)

        # Creating Resources
        log.info("Creating Resources using sanity helpers")
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory, False
        )

        # Deleting Resources
        log.info("Deleting the resources created")
        self.sanity_helpers.delete_resources()

        # Verify everything running fine
        log.info("Verifying All resources are Running and matches expected result")
        self.sanity_helpers.health_check(tries=120)
