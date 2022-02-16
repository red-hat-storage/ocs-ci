import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.utility.utils import (
    enable_huge_pages,
    disable_huge_pages,
    verify_huge_pages,
)
from ocs_ci.ocs.node import (
    wait_for_nodes_status,
)
from ocs_ci.ocs.resources.pod import (
    wait_for_pods_to_be_running,
)
from ocs_ci.framework.testlib import (
    bugzilla,
    skipif_ocs_version,
    skipif_external_mode,
    ignore_leftovers,
    E2ETest,
    tier2,
)

log = logging.getLogger(__name__)


@tier2
@skipif_external_mode
@skipif_ocs_version("<4.8")
@bugzilla("1995271")
@bugzilla("1995271")
@pytest.mark.polarion_id("OCS-2754")
@ignore_leftovers
class TestHugePages(E2ETest):
    """
    Enable huge pages post cluster deployment

    """

    @pytest.fixture(scope="function", autouse=True)
    def huge_pages_teardown(self, request):
        """
        Removes huge pages and verifies all pods are up

        """

        def finalizer():
            disable_huge_pages()

            wait_for_nodes_status(status=constants.NODE_READY, timeout=600)

            log.info("Wait for all pods to be in running state")
            wait_for_pods_to_be_running(timeout=600)

        request.addfinalizer(finalizer)

    def test_hugepages_post_odf_deployment(
        self,
    ):
        """
        Test to verify that after enabling huge pages the nodes come up with
        higher page size and all odf cluster pods come back up.

        """
        enable_huge_pages()

        wait_for_nodes_status(status=constants.NODE_READY, timeout=600)

        assert verify_huge_pages()

        log.info("Wait for all pods to be in running state")
        wait_for_pods_to_be_running(timeout=600)
