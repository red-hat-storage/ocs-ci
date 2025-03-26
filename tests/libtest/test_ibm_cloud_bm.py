import logging
import pytest

from ocs_ci.ocs.platform_nodes import IBMCloudBMNodes
from ocs_ci.framework.testlib import libtest
from ocs_ci.ocs import node
from ocs_ci.framework.pytest_customization.marks import (
    provider_client_platform_required,
)


logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def teardown(request):
    def finalizer():
        logger.info("Running restart_nodes_by_stop_and_start_teardown")
        ibmcloud = IBMCloudBMNodes()
        ibmcloud.restart_nodes_by_stop_and_start_teardown()

    request.addfinalizer(finalizer)


@libtest
@provider_client_platform_required
def test_restart_nodes_by_stop_and_start():
    """
    Check basic consistency in platform handling.
    """
    ibmcloud = IBMCloudBMNodes()
    worker_nodes = node.get_nodes(node_type="worker")
    ibmcloud.restart_nodes_by_stop_and_start(worker_nodes)
