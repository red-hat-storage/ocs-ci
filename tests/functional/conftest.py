import pytest
import logging
from ocs_ci.helpers.odf_cli import odf_cli_setup_helper
from ocs_ci.helpers.odf_cli import ODFCLIRetriever, ODFCliRunner
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs.exceptions import CephHealthException

log = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def odf_cli_setup():
    try:
        odf_cli_runner = odf_cli_setup_helper()
    except RuntimeError as ex:
        pytest.fail(str(ex))

    return odf_cli_runner


@pytest.fixture()
def init_sanity(request, nodes):
    """
    Initial Cluster sanity
    """
    sanity_helpers = Sanity()

    def finalizer():
        """
        Make sure all the nodes are Running and
        the ceph health is OK at the end of the test
        """

        # Check if all the nodes are Running otherwise start them
        log.info("Checking if all the nodes are READY otherwise start them")
        nodes.restart_nodes_by_stop_and_start_teardown()

        # Check cluster health
        try:
            log.info("Making sure ceph health is OK")
            sanity_helpers.health_check(tries=50, cluster_check=False)
        except CephHealthException as e:
            log.error(f"[Error] {e.args}")
            raise

    request.addfinalizer(finalizer)
