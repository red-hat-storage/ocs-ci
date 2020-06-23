import logging
import pytest
import time

from ocs_ci.framework.testlib import E2ETest, workloads
from ocs_ci.ocs import constants
from ocs_ci.ocs.amq import AMQ
from tests.helpers import default_storage_class

log = logging.getLogger(__name__)


@pytest.fixture(scope='function')
def test_fixture_amq(request):

    amq = AMQ()

    def teardown():
        amq.cleanup()

    request.addfinalizer(teardown)
    return amq


@workloads
class TestAMQBasics(E2ETest):
    @pytest.mark.parametrize(
        argnames=["interface"],
        argvalues=[
            pytest.param(
                constants.CEPHBLOCKPOOL, marks=pytest.mark.polarion_id("OCS-2217")
            ),
            pytest.param(
                constants.CEPHFILESYSTEM, marks=pytest.mark.polarion_id("OCS-2218")
            )
        ]
    )
    def test_install_amq_cephfs(self, interface, test_fixture_amq):
        """
        Create amq cluster and run open messages on it

        """
        # Get sc
        sc = default_storage_class(interface_type=interface)

        # Deploy amq cluster
        test_fixture_amq.setup_amq_cluster(sc.name)

        # Run open messages
        test_fixture_amq.create_messaging_on_amq()

        # Wait for some time to generate msg
        waiting_time = 60
        log.info(f"Waiting for {waiting_time}sec to generate msg")
        time.sleep(waiting_time)

        # Check messages are sent and received
        threads = test_fixture_amq.run_in_bg()
        for t in threads:
            t.join()
