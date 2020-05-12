import logging
import pytest
import time

from ocs_ci.framework.testlib import E2ETest, workloads
from ocs_ci.ocs.amq import AMQ

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
    @pytest.mark.polarion_id("OCS-612")
    def test_install_amq_cephfs(self, test_fixture_amq):
        """
        Create amq cluster and run open messages on it

        """

        # Deploy amq cluster
        test_fixture_amq.setup_amq_cluster()

        # Run open messages
        test_fixture_amq.create_messaging_on_amq()

        # Wait for some time to generate msg
        time.sleep(30)

        # Check producer pod sent messages
        test_fixture_amq.validate_messages_are_produced()

        # Check consumer pod received messages
        test_fixture_amq.validate_messages_are_consumed()