import logging
import pytest
import time

from ocs_ci.framework.pytest_customization.marks import orange_squad
from ocs_ci.framework.testlib import E2ETest, scale
from ocs_ci.ocs import constants
from ocs_ci.ocs.amq import AMQ
from ocs_ci.helpers.helpers import default_storage_class

log = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def test_fixture_amq(request):

    amq = AMQ()

    def teardown():
        amq.cleanup()

    request.addfinalizer(teardown)
    return amq


@orange_squad
@scale
@pytest.mark.skip(
    reason="Skipped due to github issue #3372, TC is failing "
    "in each test-run, priority to fix this issue is lower"
)
class TestAMQBasics(E2ETest):
    @pytest.mark.parametrize(
        argnames=["interface"],
        argvalues=[
            pytest.param(
                constants.CEPHBLOCKPOOL, marks=pytest.mark.polarion_id("OCS-424")
            )
        ],
    )
    def test_install_amq_scale(self, interface, test_fixture_amq):
        """
        Create amq cluster and run open messages on it
        """
        # Get sc
        sc = default_storage_class(interface_type=interface)

        # Deploy amq cluster
        test_fixture_amq.setup_amq_cluster(sc.name)

        # Scale 3 times, first install 3 pods, then 6 then 9
        for i in range(1, 4):
            # Number of messages to be sent and received
            num_of_messages = 10000 * i
            # Run open messages
            test_fixture_amq.create_messaging_on_amq(
                num_of_producer_pods=i * 3,
                num_of_consumer_pods=i * 3,
                value=str(num_of_messages),
            )

            # Wait for some time to generate msg
            waiting_time = i * 60
            log.info(f"Waiting for {waiting_time}sec to generate msg")
            time.sleep(waiting_time)

            # Check messages are sent and received
            threads = test_fixture_amq.run_in_bg(value=str(num_of_messages))
            for thread in threads:
                thread.result(timeout=i * 1800)
