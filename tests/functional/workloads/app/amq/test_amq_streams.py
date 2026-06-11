import logging
import pytest
import time

from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import E2ETest, workloads
from ocs_ci.ocs import constants
from ocs_ci.ocs.amq import AMQ
from ocs_ci.helpers.helpers import default_storage_class

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def test_fixture_amq(request):

    amq = AMQ()

    def teardown():
        amq.cleanup()

    request.addfinalizer(teardown)
    return amq


@magenta_squad
@workloads
class TestAMQBasics(E2ETest):
    @pytest.mark.parametrize(
        argnames=["interface"],
        argvalues=[
            pytest.param(
                constants.CEPHBLOCKPOOL, marks=pytest.mark.polarion_id("OCS-1279")
            )
        ],
    )
    def test_install_amq_backed_by_ocs(self, interface, test_fixture_amq):
        """
        Create amq cluster and run open messages on it
        """
        logger.test_step("Get default storage class")
        sc = default_storage_class(interface_type=interface)
        logger.info(f"Using storage class: {sc.name}")

        logger.test_step("Deploy AMQ cluster")
        test_fixture_amq.setup_amq_cluster(sc.name)

        logger.test_step("Create messaging on AMQ")
        test_fixture_amq.create_messaging_on_amq()

        waiting_time = 60
        logger.info(f"Waiting {waiting_time}s for messages to be generated")
        time.sleep(waiting_time)

        logger.test_step("Verify messages are sent and received")
        threads = test_fixture_amq.run_in_bg()
        for thread in threads:
            thread.result(timeout=1800)
        logger.info("All messaging threads completed successfully")
