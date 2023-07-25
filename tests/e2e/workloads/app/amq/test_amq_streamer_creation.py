import logging
import pytest

from ocs_ci.framework.testlib import E2ETest, google_api_required
from ocs_ci.ocs import constants
from ocs_ci.ocs.amq import AMQ
from ocs_ci.utility import templating
from ocs_ci.helpers.helpers import default_storage_class

log = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def test_fixture_amq(request):

    amq = AMQ()

    def teardown():
        amq.cleanup()

    request.addfinalizer(teardown)
    return amq


@google_api_required
@pytest.mark.skip(reason="Skip due to helm permission issue")
class TestAMQBasics(E2ETest):
    @pytest.mark.parametrize(
        argnames=["interface"],
        argvalues=[
            pytest.param(
                constants.CEPHBLOCKPOOL, marks=pytest.mark.polarion_id("OCS-2217")
            )
        ],
    )
    def test_install_and_run_amq_benchmark(
        self,
        interface,
        test_fixture_amq,
    ):
        """
        Create amq cluster and run open messages on it

        """
        # Get sc
        sc = default_storage_class(interface_type=interface)

        # Deploy amq cluster
        test_fixture_amq.setup_amq_cluster(sc.name)

        # Run benchmark
        amq_workload_dict = templating.load_yaml(constants.AMQ_WORKLOAD_YAML)
        amq_workload_dict["producersPerTopic"] = 3
        amq_workload_dict["consumerPerSubscription"] = 3
        result = test_fixture_amq.run_amq_benchmark(amq_workload_yaml=amq_workload_dict)
        amq_output = test_fixture_amq.validate_amq_benchmark(result, amq_workload_dict)
        # Export pgdata to google  google spreadsheet
        test_fixture_amq.export_amq_output_to_gsheet(
            amq_output=amq_output, sheet_name="E2E Workloads", sheet_index=1
        )
