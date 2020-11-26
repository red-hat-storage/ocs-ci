import logging
import pytest

from ocs_ci.framework.testlib import ManageTest, tier1
from ocs_ci.ocs.must_gather.must_gather import MustGather
from ocs_ci.ocs.must_gather.const_must_gather import GATHER_COMMANDS_VERSION
from ocs_ci.ocs.ocp import get_ocs_parsed_version

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def mustgather(request):

    mustgather = MustGather()
    mustgather.collect_must_gather()

    def teardown():
        mustgather.cleanup()

    request.addfinalizer(teardown)
    return mustgather


class TestMustGather(ManageTest):
    @tier1
    @pytest.mark.parametrize(
        argnames=["log_type"],
        argvalues=[
            pytest.param(*["CEPH"], marks=pytest.mark.polarion_id("OCS-1583")),
            pytest.param(*["JSON"], marks=pytest.mark.polarion_id("OCS-1583")),
            pytest.param(*["OTHERS"], marks=pytest.mark.polarion_id("OCS-1583")),
        ],
    )
    @pytest.mark.skipif(
        get_ocs_parsed_version() not in GATHER_COMMANDS_VERSION,
        reason=(
            "Skipping must_gather test, because there is not data for this version"
        ),
    )
    def test_must_gather(self, mustgather, log_type):
        """
        Tests functionality of: oc adm must-gather

        """
        mustgather.log_type = log_type
        mustgather.validate_must_gather()
