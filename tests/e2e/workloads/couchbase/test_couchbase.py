import logging
import pytest

from ocs_ci.framework.testlib import E2ETest, workloads

log = logging.getLogger(__name__)


@workloads
@pytest.mark.polarion_id("OCS-785")
class TestCouchBaseWorkload(E2ETest):
    """
    Deploy an CouchBase workload using operator
    """
    def test_cb_workload_simple(self, couchbase_factory_fixture):
        """
        Testing basic couchbase workload
        """
        couchbase_factory_fixture(replicas=3, skip_analyze=True)
