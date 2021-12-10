import logging
import pytest

from ocs_ci.framework.testlib import E2ETest, workloads
from ocs_ci.ocs.couchbase_new import CouchBase

log = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def couchbase(request):

    couchbase = CouchBase()

    def teardown():
        couchbase.teardown()

    request.addfinalizer(teardown)
    return couchbase


@workloads
@pytest.mark.polarion_id("OCS-785")
class TestCouchBaseWorkload(E2ETest):
    """
    Deploy an CouchBase workload using operator
    """

    def test_cb_workload_simple(self, couchbase):
        """
        Testing basic couchbase workload
        """
        couchbase.couchbase_subscription()
        couchbase.create_cb_secrets()
        couchbase.create_cb_cluster(replicas=3)
        couchbase.create_data_buckets()
        couchbase.run_workload(replicas=3)
        couchbase.export_pfoutput_to_googlesheet(
            sheet_name="E2E Workloads", sheet_index=2
        )
