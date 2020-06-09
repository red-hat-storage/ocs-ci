import logging
import pytest

from ocs_ci.framework.testlib import E2ETest, workloads
from ocs_ci.ocs.couchbase import CouchBase

log = logging.getLogger(__name__)


@pytest.fixture(scope='function')
def couchbase(request):

    couchbase = CouchBase()

    def teardown():
        couchbase.teardown()
    request.addfinalizer(teardown)
    return couchbase


@workloads
@pytest.mark.polarion_id("OCS-807")
class TestCouchBaseWorkload(E2ETest):
    """
    Deploy an CouchBase workload using operator
    """
    def test_cb_workload_simple(self, couchbase):
        """
        Testing basic couchbase workload
        """
        couchbase.setup_cb()
        couchbase.create_couchbase_worker(replicas=5)
        couchbase.run_workload(replicas=5)
        couchbase.analyze_run()
