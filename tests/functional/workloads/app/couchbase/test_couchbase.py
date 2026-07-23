import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import E2ETest, workloads, skipif_ocp_version
from ocs_ci.ocs.couchbase import CouchBase

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def couchbase(request):

    couchbase = CouchBase()

    def teardown():
        logger.info("Cleaning up CouchBase resources")
        couchbase.cleanup()

    request.addfinalizer(teardown)
    return couchbase


@magenta_squad
@skipif_ocp_version(">=4.13")
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
        logger.test_step("Create CouchBase operator subscription")
        couchbase.couchbase_subscription()

        logger.test_step("Create CouchBase secrets")
        couchbase.create_cb_secrets()

        logger.test_step("Create CouchBase cluster with 3 replicas")
        couchbase.create_cb_cluster(replicas=3)

        logger.test_step("Create CouchBase data buckets")
        couchbase.create_data_buckets()

        logger.test_step("Run CouchBase workload with 3 replicas")
        couchbase.run_workload(replicas=3)
