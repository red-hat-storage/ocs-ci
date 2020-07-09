import logging
import pytest

from ocs_ci.framework.testlib import E2ETest, workloads
from ocs_ci.ocs.couchbase import CouchBase

log = logging.getLogger(__name__)

@workloads
class TestCouchBaseWorkload(E2ETest):
    """
    Deploy an CouchBase workload using operator
    """
    @pytest.fixture()
    def cb_setup(self, amq_factory_fixture):
        """
        Creates amq cluster and run benchmarks
        """
        sc_name = default_storage_class(interface_type=constants.CEPHBLOCKPOOL)
        self.amq_workload_dict = templating.load_yaml(constants.AMQ_SIMPLE_WORKLOAD_YAML)
        self.amq, self.result = amq_factory_fixture(
            sc_name=sc_name.name, tiller_namespace="tiller",
            amq_workload_yaml=self.amq_workload_dict, run_in_bg=False
        )
    def test(self):

        pass