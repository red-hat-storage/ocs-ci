import logging
import pytest

from ocs_ci.framework.testlib import E2ETest, workloads, ignore_leftovers
from tests.disruption_helpers import Disruptions
from ocs_ci.ocs import flowtest
from tests.sanity_helpers import Sanity

log = logging.getLogger(__name__)


@workloads
@ignore_leftovers
class TestCouchBasePodRespin(E2ETest):
    """
    Deploy an CouchBase workload using operator
    """
    @pytest.fixture()
    def cb_setup(self, couchbase_factory_fixture):
        """
        Creates couchbase workload
        """
        self.cb = couchbase_factory_fixture(
            replicas=3, run_in_bg=True, skip_analyze=True
        )
        self.sanity_helpers = Sanity()

    @pytest.mark.parametrize(
        argnames=["pod_name"],
        argvalues=[
            pytest.param(
                *['osd'],
                marks=pytest.mark.polarion_id("OCS-780")),
            pytest.param(
                *['mon'],
                marks=pytest.mark.polarion_id("OCS-779")),
            pytest.param(
                *['mgr'],
                marks=pytest.mark.polarion_id("OCS-781")),
            pytest.param(
                *['couchbase'],
                marks=pytest.mark.polarion_id("OCS-786")),
        ])
    def test_run_couchbase_respin_pod(self, cb_setup, pod_name):
        log.info(f"Respin Ceph pod {pod_name}")

        if pod_name == 'couchbase':
            self.cb.respin_couchbase_app_pod()
        else:
            disruption = Disruptions()
            disruption.set_resource(resource=f'{pod_name}')
            disruption.delete_resource()

        bg_handler = flowtest.BackgroundOps()
        bg_ops = [self.cb.result]
        bg_handler.wait_for_bg_operations(bg_ops, timeout=3600)
        self.sanity_helpers.health_check()
