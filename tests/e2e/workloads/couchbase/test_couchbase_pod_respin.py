import logging
import pytest
import time

from ocs_ci.framework.testlib import E2ETest, workloads, ignore_leftovers
from ocs_ci.utility.utils import TimeoutSampler
from tests.disruption_helpers import Disruptions
from ocs_ci.utility import utils

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
    @pytest.mark.usefixtures(cb_setup.__name__)
    def test_run_couchbase_respin_pod(self, pod_name):
        log.info(f"Respin Ceph pod {pod_name}")

        if pod_name == 'couchbase':
            self.cb.respin_couchbase_app_pod()
        else:
            disruption = Disruptions()
            disruption.set_resource(resource=f'{pod_name}')
            disruption.delete_resource()

        for sample in TimeoutSampler(300, 5, self.cb.result.done):
            if sample:
                break
            else:
                logging.info(
                    "#### ....Waiting for couchbase threads to complete..."
                )
        utils.ceph_health_check()
