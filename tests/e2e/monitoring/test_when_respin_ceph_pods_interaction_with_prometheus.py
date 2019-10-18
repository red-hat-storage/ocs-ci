import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import tier4, E2ETest, ignore_leftovers
from tests import disruption_helpers
from ocs_ci.ocs.monitoring import check_pvcdata_collected_on_prometheus

logger = logging.getLogger(__name__)


@pytest.fixture()
def create_pods(pod_factory, num_of_pod=3):
    """
    Create resources for the test
    """
    pod_objs = [
        pod_factory(
            interface=constants.CEPHBLOCKPOOL,
            status=constants.STATUS_RUNNING
        ) for _ in range(num_of_pod)
    ]

    # Check for the created pvc metrics on prometheus pod
    for pod_obj in pod_objs:
        assert check_pvcdata_collected_on_prometheus(pod_obj.pvc.name), (
            f"On prometheus pod for created pvc {pod_obj.pvc.name} related data is not collected"
        )

    return pod_objs


@ignore_leftovers
@pytest.mark.polarion_id("OCS-580")
class TestRespinCephPodsAndInteractionWithPrometheus(E2ETest):
    """
    Respinning the ceph pods (i.e mon, osd, mgr) shouldn't have functional
    impact to prometheus pods, all data/metrics should be collected correctly.
    """

    @tier4
    def test_monitoring_after_respinning_ceph_pods(self, create_pods):
        """
        Test case to validate respinning the ceph pods and
        its interaction with prometheus pod
        """
        pod_objs = create_pods

        # Re-spin the ceph pods(i.e mgr, mon, osd, mds) one by one
        resource_to_delete = ['mgr', 'mon', 'osd']
        disruption = disruption_helpers.Disruptions()
        for res_to_del in resource_to_delete:
            disruption.set_resource(resource=res_to_del)
            disruption.delete_resource()

        # Check for the created pvc metrics after respinning ceph pods
        for pod_obj in pod_objs:
            assert check_pvcdata_collected_on_prometheus(pod_obj.pvc.name), (
                f"On prometheus pod for created pvc {pod_obj.pvc.name} related data is not collected"
            )
