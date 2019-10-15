import logging
import pytest

from ocs_ci.ocs import constants, ocp, defaults
from ocs_ci.framework.testlib import tier4, E2ETest
from tests.sanity_helpers import Sanity
from ocs_ci.ocs.monitoring import check_pvcdata_collected_on_prometheus
from ocs_ci.ocs.resources import pod


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


@pytest.mark.polarion_id("OCS-605")
@tier4
class TestMonitoringWhenOSDDown(E2ETest):
    """
    When one of the osd pod is down, there shouldn't be any functional impact on prometheus pod
    and also all data/metrics should be collected correctly.
    """

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance
        """
        self.sanity_helpers = Sanity()

    def test_monitoring_when_osd_down(self, create_pods):
        """
        Test case to validate monitoring when osd is down
        """

        pod_objs = create_pods

        # Get osd pods
        osd_pod_list = pod.get_osd_pods()

        # Make one of the osd down(first one)
        ocp_obj = ocp.OCP(kind=constants.DEPLOYMENT, namespace=defaults.ROOK_CLUSTER_NAMESPACE)

        params = '{"spec": {"replicas": 0}}'
        name = osd_pod_list[0].get().get('metadata').get('name')
        assert ocp_obj.patch(resource_name=name[:-17], params=params), (
            f"Failed to change the replica count of osd {name} to 0"
        )

        # Validate osd is down
        pod_obj = ocp.OCP(kind=constants.POD, namespace=defaults.ROOK_CLUSTER_NAMESPACE)
        pod_obj.wait_for_delete(resource_name=name), (
            f"Resources is not deleted {name}"
        )

        # Check for the created pvc metrics when osd is down
        for pod_obj in pod_objs:
            assert check_pvcdata_collected_on_prometheus(pod_obj.pvc.name), (
                f"On prometheus pod for created pvc {pod_obj.pvc.name} related data is not collected"
            )

        # Make osd up which was down
        params = '{"spec": {"replicas": 1}}'
        assert ocp_obj.patch(resource_name=name[:-17], params=params), (
            f"Failed to change the replica count of osd {name} to 1"
        )

        # Validate osd is up and ceph health is ok
        self.sanity_helpers.health_check()
