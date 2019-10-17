import logging
import pytest

from ocs_ci.ocs import constants, ocp, defaults
from ocs_ci.framework.testlib import tier4, E2ETest
from ocs_ci.ocs.monitoring import (
    check_pvcdata_collected_on_prometheus,)
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


@pytest.mark.polarion_id("OCS-576")
class TestPrometheusPodRestart(E2ETest):
    """
    Prometheus pod restart should not have any functional impact,
    i.e the data/metrics shouldn't be lost after the restart of prometheus pod.
    """

    @tier4
    def test_monitoring_after_restarting_prometheus_pod(self, create_pods):
        """
        Test case to validate prometheus pod restart
        should not have any functional impact
        """
        pod_objs = create_pods

        # Get the prometheus pod
        prometheus_pod_obj = pod.get_all_pods(namespace=defaults.OCS_MONITORING_NAMESPACE, selector=['prometheus'])

        for pod_object in prometheus_pod_obj:
            # Get the pvc which mounted on prometheus pod
            pod_info = pod_object.get()
            pvc_name = pod_info['spec']['volumes'][0]['persistentVolumeClaim']['claimName']

            # Restart the prometheus pod
            pod_object.delete(force=True)
            pod_obj = ocp.OCP(kind=constants.POD, namespace=defaults.OCS_MONITORING_NAMESPACE)
            assert pod_obj.wait_for_resource(
                condition='Running', selector=f'app=prometheus', timeout=60
            )

            # Check the same pvc is mounted on new pod
            pod_info = pod_object.get()
            assert pod_info['spec']['volumes'][0]['persistentVolumeClaim']['claimName'] in pvc_name, (
                f"Old pvc not found after restarting the prometheus pod {pod_object.name}"
            )

        for pod_obj in pod_objs:
            assert check_pvcdata_collected_on_prometheus(pod_obj.pvc.name), (
                f"On prometheus pod for created pvc {pod_obj.pvc.name} related data is not collected"
            )
