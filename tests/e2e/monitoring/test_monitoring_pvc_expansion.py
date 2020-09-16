import logging
import pytest

from ocs_ci.framework.testlib import tier4
from ocs_ci.ocs import ocp, constants
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.ocs.resources.pvc import get_pvc_obj
from ocs_ci.ocs.monitoring import prometheus_health_check

logger = logging.getLogger(__name__)


@tier4
@pytest.mark.polarion_id("OCS-640")
class TestMonitoringPvcExpansion(E2ETest):
    """
    Full Fill my-prometheus-claim-prometheus-k8s-0 pvc (90%)
    Increase PVC size and check prometheus health is ok
    """

    def test_monitoring_pvc_expansion(self, dc_pod_factory):
        """
        test monitoring pvc expansion
        """
        # Get my-prometheus-claim-prometheus-k8s-0 pvc object
        pvc_obj = get_pvc_obj(
            namespace=constants.MONITORING_NAMESPACE,
            name='my-prometheus-claim-prometheus-k8s-0'
        )
        pvc_obj.project = ocp.OCP(namespace=constants.MONITORING_NAMESPACE)

        # Get pvc size
        pvc_size = pvc_obj.size

        # Create FIO Pod and Fill 90% from the storage
        dc_pod_obj = dc_pod_factory(pvc=pvc_obj)
        dc_pod_obj.run_io(
            storage_type='fs', size=int(0.9 * pvc_size),
            runtime='60', fio_filename=f'{dc_pod_obj.name}_io'
        )

        # Increase storage my-prometheus-claim-prometheus-k8s-0 pvc
        pvc_obj.resize_pvc(new_size=int(self.pvc_size * 1.4), verify=True)

        # Validate the prometheus health is ok
        assert prometheus_health_check(), (
            "Prometheus cluster health is not OK"
        )
