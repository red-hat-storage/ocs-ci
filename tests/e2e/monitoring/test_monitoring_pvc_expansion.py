import logging
import pytest

from ocs_ci.framework.testlib import tier4a
from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.ocs.resources.pvc import get_pvc_obj
from ocs_ci.ocs.monitoring import prometheus_health_check
from ocs_ci.ocs.resources.pod import get_used_space_on_mount_point

log = logging.getLogger(__name__)


@tier4a
@pytest.mark.polarion_id("OCS-640")
class TestMonitoringPvcExpansion(E2ETest):
    """
    Full Fill my-prometheus-claim-prometheus-k8s-0 pvc
    Increase PVC size and check prometheus health is ok

    """

    def test_monitoring_pvc_expansion(self, pod_factory):
        """
        test monitoring pvc expansion

        """

        # Get my-prometheus-claim-prometheus-k8s-0 pvc object
        pvc_obj = get_pvc_obj(
            namespace=constants.MONITORING_NAMESPACE,
            name="my-prometheus-claim-prometheus-k8s-0",
        )

        log.info(f"Creating an app pod and mount {pvc_obj.name}")
        interface_type = constants.CEPHBLOCKPOOL
        pod_obj = pod_factory(interface=interface_type, pvc=pvc_obj)
        log.info(f"{pod_obj.name} created successfully and mounted {pvc_obj.name}")

        # Get pvc used percentage
        pvc_size = pvc_obj.size
        used_percentage = get_used_space_on_mount_point(pod_obj)
        used_space = pvc_size * float(used_percentage.strip("%")) / 100
        free_space = pvc_size - used_space
        log.info(
            f"\n{pvc_obj.name} size is {pvc_size}G\n"
            f"used_space = {used_space}G\n"
            f"free_space = {free_space}G\n"
        )

        log.info(f"Running FIO on {pod_obj.name}")
        pod_obj.run_io(
            storage_type="fs",
            size=f"{int(free_space)}G",
            runtime=1000,
            io_direction="write",
            fio_filename=f"{pod_obj.name}_f2",
        )

        # Check the prometheus health
        prometheus_health_check()

        # Increase storage my-prometheus-claim-prometheus-k8s-0 pvc
        pvc_obj.resize_pvc(new_size=int(pvc_size * 1.2), verify=True)

        # Validate the prometheus health is ok
        assert prometheus_health_check(), "Prometheus cluster health is not OK"
