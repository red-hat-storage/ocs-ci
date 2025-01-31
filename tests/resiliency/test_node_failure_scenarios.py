from ocs_ci.resiliency.resiliency_helper import Resiliency

# from ocs_ci.resiliency.resiliency_workload import workload_object
import logging
from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import green_squad, resiliency_test

log = logging.getLogger(__name__)


@green_squad
@resiliency_test
class TestResiliencyNodeFailures:
    def test_node_poweroff(self, multi_pvc_factory, fio_resiliency_workload):
        """Resiliency tests with node failures"""
        scenario = "NODE_FAILURES"
        failure_method = "POWEROFF_NODE"

        # Create pvcs with different access_modes
        size = 5
        access_modes = [constants.ACCESS_MODE_RWO]
        cephfs_pvc_objs = multi_pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            access_modes=access_modes,
            size=size,
            num_of_pvc=2,
        )

        rbd_pvc_objs = multi_pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            access_modes=access_modes,
            size=size,
            num_of_pvc=2,
        )

        # Starting Workload on the cluster
        for pv_obj in cephfs_pvc_objs + rbd_pvc_objs:
            fio_resiliency_workload(pv_obj)

        # Injecting Resiliency failures
        node_failures = Resiliency(scenario, failure_method=failure_method)
        node_failures.start()
        node_failures.cleanup()
