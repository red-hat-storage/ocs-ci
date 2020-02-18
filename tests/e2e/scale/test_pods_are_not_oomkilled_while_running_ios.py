import logging
import pytest

from ocs_ci.ocs import ocp, constants, defaults
from ocs_ci.ocs.resources.pod import get_all_pods
from ocs_ci.framework.testlib import E2ETest, scale
from tests.helpers import (
    default_storage_class,
    validate_pod_oomkilled
)
from ocs_ci.utility.utils import ceph_health_check

log = logging.getLogger(__name__)


def validate_pods_are_running_and_not_restarted(
    pod_name, pod_restart_count, namespace
):
    """
    Validate given pod is in running state and not restarted or re-spinned

    Args:
        pod_name (str): Name of the pod
        pod_restart_count (int): Restart count of pod
        namespace (str): Namespace of the pod

    Returns:
        bool : True if pod is in running state and restart
               count matches the previous one

    """
    ocp_obj = ocp.OCP(kind=constants.POD, namespace=namespace)
    pod_obj = ocp_obj.get(resource_name=pod_name)
    restart_count = pod_obj.get('status').get('containerStatuses')[0].get('restartCount')
    pod_state = pod_obj.get('status').get('phase')
    if pod_state == 'Running' and restart_count == pod_restart_count:
        log.info("Pod is running state and restart count matches with previous one")
        return True
    log.error(f"Pod is in {pod_state} state and restart count of pod {restart_count}")
    log.info(f"{pod_obj}")
    return False


@scale
@pytest.mark.parametrize(
    argnames=["interface"],
    argvalues=[
        pytest.param(
            constants.CEPHBLOCKPOOL, marks=pytest.mark.polarion_id("OCS-2048")
        ),
        pytest.param(
            constants.CEPHFILESYSTEM, marks=pytest.mark.polarion_id("OCS-2049")
        )
    ]
)
class TestPodAreNotOomkilledWhileRunningIO(E2ETest):
    """
    A test case to validate no memory leaks found
    when heavy IOs run continuously and
    ceph, cluster health is good

    """
    pvc_size_gb = 300
    io_size = 250

    @pytest.fixture()
    def base_setup(
        self, interface, pvc_factory, dc_pod_factory
    ):
        """
        A setup phase for the test:
        get all the ceph pods information,
        create maxsize pvc, pod and run IO

        """

        pod_objs = get_all_pods(
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            selector=['rook-ceph-osd-prepare', 'rook-ceph-drain-canary'],
            exclude_selector=True
        )

        # Create maxsize pvc, dc pod and run ios
        self.sc = default_storage_class(interface_type=interface)

        self.pvc_obj = pvc_factory(
            interface=interface, storageclass=self.sc, size=self.pvc_size_gb,
        )

        self.dc_pod_obj = dc_pod_factory(interface=interface, pvc=self.pvc_obj)

        log.info(f"Running FIO to fill PVC size: {self.io_size}")
        self.dc_pod_obj.run_io(
            'fs', size=self.io_size, io_direction='write', runtime=60
        )

        log.info("Waiting for IO results")
        self.dc_pod_obj.get_fio_results()

        return pod_objs

    def test_pods_are_not_oomkilled_while_running_ios(self, base_setup):
        """
        Create maxsize pvc and run IOs continuously.
        While IOs are running make sure all pods are in running state and
        not OOMKILLED.

        """

        for pod in self.pod_obj:
            pod_name = pod.get().get('metadata').get('name')
            restart_count = pod.get().get('status').get('containerStatuses')[0].get('restartCount')
            for item in pod.get().get('status').get('containerStatuses'):
                # Validate pod is oomkilled
                container_name = item.get('name')
                assert validate_pod_oomkilled(
                    pod_name=pod_name, container=container_name
                ), f"Pod {pod_name} OOMKILLED while running IOs"

            # Validate pod is running and not restarted
            assert validate_pods_are_running_and_not_restarted(
                pod_name=pod_name, pod_restart_count=restart_count,
                namespace=defaults.ROOK_CLUSTER_NAMESPACE
            ), f"Pod {pod_name} is either not running or restarted while running IOs"

        # Check ceph health is OK
        ceph_health_check()
