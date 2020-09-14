import logging
import pytest

from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.resources.pod import get_all_pods
from ocs_ci.framework.testlib import E2ETest, scale
from tests.helpers import (
    default_storage_class,
    validate_pod_oomkilled,
    validate_pods_are_running_and_not_restarted
)
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.ocs.resources.storage_cluster import get_osd_size

log = logging.getLogger(__name__)


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

    @pytest.fixture()
    def base_setup(
        self, interface, pvc_factory, pod_factory
    ):
        """
        A setup phase for the test:
        get all the ceph pods information,
        create maxsize pvc, pod and run IO

        """
        osd_size = get_osd_size()
        pvc_size_gb = osd_size * 1024
        io_size_mb = f'{int((pvc_size_gb / 2) * 1000)}M'

        pod_objs = get_all_pods(
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            selector=['noobaa', 'rook-ceph-osd-prepare', 'rook-ceph-drain-canary'],
            exclude_selector=True
        )

        # Create maxsize pvc, app pod and run ios
        self.sc = default_storage_class(interface_type=interface)

        self.pvc_obj = pvc_factory(
            interface=interface, storageclass=self.sc, size=pvc_size_gb,
        )

        self.pod_obj = pod_factory(interface=interface, pvc=self.pvc_obj)

        log.info(f"Running FIO to fill PVC size: {io_size_mb}")
        self.pod_obj.run_io(
            'fs', size=io_size_mb, io_direction='write', runtime=480
        )

        log.info("Waiting for IO results")
        self.pod_obj.get_fio_results()

        return pod_objs

    def test_pods_are_not_oomkilled_while_running_ios(self, base_setup):
        """
        Create maxsize pvc and run IOs continuously.
        While IOs are running make sure all pods are in running state and
        not OOMKILLED.

        """
        pod_objs = base_setup

        for pod in pod_objs:
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
