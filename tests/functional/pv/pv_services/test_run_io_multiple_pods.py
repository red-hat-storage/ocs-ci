import pytest

from ocs_ci.ocs.resources.pod import get_fio_rw_iops
from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    provider_mode,
    run_on_all_clients_push_missing_configs,
)
from ocs_ci.framework.testlib import ManageTest, tier2


@run_on_all_clients_push_missing_configs
@provider_mode
@green_squad
@tier2
@pytest.mark.parametrize(
    argnames=["interface"],
    argvalues=[
        pytest.param(constants.CEPHBLOCKPOOL, marks=pytest.mark.polarion_id("OCS-692")),
        pytest.param(
            constants.CEPHFILESYSTEM, marks=pytest.mark.polarion_id("OCS-693")
        ),
    ],
)
class TestIOMultiplePods(ManageTest):
    """
    Run IO on multiple pods in parallel
    """

    num_of_pvcs = 3
    pvc_size = 5

    @pytest.fixture()
    def pods(self, interface, multi_pvc_factory, pod_factory):
        """
        Prepare multiple regular pods for the test
        """
        pvc_objs = multi_pvc_factory(
            interface=interface, size=self.pvc_size, num_of_pvc=self.num_of_pvcs
        )
        return [pod_factory(interface=interface, pvc=pvc) for pvc in pvc_objs]

    @pytest.fixture()
    def deployment_pods(self, interface, multi_pvc_factory, deployment_pod_factory):
        """
        Prepare multiple deployment pods for the test
        """
        pvc_objs = multi_pvc_factory(
            interface=interface, size=self.pvc_size, num_of_pvc=self.num_of_pvcs
        )
        return [
            deployment_pod_factory(interface=interface, pvc=pvc) for pvc in pvc_objs
        ]

    def test_run_io_multiple_pods(self, pods, cluster_index):
        """
        Run IO on multiple regular pods in parallel
        """
        for pod in pods:
            pod.run_io(storage_type="fs", size="1G")
        for pod in pods:
            get_fio_rw_iops(pod)

    def test_run_io_multiple_deployment_pods(self, deployment_pods, cluster_index):
        """
        Run IO on multiple deployment pods in parallel
        """
        for pod in deployment_pods:
            pod.run_io(storage_type="fs", size="1G")
        for pod in deployment_pods:
            get_fio_rw_iops(pod)
