import pytest

from ocs_ci.ocs.resources.pod import get_fio_rw_iops
from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    provider_mode,
    run_on_all_clients,
)
from ocs_ci.framework.testlib import ManageTest, tier2


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

    num_of_pvcs = 6
    pvc_size = 5

    @pytest.fixture()
    def pods(self, interface, multi_pvc_factory, pod_factory, deployment_pod_factory):
        """
        Prepare multiple pods for the test

        Returns:
            list: Pod instances

        """
        pvc_objs = multi_pvc_factory(
            interface=interface, size=self.pvc_size, num_of_pvc=self.num_of_pvcs
        )

        pod_objs = list()
        for pvc_obj in pvc_objs[: len(pvc_objs) // 2]:
            pod_objs.append(deployment_pod_factory(interface=interface, pvc=pvc_obj))

        for pvc_obj in pvc_objs[len(pvc_objs) // 2 :]:
            pod_objs.append(pod_factory(interface=interface, pvc=pvc_obj))

        return pod_objs

    @run_on_all_clients
    def test_run_io_multiple_pods(self, pods, cluster_index):
        """
        Run IO on multiple pods in parallel
        """
        for pod in pods:
            pod.run_io(storage_type="fs", size="1G")

        for pod in pods:
            get_fio_rw_iops(pod)
