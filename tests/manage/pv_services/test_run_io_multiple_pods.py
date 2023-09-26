import pytest
from ocs_ci.ocs.resources.pod import get_fio_rw_iops
from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import ManageTest, tier2


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

    num_of_pvcs = 10
    pvc_size = 5

    @pytest.fixture()
    def pods(self, interface, pod_factory, multi_pvc_factory):
        """
        Prepare multiple pods for the test

        Returns:
            list: Pod instances

        """
        pvc_objs = multi_pvc_factory(
            interface=interface, size=self.pvc_size, num_of_pvc=self.num_of_pvcs
        )

        pod_objs = list()
        for pvc_obj in pvc_objs:
            pod_objs.append(pod_factory(pvc=pvc_obj))
        return pod_objs

    def test_run_io_multiple_pods(self, pods):
        """
        Run IO on multiple pods in parallel
        """
        for pod in pods:
            pod.run_io("fs", f"{self.pvc_size - 1}G")

        for pod in pods:
            get_fio_rw_iops(pod)
