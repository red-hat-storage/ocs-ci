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
        pytest.param(
            constants.CEPHBLOCKPOOL, marks=pytest.mark.polarion_id("OCS-1284")
        ),
        pytest.param(
            constants.CEPHFILESYSTEM, marks=pytest.mark.polarion_id("OCS-1285")
        ),
    ],
)
class TestRunIOMultipleDcPods(ManageTest):
    """
    Run IO on multiple dc pods in parallel

    Steps:
        1:- Create project
        2:- Create serviceaccount
        3:- Add serviceaccount user to privileged policy
        4:- Create storageclass
        5:- Create PVC
        6:- Create pod with kind deploymentconfig
        7:- Add serviceaccount in yaml
        8:- Add privileged as True under securityContext
        9:- Deploy yaml using oc create -f yaml_name
        10:- oc get pods -n namespace
        11:- 2 pods will be Running for 1 deploymentconfig first will be deploy pod which actual deploys dc
            and second pod will be actual deployed pod
        12:- For Deletion
        13:- oc get deploymentconfig -n namespace
        14:- get dc name and delete using oc delete deploymentconfig <dc_name> -n namespace

        Note:- Step 1,2,3,7 are not required if we deploy dc in openshift-storage namespace
    """

    num_of_pvcs = 1
    pvc_size = 5

    @pytest.fixture()
    def dc_pods(self, interface, multi_pvc_factory, dc_pod_factory):
        """
        Prepare multiple dc pods for the test

        Returns:
            list: Pod instances
        """
        pvc_objs = multi_pvc_factory(
            interface=interface, size=self.pvc_size, num_of_pvc=self.num_of_pvcs
        )

        dc_pod_objs = list()
        for pvc_obj in pvc_objs:
            dc_pod_objs.append(dc_pod_factory(pvc=pvc_obj))
        return dc_pod_objs

    def test_run_io_multiple_dc_pods(self, dc_pods):
        """
        Run IO on multiple dc pods in parallel
        """
        for dc_pod in dc_pods:
            dc_pod.run_io("fs", f"{self.pvc_size - 1}G")

        for dc_pod in dc_pods:
            get_fio_rw_iops(dc_pod)
