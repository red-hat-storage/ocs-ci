import pytest
import logging
import random
from ocs_ci.ocs import constants

from ocs_ci.framework.testlib import ManageTest, tier2
from tests.fixtures import (
    create_rbd_storageclass, create_ceph_block_pool,
    create_rbd_secret, create_pvcs, create_project,
    create_cephfs_secret, create_cephfs_storageclass,
    create_dc_pods, create_serviceaccount)


logger = logging.getLogger(__name__)


@pytest.fixture()
def init_pvc_size(request):
    """
    Initialize the PVC size for PVC creation
    """
    class_instance = request.node.cls
    class_instance.pvc_size_int = getattr(
        class_instance, 'pvc_size_int', random.randint(1, 10)
    )
    class_instance.pvc_size = f'{class_instance.pvc_size_int}Gi'


@tier2
@pytest.mark.usefixtures(
    create_project.__name__,
    create_serviceaccount.__name__,
    init_pvc_size.__name__,
)
class BaseRunIOMultipleDcPods(ManageTest):
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
    num_of_pvcs = 10
    pvc_size_int = 5
    interface = None

    def run_io_multiple_dc_pods(self):
        """
        Run IO on multiple dc pods in parallel
        """

        for pod in self.dc_pod_objs:
            pod.run_io('fs', f'{self.pvc_size_int - 1}G')

        for pod in self.dc_pod_objs:
            pod.get_fio_rw_iops(pod)


@tier2
@pytest.mark.polarion_id("OCS-1284")
@pytest.mark.usefixtures(
    create_rbd_secret.__name__,
    create_ceph_block_pool.__name__,
    create_rbd_storageclass.__name__,
    create_pvcs.__name__,
    create_dc_pods.__name__
)
class TestRunIOMultipleDcPodsRBD(BaseRunIOMultipleDcPods):
    """
    Run IO on multiple dc pods in parallel - RBD
    """
    interface = constants.CEPHBLOCKPOOL
    storage_type = 'block'

    def test_run_io_multiple_dc_pods_rbd(self):
        """
        Run IO on multiple dc pods in parallel - RBD
        """
        self.run_io_multiple_dc_pods()


@tier2
@pytest.mark.polarion_id("OCS-1285")
@pytest.mark.usefixtures(
    create_cephfs_secret.__name__,
    create_cephfs_storageclass.__name__,
    create_pvcs.__name__,
    create_dc_pods.__name__
)
class TestRunIOMultipleDcPodsCephFS(BaseRunIOMultipleDcPods):
    """
    Run IO on multiple dc pods in parallel - CephFS
    """
    interface = constants.CEPHFILESYSTEM
    storage_type = 'fs'

    def test_run_io_multiple_dc_pods_fs(self):
        """
        Run IO on multiple dc pods in parallel - CephFS
        """
        self.run_io_multiple_dc_pods()
