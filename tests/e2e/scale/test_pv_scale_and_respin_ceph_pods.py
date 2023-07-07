"""
Scale TC to perform PVC Scale and Respin of Ceph pods in parallel
"""
import logging
import pytest
import threading

from ocs_ci.helpers import helpers, disruption_helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pod
from ocs_ci.utility import utils
from ocs_ci.framework.testlib import scale, E2ETest, ignore_leftovers
from ocs_ci.framework.pytest_customization.marks import skipif_external_mode

log = logging.getLogger(__name__)


class BasePvcCreateRespinCephPods(E2ETest):
    """
    Base Class to create POD with PVC and respin ceph Pods
    """

    def create_pvc_pod(self, rbd_sc_obj, cephfs_sc_obj, number_of_pvc, size):
        """
        Function to create multiple PVC of different type and bind mount them to pods

        Args:
            rbd_sc_obj (obj_dict): rbd storageclass object
            cephfs_sc_obj (obj_dict): cephfs storageclass object
            number_of_pvc (int): pvc count to be created for each types
            size (str): size of each pvc to be created eg: '10Gi'
        """
        log.info(f"Create {number_of_pvc} pvcs and pods")
        cephfs_pvcs = helpers.create_multiple_pvc_parallel(
            cephfs_sc_obj,
            self.namespace,
            number_of_pvc,
            size,
            access_modes=[constants.ACCESS_MODE_RWO, constants.ACCESS_MODE_RWX],
        )
        rbd_pvcs = helpers.create_multiple_pvc_parallel(
            rbd_sc_obj,
            self.namespace,
            number_of_pvc,
            size,
            access_modes=[constants.ACCESS_MODE_RWO, constants.ACCESS_MODE_RWX],
        )
        # Appending all the pvc obj to base case param for cleanup and evaluation
        self.all_pvc_obj.extend(cephfs_pvcs + rbd_pvcs)

        # Create pods with above pvc list
        cephfs_pods = helpers.create_pods_parallel(
            cephfs_pvcs, self.namespace, constants.CEPHFS_INTERFACE
        )
        rbd_rwo_pvc, rbd_rwx_pvc = ([] for i in range(2))
        for pvc_obj in rbd_pvcs:
            if pvc_obj is not None:
                if type(pvc_obj) is list:
                    for pvc_ in pvc_obj:
                        if pvc_.get_pvc_access_mode == constants.ACCESS_MODE_RWX:
                            rbd_rwx_pvc.append(pvc_)
                        else:
                            rbd_rwo_pvc.append(pvc_)
                else:
                    if pvc_obj.get_pvc_access_mode == constants.ACCESS_MODE_RWX:
                        rbd_rwx_pvc.append(pvc_obj)
                    else:
                        rbd_rwo_pvc.append(pvc_obj)

        rbd_rwo_pods = helpers.create_pods_parallel(
            rbd_rwo_pvc, self.namespace, constants.CEPHBLOCKPOOL
        )
        rbd_rwx_pods = helpers.create_pods_parallel(
            rbd_rwx_pvc, self.namespace, constants.CEPHBLOCKPOOL, raw_block_pv=True
        )
        temp_pod_objs = list()
        temp_pod_objs.extend(cephfs_pods + rbd_rwo_pods)
        # Appending all the pod obj to base class param for cleanup and evaluation
        self.all_pod_obj.extend(temp_pod_objs + rbd_rwx_pods)

        # Start respective IO on all the created PODs
        threads = list()
        for pod_obj in temp_pod_objs:
            process = threading.Thread(
                target=pod_obj.run_io,
                args=(
                    "fs",
                    "512M",
                ),
            )
            process.start()
            threads.append(process)
        for pod_obj in rbd_rwx_pods:
            process = threading.Thread(
                target=pod_obj.run_io,
                args=(
                    "block",
                    "512M",
                ),
            )
            process.start()
            threads.append(process)
        for process in threads:
            process.join()

    def respin_ceph_pod(self, resource_to_delete):
        """
        Function to respin ceph pods one by one,
        delete_resource functions checks for the deleted pod back up and running

        Args:
            resource_to_delete (str): Ceph resource type to be deleted.
            eg: mgr/mon/osd/mds/cephfsplugin/rbdplugin/cephfsplugin_provisioner/rbdplugin_provisioner
        """
        disruption = disruption_helpers.Disruptions()
        disruption.set_resource(resource=resource_to_delete)
        no_of_resource = disruption.resource_count
        for i in range(0, no_of_resource):
            disruption.delete_resource(resource_id=i)
            # Validate storage pods are running
            assert pod.wait_for_storage_pods(), "ODF Pods are not in good shape"
            # Validate cluster health ok and all pods are running
            assert utils.ceph_health_check(
                delay=180
            ), "Ceph health in bad state after node reboots"

    def cleanup(self):
        """
        Function to cleanup the SC, PVC and POD objects parallel.
        """
        helpers.delete_objs_parallel(pod.get_all_pods(namespace=self.namespace))
        helpers.delete_objs_parallel(self.all_pvc_obj)
        self.rbd_sc_obj.delete()
        self.cephfs_sc_obj.delete()


@scale
@ignore_leftovers
@skipif_external_mode
@pytest.mark.parametrize(
    argnames="resource_to_delete",
    argvalues=[
        pytest.param(*["mgr"], marks=[pytest.mark.polarion_id("OCS-766")]),
        pytest.param(*["mon"], marks=[pytest.mark.polarion_id("OCS-764")]),
        pytest.param(*["osd"], marks=[pytest.mark.polarion_id("OCS-765")]),
        pytest.param(*["mds"], marks=[pytest.mark.polarion_id("OCS-613")]),
        pytest.param(
            *["cephfsplugin_provisioner"], marks=[pytest.mark.polarion_id("OCS-2641")]
        ),
        pytest.param(
            *["rbdplugin_provisioner"], marks=[pytest.mark.polarion_id("OCS-2639")]
        ),
        pytest.param(*["rbdplugin"], marks=[pytest.mark.polarion_id("OCS-2643")]),
        pytest.param(*["cephfsplugin"], marks=[pytest.mark.polarion_id("OCS-2642")]),
    ],
)
class TestPVSTOcsCreatePVCsAndRespinCephPods(BasePvcCreateRespinCephPods):
    """
    Class for PV scale Create Cluster with 1000 PVC, then Respin ceph pods parallel
    """

    @pytest.fixture()
    def setup_fixture(self, request):
        def finalizer():
            self.cleanup()

        request.addfinalizer(finalizer)

    @pytest.fixture()
    def namespace(self, project_factory):
        """
        Create a project for the test
        """
        proj_obj = project_factory()
        self.namespace = proj_obj.namespace

    @pytest.fixture()
    def storageclass(self, storageclass_factory):
        """
        Create Storage class for rbd and cephfs
        """
        self.rbd_sc_obj = storageclass_factory(interface=constants.CEPHBLOCKPOOL)
        self.cephfs_sc_obj = storageclass_factory(interface=constants.CEPHFILESYSTEM)

    def test_pv_scale_out_create_pvcs_and_respin_ceph_pods(
        self,
        namespace,
        storageclass,
        setup_fixture,
        resource_to_delete,
    ):
        pvc_count_each_itr = 10
        scale_pod_count = 120
        size = "10Gi"
        self.all_pvc_obj, self.all_pod_obj = ([] for i in range(2))

        # First Iteration call to create PVC and POD
        self.create_pvc_pod(
            self.rbd_sc_obj, self.cephfs_sc_obj, pvc_count_each_itr, size
        )
        # Re-spin the ceph pods one by one in parallel with PVC and POD creation
        while True:
            if scale_pod_count <= len(self.all_pod_obj):
                log.info(f"Create {scale_pod_count} pvc and pods")
                break
            else:
                thread1 = threading.Thread(
                    target=self.respin_ceph_pod, args=(resource_to_delete,)
                )
                thread2 = threading.Thread(
                    target=self.create_pvc_pod,
                    args=(
                        self.rbd_sc_obj,
                        self.cephfs_sc_obj,
                        pvc_count_each_itr,
                        size,
                    ),
                )
                thread1.start()
                thread2.start()
            thread1.join()
            thread2.join()

        assert utils.ceph_health_check(
            delay=180
        ), "Ceph health in bad state after pod respins"
