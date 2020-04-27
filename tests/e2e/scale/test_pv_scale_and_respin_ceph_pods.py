"""
PV Create with ceph pod respin & Memory Leak Test: Test the PVC limit
with 3 worker nodes create PVCs and check for memory leak
TO DO: This Test needs to be executed in Scaled setup,
Adding node scale is yet to be supported.
"""
import logging
import pytest
import threading
import time

from tests import helpers, disruption_helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pod
from ocs_ci.utility import utils
from ocs_ci.framework.testlib import scale, E2ETest, ignore_leftovers

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
            cephfs_sc_obj, self.namespace, number_of_pvc, size,
            access_modes=[constants.ACCESS_MODE_RWO, constants.ACCESS_MODE_RWX]
        )
        rbd_pvcs = helpers.create_multiple_pvc_parallel(
            rbd_sc_obj, self.namespace, number_of_pvc, size,
            access_modes=[constants.ACCESS_MODE_RWO, constants.ACCESS_MODE_RWX]
        )
        # Appending all the pvc obj to base case param for cleanup and evaluation
        self.all_pvc_obj.extend(cephfs_pvcs + rbd_pvcs)

        # Create pods with above pvc list
        cephfs_pods = helpers.create_pods_parallel(
            cephfs_pvcs, self.namespace, constants.CEPHFS_INTERFACE
        )
        rbd_rwo_pvc, rbd_rwx_pvc = ([] for i in range(2))
        for pvc_obj in rbd_pvcs:
            if pvc_obj.get_pvc_access_mode == constants.ACCESS_MODE_RWX:
                rbd_rwx_pvc.append(pvc_obj)
            else:
                rbd_rwo_pvc.append(pvc_obj)
        rbd_rwo_pods = helpers.create_pods_parallel(
            rbd_rwo_pvc, self.namespace, constants.CEPHBLOCKPOOL
        )
        rbd_rwx_pods = helpers.create_pods_parallel(
            rbd_rwx_pvc, self.namespace, constants.CEPHBLOCKPOOL,
            raw_block_pv=True
        )
        temp_pod_objs = list()
        temp_pod_objs.extend(cephfs_pods + rbd_rwo_pods)
        # Appending all the pod obj to base class param for cleanup and evaluation
        self.all_pod_obj.extend(temp_pod_objs + rbd_rwx_pods)

        # Start respective IO on all the created PODs
        threads = list()
        for pod_obj in temp_pod_objs:
            process = threading.Thread(target=pod_obj.run_io, args=('fs', '512M', ))
            process.start()
            threads.append(process)
        for pod_obj in rbd_rwx_pods:
            process = threading.Thread(target=pod_obj.run_io, args=('block', '512M', ))
            process.start()
            threads.append(process)
        for process in threads:
            process.join()

    def respin_ceph_pod(self, resource_to_delete):
        """
        Function to respin ceph pods one by one,
        delete_resource functions checks for the deleted pod back up and running

        Args:
            resource_to_delete (str): Ceph resource type to be deleted, eg: mgr/mon/osd/mds
        """
        disruption = disruption_helpers.Disruptions()
        disruption.set_resource(resource=resource_to_delete)
        no_of_resource = disruption.resource_count
        for i in range(0, no_of_resource):
            disruption.delete_resource(resource_id=i)

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
@pytest.mark.parametrize(
    argnames="resource_to_delete",
    argvalues=[
        pytest.param(
            *['mgr'], marks=[pytest.mark.polarion_id("OCS-766")]
        ),
        pytest.param(
            *['mon'], marks=[pytest.mark.polarion_id("OCS-764")]
        ),
        pytest.param(
            *['osd'], marks=[pytest.mark.polarion_id("OCS-765")]
        ),
        pytest.param(
            *['mds'], marks=[pytest.mark.polarion_id("OCS-613")]
        )
    ]
)
class TestPVSTOcsCreatePVCsAndRespinCephPods(BasePvcCreateRespinCephPods):
    """
    Class for PV scale Create Cluster with 1000 PVC, then Respin ceph pods
    Check for Memory leak, network and stats.
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
        self, namespace, storageclass, setup_fixture, resource_to_delete,
        memory_leak_function
    ):
        pvc_count_each_itr = 10
        scale_pod_count = 120
        size = '10Gi'
        test_run_time = 180
        self.all_pvc_obj, self.all_pod_obj = ([] for i in range(2))

        # Identify median memory value for each worker node
        median_dict = helpers.get_memory_leak_median_value()
        log.info(f"Median dict values for memory leak {median_dict}")

        # First Iteration call to create PVC and POD
        self.create_pvc_pod(self.rbd_sc_obj, self.cephfs_sc_obj, pvc_count_each_itr, size)
        # Re-spin the ceph pods one by one in parallel with PVC and POD creation
        while True:
            if scale_pod_count <= len(self.all_pod_obj):
                log.info(f"Create {scale_pod_count} pvc and pods")
                break
            else:
                thread1 = threading.Thread(target=self.respin_ceph_pod, args=(resource_to_delete, ))
                thread2 = threading.Thread(target=self.create_pvc_pod, args=(
                    self.rbd_sc_obj, self.cephfs_sc_obj, pvc_count_each_itr, size, ))
                thread1.start()
                thread2.start()
            thread1.join()
            thread2.join()

        # Added sleep for test case run time and for capturing memory leak after scale
        time.sleep(test_run_time)
        utils.ceph_health_check()
        helpers.memory_leak_analysis(median_dict)
