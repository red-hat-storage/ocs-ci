"""
Scale TC to perform PVC Create and Delete in parallel
"""
import logging
import pytest
import random
import time
import threading

from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pod
from ocs_ci.framework.testlib import scale, E2ETest, ignore_leftovers

log = logging.getLogger(__name__)


class BasePvcPodCreateDelete(E2ETest):
    """
    Base Class to create/delete PVC and POD
    """

    def create_pvc_pod(self, rbd_sc_obj, cephfs_sc_obj, number_of_pvc, size, start_io):
        """
        Function to create multiple PVC of different type and created pods on them.

        Args:
            rbd_sc_obj (obj_dict): rbd storageclass object
            cephfs_sc_obj (obj_dict): cephfs storageclass object
            number_of_pvc (int): pvc count to be created for each types
            size (str): size of each pvc to be created eg: '10Gi'
            start_io (boolean): Ture to start and False not to start IO
        """
        log.info(f"Create {number_of_pvc} pvcs and pods")
        self.delete_pod_count = round(number_of_pvc / 2)
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
        # Appending all the pod obj to base case param for cleanup and evaluation
        self.all_pod_obj.extend(temp_pod_objs + rbd_rwx_pods)

        # IO will start based on TC requirement
        if start_io:
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

    def delete_pvc_pod(self):
        """
        Function to delete pvc and pod based on the delete pod count.
        """
        log.info(f"Delete {self.delete_pod_count} pods and respective pvcs")
        temp_pod_list = random.choices(self.all_pod_obj, k=self.delete_pod_count)
        temp_pvc_list = []
        for pod_obj in temp_pod_list:
            list_counter_for_pvc = 0
            for pvc_obj in self.all_pvc_obj:
                if pvc_obj is not None:
                    if type(pvc_obj) is list:
                        for pvc_ in pvc_obj:
                            if pod.get_pvc_name(pod_obj) == pvc_.name:
                                temp_pvc_list.append(pvc_)
                                log.info(f"Deleting pvc {pvc_.name}")
                                self.all_pvc_obj[list_counter_for_pvc].remove(pvc_)
                    else:
                        if pod.get_pvc_name(pod_obj) == pvc_obj.name:
                            temp_pvc_list.append(pvc_obj)
                            log.info(f"Deleting pvc {pvc_obj.name}")
                            self.all_pvc_obj.remove(pvc_obj)
                list_counter_for_pvc += 1
            log.info(f"Deleting pod {pod_obj.name}")
            if pod_obj in self.all_pod_obj:
                self.all_pod_obj.remove(pod_obj)
        helpers.delete_objs_parallel(temp_pod_list)
        helpers.delete_objs_parallel(temp_pvc_list)

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
    argnames="start_io",
    argvalues=[
        pytest.param(*[False], marks=pytest.mark.polarion_id("OCS-682")),
        pytest.param(
            *[True],
            marks=[pytest.mark.polarion_id("OCS-679"), pytest.mark.bugzilla("1768031")],
        ),
    ],
)
class TestPVSTOcsCreateDeletePVCsWithAndWithoutIO(BasePvcPodCreateDelete):
    """
    Class for TC OCS-682 & OCS-679 Create & Delete Cluster PVC with and without IO
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

    def test_pv_scale_out_create_delete_pvcs_with_and_without_io(
        self,
        namespace,
        storageclass,
        setup_fixture,
        start_io,
    ):
        pvc_count_each_itr = 10
        scale_pod_count = 120
        size = "10Gi"
        test_run_time = 180
        self.all_pvc_obj, self.all_pod_obj = ([] for i in range(2))
        self.delete_pod_count = 0

        # First Iteration call to create PVC and POD
        self.create_pvc_pod(
            self.rbd_sc_obj, self.cephfs_sc_obj, pvc_count_each_itr, size, start_io
        )

        # Continue to iterate till the scale pvc limit is reached
        # Also continue to perform create and delete of pod, pvc in parallel
        while True:
            if scale_pod_count <= len(self.all_pod_obj):
                log.info(f"Created {scale_pod_count} pvc and pods")
                break
            else:
                log.info(
                    f"Create {pvc_count_each_itr} and in parallel delete {self.delete_pod_count}"
                    " pods & pvc"
                )
                thread1 = threading.Thread(target=self.delete_pvc_pod, args=())
                thread2 = threading.Thread(
                    target=self.create_pvc_pod,
                    args=(
                        self.rbd_sc_obj,
                        self.cephfs_sc_obj,
                        pvc_count_each_itr,
                        size,
                        start_io,
                    ),
                )
                thread1.start()
                thread2.start()
            thread1.join()
            thread2.join()
