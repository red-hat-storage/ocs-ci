"""
Test to measure pvc scale creation & deletion time.
Total pvc count would be 1500
"""
import logging
import random
import pytest
import threading

from tests import helpers
from ocs_ci.framework.testlib import scale, E2ETest, polarion_id
from ocs_ci.ocs import constants

log = logging.getLogger(__name__)


@scale
class TestPVCCreationDeletionScale(E2ETest):
    """
    Base class for PVC scale creation and deletion
    """
    @pytest.fixture()
    def namespace(self, project_factory):
        """
        Create a new project
        """
        proj_obj = project_factory()
        self.namespace = proj_obj.namespace

    @pytest.mark.parametrize(
        argnames=["access_mode", "interface"],
        argvalues=[
            pytest.param(
                *[constants.ACCESS_MODE_RWO, constants.CEPHBLOCKPOOL],
                marks=pytest.mark.polarion_id("OCS-1225")
            ),
            pytest.param(
                *[constants.ACCESS_MODE_RWX, constants.CEPHBLOCKPOOL],
                marks=pytest.mark.polarion_id("OCS-2010")
            ),
            pytest.param(
                *[constants.ACCESS_MODE_RWX, constants.CEPHFS_INTERFACE],
                marks=pytest.mark.polarion_id("OCS-2008")
            ),
        ]
    )
    @pytest.mark.usefixtures(namespace.__name__)
    def test_multiple_pvc_creation_deletion_scale(
        self,
        namespace,
        access_mode,
        interface
    ):
        """
        Measuring PVC creation time while scaling PVC
        Measure PVC deletion time after creation test
        """
        number_of_pvc = 5
        log.info(
            f"Start creating {access_mode}-{interface} {number_of_pvc} PVC"
        )

        if interface == constants.CEPHBLOCKPOOL:
            self.sc_obj = constants.DEFAULT_STORAGECLASS_RBD
        elif interface == constants.CEPHFS_INTERFACE:
            self.sc_obj = constants.DEFAULT_STORAGECLASS_CEPHFS

        # Create PVC
        pvc_objs = helpers.create_multiple_pvcs(
            sc_name=self.sc_obj,
            namespace=self.namespace,
            number_of_pvc=number_of_pvc,
            size=f"{random.randrange(5, 105, 5)}Gi",
            access_mode=access_mode
        )

        # Check for PVC status using threads
        threads = list()

        self.start_pvc_threads(pvc_objs, threads)

        # Get pvc_name, require pvc_name to fetch creation time data from log
        pvc_name_list, pv_name_list = ([] for i in range(2))

        self.set_pvc_names(pvc_objs, pvc_name_list, pv_name_list)

        # Get PVC creation time
        pvc_create_time = helpers.measure_pvc_creation_time_bulk(
            interface=interface, pvc_name_list=pvc_name_list
        )

        inner_data = f"{self.sc_obj}-{access_mode}"

        helpers.make_csv_output(pvc_create_time,
            {
                'adjective': 'Create',
                'inner_data': inner_data,
                'suffix': 'create'
            })
        # Delete PVC
        self.delete_pvcs(pvc_objs)

        # Get PVC deletion time
        pvc_deletion_time = helpers.measure_pv_deletion_time_bulk(
            interface=interface, pv_name_list=pv_name_list
        )

        helpers.make_csv_output(pvc_deletion_time,
            {
                'adjective': 'Delete',
                'inner_data': inner_data,
                'suffix': 'delete'
            })

    # To do: This should get replaced by calls to
    # helpers.create_pvcs_parallel
    def start_pvc_threads(self, pvc_objs, threads):
        """
        Start pvc threads
        
        Args:
            pvc_objs (list): pvcs to be started
        """
        for obj in pvc_objs:
            process = threading.Thread(
                target=helpers.wait_for_resource_state,
                args=(obj, constants.STATUS_BOUND, )
            )
            process.start()
            threads.append(process)
        for process in threads:
            process.join()

    # To do:  This should either get replaced by calls to
    # helpers.delete_objs_parallel or if it's considered
    # simple enough, moved to helpers.
    def delete_pvcs(self, pvc_objs):
        """
        Delete pvcs
        
        Args:
            pvc_objs (list): pvcs to be deleted

        """
        for obj in pvc_objs:
            obj.delete()
            obj.ocp.wait_for_delete(obj.name)

    def set_pvc_names(self, pvc_objs, pvc_name, pv_name):
        """
        Set pvc_name and pv_name lists from the pvc_obj data:

        Args:
            pvc_obj (list): pvc object list
            pvc_name (list): pvc list
            pv_name (list): pv list

        Output:
            pvc_name and pv_name lists are set

        """
        for lpvc_obj in pvc_objs:
            lpvc_obj.reload()
            pvc_name.append(lpvc_obj.name)
            pv_name.append(lpvc_obj.backed_pv)

    def setup_pvc_threads(self, sc_obj, storage_obj, threads):
        """
        Common pvc thread setup routine. Threads are started and appended
        to a thread list to be joined later

        Args:
            sc_obj (str) -- Either RBD or CEPHFS default storage class
            storage_obj (list) -- pvc storage objects
            threads (list) -- thread list (external so that all threads can
                              be joined later)

        Output:
            threads list has new processes appended to it.

        """
        for mode in [constants.ACCESS_MODE_RWO, constants.ACCESS_MODE_RWX]:
            storage_obj.extend(helpers.create_multiple_pvcs(
                sc_name=sc_obj, namespace=self.namespace,
                number_of_pvc=self.number_of_pvc,
                size=f"{random.randrange(5, 105, 5)}Gi", access_mode=mode)
            )
        self.start_pvc_threads(storage_obj, threads)


    def make_csv_output_all_4_type(self,
        helper_func,
        fs_list,
        rbd_list,
        parm_name,
        csv_text
    ):
        """
        Common csv_output routine

        Args:
            helper_function (func): helper function used to measure
                                    creation or deletion times
            fs_list (list): cephfs keyword parameters
            rbd_list (list): rbd keyword parameters
            parm_name (str): name of keyword parameter
            csv_text (dict): Text fields for file names and log messages

        Output:
            csv files in /tmp are created (from make_csv_output call)

        """
        # Get PVC creation or deletion time
        fs_named_parm = {parm_name: fs_list}
        fs_pvc_func_time = helper_func(
            interface=constants.CEPHFS_INTERFACE, **fs_named_parm
        )
        rbd_named_parm = {parm_name: rbd_list}
        rbd_pvc_func_time = helper_func(
            interface=constants.CEPHBLOCKPOOL, **rbd_named_parm
        )
        fs_pvc_func_time.update(rbd_pvc_func_time)
        helpers.make_csv_output(fs_pvc_func_time, csv_text)


    @polarion_id('OCS-1885')
    @pytest.mark.usefixtures(namespace.__name__)
    def test_all_4_type_pvc_creation_deletion_scale(self, namespace):
        """
        Measuring PVC creation time while scaling PVC of all 4 types,
        Total 1500 PVCs will be created, i.e. 375 each pvc type
        Measure PVC deletion time in scale env
        """
        self.number_of_pvc = 5
        log.info(f"Start creating {self.number_of_pvc} PVC of all 4 types")

        cephfs_sc_obj = constants.DEFAULT_STORAGECLASS_CEPHFS
        rbd_sc_obj = constants.DEFAULT_STORAGECLASS_RBD
        threads = list()

        # Create all 4 types of PVC
        fs_pvc_obj, rbd_pvc_obj = ([] for i in range(2))
        self.setup_pvc_threads(cephfs_sc_obj, fs_pvc_obj, threads)
        self.setup_pvc_threads(rbd_sc_obj, rbd_pvc_obj, threads)

        # Get pvc_name, require pvc_name to fetch creation time data from log
        fs_pvc_name, rbd_pvc_name = ([] for i in range(2))
        fs_pv_name, rbd_pv_name = ([] for i in range(2))

        self.set_pvc_names(fs_pvc_obj, fs_pvc_name, fs_pv_name)
        self.set_pvc_names(rbd_pvc_obj, rbd_pvc_name, rbd_pv_name)

        self.make_csv_output_all_4_type(
            helpers.measure_pvc_creation_time_bulk,
            fs_pvc_name,
            rbd_pvc_name,
            'pvc_name_list',
            {
                'adjective': 'Create',
                'inner_data': 'Creation',
                'prefix': 'All-type-PVC',
                'suffix': 'Scale'
            }
        )

        # Delete PVC
        pvc_objs = fs_pvc_obj + rbd_pvc_obj

        self.delete_pvcs(pvc_objs)

        self.make_csv_output_all_4_type(
            helpers.measure_pv_deletion_time_bulk,
            fs_pv_name,
            rbd_pv_name,
            'pv_name_list',
            {
                'adjective': 'Delete',
                'inner_data': 'Deletion',
                'prefix': 'All-type-PVC',
                'suffix': 'Scale'
            }
        )
