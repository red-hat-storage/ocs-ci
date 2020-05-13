"""
Test to measure pvc scale creation & deletion time. Total pvc count would be 1500
"""
import logging
import random
import pytest

from tests import helpers
from ocs_ci.framework.testlib import scale, E2ETest, polarion_id
from ocs_ci.ocs import constants
from ocs_ci.utility.utils import ocsci_log_path

log = logging.getLogger(__name__)


def actions_in_scale():
    # TO DO: All the tasks that should be run while the large number of pvcs
    # are open should go here.
    log.info("in actions_in_scale")


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
    def test_multiple_pvc_creation_deletion_scale(self, namespace, access_mode, interface):
        """
        Measuring PVC creation time while scaling PVC
        Measure PVC deletion time after creation test
        """
        number_of_pvc = 1500
        log.info(f"Start creating {access_mode}-{interface} {number_of_pvc} PVC")

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
        helpers.handle_threading(
            pvc_objs,
            (lambda x: helpers.wait_for_resource_state),
            (lambda x: (x, constants.STATUS_BOUND))
        )

        # Get pvc_name, require pvc_name to fetch creation time data from log
        helpers.handle_threading(
            pvc_objs,
            (lambda x: x.reload)
        )

        pvc_name_list = [x.name for x in pvc_objs]
        pv_name_list = [x.backed_pv for x in pvc_objs]
        helpers.handle_threading(
            pvc_objs,
            (lambda x: x.name)
        )
        helpers.handle_threading(
            pvc_objs,
            (lambda x: x.backed_pv)
        )

        # Get PVC creation time
        pvc_create_time = helpers.measure_pvc_creation_time_bulk(
            interface=interface, pvc_name_list=pvc_name_list
        )

        log_path = f"{ocsci_log_path()}/{self.sc_obj}-{access_mode}"
        helpers.write_csv_data(
            pvc_create_time,
            f"{log_path}-creation-time.csv",
            "Create"
        )

        actions_in_scale()

        # Delete PVC
        helpers.delete_objs(pvc_objs)

        # Get PVC deletion time
        pvc_deletion_time = helpers.measure_pv_deletion_time_bulk(
            interface=interface, pv_name_list=pv_name_list
        )

        # Update result to csv file.
        helpers.write_csv_data(
            pvc_deletion_time,
            f"{log_path}-deletion-time.csv",
            "Delete"
        )

    @polarion_id('OCS-1885')
    @pytest.mark.usefixtures(namespace.__name__)
    def test_all_4_type_pvc_creation_deletion_scale(self, namespace):
        """
        Measuring PVC creation time while scaling PVC of all 4 types, Total 1500 PVCs
        will be created, i.e. 375 each pvc type
        Measure PVC deletion time in scale env
        """
        number_of_pvc = 375
        log.info(f"Start creating {number_of_pvc} PVC of all 4 types")

        cephfs_sc_obj = constants.DEFAULT_STORAGECLASS_CEPHFS
        rbd_sc_obj = constants.DEFAULT_STORAGECLASS_RBD

        # Create all 4 types of PVC
        fs_pvc_obj, rbd_pvc_obj = ([] for i in range(2))
        for mode in [constants.ACCESS_MODE_RWO, constants.ACCESS_MODE_RWX]:
            fs_pvc_obj.extend(helpers.create_multiple_pvcs(
                sc_name=cephfs_sc_obj, namespace=self.namespace, number_of_pvc=number_of_pvc,
                size=f"{random.randrange(5, 105, 5)}Gi", access_mode=mode)
            )
            rbd_pvc_obj.extend(helpers.create_multiple_pvcs(
                sc_name=rbd_sc_obj, namespace=self.namespace, number_of_pvc=number_of_pvc,
                size=f"{random.randrange(5, 105, 5)}Gi", access_mode=mode)
            )

        # Check for PVC status using threads
        helpers.handle_threading(
            fs_pvc_obj,
            (lambda x: helpers.wait_for_resource_state),
            (lambda x: (x, constants.STATUS_BOUND))
        )
        helpers.handle_threading(
            rbd_pvc_obj,
            (lambda x: helpers.wait_for_resource_state),
            (lambda x: (x, constants.STATUS_BOUND))
        )

        # Get pvc_name, require pvc_name to fetch creation time data from log
        helpers.handle_threading(
            fs_pvc_obj,
            (lambda x: x.reload)
        )
        helpers.handle_threading(
            rbd_pvc_obj,
            (lambda x: x.reload)
        )

        fs_pvc_name = [x.name for x in fs_pvc_obj]
        fs_pv_name = [x.backed_pv for x in fs_pvc_obj]
        rbd_pvc_name = [x.name for x in rbd_pvc_obj]
        rbd_pv_name = [x.backed_pv for x in rbd_pvc_obj]
        helpers.handle_threading(
            fs_pvc_obj,
            (lambda x: x.name)
        )
        helpers.handle_threading(
            rbd_pvc_obj,
            (lambda x: x.name)
        )
        helpers.handle_threading(
            fs_pvc_obj,
            (lambda x: x.backed_pv)
        )
        helpers.handle_threading(
            rbd_pvc_obj,
            (lambda x: x.backed_pv)
        )

        # Get PVC creation time
        fs_pvc_create_time = helpers.measure_pvc_creation_time_bulk(
            interface=constants.CEPHFS_INTERFACE, pvc_name_list=fs_pvc_name
        )
        rbd_pvc_create_time = helpers.measure_pvc_creation_time_bulk(
            interface=constants.CEPHBLOCKPOOL, pvc_name_list=rbd_pvc_name
        )
        fs_pvc_create_time.update(rbd_pvc_create_time)

        log_path = f"{ocsci_log_path()}/All-type-PVC"
        helpers.write_csv_data(
            fs_pvc_create_time,
            f"{log_path}-creation-time.csv",
            "Create"
        )
        actions_in_scale()

        # Delete PVC
        pvc_objs = fs_pvc_obj + rbd_pvc_obj
        helpers.delete_objs(pvc_objs)

        # Get PVC deletion time
        fs_pvc_deletion_time = helpers. measure_pv_deletion_time_bulk(
            interface=constants.CEPHFS_INTERFACE, pv_name_list=fs_pv_name
        )
        rbd_pvc_deletion_time = helpers.measure_pv_deletion_time_bulk(
            interface=constants.CEPHBLOCKPOOL, pv_name_list=rbd_pv_name
        )
        fs_pvc_deletion_time.update(rbd_pvc_deletion_time)

        # TODO: Update below code with google API, to record value in spreadsheet
        # TODO: For now observing Google API limit to write more than 100 writes
        helpers.write_csv_data(
            fs_pvc_deletion_time,
            f"{log_path}-deletion-time.csv",
            "Delete"
        )
