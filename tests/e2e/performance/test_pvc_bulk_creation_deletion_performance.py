"""
Test to verify PVC creation performance
"""
import logging
import pytest
import math
import ocs_ci.ocs.exceptions as ex
import ocs_ci.ocs.resources.pvc as pvc
from concurrent.futures import ThreadPoolExecutor
from ocs_ci.framework.testlib import performance, E2ETest, polarion_id, bugzilla
from ocs_ci.helpers import helpers
from ocs_ci.ocs import defaults, constants

log = logging.getLogger(__name__)


@performance
class TestPVCCreationPerformance(E2ETest):
    """
    Test to verify PVC creation and deletion performance
    """

    pvc_size = "1Gi"

    @pytest.fixture()
    def base_setup(self, interface_type, storageclass_factory):
        """
        A setup phase for the test

        Args:
            interface_type: Interface type
            storageclass_factory: A fixture to create everything needed for a
                storageclass
        """
        self.interface = interface_type
        self.sc_obj = storageclass_factory(self.interface)

    @pytest.mark.parametrize(
        argnames=["interface_type", "bulk_size"],
        argvalues=[
            pytest.param(
                *[constants.CEPHBLOCKPOOL, 60],
                marks=[pytest.mark.performance],
            ),
            pytest.param(
                *[constants.CEPHBLOCKPOOL, 240],
                marks=[pytest.mark.performance],
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM, 60],
                marks=[pytest.mark.performance],
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM, 240],
                marks=[pytest.mark.performance],
            ),
        ],
    )
    @pytest.mark.usefixtures(base_setup.__name__)
    @polarion_id("OCS-1620")
    def test_bulk_pvc_creation_deletion_measurement_performance(
        self, teardown_factory, bulk_size
    ):

        """
        Measuring PVC creation and deletion time of bulk_size (120/720) PVCs

        Args:
            teardown_factory: A fixture used when we want a new resource that was created during the tests
                               to be removed in the teardown phase.
            bulk_size: Size of the bulk to be tested
        Returns:

        """
        bulk_creation_time_limit = bulk_size / 2
        log.info(f"Start creating new {bulk_size} PVCs")

        pvc_objs, yaml_creation_dir = helpers.create_multiple_pvcs(
            sc_name=self.sc_obj.name,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            number_of_pvc=bulk_size,
            size=self.pvc_size,
            burst=True,
        )
        logging.info(f"PVC creation dir is {yaml_creation_dir}")

        for pvc_obj in pvc_objs:
            pvc_obj.reload()
            teardown_factory(pvc_obj)
        with ThreadPoolExecutor(max_workers=5) as executor:
            for pvc_obj in pvc_objs:
                executor.submit(
                    helpers.wait_for_resource_state, pvc_obj, constants.STATUS_BOUND
                )

                executor.submit(pvc_obj.reload)

        start_time = helpers.get_provision_time(
            self.interface, pvc_objs, status="start"
        )
        end_time = helpers.get_provision_time(self.interface, pvc_objs, status="end")
        total_time = (end_time - start_time).total_seconds()
        logging.info(f"{bulk_size} Bulk PVCs creation time is {total_time} seconds.")

        if total_time > bulk_creation_time_limit:
            raise ex.PerformanceException(
                f"{bulk_size} Bulk PVCs creation time is {total_time} and "
                f"greater than {bulk_creation_time_limit} seconds"
            )

        pv_names_list = []
        for pvc_obj in pvc_objs:
            pv_names_list.append(pvc_obj.backed_pv)

        logging.info(f"Starting to delete bulk of {bulk_size} PVCs")
        helpers.delete_bulk_pvcs(yaml_creation_dir, pv_names_list)
        logging.info(f"Deletion of bulk of {bulk_size} PVCs successfully completed")

        start_deletion_time = helpers.get_pvc_bulk_deletion_time(
            self.interface, pv_names_list, status="start"
        )
        end_deletion_time = helpers.get_pvc_bulk_deletion_time(
            self.interface, pv_names_list, status="end"
        )

        total_deletion_time = (end_deletion_time - start_deletion_time).total_seconds()
        logging.info(
            f"{bulk_size} Bulk PVCs deletion time is {total_deletion_time} seconds."
        )

    @pytest.fixture()
    def base_setup_creation_after_deletion(
        self, interface_iterate, storageclass_factory
    ):
        """
        A setup phase for the test

        Args:
            interface_iterate: A fixture to iterate over ceph interfaces
            storageclass_factory: A fixture to create everything needed for a
                storageclass
        """
        self.interface = interface_iterate
        self.sc_obj = storageclass_factory(self.interface)

    @pytest.mark.usefixtures(base_setup_creation_after_deletion.__name__)
    @polarion_id("OCS-1270")
    @bugzilla("1741612")
    def test_bulk_pvc_creation_after_deletion_performance(self, teardown_factory):
        """
        Measuring PVC creation time of bulk of 75% of initial PVC bulk (120) in the same
        rate after deleting ( serial deletion) 75% of the initial PVCs.

        Args:
            teardown_factory: A fixture used when we want a new resource that was created during the tests
                               to be removed in the teardown phase.
        Returns:

        """
        initial_number_of_pvcs = 120
        number_of_pvcs = math.ceil(initial_number_of_pvcs * 0.75)

        log.info(f"Start creating new {initial_number_of_pvcs} PVCs in a bulk")
        pvc_objs = helpers.create_multiple_pvcs(
            sc_name=self.sc_obj.name,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            number_of_pvc=initial_number_of_pvcs,
            size=self.pvc_size,
            burst=True,
        )[0]
        for pvc_obj in pvc_objs:
            teardown_factory(pvc_obj)
        with ThreadPoolExecutor() as executor:
            for pvc_obj in pvc_objs:
                executor.submit(
                    helpers.wait_for_resource_state, pvc_obj, constants.STATUS_BOUND
                )

                executor.submit(pvc_obj.reload)
        log.info("Deleting 75% of the PVCs - 90 PVCs")
        assert pvc.delete_pvcs(
            pvc_objs[:number_of_pvcs], True
        ), "Deletion of 75% of PVCs failed"
        log.info("Re-creating the 90 PVCs")
        pvc_objs = helpers.create_multiple_pvcs(
            sc_name=self.sc_obj.name,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            number_of_pvc=number_of_pvcs,
            size=self.pvc_size,
            burst=True,
        )[0]
        start_time = helpers.get_provision_time(
            self.interface, pvc_objs, status="start"
        )
        end_time = helpers.get_provision_time(self.interface, pvc_objs, status="end")
        total = end_time - start_time
        total_time = total.total_seconds()
        logging.info(f"Deletion time of {number_of_pvcs} is {total_time} seconds.")

        if total_time > 50:
            raise ex.PerformanceException(
                f"{number_of_pvcs} PVCs creation (after initial deletion of "
                f"75% of PVCs) time is {total_time} and greater than 50 seconds."
            )
        logging.info(f"{number_of_pvcs} PVCs creation time took less than a 50 seconds")
