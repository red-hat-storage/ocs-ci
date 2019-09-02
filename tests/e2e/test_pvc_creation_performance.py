"""
Test to verify PVC creation performance
"""
import logging
import pytest
import math
import ocs_ci.ocs.exceptions as ex
import ocs_ci.ocs.resources.pvc as pvc
from concurrent.futures import ThreadPoolExecutor
from ocs_ci.framework.testlib import (
    performance, E2ETest, polarion_id, bugzilla
)
from tests import helpers
from ocs_ci.ocs import defaults, constants


log = logging.getLogger(__name__)


@performance
class TestPVCCreationPerformance(E2ETest):
    """
    Test to verify PVC creation performance
    """
    pvc_size = '1Gi'

    @pytest.fixture()
    def base_setup(
        self, request, interface_iterate, storageclass_factory
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

    @pytest.mark.usefixtures(base_setup.__name__)
    @polarion_id('OCS-1225')
    @bugzilla('1740139')
    def test_pvc_creation_measurement_performance(self, teardown_factory):
        """
        Measuring PVC creation time
        """
        log.info('Start creating new PVC')

        pvc_obj = helpers.create_pvc(
            sc_name=self.sc_obj.name, size=self.pvc_size
        )
        helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND)
        pvc_obj.reload()
        teardown_factory(pvc_obj)
        create_time = helpers.measure_pvc_creation_time(
            self.interface, pvc_obj.name
        )
        if create_time > 1:
            raise ex.PerformanceException(
                f"PVC creation time is {create_time} and greater than 1 second"
            )
        logging.info("PVC creation took less than a 1 second")

    @pytest.mark.usefixtures(base_setup.__name__)
    @polarion_id('OCS-1620')
    @bugzilla('1741612')
    def test_multiple_pvc_creation_measurement_performance(
        self, teardown_factory
    ):
        """
        Measuring PVC creation time of 120 PVCs in 60 seconds
        """
        number_of_pvcs = 120
        log.info('Start creating new 120 PVCs')

        pvc_objs = helpers.create_multiple_pvcs(
            sc_name=self.sc_obj.name,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            number_of_pvc=number_of_pvcs,
            size=self.pvc_size,
        )
        for pvc_obj in pvc_objs:
            teardown_factory(pvc_obj)
        with ThreadPoolExecutor(max_workers=5) as executor:
            for pvc_obj in pvc_objs:
                executor.submit(
                    helpers.wait_for_resource_state, pvc_obj,
                    constants.STATUS_BOUND
                )

                executor.submit(pvc_obj.reload)
        start_time = helpers.get_start_creation_time(
            self.interface, pvc_objs[0].name
        )
        end_time = helpers.get_end_creation_time(
            self.interface, pvc_objs[number_of_pvcs - 1].name,
        )
        total = end_time - start_time
        total_time = total.total_seconds()
        if total_time > 60:
            raise ex.PerformanceException(
                f"{number_of_pvcs} PVCs creation time is {total_time} and "
                f"greater than 60 seconds"
            )
        logging.info(
            f"{number_of_pvcs} PVCs creation time took less than a 60 seconds"
        )

    @pytest.mark.usefixtures(base_setup.__name__)
    @polarion_id('OCS-1270')
    @bugzilla('1741612')
    def test_multiple_pvc_creation_after_deletion_performance(
        self, teardown_factory
    ):
        """
        Measuring PVC creation time of 75% of initial PVCs (120) in the same
        rate after deleting 75% of the initial PVCs
        """
        initial_number_of_pvcs = 120
        number_of_pvcs = math.ceil(initial_number_of_pvcs * 0.75)

        log.info('Start creating new 120 PVCs')
        pvc_objs = helpers.create_multiple_pvcs(
            sc_name=self.sc_obj.name,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            number_of_pvc=initial_number_of_pvcs,
            size=self.pvc_size,
        )
        for pvc_obj in pvc_objs:
            teardown_factory(pvc_obj)
        with ThreadPoolExecutor() as executor:
            for pvc_obj in pvc_objs:
                executor.submit(
                    helpers.wait_for_resource_state, pvc_obj,
                    constants.STATUS_BOUND
                )

                executor.submit(pvc_obj.reload)
        log.info('Deleting 75% of the PVCs - 90 PVCs')
        assert pvc.delete_pvcs(pvc_objs[:number_of_pvcs], True), (
            "Deletion of 75% of PVCs failed"
        )
        log.info('Re-creating the 90 PVCs')
        pvc_objs = helpers.create_multiple_pvcs(
            sc_name=self.sc_obj.name,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            number_of_pvc=number_of_pvcs,
            size=self.pvc_size,
        )
        start_time = helpers.get_start_creation_time(
            self.interface, pvc_objs[0].name
        )
        end_time = helpers.get_end_creation_time(
            self.interface, pvc_objs[number_of_pvcs - 1].name,
        )
        total = end_time - start_time
        total_time = total.total_seconds()
        if total_time > 45:
            raise ex.PerformanceException(
                f"{number_of_pvcs} PVCs creation (after initial deletion of "
                f"75%) time is {total_time} and greater than 45 seconds"
            )
        logging.info(
            f"{number_of_pvcs} PVCs creation time took less than a 45 seconds"
        )
