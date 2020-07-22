"""
Test to verify PVC creation performance
"""
import logging
import pytest
import math
import statistics
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

    def samples_creation(self,samples_num,teardown_factory, pvc_size):
        """

        :param pvc_size:
        :param teardown_factory:
        :param action:
        :param samples_num:
        :return:
        """
        time_measures = []
        for i in range(samples_num):
            log.info(f'Start creation of PVC number {i + 1}.')

            pvc_obj = helpers.create_pvc(
                sc_name=self.sc_obj.name, size=pvc_size)
            helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND)
            pvc_obj.reload()
            teardown_factory(pvc_obj)
            create_time = helpers.measure_pvc_creation_time(
                self.interface, pvc_obj.name
            )
            logging.info(f"PVC created in {create_time} seconds")

            time_measures.append(create_time)
        return time_measures


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

    @pytest.mark.parametrize(
        argnames=["pvc_size"],
        argvalues=[
            pytest.param(
                *['1Gi'], marks=pytest.mark.polarion_id("OCS-1225")
            ),
            pytest.param(
                *['10Gi'], marks=pytest.mark.polarion_id("OCS-2010")
            ),
            pytest.param(
                *['100Gi'], marks=pytest.mark.polarion_id("OCS-2011")
            ),
            pytest.param(
                *['1Ti'], marks=pytest.mark.polarion_id("OCS-2008")
            ),
            pytest.param(
                *['2Ti'], marks=pytest.mark.polarion_id("OCS-2009")
            ),
        ]
    )

    @pytest.mark.usefixtures(base_setup.__name__)
    def test_pvc_creation_measurement_performance(self, teardown_factory, pvc_size):
        """
        The test measures PVC creation times for sample_num volumes
        (limit for the creation time for pvc is defined in accepted_create_time)
        and compares the creation time of each to the accepted_create_time ( if greater - fails the test)
        If all the measures are up to the accepted value
        The test calculates .... difference between creation time of each one of the PVCs and the average
        is not more than Accepted diff ( currently 5%)
                Args:
            teardown factory : A fixture used when we want a new resource that was created during the tests
                               to be removed in the teardown phase:
            pvc_size: Size of the created PVC
         """
        samples_num = 3
        accepted_deviation_percent = 5
        accepted_create_time = 3

        create_measures = self.samples_creation(samples_num, teardown_factory, pvc_size)
        log.info(f"Current measures are {create_measures}")

        for i in range(samples_num):
            if create_measures[i] > accepted_create_time:
                raise ex.PerformanceException(
                    f"PVC creation time is {create_measures[i]} and greater than {accepted_create_time} seconds."
                )

        average = statistics.mean(create_measures)
        st_deviation = statistics.stdev(create_measures)
        log.info(f"The average creation time for the sampled {samples_num} PVCs is {average}.")

        st_deviation_percent =abs(st_deviation - average) / average * 100.0
        if st_deviation > accepted_deviation_percent:
            raise ex.PerformanceException(
            f"PVC create time deviation is {st_deviation_percent}% and is greater than the allowed {accepted_deviation_percent}%."
            )

    @pytest.mark.usefixtures(base_setup.__name__)
    @polarion_id('OCS-1620')
    def test_multiple_pvc_creation_measurement_performance(
        self, teardown_factory
    ):
        """
        Measuring PVC creation time of 120 PVCs in 180 seconds
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
            pvc_obj.reload()
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
        if total_time > 180:
            raise ex.PerformanceException(
                f"{number_of_pvcs} PVCs creation time is {total_time} and "
                f"greater than 180 seconds"
            )
        logging.info(
            f"{number_of_pvcs} PVCs creation time took {total_time} seconds"
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
