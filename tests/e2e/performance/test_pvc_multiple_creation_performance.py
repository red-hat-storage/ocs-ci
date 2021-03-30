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
    Test to verify PVC creation performance
    """

    pvc_size = "1Gi"

    @pytest.fixture()
    def base_setup(self, request, interface_iterate, storageclass_factory):
        """
        A setup phase for the test

        Args:
            interface_iterate: A fixture to iterate over ceph interfaces
            storageclass_factory: A fixture to create everything needed for a
                storageclass
        """
        self.interface = interface_iterate
        self.sc_obj = storageclass_factory(self.interface)

    # @pytest.mark.parametrize(
    #     argnames=["pvc_size"],
    #     argvalues=[
    #         pytest.param(*["1Gi"], marks=pytest.mark.polarion_id("OCS-1225")),
    #         pytest.param(*["10Gi"], marks=pytest.mark.polarion_id("OCS-2010")),
    #         pytest.param(*["100Gi"], marks=pytest.mark.polarion_id("OCS-2011")),
    #         pytest.param(*["1Ti"], marks=pytest.mark.polarion_id("OCS-2008")),
    #         pytest.param(*["2Ti"], marks=pytest.mark.polarion_id("OCS-2009")),
    #     ],
    # )

    @pytest.mark.usefixtures(base_setup.__name__)
    @polarion_id("OCS-1620")
    def test_multiple_pvc_creation_measurement_performance(self, teardown_factory):
        """
        Measuring PVC creation time of 120 PVCs in 180 seconds

        Args:
            teardown_factory: A fixture used when we want a new resource that was created during the tests
                               to be removed in the teardown phase.
        Returns:

        """
        number_of_pvcs = 120
        multi_creation_time_limit = 60
        log.info("Start creating new 120 PVCs")

        pvc_objs = helpers.create_multiple_pvcs(
            sc_name=self.sc_obj.name,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            number_of_pvc=number_of_pvcs,
            size=self.pvc_size,
            burst=True,
        )
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
        total = end_time - start_time
        total_time = total.total_seconds()
        logging.info(f"{number_of_pvcs} PVCs creation time is {total_time} seconds.")

        if total_time > 60:
            raise ex.PerformanceException(
                f"{number_of_pvcs} PVCs creation time is {total_time} and "
                f"greater than {multi_creation_time_limit} seconds"
            )
        logging.info(f"{number_of_pvcs} PVCs creation time took {total_time} seconds")

    @pytest.mark.usefixtures(base_setup.__name__)
    @polarion_id("OCS-1270")
    @bugzilla("1741612")
    def test_multiple_pvc_creation_after_deletion_performance(self, teardown_factory):
        """
        Measuring PVC creation time of 75% of initial PVCs (120) in the same
        rate after deleting 75% of the initial PVCs.

        Args:
            teardown_factory: A fixture used when we want a new resource that was created during the tests
                               to be removed in the teardown phase.
        Returns:

        """
        initial_number_of_pvcs = 120
        number_of_pvcs = math.ceil(initial_number_of_pvcs * 0.75)

        log.info("Start creating new 120 PVCs")
        pvc_objs = helpers.create_multiple_pvcs(
            sc_name=self.sc_obj.name,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            number_of_pvc=initial_number_of_pvcs,
            size=self.pvc_size,
            burst=True,
        )
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
        )
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
