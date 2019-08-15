"""
Test to verify PVC creation performance
"""
import logging
import pytest

from ocs_ci.framework.testlib import tier1, E2ETest, polarion_id, bugzilla
from tests.helpers import (
    create_pvc, measure_pvc_creation_time, create_multiple_pvcs,
    wait_for_resource_state)
from ocs_ci.ocs import defaults, constants

log = logging.getLogger(__name__)


@tier1
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

        pvc_obj = create_pvc(sc_name=self.sc_obj.name, size=self.pvc_size)
        teardown_factory(pvc_obj)
        create_time = measure_pvc_creation_time(self.interface, pvc_obj.name)
        if create_time > 1:
            raise AssertionError(
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

        pvc_objs = create_multiple_pvcs(
            sc_name=self.sc_obj.name,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            number_of_pvc=number_of_pvcs,
            size=self.pvc_size,
            wait=False
        )
        for pvc_obj in pvc_objs:
            teardown_factory(pvc_obj)
        for pvc_obj in pvc_objs:
            wait_for_resource_state(pvc_obj, constants.STATUS_BOUND)
        start_time = measure_pvc_creation_time(
            self.interface, pvc_objs[0].name, return_start_time=True
        )
        end_time = measure_pvc_creation_time(
            self.interface, pvc_objs[number_of_pvcs - 1].name,
            return_end_time=True
        )
        total = end_time - start_time
        total_time = total.total_seconds()
        if total_time > 60:
            raise AssertionError(
                f"{number_of_pvcs} PVCs creation time is {total_time} and "
                f"greater than 60 seconds"
            )
        logging.info(
            f"{number_of_pvcs} PVCs creation time took less than a 60 seconds"
        )
