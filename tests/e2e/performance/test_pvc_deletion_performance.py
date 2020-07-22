"""
Test to verify PVC deletion performance
"""
import logging
import pytest
import ocs_ci.ocs.exceptions as ex
from ocs_ci.framework.testlib import (
    performance, E2ETest
)
from tests import helpers
from ocs_ci.ocs import constants

from ocs_ci.utility.performance_dashboard import push_to_pvc_time_dashboard

log = logging.getLogger(__name__)


@performance
class TestPVCDeletionPerformance(E2ETest):
    """
    Test to verify PVC deletion performance
    """

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
                *['1Gi'], marks=pytest.mark.polarion_id("OCS-2005")
            ),
            pytest.param(
                *['10Gi'], marks=pytest.mark.polarion_id("OCS-2006")
            ),
            pytest.param(
                *['100Gi'], marks=pytest.mark.polarion_id("OCS-2007")
            ),
            pytest.param(
                *['1Ti'], marks=pytest.mark.polarion_id("OCS-2003")
            ),
            pytest.param(
                *['2Ti'], marks=pytest.mark.polarion_id("OCS-2004")
            ),
        ]
    )
    @pytest.mark.usefixtures(base_setup.__name__)
    def test_pvc_deletion_measurement_performance(self, teardown_factory, pvc_size):
        """
        Measuring PVC deletion time is within supported limits
        """
        logging.info('Start creating new PVC')

        pvc_obj = helpers.create_pvc(
            sc_name=self.sc_obj.name, size=pvc_size
        )
        helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND)
        pvc_obj.reload()
        pv_name = pvc_obj.backed_pv
        pvc_reclaim_policy = pvc_obj.reclaim_policy
        teardown_factory(pvc_obj)
        pvc_obj.delete()
        logging.info('Start deletion of PVC')
        pvc_obj.ocp.wait_for_delete(pvc_obj.name)
        if pvc_reclaim_policy == constants.RECLAIM_POLICY_DELETE:
            helpers.validate_pv_delete(pvc_obj.backed_pv)
        delete_time = helpers.measure_pvc_deletion_time(
            self.interface, pv_name
        )
        # Deletion time for CephFS PVC is a little over 3 seconds
        deletion_time = 4 if self.interface == constants.CEPHFILESYSTEM else 3
        logging.info(f"PVC deleted in {delete_time} seconds")
        if delete_time > deletion_time:
            raise ex.PerformanceException(
                f"PVC deletion time is {delete_time} and greater than {deletion_time} second"
            )
        push_to_pvc_time_dashboard(self.interface,"deletion",delete_time)
