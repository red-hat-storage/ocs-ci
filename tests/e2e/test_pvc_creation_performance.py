"""
Test to verify PVC creation performance
"""
import logging
import pytest

from ocs_ci.framework.testlib import tier1, E2ETest, polarion_id, bugzilla
from tests.helpers import create_pvc, measure_pvc_creation_time

log = logging.getLogger(__name__)


@tier1
class TestPVCCreationPerformance(E2ETest):
    """
    Test to verify PVC creation performance
    """
    pvc_size = '1Gi'

    @pytest.fixture()
    def base_setup(self, request, storageclass_factory):
        """
        A setup phase for the test

        Args:
            storageclass_factory: A fixture to create everything needed for a
                storageclass
        """
        self.sc_obj = storageclass_factory()

    @pytest.mark.usefixtures(base_setup.__name__)
    @polarion_id('OCS-1225')
    @bugzilla('1740139')
    def test_pvc_creation_measurement_performance(self):
        """
        Measuring PVC creation time
        """
        log.info('Start creating new PVC')

        pvc_obj = create_pvc(sc_name=self.sc_obj.name, size=self.pvc_size)
        create_time = measure_pvc_creation_time('CephBlockPool', pvc_obj.name)
        if create_time > 1:
            raise AssertionError(
                f"PVC creation time is {create_time} and greater than 1 second"
            )
        logging.info("PVC creation took less than a 1 second")
