"""
Test to verify concurrent creation and deletion of multiple PVCs
"""
import logging
import time
from concurrent.futures import ThreadPoolExecutor
import pytest

from ocs_ci.framework.testlib import tier1, E2ETest
from tests.helpers import create_pvc

log = logging.getLogger(__name__)


@tier1
class TestPVCCreationPerformance(E2ETest):
    """
    Test to verify concurrent creation and deletion of multiple PVCs
    """
    num_of_pvcs = 100
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
    def test_pvc_creation_measurement_performance(self):
        """
        Measuring PVC creation time
        """
        executor = ThreadPoolExecutor(max_workers=1)

        log.info('Start creating new PVCs')

        new_pvc_obj = create_pvc(
            sc_name=self.sc_obj.name, size=self.pvc_size, wait=False,
            measure_time=True
        )
        from ipdb import set_trace;set_trace()
