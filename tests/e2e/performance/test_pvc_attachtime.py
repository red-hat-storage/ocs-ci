import logging
import pytest
from ocs_ci.ocs import constants
import ocs_ci.ocs.exceptions as ex
from ocs_ci.framework.testlib import E2ETest, performance
from tests.helpers import pod_start_time

log = logging.getLogger(__name__)


@performance
@pytest.mark.parametrize(
    argnames=["interface"],
    argvalues=[
        pytest.param(
            constants.CEPHBLOCKPOOL, marks=pytest.mark.polarion_id("OCS-2044")
        ),
        pytest.param(
            constants.CEPHFILESYSTEM, marks=pytest.mark.polarion_id("OCS-2043")
        )
    ]
)
class TestPodStartTime(E2ETest):
    """
    Measure time to start pod with PVC attached
    """
    pvc_size = 5

    @pytest.fixture()
    def pod(self, interface, pod_factory, pvc_factory):
        """
        Prepare pod for the test

        Returns:
            pod obj: Pod instance

        """
        pvc_obj = pvc_factory(
            interface=interface, size=self.pvc_size
        )
        pod_obj = pod_factory(pvc=pvc_obj)
        return pod_obj

    def test_pod_start_time(self, pod):
        """
        Test to log pod start time
        """
        start_time_dict = pod_start_time(pod)
        start_time = start_time_dict['web-server']
        logging.info(f'pod start time: {start_time} seconds')
        if start_time > 30:
            raise ex.PerformanceException(
                f'pod start time is {start_time},'
                f'which is greater than 30 seconds'
            )
