"""
Test to verify PVC behavior when full of data with I/O
"""
import logging
import pytest
import ocs_ci.ocs.exceptions as ex

from ocs_ci.framework.testlib import tier1, E2ETest
from ocs_ci.ocs import constants


log = logging.getLogger(__name__)


@pytest.mark.parametrize(
    argnames=["interface", "access_mode"],
    argvalues=[
        pytest.param(
            *[constants.CEPHBLOCKPOOL, constants.ACCESS_MODE_RWO],
            marks=pytest.mark.polarion_id("OCS-852")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, constants.ACCESS_MODE_RWO],
            marks=[
                pytest.mark.polarion_id("OCS-853"),
                pytest.mark.bugzilla("1745344")
            ]
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, constants.ACCESS_MODE_RWX],
            marks=[
                pytest.mark.polarion_id("OCS-854"),
                pytest.mark.bugzilla("1745344")
            ]
        )
    ]
)
@tier1
class TestPVCFullWithIO(E2ETest):
    """
    Test to verify PVC behavior when full with data
    """
    pvc_size_gb = 50

    @pytest.fixture()
    def base_setup(
        self, request, interface, access_mode, pvc_factory, pod_factory
    ):
        """
        A setup phase for the test
        """
        self.pvc_obj = pvc_factory(
            interface=interface, size=self.pvc_size_gb, access_mode=access_mode
        )
        self.pod_obj = pod_factory(interface=interface, pvc=self.pvc_obj)

    @pytest.mark.usefixtures(base_setup.__name__)
    def test_pvc_no_space_left(self):
        """
        Writing data to PVC to reach limit
        """
        log.info(f"Running FIO to fill PVC size: {self.pvc_size_gb}")
        self.pod_obj.run_io(
            'fs', size=self.pvc_size_gb, io_direction='write', runtime=60
        )
        log.info("Waiting for IO results")
        try:
            self.pod_obj.get_fio_results()
        except ex.CommandFailed as cf:
            if "No space left on device" not in cf.__str__():
                raise
            else:
                log.info(f"FIO succeeded to fill the PVC with data")
