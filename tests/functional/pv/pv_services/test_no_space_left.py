"""
Test to verify PVC behavior when full of data with I/O
"""
import logging
import pytest
import ocs_ci.ocs.exceptions as ex

from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import tier2, ManageTest
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import get_used_space_on_mount_point

log = logging.getLogger(__name__)


@green_squad
@pytest.mark.parametrize(
    argnames=["interface"],
    argvalues=[
        pytest.param(
            *[constants.CEPHBLOCKPOOL], marks=pytest.mark.polarion_id("OCS-852")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM],
            marks=[pytest.mark.polarion_id("OCS-853"), pytest.mark.bugzilla("1745344")],
        ),
    ],
)
@tier2
class TestPVCFullWithIORWO(ManageTest):
    """
    Test to verify PVC behavior when full with data
    """

    pvc_size_gb = 5

    @pytest.fixture()
    def base_setup(self, request, interface, pvc_factory, pod_factory):
        """
        A setup phase for the test
        """
        self.interface = interface
        self.pvc_obj = pvc_factory(
            interface=self.interface,
            size=self.pvc_size_gb,
        )
        self.pod_obj = pod_factory(interface=self.interface, pvc=self.pvc_obj)

    def test_pvc_no_space_left(self, base_setup, pod_factory):
        """
        Writing data to PVC to reach limit
        """
        log.info(f"Running FIO to fill PVC size: {self.pvc_size_gb}")
        self.pod_obj.run_io(
            "fs", size=self.pvc_size_gb, io_direction="write", runtime=60
        )
        log.info("Waiting for IO results")
        try:
            self.pod_obj.get_fio_results()
        except ex.CommandFailed as cf:
            if "No space left on device" not in str(cf):
                raise
            else:
                used_space = get_used_space_on_mount_point(self.pod_obj)
                assert used_space == "100%", (
                    f"The used space is not 100% but {used_space} which means "
                    f"the device is not full"
                )
                log.info("FIO succeeded to fill the PVC with data")
        log.info("Deleting the pod and attaching the full PVC to a new pod")
        self.pod_obj.delete()
        self.pod_obj.ocp.wait_for_delete(resource_name=self.pod_obj.name)
        log.info("Creating a new Pod with the existing full PVC")
        self.pod_obj = pod_factory(interface=self.interface, pvc=self.pvc_obj)
        used_space = get_used_space_on_mount_point(self.pod_obj)
        assert (
            used_space == "100%"
        ), f"The used space is not 100% but {used_space} from the new pod"


@green_squad
@pytest.mark.polarion_id("OCS-854")
@pytest.mark.bugzilla("1745344")
@tier2
class TestPVCFullWithIORWX(ManageTest):
    """
    Test to verify PVC behavior when full with data
    """

    pvc_size_gb = 5

    @pytest.fixture()
    def base_setup(self, request, pvc_factory, pod_factory):
        """
        A setup phase for the test
        """
        self.interface = constants.CEPHFILESYSTEM
        self.pvc_obj = pvc_factory(
            interface=self.interface,
            size=self.pvc_size_gb,
            access_mode=constants.ACCESS_MODE_RWX,
        )
        self.pod_obj1 = pod_factory(interface=self.interface, pvc=self.pvc_obj)
        self.pod_obj2 = pod_factory(interface=self.interface, pvc=self.pvc_obj)

    def test_pvc_no_space_left(self, base_setup):
        """
        Writing data to PVC to reach limit
        """
        log.info(f"Running FIO to fill PVC size: {self.pvc_size_gb}")
        self.pod_obj1.run_io(
            "fs", size=self.pvc_size_gb, io_direction="write", runtime=60
        )
        log.info("Waiting for IO results")
        try:
            self.pod_obj1.get_fio_results()
        except ex.CommandFailed as cf:
            if "No space left on device" not in str(cf):
                raise
            else:
                used_space = get_used_space_on_mount_point(self.pod_obj1)
                assert used_space == "100%", (
                    f"The used space is not 100% but {used_space} which means "
                    f"the device is not full"
                )
                used_space = get_used_space_on_mount_point(self.pod_obj2)
                assert used_space == "100%", (
                    f"The used space is not 100% but {used_space} which means "
                    f"the device is not full"
                )
                log.info("FIO succeeded to fill the PVC with data")
        log.info("Deleting the first pod and checking used size from the 2nd pod")
        self.pod_obj1.delete()
        used_space = get_used_space_on_mount_point(self.pod_obj2)
        assert (
            used_space == "100%"
        ), f"The used space is not 100% but {used_space} from 2nd pod"
