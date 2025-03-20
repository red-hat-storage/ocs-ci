import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import magenta_squad, workloads
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.ocs import constants
from ocs_ci.helpers.cnv_helpers import run_fio, check_fio_status

log = logging.getLogger(__name__)


@magenta_squad
@workloads
@pytest.mark.polarion_id("OCS-5241")
class TestVmOperations(E2ETest):
    """
    Tests for VM operations
    """

    def test_vm_lifecycle_and_io(self, setup_cnv, cnv_workload):
        """
        This test performs the VM lifecycle operations and IO

        Steps:
        1) Create a VM using a standalone PVC/DV/DVT
            a) Create a cdi source with a registry url pointing to the source image
            b) Create a PVC using this source image backed with an odf storageclass
            c) Create a secret using a statically manged public SSH key and add this secret name to the VM spec for ssh
            d) Create a VM using the above PVC
        2) Start the VM using virtctl command and wait for the VM to reach running state
        3) SSH to the VM and create some data on the PVC mount point
        4) SCP that create data in step-3 to localmachine
        5) Stop the VM
        6) Delete the VM (as part of factory teardown)

        """
        file_path = "/io_tests"
        fio_service_name = "fio_test"
        volume_interface = [
            constants.VM_VOLUME_PVC,
            constants.VM_VOLUME_DV,
            constants.VM_VOLUME_DVT,
        ]
        for vl_if in volume_interface:
            vm_obj = cnv_workload(
                volume_interface=vl_if, source_url=constants.CNV_FEDORA_SOURCE
            )

            run_fio(vm_obj, filename=file_path, fio_service_name="fio_test")

            vm_obj.restart()
            if check_fio_status(vm_obj, fio_service_name):
                log.info("FIO resumed after restarting VM")
            vm_obj.stop()
