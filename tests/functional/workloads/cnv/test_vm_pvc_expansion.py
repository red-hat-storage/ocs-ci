import logging
import random

from ocs_ci.framework.pytest_customization.marks import workloads, magenta_squad
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.ocs import constants
from ocs_ci.helpers.cnv_helpers import cal_md5sum_vm, run_dd_io

log = logging.getLogger(__name__)


@magenta_squad
@workloads
class TestVmPvcExpansion(E2ETest):
    """
    Tests for VM PVC Expansion
    """

    def test_pvc_expansion(self, cnv_workload):
        """
        Test PVC expansion for a CNV VM workload.
        """
        vm_obj = cnv_workload(
            volume_interface=constants.VM_VOLUME_PVC,
            source_url=constants.CNV_FEDORA_SOURCE,
        )[-1]
        vm_pvc_obj = vm_obj.get_vm_pvc_obj()
        log.info(f"Initial PVC size: {vm_pvc_obj.size} GiB")
        file_path = "/source_file.txt"
        source_csum = run_dd_io(vm_obj=vm_obj, file_path=file_path, verify=True)
        log.info(f"Checksum before resize: {source_csum}")
        new_size = random.randint(vm_pvc_obj.size + 1, vm_pvc_obj.size + 5)
        log.info(f"Resizing PVC to {new_size} GiB")
        vm_pvc_obj.resize_pvc(new_size, True)
        vm_pvc_obj_n = vm_obj.get_vm_pvc_obj()
        log.info(f"New PVC size: {vm_pvc_obj_n.size} GiB")
        res_csum = cal_md5sum_vm(vm_obj=vm_obj, file_path=file_path)
        log.info(f"Checksum after resize: {res_csum}")
        assert source_csum == res_csum and vm_pvc_obj_n.size == new_size, (
            f"Failed: PVC expansion or MD5 mismatch for VM '{vm_obj.name}'. "
            f"Expected size: {new_size} GiB, but got: {vm_pvc_obj.size} GiB."
        )
