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
        Test PVC expansion for a CNV VM workload
        """
        vm_obj = cnv_workload(
            volume_interface=constants.VM_VOLUME_PVC,
            source_url=constants.CNV_FEDORA_SOURCE,
        )[-1]
        vm_pvc_obj = vm_obj.get_vm_pvc_obj()
        log.info(vm_pvc_obj.size)
        # writing data to the PVC
        file_paths = "/source_file.txt"
        source_csum = run_dd_io(vm_obj=vm_obj, file_path=file_paths, verify=True)
        log.info(source_csum)
        # Resize PVC to a random size between 31 and 35 GiB as 30GiB is default
        new_size = random.randint(31, 35)
        log.info(f"Size of VM PVC before expansion: {vm_pvc_obj.size}")
        vm_pvc_obj.resize_pvc(new_size)
        log.info(f"Size of VM PVC after expansion: {vm_pvc_obj.size}")
        res_csum = cal_md5sum_vm(vm_obj=vm_obj, file_path=file_paths)
        log.info(res_csum)
        assert source_csum == res_csum and vm_obj.get_vm_pvc_obj().size == new_size, (
            f"Failed: Either VM PVC Expansion or MD5 comparison of {vm_obj.name} before and after "
            f"PVC expansion"
        )
