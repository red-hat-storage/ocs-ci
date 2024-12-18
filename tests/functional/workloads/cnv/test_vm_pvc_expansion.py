import logging
import random

from ocs_ci.framework.pytest_customization.marks import workloads, magenta_squad
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.ocs import constants

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
        # Resize PVC to a random size between 31 and 35 GiB
        new_size = random.randint(31, 35)
        log.info(vm_pvc_obj.size)
        vm_pvc_obj.resize_pvc(new_size)
        pass
