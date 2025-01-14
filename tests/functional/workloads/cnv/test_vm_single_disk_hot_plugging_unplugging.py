import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import magenta_squad, workloads
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.helpers.helpers import create_pvc
from ocs_ci.ocs import constants

log = logging.getLogger(__name__)


@magenta_squad
@workloads
@pytest.mark.polarion_id("OCS-5243")
class TestVmOperations(E2ETest):
    """
    Tests for VM operations
    """

    def test_vm_single_disk_hot_plugging_unplugging(self, cnv_workload, setup_cnv):
        """
        Test for a single disk Hot Plugging and Hot Unplugging

        """
        vm_obj = cnv_workload(volume_interface=constants.VM_VOLUME_PVC)
        vm_obj.run_ssh_cmd(
            command="dd if=/dev/zero of=/dd_file.txt bs=1024 count=102400"
        )
        pvc_obj = create_pvc(
            sc_name=constants.DEFAULT_CNV_CEPH_RBD_SC,
            namespace=vm_obj.namespace,
            size="20Gi",
            access_mode=constants.ACCESS_MODE_RWX,
            volume_mode=constants.VOLUME_MODE_BLOCK,
        )
        vm_obj.addvolme(volume_name=pvc_obj.name)
        vm_obj.removevolume(volume_name=pvc_obj.name)
