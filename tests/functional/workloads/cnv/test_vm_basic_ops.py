import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import E2ETest, workloads
from ocs_ci.helpers.cnv_helpers import create_vm_using_standalone_pvc
from ocs_ci.helpers.helpers import create_pvc
from ocs_ci.ocs import constants

log = logging.getLogger(__name__)


@magenta_squad
@workloads
@pytest.mark.polarion_id("OCS-807")
class TestVmOperations(E2ETest):
    """
    Tests for VM operations
    """

    def test_vm_lifecycle_and_io(self):
        """
        This test performs the VM lifecycle operations and IO

        Steps:
        1) Create a VM using a standalone PVC
            a) Create a cdi source with a registry url pointing to the source image
            b) Create a PVC using this source image backed with an odf storageclass
            c) Create a secret using a statically manged public SSH key and add this secret name to the VM spec for ssh
            d) Create a VM using the above PVC
        2) Start the VM using virtctl command and wait for the VM to reach running state
        3) SSH to the VM and create some data on the PVC mount point
        4) SCP that create data in step-3 to localmachine
        5) Stop the VM
        6) Delete the VM

        """
        self.vm_obj = create_vm_using_standalone_pvc(running=True)
        self.vm_obj.run_ssh_cmd(
            command="dd if=/dev/zero of=/dd_file.txt bs=1024 count=102400"
        )
        self.vm_obj.scp_from_vm(local_path="/tmp", vm_src_path="/dd_file.txt")
        self.vm_obj.stop()
        self.vm_obj.delete()

    def test_vm_single_disk_hot_plugging_unplugging(self, project_factory):
        """
        Test for a single disk Hot Plugging and Hot Unplugging
        """
        proj_obj = project_factory()
        self.vm_obj = create_vm_using_standalone_pvc(running=True)
        self.vm_obj.run_ssh_cmd(
            command="dd if=/dev/zero of=/dd_file.txt bs=1024 count=102400"
        )
        pvc_obj = create_pvc(
            sc_name=constants.DEFAULT_CNV_CEPH_RBD_SC,
            namespace=proj_obj,
            size=20,
            access_mode=constants.ACCESS_MODE_RWX,
            volume_mode="Block",
        )
        self.vm_obj.addvolme(volume_name=pvc_obj)
        self.vm_obj.removevolume(volume_name=pvc_obj)
