import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import E2ETest, workloads
from ocs_ci.helpers.cnv_helpers import (
    create_vm_using_standalone_pvc,
    get_pvc_from_vm,
    get_secret_from_vm,
    get_volumeimportsource,
)
from ocs_ci.helpers.helpers import create_project

log = logging.getLogger(__name__)


@magenta_squad
@workloads
@pytest.mark.polarion_id("OCS-5241")
class TestVmOperations(E2ETest):
    """
    Tests for VM operations
    """

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        teardown function
        """

        def finalizer():
            pvc_obj = get_pvc_from_vm(self.vm_obj)
            secret_obj = get_secret_from_vm(self.vm_obj)
            volumeimportsource_obj = get_volumeimportsource(pvc_obj=pvc_obj)
            self.vm_obj.delete()
            pvc_obj.delete()
            secret_obj.delete()
            volumeimportsource_obj.delete()
            self.proj_obj.delete(resource_name=self.proj_obj.namespace)

        request.addfinalizer(finalizer)

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
        6) Delete the VM (as part of teardown)

        """
        self.proj_obj = create_project()
        self.vm_obj = create_vm_using_standalone_pvc(
            running=True, namespace=self.proj_obj.namespace
        )
        self.vm_obj.run_ssh_cmd(
            command="dd if=/dev/zero of=/dd_file.txt bs=1024 count=102400"
        )
        self.vm_obj.scp_from_vm(local_path="/tmp", vm_src_path="/dd_file.txt")
        self.vm_obj.stop()
