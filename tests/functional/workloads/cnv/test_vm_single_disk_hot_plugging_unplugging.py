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
from ocs_ci.helpers.helpers import create_pvc, create_project
from ocs_ci.ocs import constants

log = logging.getLogger(__name__)


@magenta_squad
@workloads
@pytest.mark.polarion_id("OCS-807")
class TestVmOperations(E2ETest):
    """
    Tests for VM operations
    """

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        def finalizer():

            pvc_obj = get_pvc_from_vm(self.vm_obj)
            secret_obj = get_secret_from_vm(self.vm_obj)
            volumeimportsource_obj = get_volumeimportsource(pvc_obj=pvc_obj)
            self.vm_obj.delete()
            pvc_obj.delete()
            secret_obj.delete()
            volumeimportsource_obj.delete()
            self.proj_obj.delete()

        request.addfinalizer(finalizer)


def test_vm_single_disk_hot_plugging_unplugging(self):
    """
    Test for a single disk Hot Plugging and Hot Unplugging
    """
    self.proj_obj = create_project()
    self.vm_obj = create_vm_using_standalone_pvc(
        running=True, namespace=self.proj_obj.namespace
    )
    self.vm_obj.run_ssh_cmd(
        command="dd if=/dev/zero of=/dd_file.txt bs=1024 count=102400"
    )
    pvc_obj = create_pvc(
        sc_name=constants.DEFAULT_CNV_CEPH_RBD_SC,
        namespace=self.proj_obj.namespace,
        size=20,
        access_mode=constants.ACCESS_MODE_RWX,
        volume_mode=constants.VOLUME_MODE_BLOCK,
    )
    self.vm_obj.addvolme(volume_name=pvc_obj.name)
    self.vm_obj.removevolume(volume_name=pvc_obj.name)
