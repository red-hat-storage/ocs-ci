import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import magenta_squad, workloads
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.helpers.cnv_helpers import check_fio_status

log = logging.getLogger(__name__)


@magenta_squad
@workloads
@pytest.mark.polarion_id("OCS-5241")
class TestVmOperations(E2ETest):
    """
    Tests for VM operations
    """

    def test_vm_lifecycle_and_io(self, setup_cnv, project_factory, multi_cnv_workload):
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

        # Create a project
        proj_obj = project_factory()

        (
            self.vm_objs_def,
            self.vm_objs_aggr,
            self.sc_obj_def_compr,
            self.sc_obj_aggressive,
        ) = multi_cnv_workload(namespace=proj_obj.namespace)
        all_vm_list = self.vm_objs_def + self.vm_objs_aggr

        for vm_obj in all_vm_list:
            vm_obj.restart()
            if check_fio_status(vm_obj):
                log.info("FIO started after restarting VM")
