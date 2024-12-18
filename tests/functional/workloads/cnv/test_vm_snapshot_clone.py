import logging
from ocs_ci.framework.pytest_customization.marks import workloads, magenta_squad
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.helpers.cnv_helpers import cal_md5sum_vm, run_dd_io
from ocs_ci.ocs import constants

log = logging.getLogger(__name__)


@magenta_squad
@workloads
class TestVMSnapshotClone(E2ETest):
    """
    Tests for VM PVC Expansion
    """

    def test_vm_snapshot_clone(
        self, cnv_workload, snapshot_factory, snapshot_restore_factory
    ):
        """
        creates snapshot of a deployed vm and clones it
        """

        # create a VM
        vm_obj = cnv_workload(
            volume_interface=constants.VM_VOLUME_PVC,
            source_url=constants.CNV_FEDORA_SOURCE,
        )[-1]

        # put some content onto it
        file_paths = ["/source_file.txt", "/new_file.txt"]
        source_csum = run_dd_io(vm_obj=vm_obj, file_path=file_paths[0], verify=True)
        vm_obj.stop()

        # take a snapshot
        pvc_obj = vm_obj.get_vm_pvc_obj()
        snap_obj = snapshot_factory(pvc_obj)

        # restoring the snapshot
        res_snap_obj = snapshot_restore_factory(
            snapshot_obj=snap_obj,
            storageclass=vm_obj.sc_name,
            size=vm_obj.pvc_size,
            volume_mode=snap_obj.parent_volume_mode,
            access_mode=vm_obj.pvc_access_mode,
            status=constants.STATUS_BOUND,
            timeout=300,
        )
        # verify snapshot and data persist
        # restore the snapshot to a new PVC
        res_vm_obj = cnv_workload(
            volume_interface=constants.VM_VOLUME_PVC,
            source_url=constants.CNV_FEDORA_SOURCE,
            pvc_obj=res_snap_obj,
            namespace=vm_obj.namespace,
        )[1]
        # make sure data integrity is present
        run_dd_io(vm_obj=res_vm_obj, file_path=file_paths[1], verify=True)
        # getting the md5sum from the clone vm
        res_csum = cal_md5sum_vm(vm_obj=res_vm_obj, file_path=file_paths[0])
        assert source_csum == res_csum, (
            f"Failed: MD5 comparison between source {vm_obj.name} and cloned "
            f"{res_vm_obj.name} VMs"
        )
        res_vm_obj.stop()
