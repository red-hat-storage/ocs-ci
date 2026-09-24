"""
FDF Standalone HCP — PVC Lifecycle from Client (RHSTOR-8290 / OCS-8192).

End-to-end PVC lifecycle validation from a hosted cluster client:
create PVC, attach pod, write data, verify readback, expand volume,
create snapshot, restore from snapshot, and verify data integrity.
Runs for both RBD and CephFS interfaces.
"""

import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    fdf_standalone_required,
    purple_squad,
    tier1,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import cal_md5sum, verify_data_integrity

logger = logging.getLogger(__name__)


@fdf_standalone_required
@purple_squad
@tier1
@pytest.mark.polarion_id("OCS-8192")
class TestFDFHCPPVCLifecycle:
    """
    Verify full PVC lifecycle from hosted cluster client with FDF standalone.
    """

    @pytest.mark.parametrize(
        "interface",
        [constants.CEPHBLOCKPOOL, constants.CEPHFILESYSTEM],
        ids=["RBD", "CephFS"],
    )
    def test_pvc_lifecycle_from_client(
        self,
        interface,
        pvc_factory,
        pod_factory,
        snapshot_factory,
        snapshot_restore_factory,
    ):
        """
        From the hosted cluster client:
        1. Create PVC and attach pod
        2. Write data and verify readback (md5sum)
        3. Expand PVC from 5Gi to 10Gi
        4. Create VolumeSnapshot
        5. Restore snapshot to new PVC and verify data integrity
        """
        logger.info("Testing %s PVC lifecycle from client", interface)

        logger.info("Creating PVC and pod")
        pvc_obj = pvc_factory(interface=interface, size=5)
        pod_obj = pod_factory(pvc=pvc_obj)

        logger.info("Writing data")
        pod_obj.run_io(
            storage_type="fs",
            size="1G",
            io_direction="wo",
            runtime=0,
        )
        pod_obj.get_fio_results()
        md5sum_original = cal_md5sum(pod_obj, "fio-rand-write")

        logger.info("Verifying data readback")
        verify_data_integrity(pod_obj, "fio-rand-write", md5sum_original)

        logger.info("Expanding PVC from 5Gi to 10Gi")
        pvc_obj.resize_pvc(10, True)

        logger.info("Creating VolumeSnapshot")
        snap_obj = snapshot_factory(pvc_obj)

        logger.info("Restoring from snapshot")
        pvc_size = (
            pvc_obj.get()
            .get("spec", {})
            .get("resources", {})
            .get("requests", {})
            .get("storage", "10Gi")
        )
        restored_pvc = snapshot_restore_factory(
            snapshot_obj=snap_obj,
            size=str(pvc_size),
        )
        restored_pod = pod_factory(pvc=restored_pvc)

        logger.info("Verifying data integrity on restored volume")
        verify_data_integrity(restored_pod, "fio-rand-write", md5sum_original)

        logger.info("%s PVC lifecycle from client verified", interface)
