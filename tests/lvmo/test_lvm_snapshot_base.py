import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    skipif_lvm_not_installed,
    aqua_squad,
)
from ocs_ci.framework.testlib import skipif_ocs_version, ManageTest, acceptance
from ocs_ci.ocs import constants
from ocs_ci.utility.utils import get_ocp_version
from ocs_ci.ocs.cluster import LVM
from ocs_ci.ocs.resources.pod import cal_md5sum
from ocs_ci.ocs.exceptions import Md5CheckFailed

logger = logging.getLogger(__name__)


@aqua_squad
@pytest.mark.parametrize(
    argnames=["volume_mode", "volume_binding_mode"],
    argvalues=[
        pytest.param(
            *[constants.VOLUME_MODE_FILESYSTEM, constants.WFFC_VOLUMEBINDINGMODE],
            marks=pytest.mark.polarion_id("OCS-3956"),
        ),
        pytest.param(
            *[constants.VOLUME_MODE_BLOCK, constants.WFFC_VOLUMEBINDINGMODE],
            marks=pytest.mark.polarion_id("OCS-3958"),
        ),
        pytest.param(
            *[constants.VOLUME_MODE_FILESYSTEM, constants.IMMEDIATE_VOLUMEBINDINGMODE],
            marks=pytest.mark.polarion_id("OCS-3955"),
        ),
        pytest.param(
            *[constants.VOLUME_MODE_BLOCK, constants.IMMEDIATE_VOLUMEBINDINGMODE],
            marks=pytest.mark.polarion_id("OCS-3957"),
        ),
    ],
)
class TestLvmSnapshot(ManageTest):
    """
    Test pvc clone for LVM

    """

    ocp_version = get_ocp_version()
    pvc_size = 100
    access_mode = constants.ACCESS_MODE_RWO

    @tier1
    @acceptance
    @skipif_lvm_not_installed
    @skipif_ocs_version("<4.11")
    def test_create_snapshot_from_pvc(
        self,
        volume_mode,
        volume_binding_mode,
        project_factory,
        lvm_storageclass_factory,
        snapshot_factory,
        snapshot_restore_factory,
        pvc_factory,
        pod_factory,
    ):
        """
        test create delete snapshot
        .* Create PVC
        .* Create POD
        .* Run IO
        .* Create Snapshot
        .* Create pvc from Snapshot
        .* Attach pod
        .* Run IO

        """
        lvm = LVM(fstrim=True, fail_on_thin_pool_not_empty=True)
        logger.info(f"LVMCluster version is {lvm.get_lvm_version()}")
        logger.info(
            f"Lvm thin-pool overprovisionRation is {lvm.get_lvm_thin_pool_config_overprovision_ratio()}"
        )
        logger.info(
            f"Lvm thin-pool sizePrecent is {lvm.get_lvm_thin_pool_config_size_percent()}"
        )

        proj_obj = project_factory()

        sc_obj = lvm_storageclass_factory(volume_binding_mode)

        status = constants.STATUS_PENDING
        if volume_binding_mode == constants.IMMEDIATE_VOLUMEBINDINGMODE:
            status = constants.STATUS_BOUND
        pvc_obj = pvc_factory(
            project=proj_obj,
            interface=None,
            storageclass=sc_obj,
            size=self.pvc_size,
            status=status,
            access_mode=self.access_mode,
            volume_mode=volume_mode,
        )

        block = False
        if volume_mode == constants.VOLUME_MODE_BLOCK:
            block = True
        pod_obj = pod_factory(pvc=pvc_obj, raw_block_pv=block)
        origin_pod_md5 = ""
        storage_type = "fs"
        block = False
        if volume_mode == constants.VOLUME_MODE_BLOCK:
            storage_type = "block"
            block = True
        pod_obj.run_io(
            storage_type,
            size="5g",
            rate="1500m",
            runtime=0,
            invalidate=0,
            buffer_compress_percentage=60,
            buffer_pattern="0xdeadface",
            bs="1024K",
            jobs=1,
            readwrite="readwrite",
        )
        pod_obj.get_fio_results()
        if not block:
            origin_pod_md5 = cal_md5sum(
                pod_obj=pod_obj, file_name="fio-rand-readwrite", block=block
            )

        logger.info(f"Creating snapshot from {pvc_obj.name}")
        snapshot = snapshot_factory(pvc_obj)

        logger.info(f"Creating restore from snapshot {snapshot.name}")
        pvc_restore = snapshot_restore_factory(
            snapshot,
            storageclass=sc_obj.name,
            restore_pvc_name=f"{pvc_obj.name}-restore",
            size=str(self.pvc_size * 1024 * 1024 * 1024),
            volume_mode=volume_mode,
            restore_pvc_yaml=constants.CSI_LVM_PVC_RESTORE_YAML,
            access_mode=self.access_mode,
            status=status,
        )

        logger.info(f"Attaching pod to pvc restore {pvc_restore.name}")
        restored_pod_obj = pod_factory(pvc=pvc_restore, raw_block_pv=block)
        if not block:
            restored_pod_md5 = cal_md5sum(
                pod_obj=restored_pod_obj,
                file_name="fio-rand-readwrite",
                block=block,
            )
            if restored_pod_md5 != origin_pod_md5:
                raise Md5CheckFailed(
                    f"origin pod {pod_obj.name} md5 value {origin_pod_md5} "
                    f"is not the same as restored pod {restored_pod_obj.name} md5 "
                    f"value {restored_pod_md5}"
                )

        restored_pod_obj.run_io(
            storage_type,
            size="1g",
            rate="1500m",
            runtime=0,
            invalidate=0,
            buffer_compress_percentage=60,
            buffer_pattern="0xdeadface",
            bs="1024K",
            jobs=1,
            readwrite="readwrite",
        )
        restored_pod_obj.get_fio_results()
        if not block:
            restored_pod_md5_second = cal_md5sum(
                pod_obj=restored_pod_obj,
                file_name="fio-rand-readwrite",
                block=block,
            )
            if restored_pod_md5_second == origin_pod_md5:
                raise Md5CheckFailed(
                    f"origin pod {pod_obj.name} md5 value {origin_pod_md5} "
                    f"is not suppose to be the same as restored pod {restored_pod_obj.name} md5 "
                    f"value {restored_pod_md5_second}"
                )
