import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    skipif_lvm_not_installed,
    aqua_squad,
)
from ocs_ci.framework.testlib import skipif_ocs_version, ManageTest, acceptance
from ocs_ci.ocs import constants
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
            marks=pytest.mark.polarion_id("OCS-3962"),
        ),
        pytest.param(
            *[constants.VOLUME_MODE_BLOCK, constants.WFFC_VOLUMEBINDINGMODE],
            marks=pytest.mark.polarion_id("OCS-3960"),
        ),
        pytest.param(
            *[constants.VOLUME_MODE_FILESYSTEM, constants.IMMEDIATE_VOLUMEBINDINGMODE],
            marks=pytest.mark.polarion_id("OCS-3959"),
        ),
        pytest.param(
            *[constants.VOLUME_MODE_BLOCK, constants.IMMEDIATE_VOLUMEBINDINGMODE],
            marks=pytest.mark.polarion_id("OCS-3961"),
        ),
    ],
)
class TestLvmSnapshot(ManageTest):
    """
    Test snapshot for LVM

    """

    pvc_size = 100
    access_mode = constants.ACCESS_MODE_RWO

    @tier1
    @acceptance
    @skipif_lvm_not_installed
    @skipif_ocs_version("<4.11")
    def test_create_clone_from_pvc(
        self,
        volume_mode,
        volume_binding_mode,
        project_factory,
        lvm_storageclass_factory,
        pvc_clone_factory,
        pvc_factory,
        pod_factory,
        threading_lock,
    ):
        """
        test create delete snapshot
        .* Create PVC
        .* Create POD
        .* Run IO
        .* Check MD5 on file
        .* Create clone
        .* Attach pod to clone
        .* Check MD% on clone
        .* Run IO

        """
        lvm = LVM(
            fstrim=True, fail_on_thin_pool_not_empty=True, threading_lock=threading_lock
        )
        logger.info(f"LVMCluster version is {lvm.get_lvm_version()}")
        logger.info(
            f"Lvm thin-pool overprovisionRation is {lvm.get_lvm_thin_pool_config_overprovision_ratio()}"
        )
        logger.info(
            f"Lvm thin-pool sizePrecent is {lvm.get_lvm_thin_pool_config_size_percent()}"
        )

        proj_obj = project_factory()

        sc_obj = lvm_storageclass_factory(volume_binding=volume_binding_mode)

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
        origin_pod_md5 = 0
        if not block:
            origin_pod_md5 = cal_md5sum(
                pod_obj=pod_obj, file_name="fio-rand-readwrite", block=block
            )

        logger.info(f"Creating clone from {pvc_obj.name}")
        clone = pvc_clone_factory(
            pvc_obj=pvc_obj, status=status, volume_mode=volume_mode
        )

        restored_pod_obj = pod_factory(pvc=clone, raw_block_pv=block)
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
