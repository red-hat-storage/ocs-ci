import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier2,
    skipif_lvm_not_installed,
    aqua_squad,
)
from ocs_ci.framework.testlib import skipif_ocs_version, ManageTest
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import LVM
from ocs_ci.ocs.exceptions import LvSizeWrong

logger = logging.getLogger(__name__)


@aqua_squad
@tier2
@skipif_lvm_not_installed
@skipif_ocs_version("<4.11")
class TestLvmCloneBiggerThanDisk(ManageTest):
    """
    Test lvm clone bigger than disk

    """

    @pytest.mark.parametrize(
        argnames=["volume_mode", "volume_binding_mode"],
        argvalues=[
            pytest.param(
                *[constants.VOLUME_MODE_FILESYSTEM, constants.WFFC_VOLUMEBINDINGMODE],
                marks=pytest.mark.polarion_id("OCS-4383"),
            ),
            pytest.param(
                *[constants.VOLUME_MODE_BLOCK, constants.WFFC_VOLUMEBINDINGMODE],
                marks=pytest.mark.polarion_id("OCS-4383"),
            ),
            pytest.param(
                *[
                    constants.VOLUME_MODE_FILESYSTEM,
                    constants.IMMEDIATE_VOLUMEBINDINGMODE,
                ],
                marks=pytest.mark.polarion_id("OCS-4383"),
            ),
            pytest.param(
                *[constants.VOLUME_MODE_BLOCK, constants.IMMEDIATE_VOLUMEBINDINGMODE],
                marks=pytest.mark.polarion_id("OCS-4383"),
            ),
        ],
    )
    def test_create_clone_from_pvc_bigger_than_disk(
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
        .* Check one disk size
        .* Create PVC bigger than disk size
        .* Create POD
        .* Run IO PVC
        .* Create clone
        .* Attach pod
        .* Check LV size
        .* Run IO

        """

        access_mode = constants.ACCESS_MODE_RWO

        lvm = LVM(
            fstrim=True, fail_on_thin_pool_not_empty=True, threading_lock=threading_lock
        )
        disk1 = lvm.pv_data["pv_list"][0]
        disk_size = lvm.pv_data[disk1]["pv_size"]
        pvc_size = int(float(disk_size)) * 2
        thin_pool_size = lvm.get_thin_pool1_size()
        # the ratio of the PVC IO. 1 = 100%
        first_io_ratio = 0.6
        second_io_ratio = 0.3
        fio_size = (
            int(str(int(float(pvc_size)) * first_io_ratio).split(".")[0])
            if "." in str(int(float(pvc_size)) * first_io_ratio)
            else int(float(pvc_size) * first_io_ratio)
        )
        second_fio_size = (
            int(str(int(float(pvc_size)) * second_io_ratio).split(".")[0])
            if "." in str(int(float(pvc_size)) * second_io_ratio)
            else int(float(pvc_size) * second_io_ratio)
        )
        if volume_mode == constants.VOLUME_MODE_BLOCK:
            readwrite = "write"
            filename = "/dev/rbdblock"

        else:
            readwrite = "readwrite"
            filename = "fio.txt"

        proj_obj = project_factory()

        sc_obj = lvm_storageclass_factory(volume_binding_mode)

        status = constants.STATUS_PENDING
        if volume_binding_mode == constants.IMMEDIATE_VOLUMEBINDINGMODE:
            status = constants.STATUS_BOUND
        pvc_obj = pvc_factory(
            project=proj_obj,
            interface=None,
            storageclass=sc_obj,
            size=pvc_size,
            status=status,
            access_mode=access_mode,
            volume_mode=volume_mode,
        )

        block = True if volume_mode == constants.VOLUME_MODE_BLOCK else False
        pod_obj = pod_factory(pvc=pvc_obj, raw_block_pv=block)
        lv_size = lvm.get_lv_size_of_pvc(pvc_obj)
        if int(float(lv_size)) != pvc_size:
            raise LvSizeWrong(
                f"❌Lv size {lv_size} is not the same as pvc size {pvc_size}"
            )

        storage_type = "fs"
        block = False
        if volume_mode == constants.VOLUME_MODE_BLOCK:
            storage_type = "block"
            block = True
        pod_obj.run_io(
            storage_type,
            size=f"{fio_size}g",
            rate="1500M",
            fio_filename=filename,
            direct=1,
            invalidate=0,
            rate_process=None,
            buffer_pattern="0xdeadface",
            bs="100M",
            jobs=1,
            runtime=0,
            readwrite=readwrite,
            rw_ratio=50,
        )
        pod_obj.get_fio_results()
        lvm.compare_percent_data_from_pvc(pvc_obj, fio_size)

        logger.info(f"ℹ ️Creating clone from {pvc_obj.name}")
        clone = pvc_clone_factory(
            pvc_obj=pvc_obj, status=status, volume_mode=volume_mode
        )

        if volume_binding_mode == constants.IMMEDIATE_VOLUMEBINDINGMODE:
            lvm.compare_percent_data_from_pvc(clone, fio_size)

        restored_pod_obj = pod_factory(pvc=clone, raw_block_pv=block)

        logger.info(f"ℹ ️LVMCluster version is {lvm.get_lvm_version()}")
        logger.info(
            f"ℹ️ Lvm thin-pool overprovisionRation is {lvm.get_lvm_thin_pool_config_overprovision_ratio()}"
        )
        logger.info(
            f"ℹ️ Lvm thin-pool sizePercent is {lvm.get_lvm_thin_pool_config_size_percent()}"
        )

        logger.info(f"ℹ️Attaching pod to pvc restore {clone.name}")

        if volume_binding_mode == constants.WFFC_VOLUMEBINDINGMODE:
            lvm.compare_percent_data_from_pvc(clone, fio_size)
        lv_name = lvm.get_lv_name_from_pvc(clone)

        before_fio_thin_pool_util = lvm.get_thin_pool1_data_percent()
        logger.info(
            f"ℹ️ lv {lv_name} from pvc {clone.name} utilization after fio is {before_fio_thin_pool_util}"
        )

        restored_pod_obj.run_io(
            storage_type,
            size=f"{second_fio_size}g",
            rate="1500M",
            runtime=0,
            rate_process=None,
            fio_filename=f"second-{filename}",
            buffer_pattern="0xdeadface",
            direct=1,
            invalidate=0,
            bs="100M",
            jobs=1,
            readwrite=readwrite,
        )

        restored_pod_obj.get_fio_results()

        if block:
            if fio_size > second_fio_size:
                after_expected_util = (
                    float(fio_size + second_fio_size) / float(thin_pool_size) * 100
                )

                lv_util_after_second_fio = float(fio_size)
            else:
                after_expected_util = (
                    float(second_fio_size + fio_size) / float(thin_pool_size) * 100
                )

                lv_util_after_second_fio = float(second_fio_size)

        else:
            after_expected_util = (
                float(fio_size + second_fio_size) / float(thin_pool_size)
            ) * 100
            lv_util_after_second_fio = float(fio_size + second_fio_size)

        lvm.compare_percent_data_from_pvc(clone, lv_util_after_second_fio)
        lvm.compare_thin_pool_data_percent(
            data_percent=after_expected_util,
            sampler=True,
            timeout=10,
            wait=1,
        )
