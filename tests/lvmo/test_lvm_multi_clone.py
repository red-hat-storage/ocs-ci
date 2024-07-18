import logging
import time

import pytest
import concurrent.futures

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
            marks=pytest.mark.polarion_id("OCS-3975"),
        ),
        pytest.param(
            *[constants.VOLUME_MODE_BLOCK, constants.WFFC_VOLUMEBINDINGMODE],
            marks=pytest.mark.polarion_id("OCS-3975"),
        ),
        pytest.param(
            *[constants.VOLUME_MODE_FILESYSTEM, constants.IMMEDIATE_VOLUMEBINDINGMODE],
            marks=pytest.mark.polarion_id("OCS-3975"),
        ),
        pytest.param(
            *[constants.VOLUME_MODE_BLOCK, constants.IMMEDIATE_VOLUMEBINDINGMODE],
            marks=pytest.mark.polarion_id("OCS-3975"),
        ),
    ],
)
class TestLvmMultiClone(ManageTest):
    """
    Test multi clone and restore for LVM

    """

    pvc_size = 100
    access_mode = constants.ACCESS_MODE_RWO
    pvc_num = 5

    @tier1
    @acceptance
    @skipif_lvm_not_installed
    @skipif_ocs_version("<4.11")
    def test_create_multi_clone_from_pvc(
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
        test create delete multi snapshot
        .* Create 5 PVC
        .* Create 5 POD
        .* Run IO
        .* Create 5 clones
        .* Create 5 pvc from clone
        .* Attach 5 pod
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
            f"Lvm thin-pool sizePercent is {lvm.get_lvm_thin_pool_config_size_percent()}"
        )

        proj_obj = project_factory()

        sc_obj = lvm_storageclass_factory(volume_binding_mode)

        status = constants.STATUS_PENDING
        if volume_binding_mode == constants.IMMEDIATE_VOLUMEBINDINGMODE:
            status = constants.STATUS_BOUND
        futures = []
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.pvc_num)
        for exec_num in range(0, self.pvc_num):
            futures.append(
                executor.submit(
                    pvc_factory,
                    project=proj_obj,
                    interface=None,
                    storageclass=sc_obj,
                    size=self.pvc_size,
                    status=status,
                    access_mode=self.access_mode,
                    volume_mode=volume_mode,
                )
            )
        pvc_objs = []
        for future in concurrent.futures.as_completed(futures):
            pvc_objs.append(future.result())

        block = False
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.pvc_num)
        if volume_mode == constants.VOLUME_MODE_BLOCK:
            block = True
        futures_pods = []
        pods_objs = []
        for pvc in pvc_objs:
            futures_pods.append(
                executor.submit(pod_factory, pvc=pvc, raw_block_pv=block)
            )
        for future_pod in concurrent.futures.as_completed(futures_pods):
            pods_objs.append(future_pod.result())

        storage_type = "fs"
        block = False
        if volume_mode == constants.VOLUME_MODE_BLOCK:
            storage_type = "block"
            block = True
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.pvc_num)
        futures_fio = []
        for pod in pods_objs:
            futures_fio.append(
                executor.submit(
                    pod.run_io,
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
            )
        for _ in concurrent.futures.as_completed(futures_fio):
            logger.info("Some pod submitted FIO")
        concurrent.futures.wait(futures_fio)
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.pvc_num)
        futures_results = []
        for pod in pods_objs:
            futures_results.append(executor.submit(pod.get_fio_results()))
        for _ in concurrent.futures.as_completed(futures_results):
            logger.info("Just waiting for fio jobs results")
        concurrent.futures.wait(futures_results)
        origin_pods_md5 = []
        if not block:
            executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.pvc_num)
            futures_md5 = []
            for pod in pods_objs:
                futures_md5.append(
                    executor.submit(
                        cal_md5sum,
                        pod_obj=pod,
                        file_name="fio-rand-readwrite",
                        block=block,
                    )
                )
            for future_md5 in concurrent.futures.as_completed(futures_md5):
                origin_pods_md5.append(future_md5.result())

        logger.info("Creating snapshot from pvc objects")
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.pvc_num)
        futures_clone = []
        clone_objs = []
        for pvc in pvc_objs:
            futures_clone.append(
                executor.submit(
                    pvc_clone_factory,
                    pvc_obj=pvc,
                    status=status,
                    volume_mode=volume_mode,
                )
            )
        for future_clone in concurrent.futures.as_completed(futures_clone):
            clone_objs.append(future_clone.result())
        concurrent.futures.wait(futures_clone)

        logger.info("Attaching pods to pvcs restores")
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.pvc_num)
        futures_restored_pods = []
        restored_pods_objs = []

        for pvc in clone_objs:
            futures_restored_pods.append(
                executor.submit(pod_factory, pvc=pvc, raw_block_pv=block)
            )
        for future_restored_pod in concurrent.futures.as_completed(
            futures_restored_pods
        ):
            restored_pods_objs.append(future_restored_pod.result())
        concurrent.futures.wait(futures_restored_pods)
        time.sleep(10)

        if not block:
            executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.pvc_num)
            futures_restored_pods_md5 = []
            restored_pods_md5 = []
            for restored_pod in restored_pods_objs:
                futures_restored_pods_md5.append(
                    executor.submit(
                        cal_md5sum,
                        pod_obj=restored_pod,
                        file_name="fio-rand-readwrite",
                        block=block,
                    )
                )
            for future_restored_pod_md5 in concurrent.futures.as_completed(
                futures_restored_pods_md5
            ):
                restored_pods_md5.append(future_restored_pod_md5.result())
            for pod_num in range(0, self.pvc_num):
                if origin_pods_md5[pod_num] != restored_pods_md5[pod_num]:
                    raise Md5CheckFailed(
                        f"origin pod {pods_objs[pod_num]} md5 value {origin_pods_md5[pod_num]} "
                        f"is not the same as restored pod {restored_pods_objs[pod_num]} md5 "
                        f"value {restored_pods_md5[pod_num]}"
                    )
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.pvc_num)
        futures_restored_pods_fio = []

        for restored_pod in restored_pods_objs:
            futures_restored_pods_fio.append(
                executor.submit(
                    restored_pod.run_io,
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
            )
        for _ in concurrent.futures.as_completed(futures_restored_pods_fio):
            logger.info("Waiting for all fio pods submission")
        concurrent.futures.wait(futures_restored_pods_fio)

        executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.pvc_num)
        futures_restored_pods_fio_results = []
        for restored_pod in restored_pods_objs:
            futures_restored_pods_fio_results.append(
                executor.submit(restored_pod.get_fio_results())
            )
        for _ in concurrent.futures.as_completed(futures_restored_pods_fio_results):
            logger.info("Finished waiting for some pod")
        concurrent.futures.wait(futures_restored_pods_fio_results)
        if not block:
            executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.pvc_num)
            futures_restored_pods_md5_after_fio = []
            restored_pods_md5_after_fio = []
            for restored_pod in restored_pods_objs:
                futures_restored_pods_md5_after_fio.append(
                    executor.submit(
                        cal_md5sum,
                        pod_obj=restored_pod,
                        file_name="fio-rand-readwrite",
                        block=block,
                    )
                )
            for future_restored_pods_md5_after_fio in concurrent.futures.as_completed(
                futures_restored_pods_md5_after_fio
            ):
                restored_pods_md5_after_fio.append(
                    future_restored_pods_md5_after_fio.result()
                )

            for pod_num in range(0, self.pvc_num):
                if restored_pods_md5_after_fio[pod_num] == origin_pods_md5[pod_num]:
                    raise Md5CheckFailed(
                        f"origin pod {pods_objs[pod_num].name} md5 value {origin_pods_md5[pod_num]} "
                        f"is not suppose to be the same as restored pod {restored_pods_objs[pod_num].name} md5 "
                        f"value {restored_pods_md5_after_fio[pod_num]}"
                    )
