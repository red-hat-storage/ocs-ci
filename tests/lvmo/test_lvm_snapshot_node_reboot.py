import logging
import time
import pytest

from ocs_ci.framework.pytest_customization.marks import tier2, skipif_lvm_not_installed
from ocs_ci.framework.testlib import skipif_ocs_version, ManageTest
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import LVM
from ocs_ci.ocs.exceptions import (
    LvSizeWrong,
    CommandFailed,
    PodDidNotReachRunningState,
)
from ocs_ci.ocs.node import wait_for_nodes_status
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.ocs.ocp import switch_to_project
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.node import get_node_objs

logger = logging.getLogger(__name__)


@tier2
@skipif_lvm_not_installed
@skipif_ocs_version("<4.11")
class TestLvmSnapshotNodeReboot(ManageTest):
    """
    Test lvm snapshot with node reboot

    """

    @pytest.mark.parametrize(
        argnames=["volume_mode", "volume_binding_mode"],
        argvalues=[
            pytest.param(
                *[constants.VOLUME_MODE_FILESYSTEM, constants.WFFC_VOLUMEBINDINGMODE],
                marks=pytest.mark.polarion_id("OCS-3991"),
            ),
            pytest.param(
                *[constants.VOLUME_MODE_BLOCK, constants.WFFC_VOLUMEBINDINGMODE],
                marks=pytest.mark.polarion_id("OCS-3991"),
            ),
            pytest.param(
                *[
                    constants.VOLUME_MODE_FILESYSTEM,
                    constants.IMMEDIATE_VOLUMEBINDINGMODE,
                ],
                marks=pytest.mark.polarion_id("OCS-3991"),
            ),
            pytest.param(
                *[constants.VOLUME_MODE_BLOCK, constants.IMMEDIATE_VOLUMEBINDINGMODE],
                marks=pytest.mark.polarion_id("OCS-3991"),
            ),
        ],
    )
    def test_create_snapshot_with_node_reboot(
        self,
        nodes,
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
        test create snapshot reboot
        .* Create PVC
        .* Create POD
        .* Run IO PVC
        .* Create Snapshot
        .* Create pvc from Snapshot
        .* Attach pod
        .* Reboot node
        .* Check LV size
        .* Run IO
        .* Create another restore
        .* Attach Pod and check utilization

        """

        access_mode = constants.ACCESS_MODE_RWO

        lvm = LVM(fstrim=True, fail_on_thin_pool_not_empty=False)
        disk1 = lvm.pv_data["pv_list"][0]
        disk_size = lvm.pv_data[disk1]["pv_size"]
        pvc_size = int(float(disk_size)) * 2
        thin_pool_size = lvm.get_thin_pool1_size()
        first_io_ratio = 0.1
        second_io_ratio = 0.05

        fio_size = round(float(pvc_size) * first_io_ratio)
        second_fio_size = round(float(pvc_size) * second_io_ratio)

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

        block = False
        if volume_mode == constants.VOLUME_MODE_BLOCK:
            block = True
        pod_name_list = []
        pod_1_start_time = time.time()
        pod_obj = pod_factory(pvc=pvc_obj, raw_block_pv=block)
        pod_1_end_time = time.time()
        pod_1_elapsed = pod_1_end_time - pod_1_start_time
        logger.info(f"Pod {pod_obj.name} elapsed time to running is {pod_1_elapsed}")
        pod_name_list.append(pod_obj.name)
        lv_size = lvm.get_lv_size_of_pvc(pvc_obj)
        if int(float(lv_size)) != pvc_size:
            raise LvSizeWrong(
                f"❌ Lv size {lv_size} is not the same as pvc size {pvc_size}"
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

        logger.info(f"ℹ️ Creating snapshot from {pvc_obj.name}")
        snapshot = snapshot_factory(pvc_obj)
        lvm.compare_percent_data_from_pvc(snapshot, fio_size)

        logger.info(f"ℹ ️Creating restore from snapshot {snapshot.name}")
        pvc_restore = snapshot_restore_factory(
            snapshot,
            storageclass=sc_obj.name,
            restore_pvc_name=f"{pvc_obj.name}-restore",
            size=str(pvc_size * 1024 * 1024 * 1024),
            volume_mode=volume_mode,
            restore_pvc_yaml=constants.CSI_LVM_PVC_RESTORE_YAML,
            access_mode=access_mode,
            status=status,
        )
        if volume_binding_mode == constants.IMMEDIATE_VOLUMEBINDINGMODE:
            lvm.compare_percent_data_from_pvc(pvc_restore, fio_size)

        pod_2_start_time = time.time()
        restored_pod_obj = pod_factory(pvc=pvc_restore, raw_block_pv=block)
        pod_2_end_time = time.time()
        pod_2_elapsed_time_to_running = pod_2_end_time - pod_2_start_time
        logger.info(
            f"Pod {restored_pod_obj.name} got to running time in {pod_2_elapsed_time_to_running}"
        )
        pod_name_list.append(restored_pod_obj.name)

        logger.info(f"ℹ ️LVMCluster version is {lvm.get_lvm_version()}")
        logger.info(
            f"ℹ️ Lvm thin-pool overprovisionRation is {lvm.get_lvm_thin_pool_config_overprovision_ratio()}"
        )
        logger.info(
            f"ℹ️ Lvm thin-pool sizePercent is {lvm.get_lvm_thin_pool_config_size_percent()}"
        )

        logger.info(f"ℹ️Attaching pod to pvc restore {pvc_restore.name}")

        if volume_binding_mode == constants.WFFC_VOLUMEBINDINGMODE:
            lvm.compare_percent_data_from_pvc(pvc_restore, fio_size)
        lv_name = lvm.get_lv_name_from_pvc(pvc_restore)

        before_fio_thin_pool_util = lvm.get_thin_pool1_data_percent()
        logger.info(
            f"ℹ️ lv {lv_name} from pvc {pvc_restore.name} utilization after fio is {before_fio_thin_pool_util}"
        )

        nodes_ocs = get_node_objs()
        nodes_names = []
        for node in nodes_ocs:
            nodes_names.append(node.data["metadata"]["name"])
        logger.info(f"Rebooting {nodes_names}")
        nodes.restart_nodes(nodes_ocs, force=False, wait=False)
        logger.info(f"Waiting for nodes {nodes_names} to be in READY state")
        wait_for_nodes_status(node_names=nodes_names, timeout=300)
        logger.info(f"Nodes {nodes_names} in ready state")
        lvm.wait_for_lvm_pod_running()

        @retry(CommandFailed, tries=30, delay=3)
        def switch_project_after_reboot():
            switch_to_project(project_name=proj_obj.namespace)

        switch_project_after_reboot()

        if not wait_for_pods_to_be_running(
            pod_names=pod_name_list, sleep=3, namespace=proj_obj.namespace
        ):
            raise PodDidNotReachRunningState

        lvm.compare_percent_data_from_pvc(pvc_restore, fio_size)

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

        lvm.compare_percent_data_from_pvc(pvc_restore, lv_util_after_second_fio)
        lvm.compare_thin_pool_data_percent(
            data_percent=after_expected_util,
            sampler=True,
            timeout=10,
            wait=1,
        )
        pvc_restore_second_start_time = time.time()
        pvc_restore_second = snapshot_restore_factory(
            snapshot,
            storageclass=sc_obj.name,
            restore_pvc_name=f"{pvc_obj.name}-second-restore",
            size=str(pvc_size * 1024 * 1024 * 1024),
            volume_mode=volume_mode,
            restore_pvc_yaml=constants.CSI_LVM_PVC_RESTORE_YAML,
            access_mode=access_mode,
            status=status,
            timeout=300,
        )
        pvc_restore_second_end_time = time.time()
        logger.info(
            f"Pvc creation time is {(pvc_restore_second_end_time - pvc_restore_second_start_time)}"
        )
        pod_3_start_time = time.time()
        restored_pod_obj_second = pod_factory(
            pvc=pvc_restore_second, raw_block_pv=block
        )
        pod_3_end_time = time.time()
        pod_3_elapsed_time = pod_3_end_time - pod_3_start_time
        logger.info(
            f"Pod {restored_pod_obj_second.name} got to running state after {pod_3_elapsed_time}"
        )
        logger.info(f"Second pod {restored_pod_obj_second.name} was created")
        lvm.compare_percent_data_from_pvc(pvc_restore_second, fio_size)
