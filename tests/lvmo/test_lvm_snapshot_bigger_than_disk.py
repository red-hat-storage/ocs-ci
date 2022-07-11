import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import tier1, skipif_lvm_not_installed
from ocs_ci.framework.testlib import skipif_ocs_version, ManageTest
from ocs_ci.ocs import constants
from ocs_ci.utility.utils import get_ocp_version
from ocs_ci.ocs.cluster import LVM
from ocs_ci.ocs.exceptions import LvSizeWrong, LvThinUtilNotChanged
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    argnames=["volume_mode", "volume_binding_mode"],
    argvalues=[
        pytest.param(
            *[constants.VOLUME_MODE_FILESYSTEM, constants.WFFC_VOLUMEBINDINGMODE],
            marks=pytest.mark.polarion_id("OCS-3983"),
        ),
        pytest.param(
            *[constants.VOLUME_MODE_BLOCK, constants.WFFC_VOLUMEBINDINGMODE],
            marks=pytest.mark.polarion_id("OCS-3983"),
        ),
        pytest.param(
            *[constants.VOLUME_MODE_FILESYSTEM, constants.IMMEDIATE_VOLUMEBINDINGMODE],
            marks=pytest.mark.polarion_id("OCS-3983"),
        ),
        pytest.param(
            *[constants.VOLUME_MODE_BLOCK, constants.IMMEDIATE_VOLUMEBINDINGMODE],
            marks=pytest.mark.polarion_id("OCS-3983"),
        ),
    ],
)
class TestLvmSnapshotBiggerThanDisk(ManageTest):
    """
    Test lvm snapshot bigger than disk

    """

    ocp_version = get_ocp_version()
    logger.info(f"OCP version {ocp_version}")
    access_mode = constants.ACCESS_MODE_RWO

    @pytest.fixture()
    def init_lvm(self, volume_mode):
        self.lvm = LVM()
        disk1 = self.lvm.pv_data["pv_list"][0]
        self.disk_size = self.lvm.pv_data[disk1]["pv_size"]
        self.pvc_size = int(float(self.disk_size)) * 2
        self.thin_pool_size = self.lvm.get_thin_pool1_size()
        first_io_ratio = 0.6
        second_io_ratio = 0.3
        self.fio_size = (
            int(str(int(float(self.pvc_size)) * first_io_ratio).split(".")[0])
            if "." in str(int(float(self.pvc_size)) * first_io_ratio)
            else int(float(self.pvc_size) * first_io_ratio)
        )
        self.second_fio_size = (
            int(str(int(float(self.pvc_size)) * second_io_ratio).split(".")[0])
            if "." in str(int(float(self.pvc_size)) * second_io_ratio)
            else int(float(self.pvc_size) * second_io_ratio)
        )
        if volume_mode == constants.VOLUME_MODE_BLOCK:
            self.readwrite = "write"
            self.second_readwrite = "write"
            self.filename = "/dev/rbdblock"

        else:
            self.readwrite = "readwrite"
            self.filename = "fio.txt"

    @pytest.fixture()
    def namespace(self, project_factory_class):
        self.proj_obj = project_factory_class()
        self.proj = self.proj_obj.namespace

    @pytest.fixture()
    def storageclass(self, lvm_storageclass_factory_class, volume_binding_mode):
        self.sc_obj = lvm_storageclass_factory_class(volume_binding_mode)

    @pytest.fixture()
    def pvc(self, pvc_factory_class, volume_mode, volume_binding_mode):
        self.status = constants.STATUS_PENDING
        if volume_binding_mode == constants.IMMEDIATE_VOLUMEBINDINGMODE:
            self.status = constants.STATUS_BOUND
        self.pvc_obj = pvc_factory_class(
            project=self.proj_obj,
            interface=None,
            storageclass=self.sc_obj,
            size=self.pvc_size,
            status=self.status,
            access_mode=self.access_mode,
            volume_mode=volume_mode,
        )

    @pytest.fixture()
    def pod(self, pod_factory_class, volume_mode):
        self.block = False
        if volume_mode == constants.VOLUME_MODE_BLOCK:
            self.block = True
        self.pod_obj = pod_factory_class(pvc=self.pvc_obj, raw_block_pv=self.block)

        lv_size = self.lvm.get_lv_size_of_pvc(self.pvc_obj)
        if int(float(lv_size)) != self.pvc_size:
            raise LvSizeWrong(
                f"❌Lv size {lv_size} is not the same as pvc size {self.pvc_size}"
            )

    @pytest.fixture()
    def run_io(self, volume_mode):
        self.fs = "fs"
        self.block = False
        if volume_mode == constants.VOLUME_MODE_BLOCK:
            self.fs = "block"
            self.block = True
        self.pod_obj.run_io(
            self.fs,
            size=f"{self.fio_size}g",
            rate="1500M",
            fio_filename=self.filename,
            direct=1,
            invalidate=0,
            rate_process=None,
            buffer_pattern="0xdeadface",
            bs="100M",
            jobs=1,
            runtime=0,
            readwrite=self.readwrite,
            rw_ratio=50,
        )
        self.pod_obj.get_fio_results()
        self.lvm.compare_percent_data_from_pvc(self.pvc_obj, self.fio_size)

    @pytest.fixture()
    def create_snapshot(self, snapshot_factory):
        logger.info(f"ℹ️Creating snapshot from {self.pvc_obj.name}")
        self.snapshot = snapshot_factory(self.pvc_obj)
        self.lvm.compare_percent_data_from_pvc(self.snapshot, self.fio_size)

    @pytest.fixture()
    def create_restore(
        self, snapshot_restore_factory, volume_mode, volume_binding_mode
    ):
        logger.info(f"ℹ️Creating restore from snapshot {self.snapshot.name}")
        self.pvc_restore = snapshot_restore_factory(
            self.snapshot,
            storageclass=self.sc_obj.name,
            restore_pvc_name=f"{self.pvc_obj.name}-restore",
            size=str(self.pvc_size * 1024 * 1024 * 1024),
            volume_mode=volume_mode,
            restore_pvc_yaml=constants.CSI_LVM_PVC_RESTORE_YAML,
            access_mode=self.access_mode,
            status=self.status,
        )
        if volume_binding_mode == constants.IMMEDIATE_VOLUMEBINDINGMODE:
            self.lvm.compare_percent_data_from_pvc(self.pvc_restore, self.fio_size)

    @tier1
    @skipif_lvm_not_installed
    @skipif_ocs_version("<4.10")
    def test_create_snapshot_from_pvc_bigger_than_disk(
        self,
        init_lvm,
        namespace,
        storageclass,
        pvc,
        pod,
        run_io,
        create_snapshot,
        create_restore,
        pod_factory,
        volume_binding_mode,
    ):
        """
        test create delete snapshot
        .* Check one disk size
        .* Create PVC with disk size + 50g
        .* Create POD
        .* Run IO PVC size - 10G
        .* Create Snapshot
        .* Create pvc from Snapshot
        .* Attach pod
        .* Check LV size
        .* Run IO

        """
        logger.info(f"ℹ️LVMCluster version is {self.lvm.get_lvm_version()}")
        logger.info(
            f"ℹ️Lvm thin-pool overprovisionRation is {self.lvm.get_lvm_thin_pool_config_overprovision_ratio()}"
        )
        logger.info(
            f"ℹ️Lvm thin-pool sizePrecent is {self.lvm.get_lvm_thin_pool_config_size_percent()}"
        )

        logger.info(f"ℹ️Attaching pod to pvc restore {self.pvc_restore.name}")
        restored_pod_obj = pod_factory(pvc=self.pvc_restore, raw_block_pv=self.block)
        if volume_binding_mode == constants.WFFC_VOLUMEBINDINGMODE:
            self.lvm.compare_percent_data_from_pvc(self.pvc_restore, self.fio_size)
        lv_name = self.lvm.get_lv_name_from_pvc(self.pvc_restore)

        before_fio_thin_pool_util = self.lvm.get_thin_pool1_data_percent()
        logger.info(
            f"ℹ️ lv {lv_name} from pvc {self.pvc_restore.name} utilization after fio is {before_fio_thin_pool_util}"
        )

        restored_pod_obj.run_io(
            self.fs,
            size=f"{self.second_fio_size}g",
            rate="1500M",
            runtime=0,
            rate_process=None,
            fio_filename=f"second-{self.filename}",
            buffer_pattern="0xdeadface",
            direct=1,
            invalidate=0,
            bs="100M",
            jobs=1,
            readwrite=self.readwrite,
        )

        restored_pod_obj.get_fio_results()

        if self.block:
            # WA for https://bugzilla.redhat.com/show_bug.cgi?id=2107859
            # if not self.block:
            if self.fio_size > self.second_fio_size:
                # WA for https://bugzilla.redhat.com/show_bug.cgi?id=2107859 - should be only self.fio_size
                after_expected_util = (
                    float(self.fio_size + self.second_fio_size)
                    / float(self.thin_pool_size)
                    * 100
                )

                lv_util_after_second_fio = float(self.fio_size)
            else:
                # WA for https://bugzilla.redhat.com/show_bug.cgi?id=2107859 - should be only self.second_fio_size
                after_expected_util = (
                    float(self.second_fio_size + self.fio_size)
                    / float(self.thin_pool_size)
                    * 100
                )

                lv_util_after_second_fio = float(self.second_fio_size)

        else:
            after_expected_util = (
                float(self.fio_size + self.second_fio_size) / float(self.thin_pool_size)
            ) * 100
            lv_util_after_second_fio = float(self.fio_size + self.second_fio_size)

        self.lvm.compare_percent_data_from_pvc(
            self.pvc_restore, lv_util_after_second_fio
        )
        try:
            for thin_util_data in TimeoutSampler(
                timeout=60, sleep=1, func=self.lvm.get_thin_pool1_data_percent
            ):

                if (float(thin_util_data) - float(after_expected_util)) < 0.5:
                    logger.info(
                        f"✅✅✅ Test passed - ️utilization from sampler {thin_util_data}"
                        f" is around 0.5% from {after_expected_util}"
                    )
                    break
                logger.info(
                    f"⌛❎ℹ️ utilization from sampler of thin-pool util {thin_util_data}, should be {after_expected_util}"
                )
        except TimeoutError:
            raise LvThinUtilNotChanged(
                f"❌Utilization of thin-pool before restored pod {restored_pod_obj.name} fio is"
                f"{before_fio_thin_pool_util} and after fio is {after_expected_util}"
                f"which should not be the same"
            )
