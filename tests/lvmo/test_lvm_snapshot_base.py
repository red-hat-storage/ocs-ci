import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import tier1, skipif_lvm_not_installed
from ocs_ci.framework.testlib import skipif_ocs_version, ManageTest

from ocs_ci.utility import templating
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility.utils import get_ocp_version
from ocs_ci.ocs.cluster import LVM
from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.ocs.resources.pod import cal_md5sum
from ocs_ci.ocs.exceptions import Md5CheckFailed

logger = logging.getLogger(__name__)


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

    @pytest.fixture()
    def namespace(self, project_factory_class):
        self.proj_obj = project_factory_class()
        self.proj = self.proj_obj.namespace

    @pytest.fixture()
    def storageclass(self, volume_binding_mode, storageclass_factory_class):
        if volume_binding_mode == constants.WFFC_VOLUMEBINDINGMODE:
            sc_ocp_obj = OCP(kind="StorageClass", resource_name=constants.LVM_SC)
            self.sc_obj = OCS(**sc_ocp_obj.data)
        elif volume_binding_mode == constants.IMMEDIATE_VOLUMEBINDINGMODE:
            sc_obj = templating.load_yaml(constants.CSI_LVM_STORAGECLASS_YAML)
            sc_obj["metadata"]["name"] = create_unique_resource_name(
                resource_description="immediate-test",
                resource_type="storageclass",
            )
            sc_obj["volumeBindingMode"] = constants.IMMEDIATE_VOLUMEBINDINGMODE
            self.sc_obj = storageclass_factory_class(custom_data=sc_obj)

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

    @pytest.fixture()
    def run_io(self, volume_mode):
        self.fs = "fs"
        self.block = False
        if volume_mode == constants.VOLUME_MODE_BLOCK:
            self.fs = "block"
            self.block = True
        self.pod_obj.run_io(
            self.fs,
            size="25g",
            rate="1500m",
            runtime=0,
            invalidate=0,
            buffer_compress_percentage=60,
            buffer_pattern="0xdeadface",
            bs="1024K",
            jobs=1,
            readwrite="readwrite",
        )
        self.pod_obj.get_fio_results()
        if not self.block:
            self.origin_pod_md5 = cal_md5sum(
                pod_obj=self.pod_obj, file_name="fio-rand-readwrite", block=self.block
            )

    @pytest.fixture()
    def create_snapshot(self, snapshot_factory):
        logger.info(f"Creating snapshot from {self.pvc_obj.name}")
        self.snapshot = snapshot_factory(self.pvc_obj)

    @pytest.fixture()
    def create_restore(self, snapshot_restore_factory, volume_mode):
        logger.info(f"Creating restore from snapshot {self.snapshot.name}")
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

    @tier1
    @skipif_lvm_not_installed
    @skipif_ocs_version("<4.10")
    def test_create_snapshot_from_pvc(
        self,
        namespace,
        storageclass,
        pvc,
        pod,
        run_io,
        create_snapshot,
        create_restore,
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
        lvm = LVM()
        logger.info(f"LVMCluster version is {lvm.get_lvm_version()}")
        logger.info(
            f"Lvm thin-pool overprovisionRation is {lvm.get_lvm_thin_pool_config_overprovision_ratio()}"
        )
        logger.info(
            f"Lvm thin-pool sizePrecent is {lvm.get_lvm_thin_pool_config_size_percent()}"
        )

        logger.info(f"Attaching pod to pvc restore {self.pvc_restore.name}")
        restored_pod_obj = pod_factory(pvc=self.pvc_restore, raw_block_pv=self.block)
        if not self.block:
            restored_pod_md5 = cal_md5sum(
                pod_obj=self.pod_obj, file_name="fio-rand-readwrite", block=self.block
            )
            if restored_pod_md5 != self.origin_pod_md5:
                raise Md5CheckFailed(
                    f"origin pod {self.pod_obj.name} md5 value {self.origin_pod_md5} "
                    f"is not the same as restored pod {restored_pod_obj.name} md5 "
                    f"value {restored_pod_md5}"
                )

        restored_pod_obj.run_io(
            self.fs,
            size="10g",
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
        if not self.block:
            restored_pod_md5_second = cal_md5sum(
                pod_obj=restored_pod_obj,
                file_name="fio-rand-readwrite",
                block=self.block,
            )
            if restored_pod_md5_second == self.origin_pod_md5:
                raise Md5CheckFailed(
                    f"origin pod {self.pod_obj.name} md5 value {self.origin_pod_md5} "
                    f"is not suppose to be the same as restored pod {restored_pod_obj.name} md5 "
                    f"value {restored_pod_md5_second}"
                )
