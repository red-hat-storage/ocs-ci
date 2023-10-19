import pytest
import logging

from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    skipif_lvm_not_installed,
    aqua_squad,
)
from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import skipif_ocs_version, ManageTest
from ocs_ci.ocs.cluster import LVM


log = logging.getLogger(__name__)


@aqua_squad
@pytest.mark.parametrize(
    argnames=["volume_mode", "volume_binding_mode", "status"],
    argvalues=[
        pytest.param(
            *[
                constants.VOLUME_MODE_FILESYSTEM,
                constants.WFFC_VOLUMEBINDINGMODE,
                constants.STATUS_PENDING,
            ],
        ),
        pytest.param(
            *[
                constants.VOLUME_MODE_BLOCK,
                constants.WFFC_VOLUMEBINDINGMODE,
                constants.STATUS_PENDING,
            ],
        ),
        pytest.param(
            *[
                constants.VOLUME_MODE_FILESYSTEM,
                constants.IMMEDIATE_VOLUMEBINDINGMODE,
                constants.STATUS_BOUND,
            ],
        ),
        pytest.param(
            *[
                constants.VOLUME_MODE_BLOCK,
                constants.IMMEDIATE_VOLUMEBINDINGMODE,
                constants.STATUS_BOUND,
            ],
        ),
    ],
)
class TestLVMPVCResize(ManageTest):
    """
    Testing PVC resize on LVM cluster beyond thinpool size, but within overprovisioning rate

    """

    access_mode = constants.ACCESS_MODE_RWO
    block = False

    @pytest.fixture()
    def init_lvm(self, threading_lock):
        self.lvm = LVM(
            fstrim=True, fail_on_thin_pool_not_empty=True, threading_lock=threading_lock
        )
        disk1 = self.lvm.pv_data["pv_list"][0]
        log.info(f"PV List: {self.lvm.pv_data['pv_list']}")
        self.disk_size = self.lvm.pv_data[disk1]["pv_size"]
        self.thin_pool_size = float(self.lvm.get_thin_pool1_size())
        self.int_tp_size = int(self.thin_pool_size)

    @pytest.fixture()
    def storageclass(self, lvm_storageclass_factory_class, volume_binding_mode):
        self.sc_obj = lvm_storageclass_factory_class(volume_binding=volume_binding_mode)

    @pytest.fixture()
    def namespace(self, project_factory):
        self.proj_obj = project_factory()
        self.proj = self.proj_obj.namespace

    @tier1
    @skipif_ocs_version("<4.11")
    @skipif_lvm_not_installed
    def test_pvc_resize(
        self,
        init_lvm,
        status,
        volume_mode,
        storageclass,
        namespace,
        pvc_factory,
        pod_factory,
    ):
        if volume_mode == constants.VOLUME_MODE_BLOCK:
            self.block = True
        self.pvc_size_at_start = self.int_tp_size * 0.7
        self.pvc_size_resize = int(self.int_tp_size * 1.1)
        pvc_obj = pvc_factory(
            project=self.proj_obj,
            interface=None,
            storageclass=self.sc_obj,
            size=self.pvc_size_at_start,
            status=status,
            access_mode=self.access_mode,
            volume_mode=volume_mode,
        )
        pod_obj = pod_factory(pvc=pvc_obj, raw_block_pv=self.block)
        log.info(f"{pod_obj} created")

        pvc_obj.resize_pvc(self.pvc_size_resize, True)
