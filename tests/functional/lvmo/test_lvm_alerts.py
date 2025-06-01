import logging
import time
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    skipif_lvm_not_installed,
    aqua_squad,
)
from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import skipif_ocs_version, ManageTest
from ocs_ci.ocs.cluster import LVM
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.utility.lvmo_utils import lvmo_health_check


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
class TestLvmCapacityAlerts(ManageTest):
    """
    Test Alerts when LVM capacity is exceeded 75%, 85%

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
        self.pvc_size = int(self.thin_pool_size)

    @pytest.fixture()
    def storageclass(self, lvm_storageclass_factory_class, volume_binding_mode):
        self.sc_obj = lvm_storageclass_factory_class(volume_binding=volume_binding_mode)

    @pytest.fixture()
    def namespace(self, project_factory):
        self.proj_obj = project_factory()
        self.proj = self.proj_obj.namespace

    @tier1
    @skipif_lvm_not_installed
    @skipif_ocs_version("<4.11")
    def test_thin_pool_capacity_alert(
        self,
        namespace,
        init_lvm,
        storageclass,
        status,
        volume_mode,
        pvc_factory,
        pod_factory,
        volume_binding_mode,
        threading_lock,
    ):
        """

        Test to verify thin pool capacity alert:
        1. run io up to 70, check alerts - no alert expected
        2. run io up to 76, check alerts
        3. run io up to 86, check alerts - critical alert expected

        """
        log.info("Test Started successfully")
        log.info(f"LVMCluster version is {self.lvm.get_lvm_version()}")
        log.info(
            f"Lvm thin-pool overprovisionRation is {self.lvm.get_lvm_thin_pool_config_overprovision_ratio()}"
        )
        log.info(
            f"Lvm thin-pool sizePrecent is {self.lvm.get_lvm_thin_pool_config_size_percent()}"
        )
        size_to_70 = f"{int(float(self.thin_pool_size)*0.7)}Gi"
        size_to_76 = f"{int(float(self.thin_pool_size)*0.06)}Gi"
        size_to_86 = f"{int(float(self.thin_pool_size)*0.1)}Gi"
        sizes_list = [
            {
                "size_to_fill": size_to_70,
                "file_name": "run-to-70",
                "pvc_expected_size": f"{float(self.pvc_size)*0.7}",
                "alert": None,
            },
            {
                "size_to_fill": size_to_76,
                "file_name": "run-to-76",
                "pvc_expected_size": f"{float(self.pvc_size)*0.06}",
                "alert": constants.TOPOLVM_ALERTS.get("tp_data_75_precent"),
            },
            {
                "size_to_fill": size_to_86,
                "file_name": "run-to-86",
                "pvc_expected_size": f"{float(self.pvc_size)*0.1}",
                "alert": constants.TOPOLVM_ALERTS.get("tp_data_85_precent"),
            },
        ]

        log.info(f"LV Size:{self.thin_pool_size}")
        self.metric_data = dict()
        pvc_list = list()
        pods_list = list()
        storage_type = "fs"
        if volume_mode == constants.VOLUME_MODE_BLOCK:
            storage_type = "block"
            self.block = True

        for size in sizes_list:
            log.info(
                f"{size.get('size_to_fill')}, {size.get('file_name')}, {size.get('pvc_expected_size')}"
            )
            pvc_list.append(
                pvc_factory(
                    project=self.proj_obj,
                    interface=None,
                    storageclass=self.sc_obj,
                    size=self.pvc_size,
                    status=status,
                    access_mode=self.access_mode,
                    volume_mode=volume_mode,
                )
            )
            pods_list.append(pod_factory(pvc=pvc_list[-1], raw_block_pv=self.block))

            pods_list[-1].run_io(
                storage_type=storage_type,
                size=size.get("size_to_fill"),
                rw_ratio=0,
                jobs=1,
                runtime=0,
                depth=4,
                rate="1250m",
                rate_process=None,
                bs="100M",
                end_fsync=0,
                invalidate=0,
                buffer_pattern=None,
                readwrite="write",
                direct=1,
                verify=False,
                timeout=1800,
            )
            pods_list[-1].get_fio_results(timeout=1800)

            # Workaround for BZ-2108018
            minimal_pvc = pvc_factory(
                project=self.proj_obj,
                interface=None,
                storageclass=self.sc_obj,
                size="1",
                status=status,
                access_mode=self.access_mode,
                volume_mode=volume_mode,
            )
            mini_pod = pod_factory(pvc=minimal_pvc, raw_block_pv=self.block)
            log.info(f"{mini_pod} created")
            mini_pod.delete(wait=True)
            minimal_pvc.delete(wait=True)

            for sample in TimeoutSampler(
                150, 30, self.lvm.check_for_alert, size.get("alert")
            ):
                if size["file_name"] == "run-to-70":
                    time.sleep(60)
                    break
                else:
                    if sample:
                        break
            # End of workaround

            self.lvm.compare_percent_data_from_pvc(
                pvc_list[-1], float(size["pvc_expected_size"])
            )
            expected_os_values = [
                self.lvm.get_thin_pool1_data_percent(),
                self.lvm.get_thin_pool_metadata(),
                self.lvm.get_thin_pool1_size(),
                self.lvm.get_vg_free(),
                self.lvm.get_vg_size(),
            ]

            for metric, expected in zip(constants.TOPOLVM_METRICS, expected_os_values):
                self.lvm.validate_metrics_vs_operating_system_stats(metric, expected)

            log.info(f"getting alerts: {self.lvm.get_thin_provisioning_alerts()}")
            if size["file_name"] == "run-to-70":
                assert not self.lvm.check_for_alert(
                    size.get("alert")
                ), "Alert already exists"
            else:
                log.info(f"size: {size['file_name']}")
                assert self.lvm.check_for_alert(size.get("alert")), "Alert not found"

        lvmo_health_check()
