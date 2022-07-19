import time
import logging


import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import skipif_ocs_version, ManageTest
from ocs_ci.ocs.cluster import LVM
from ocs_ci.ocs.node import get_worker_nodes
from ocs_ci.utility.prometheus import PrometheusAPI

log = logging.getLogger(__name__)
new_prom = PrometheusAPI()

topolvm_metrics = [
    "topolvm_thinpool_data_percent",
    "topolvm_thinpool_metadata_percent",
    "topolvm_thinpool_size_bytes",
    "topolvm_volumegroup_available_bytes",
    "topolvm_volumegroup_size_bytes",
]

topolvm_alerts = {
    "vg_data_75_precent": "VolumeGroupUsageAtThresholdNearFull",
    "vg_data_85_precent": "VolumeGroupUsageAtThresholdCritical",
    "tp_data_75_precent": "ThinPoolDataUsageAtThresholdNearFull",
    "tp_data_85_precent": "ThinPoolDataUsageAtThresholdCritical",
    "metadata_75_precent": "ThinPoolMetaDataUsageAtThresholdNearFull",
    "metadata_85_precent": "ThinPoolMetaDataUsageAtThresholdCritical",
}


def get_thin_provisioning_alerts():
    """
    Get the list of alerts that active in the cluster

    Returns:
        list: alrets name

    """
    alert_full = new_prom.get("alerts")
    alerts_data = alert_full.json().get("data").get("alerts")
    alerts_names = list()
    for entity in alerts_data:
        log.debug(entity.get("labels").get("alertname"))
        alerts_names.append(entity.get("labels").get("alertname"))

    return alerts_names


def check_for_alert(alert_name):
    """
    Check to see if a given alert is available

    Args:
        alert_name (str): Alert name

    Returns:
    bool: True if alert is available else False

    """
    if alert_name in get_thin_provisioning_alerts():

        return True

    return False


def parse_topolvm_metrics(metrics):
    """
    Returns the name and value of topolvm metrics

    Args:
        metric_name (list): metrics name to be paesed

    Returns:
        dict: topolvm metrics by: names: value
    """
    metrics_short = dict()

    for metric_name in metrics:
        metric_full = new_prom.query(metric_name)
        metric_value = metric_full[0].get("value")[1]
        log.info(f"{metric_name} : {metric_value}")
        metrics_short[metric_name] = metric_value

    return metrics_short


class TestLvmCapacityAlerts(ManageTest):
    """
    Test Alerts when LVM capacity is exceeded 75%, 85%

    """

    access_mode = constants.ACCESS_MODE_RWO
    volume_mode = constants.VOLUME_MODE_FILESYSTEM
    volume_binding_mode = constants.WFFC_VOLUMEBINDINGMODE
    node_name = get_worker_nodes()[0]

    @pytest.fixture()
    def init_lvm(self):
        self.lvm = LVM()
        disk1 = self.lvm.pv_data["pv_list"][0]
        self.disk_size = self.lvm.pv_data[disk1]["pv_size"]
        self.thin_pool_size = float(self.lvm.get_thin_pool1_size())
        self.pvc_size = int(self.thin_pool_size)

    @pytest.fixture()
    def storageclass(
        self,
        lvm_storageclass_factory_class,
        volume_binding_mode=constants.WFFC_VOLUMEBINDINGMODE,
    ):
        self.sc_obj = lvm_storageclass_factory_class(volume_binding_mode)

    @pytest.fixture()
    def namespace(self, project_factory_class):
        self.proj_obj = project_factory_class()
        self.proj = self.proj_obj.namespace

    @pytest.fixture()
    def pvc(self, pvc_factory_class):
        log.info("Fixture PVC called")
        volume_mode = self.volume_mode
        self.status = constants.STATUS_PENDING
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
    def pod(self, pod_factory_class):
        self.block = False
        self.pod_obj = pod_factory_class(pvc=self.pvc_obj, raw_block_pv=self.block)

    @skipif_ocs_version("<4.10")
    def test_thin_pool_capacity_alert(
        self, namespace, init_lvm, storageclass, pvc, pod
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
                "pvc_expected_size": f"{float(self.pvc_size)*0.76}",
                "alert": topolvm_alerts.get("tp_data_75_precent"),
            },
            {
                "size_to_fill": size_to_86,
                "file_name": "run-to-86",
                "pvc_expected_size": f"{float(self.pvc_size)*0.86}",
                "alert": topolvm_alerts.get("tp_data_85_precent"),
            },
        ]

        log.info(f"LV Size:{self.thin_pool_size}")
        self.metric_data = dict()
        for size in sizes_list:
            log.info(
                f"{size.get('size_to_fill')}, {size.get('file_name')}, {size.get('pvc_expected_size')}"
            )
            self.pod_obj.run_io(
                storage_type="fs",
                size=size.get("size_to_fill"),
                rw_ratio=0,
                jobs=1,
                runtime=0,
                depth=4,
                rate="1250m",
                rate_process=None,
                fio_filename=size.get("file_name"),
                bs="100M",
                end_fsync=0,
                invalidate=None,
                buffer_pattern=None,
                readwrite="write",
                direct=1,
                verify=False,
            )
            self.pod_obj.get_fio_results()
            # This is a workaround for BZ-2108018
            log.info("Wait 10 minutes for metrics refresh interval")
            time.sleep(600)
            # End of workaround
            self.lvm.compare_percent_data_from_pvc(
                self.pvc_obj, float(size["pvc_expected_size"])
            )

            val = parse_topolvm_metrics(topolvm_metrics)
            self.metric_data = val
            log.info(self.metric_data)
            log.info(f"getting alerts: {get_thin_provisioning_alerts()}")
            if size["file_name"] == "run-to-70":
                assert not check_for_alert(size.get("alert")), "Alert already exists"
            else:
                log.info(f"size: {size['file_name']}")
                assert check_for_alert(size.get("alert")), "Alert not found"
