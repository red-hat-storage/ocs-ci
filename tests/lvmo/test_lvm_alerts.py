import logging
import json

import pytest

from pprint import pprint
from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import skipif_ocs_version, ManageTest
from ocs_ci.utility.utils import get_ocp_version
from ocs_ci.ocs.cluster import LVM
from ocs_ci.ocs.node import get_worker_nodes
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility.prometheus import PrometheusAPI

log = logging.getLogger(__name__)

topolvm_metrics = [
    "topolvm_thinpool_data_percent",
    "topolvm_thinpool_metadata_percent",
    "topolvm_thinpool_size_bytes",
    "topolvm_volumegroup_available_bytes",
    "topolvm_volumegroup_size_bytes",
]

topolvm_alerts = []


def get_thin_provisioning_alerts():
    new_prom = PrometheusAPI()
    alert_full = new_prom.get("alerts")
    alerts_data = alert_full.json().get("data").get("alerts")
    alerts_names = list()
    for entity in alerts_data:
        pprint(entity.get("labels").get("alertname"))
        alerts_names.append(entity.get("labels").get("alertname"))

    return alerts_names


def parse_topolvm_metrics(metric_name):
    """
    Returns the value of the specified metric name

    Args:
        metric_name (string): metric name to be paesed

    Returns:
        str: metric values represened as string
    """

    new_prom = PrometheusAPI()
    metric_full = new_prom.query(metric_name)
    metric_value = metric_full[0].get("value")[1]

    log.info(f"{metric_name} : {metric_value}")
    return metric_value


# functions below to be moved to LVMCluster class after merge of PR#6163
def parse_lvs_cmd(node_name):
    """_summary_

    Args:
        node_name (str): name of the node to run 'lvs' command

    Returns:
        list: list contains dictionaries with each logical volue attributes
    """

    oc_obj = OCP()
    output = oc_obj.exec_oc_debug_cmd(
        node_name, cmd_list=["sudo lvs --reportformat json"]
    )
    output_json = json.loads(output)
    output_info = output_json.get("report")[0].get("lv")
    log.debug(f"lvs json output: {output_info}")
    log.info(f"total {len(output_info)} logical volume detected on node: {node_name}")
    return output_info


def logical_volumes_data_by_name(lvs):
    lvs_by_name = dict()
    for lv in lvs:
        lvs_by_name[lv.get("lv_name")] = lv
        log.info(f"LVS_BY_NAME: {lvs_by_name}")

    return lvs_by_name


def get_logical_volume_names(lvs):
    lvs_names = list()
    for lv in lvs:
        lvs_names.append(lv.get("lv_name"))

    return lvs_names


def get_lv_size(lv_name, lvs_info):
    lv = logical_volumes_data_by_name(lvs_info)
    size = lv.get(lv_name).get("lv_size")
    char_to_replace = {"<": "", "g": ""}
    return size.translate(str.maketrans(char_to_replace))


def get_lv_data_precent(lv_name, lvs_info):
    lv = logical_volumes_data_by_name(lvs_info)
    return lv.get(lv_name).get("data_percent")


def get_lv_vg_name(lv_name, lvs_info):
    lv = logical_volumes_data_by_name(lvs_info)
    return lv.get(lv_name).get("vg_name")


def get_lv_metadata_precent(lv_name, lvs_info):
    lv = logical_volumes_data_by_name(lvs_info)
    return lv.get(lv_name).get("metadata_percent")


def get_lv_thin_pool_name(lv_name, lvs_info):
    lv = logical_volumes_data_by_name(lvs_info)
    return lv.get(lv_name).get("pool_lv")


########################################################################
# End of part that need to be moved into cluster.py


class TestLvmCapacityAlerts(ManageTest):
    """
    Test Alerts when LVM capacity is exceeded 75%, 85%

    """

    ocp_version = get_ocp_version()
    access_mode = constants.ACCESS_MODE_RWO
    volume_mode = constants.VOLUME_MODE_FILESYSTEM
    volume_binding_mode = constants.WFFC_VOLUMEBINDINGMODE
    node_name = get_worker_nodes()[0]
    lvs_info = parse_lvs_cmd(node_name)

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
    def pvc(self, pvc_factory_class, lvs_info=lvs_info):
        log.info("Fixture PVC called")
        volume_mode = self.volume_mode
        self.status = constants.STATUS_PENDING
        self.pvc_obj = pvc_factory_class(
            project=self.proj_obj,
            interface=None,
            storageclass=self.sc_obj,
            size=int(float(get_lv_size("thin-pool-1", lvs_info))),
            status=self.status,
            access_mode=self.access_mode,
            volume_mode=volume_mode,
        )

    @pytest.fixture()
    def pod(self, pod_factory_class):
        self.block = False
        self.pod_obj = pod_factory_class(pvc=self.pvc_obj, raw_block_pv=self.block)

    @skipif_ocs_version("<4.10")
    def test_thin_pool_capacity_alert(self, namespace, storageclass, pvc, pod):
        # for metric_name in topolvm_metrics:
        #     parse_topolvm_metrics(metric_name)
        # get_thin_provisioning_alerts()
        # node_name = "apolak-jon29"
        # pprint(parse_lvs_cmd(node_name))
        log.info("Test Started successfully")
        lvm = LVM()
        log.info(f"LVMCluster version is {lvm.get_lvm_version()}")
        log.info(
            f"Lvm thin-pool overprovisionRation is {lvm.get_lvm_thin_pool_config_overprovision_ratio()}"
        )
        log.info(
            f"Lvm thin-pool sizePrecent is {lvm.get_lvm_thin_pool_config_size_percent()}"
        )
        lvs_info = parse_lvs_cmd(self.node_name)
        size_full = get_lv_size("thin-pool-1", lvs_info)
        size_to_70 = f"{int(float(size_full)*0.7)}Gi"
        size_to_76 = f"{int(float(size_full)*0.06)}Gi"
        size_to_86 = f"{int(float(size_full)*0.1)}Gi"
        sizes_list = [
            {"size_to_fill": size_to_70, "file_name": "run-to-70"},
            {"size_to_fill": size_to_76, "file_name": "run-to-76"},
            {"size_to_fill": size_to_86, "file_name": "run-to-86"},
        ]

        log.info(f"LV Size:{size_full}")
        for size in sizes_list:
            log.info({size.get("size_to_fill")}, {size.get("file_name")})
            self.pod_obj.run_io(
                storage_type="fs",
                size=size.get("size_to_fill"),
                io_direction="w",
                rw_ratio=0,
                jobs=1,
                runtime=1800,
                depth=4,
                rate="250m",
                rate_process="poisson",
                fio_filename=size.get("file_name"),
                bs="4M",
                end_fsync=0,
                invalidate=None,
                buffer_compress_percentage=None,
                buffer_pattern=None,
                readwrite=None,
                direct=1,
                verify=False,
                fio_installed=False,
            )
            log.info(self.pod_obj.get_fio_results(timeout=1800))
            for metric in topolvm_metrics:
                parse_topolvm_metrics(metric)
