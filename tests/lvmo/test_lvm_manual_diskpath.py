import logging
import os

from ocs_ci.ocs import constants
from ocs_ci.utility.templating import load_yaml, dump_data_to_temp_yaml
from ocs_ci.utility.lvmo_utils import (
    delete_lvm_cluster,
    get_blockdevices,
    get_disks_by_path,
    lvmo_health_check,
)
from ocs_ci.framework.pytest_customization.marks import skipif_lvm_not_installed
from ocs_ci.framework.testlib import skipif_ocs_version
from ocs_ci.ocs.ocp import OCP


log = logging.getLogger(__name__)


LVMCLUSTER_TEMPLATE = os.path.join(
    constants.TEMPLATE_DEPLOYMENT_DIR_LVMO, "lvm-cluster-default.yaml"
)


def create_lvm_cluster_cr_with_device_selector(disks):
    """
    Creates LVMCluster configuration file with deviceSelector and disks list

    Args:
        disks (list): list of disks (str) that selected for lvm-cluster

    """
    lvm_cr_dict = load_yaml(LVMCLUSTER_TEMPLATE)
    lvm_cr_dict["spec"]["storage"]["deviceClasses"][0]["deviceSelector"] = {
        "paths": disks
    }
    tmp_file_path = "/tmp/lvm_cr.yaml"
    dump_data_to_temp_yaml(lvm_cr_dict, tmp_file_path)

    return tmp_file_path


@skipif_lvm_not_installed
@skipif_ocs_version("<4.12")
def test_create_lvm_cluster_w_manual_disk_selection(by="name"):
    """
    Test creation of lvm cluster with manual disk selection,
    by disks name or by disks path

    """
    if by == "name":
        disks = get_blockdevices()
    elif by == "path":
        disks = get_disks_by_path()

    disks_to_use = int(len(disks) / 2) + 1
    log.info(f"Creating cluster with {disks_to_use} disks")
    lvm_cr = create_lvm_cluster_cr_with_device_selector(disks[:disks_to_use])
    delete_lvm_cluster()
    oc_obj = OCP()
    oc_obj.create(yaml_file=lvm_cr)
    lvmo_health_check()
