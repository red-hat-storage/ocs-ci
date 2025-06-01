import logging
import os
import pytest

from tempfile import NamedTemporaryFile

from ocs_ci.ocs import constants
from ocs_ci.utility.templating import load_yaml, dump_data_to_temp_yaml
from ocs_ci.utility.lvmo_utils import (
    get_sno_blockdevices,
    get_sno_disks_by_path,
    lvmo_health_check,
)
from ocs_ci.framework.pytest_customization.marks import (
    skipif_lvm_not_installed,
    tier1,
    aqua_squad,
)
from ocs_ci.framework.testlib import skipif_ocs_version
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility.lvmo_utils import get_lvm_cluster_name


log = logging.getLogger(__name__)

lvmcluster_template = (
    "lvms-cluster.yaml"
    if "lvms" in get_lvm_cluster_name()
    else "lvm-cluster-default.yaml"
)

LVMCLUSTER_TEMPLATE = os.path.join(
    constants.TEMPLATE_DEPLOYMENT_DIR_LVMO, lvmcluster_template
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
    lvm_cluster_cr = NamedTemporaryFile(
        mode="w+", prefix="lvm_cluster_cr", delete=False
    )
    dump_data_to_temp_yaml(lvm_cr_dict, lvm_cluster_cr.name)

    return lvm_cluster_cr.name


@aqua_squad
@tier1
@skipif_lvm_not_installed
@skipif_ocs_version("<4.12")
@pytest.mark.parametrize(
    argnames=["by", "select_all"],
    argvalues=[
        pytest.param(
            *["path", False],
        ),
        pytest.param(
            *["path", True],
        ),
        pytest.param(
            *["name", False],
        ),
        pytest.param(
            *["name", True],
        ),
    ],
)
def test_create_lvm_cluster_w_manual_disk_selection(remove_lvm_cluster, by, select_all):
    """
    Test creation of lvm cluster with manual disk selection,
    by disks name or by disks path

    """
    if by == "name":
        disks = get_sno_blockdevices()
    elif by == "path":
        disks = get_sno_disks_by_path()

    if len(disks) == 1:
        disks_to_use = 1
    elif select_all:
        disks_to_use = len(disks)
    else:
        disks_to_use = int(len(disks) / 2) + 1
    log.info(f"Creating cluster with {disks_to_use} disks")
    lvm_cr = create_lvm_cluster_cr_with_device_selector(disks[:disks_to_use])
    oc_obj = OCP()
    oc_obj.create(yaml_file=lvm_cr)
    lvmo_health_check()
