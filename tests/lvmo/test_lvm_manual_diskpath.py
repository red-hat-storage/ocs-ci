import logging
import os
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework import config
from ocs_ci.utility.templating import load_yaml, dump_data_to_temp_yaml
from ocs_ci.utility.lvmo_utils import delete_lvm_cluster


log = logging.getLogger(__name__)


LVMCLUSTER_TEMPLATE = os.path.join(
    constants.TEMPLATE_DEPLOYMENT_DIR_LVMO, "lvm-cluster-default.yaml"
)

storage_disks_count = config.DEPLOYMENT.get("lvmo_disks")


def create_lvm_cluster_cr_with_device_selector(disks):
    """_summary_

    Args:
        disks (list): _description_
    """
    lvm_cr_dict = load_yaml(LVMCLUSTER_TEMPLATE)
    lvm_cr_dict["spec"]["storage"]["deviceClasses"][0]["deviceSelector"] = {
        "paths": disks
    }
    tmp_file_path = "/tmp/lvm_cr.yaml"
    dump_data_to_temp_yaml(lvm_cr_dict, tmp_file_path)

    return tmp_file_path


@pytest.mark.parametrize(
    argnames=["by"], argvalues=[pytest.param(*["name"])[pytest.param(*["path"])]]
)
def test_create_lvm_cluster_w_manual_disk_selection(by):
    delete_lvm_cluster()
