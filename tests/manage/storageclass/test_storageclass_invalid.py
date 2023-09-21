import logging

import pytest
import os.path
import yaml

from ocs_ci.ocs.resources.pvc import PVC
from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import tier3, ManageTest
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility import templating


logger = logging.getLogger(__name__)

OCS_311_CEPHFS_PARAMS = {
    "template_dir": constants.TEMPLATE_CSI_FS_DIR,
    "values": {
        "storageclass_name": "invalid-storageclass",
        "provisioner": "invalid_provisioner",
        "monitors": "invalid_monitors",
        "provision_volume": "invalid_provisioner_volume",
        "ceph_pool": "invalid_pool",
        "root_path": "invalid_root_path",
        "provisioner_secret_name": "invalid_provisioner_secret_name",
        "provisioner_secret_namespace": "invalid_provisioner_secret_namespace",
        "node_stage_secret_name": "invalid_node_stage_secret_name",
        "node_stage_secret_namespace": "invalid_node_stage_secret_namespace",
        "mounter": "invalid_mounter",
        "reclaim_policy": "Delete",
    },
}

OCS_341_RBD_PARAMS = {
    "template_dir": constants.TEMPLATE_CSI_RBD_DIR,
    "values": {
        "storageclass_name": "invalid-storageclass",
        "provisioner": "invalid_provisioner",
        "monitors": "invalid_monitors",
        "pool": "invalid_pool",
        "imageFormat": "invalid_format",
        "imageFeatures": "invalid_features",
        "provisioner_secret_name": "invalid_provisioner_secret_name",
        "provisioner_secret_namespace": "invalid_provisioner_secret_namespace",
        "node_stage_secret_name": "invalid_node_stage_secret_name",
        "node_stage_secret_namespace": "invalid_node_stage_secret_namespace",
        "mounter": "invalid_mounter",
        "reclaim_policy": "Delete",
    },
}


@pytest.fixture(
    params=[
        pytest.param(OCS_311_CEPHFS_PARAMS, marks=pytest.mark.polarion_id("OCS-331")),
        pytest.param(OCS_341_RBD_PARAMS, marks=pytest.mark.polarion_id("OCS-341")),
    ],  # TODO: add more test case parameters
    ids=["CephFS", "RBD"],
)
def invalid_storageclass(request):
    """
    Creates a CephFS or RBD StorageClass with invalid parameters.

    Storageclass is removed at the end of test.

    Returns:
        str: Name of created StorageClass
    """
    logger.info(
        f"SETUP - creating storageclass "
        f"{request.param['values']['storageclass_name']}"
    )
    yaml_path = os.path.join(request.param["template_dir"], "storageclass.yaml")
    with open(yaml_path, "r") as fd:
        yaml_data = yaml.safe_load(fd)
    yaml_data.update(request.param["values"])
    storageclass = OCS(**yaml_data)
    sc_data = storageclass.create()

    logger.debug("Check that storageclass has assigned creationTimestamp")
    assert sc_data["metadata"]["creationTimestamp"]

    yield sc_data

    logger.info(
        f"TEARDOWN - removing storageclass "
        f"{request.param['values']['storageclass_name']}"
    )
    storageclass.delete()


@green_squad
@tier3
class TestStorageClassInvalid(ManageTest):
    def test_storageclass_invalid(self, invalid_storageclass):
        """
        Test that Persistent Volume Claim can not be created from misconfigured
        CephFS Storage Class.
        """
        pvc_data = templating.load_yaml(constants.CSI_PVC_YAML)
        pvc_name = helpers.create_unique_resource_name("test", "pvc")
        pvc_data["metadata"]["name"] = pvc_name
        pvc_data["metadata"]["namespace"] = config.ENV_DATA["cluster_namespace"]
        pvc_data["spec"]["storageClassName"] = invalid_storageclass["metadata"]["name"]
        logger.info(
            f"Create PVC {pvc_name} "
            f"with storageClassName "
            f"{invalid_storageclass['metadata']['name']}"
        )
        pvc = PVC(**pvc_data)
        pvc.create()

        pvc_status = pvc.status
        logger.debug(f"Status of PVC {pvc_name} after creation: {pvc_status}")
        assert pvc_status == constants.STATUS_PENDING

        logger.info(
            f"Waiting for status '{constants.STATUS_BOUND}' "
            f"for 60 seconds (it shouldn't change)"
        )
        with pytest.raises(TimeoutExpiredError):
            # raising TimeoutExpiredError is expected behavior
            pvc_status_changed = pvc.ocp.wait_for_resource(
                resource_name=pvc_name,
                condition=constants.STATUS_BOUND,
                timeout=60,
                sleep=20,
            )
            logger.debug("Check that PVC status did not changed")
            assert not pvc_status_changed

        pvc_status = pvc.status
        logger.info(f"Status of PVC {pvc_name} after 60 seconds: {pvc_status}")
        assert_msg = (
            f"PVC {pvc_name} hasn't reached status " f"{constants.STATUS_PENDING}"
        )
        assert pvc_status == constants.STATUS_PENDING, assert_msg

        logger.info(f"Deleting PVC {pvc_name}")
        pvc.delete()
