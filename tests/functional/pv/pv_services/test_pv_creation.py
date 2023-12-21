"""
A test for creating a PV
"""
import logging
import os

import pytest
import yaml

from ocs_ci.framework import config
from ocs_ci.ocs import exceptions, ocp
from ocs_ci.ocs.constants import TEMPLATE_PV_PVC_DIR
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.utility import templating, utils

log = logging.getLogger(__name__)

PV_YAML = os.path.join(TEMPLATE_PV_PVC_DIR, "PersistentVolume.yaml")
TEMP_YAML_FILE = "test.yaml"
VOLUME_DELETED = 'persistentvolume "{volume_name}" deleted'


OCP = ocp.OCP(kind="PersistentVolume", namespace=config.ENV_DATA["cluster_namespace"])


@pytest.fixture(scope="class")
def test_fixture(request):
    """
    Create disks
    """
    self = request.node.cls

    def finalizer():
        teardown(self)

    request.addfinalizer(finalizer)


def teardown(self):
    """
    Tearing down the environment
    """
    if os.path.exists(TEMP_YAML_FILE):
        assert delete_pv(self.pv_name)
        assert not verify_pv_exist(self.pv_name)
        utils.delete_file(TEMP_YAML_FILE)


def create_pv(pv_data):
    """
    Create a new Persistent Volume
    """
    file_y = templating.generate_yaml_from_jinja2_template_with_data(PV_YAML, **pv_data)
    with open(TEMP_YAML_FILE, "w") as yaml_file:
        yaml.dump(file_y, yaml_file, default_flow_style=False)
        log.info("Creating new Persistent Volume")
    assert OCP.create(yaml_file=TEMP_YAML_FILE)
    return OCP.wait_for_resource(
        resource_name=pv_data["pv_name"], condition="Available"
    )


def delete_pv(pv_name):
    """
    Delete a Persistent Volume by given name
    """
    log.info(f"Deleting the Persistent Volume {pv_name}")
    stat = OCP.delete(TEMP_YAML_FILE)
    if stat in VOLUME_DELETED.format(volume_name=pv_name):
        return True
    return False


def verify_pv_exist(pv_name):
    """
    Verify a Persistent Volume exists by a given name
    """
    try:
        OCP.get(pv_name)
    except exceptions.CommandFailed:
        log.info(f"PV {pv_name} doesn't exist")
        return False
    log.info(f"PV {pv_name} exist")
    return True


# @tier1
# Test case is disabled.
# The Recycle reclaim policy is deprecated in OpenShift Container Platform 4.
# Dynamic provisioning is recommended for equivalent and better functionality.
@green_squad
@pytest.mark.usefixtures(
    test_fixture.__name__,
)
class TestPvCreation(ManageTest):
    """
    Testing PV creation
    """

    pv_data = {}
    pv_name = "my-pv1"
    pv_data["pv_name"] = pv_name
    pv_data["pv_size"] = "3Gi"

    def test_pv_creation(self):
        """
        Test PV creation
        """
        assert create_pv(self.pv_data)
        assert verify_pv_exist(self.pv_name)
