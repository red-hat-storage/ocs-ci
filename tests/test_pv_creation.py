"""
A test for creating a PV
"""
import os
import logging
import ocs.defaults as defaults
import yaml

from ocs import exceptions
from ocs import ocp
from utility import utils, templating
from ocsci.enums import StatusOfTest


log = logging.getLogger(__name__)

PV_YAML = os.path.join("templates/ocs-deployment", "PersistentVolume.yaml")
TEMP_YAML_FILE = 'test.yaml'
VOLUME_DELETED = 'persistentvolume "{volume_name}" deleted'


OCP = ocp.OCP(
    kind='PersistentVolume', namespace=defaults.ROOK_CLUSTER_NAMESPACE
)


def create_pv(**kwargs):
    """
    Create a new Persistent Volume
    """
    file_y = templating.generate_yaml_from_jinja2_template_with_data(
        PV_YAML, **kwargs
    )
    with open(TEMP_YAML_FILE, 'w') as yaml_file:
        yaml.dump(file_y, yaml_file, default_flow_style=False)
        log.info(f"Creating new Persistent Volume")
    assert OCP.create(yaml_file=TEMP_YAML_FILE)
    return OCP.wait(resource_name=kwargs['pv_name'], condition='Available')


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


def run(**kwargs):
    """
    A simple function to exercise a resource creation through api-client
    """

    pv_data = {}
    pv_name = 'my-pv1'
    pv_data['pv_name'] = pv_name
    pv_data['pv_size'] = '3Gi'
    assert create_pv(**pv_data)
    assert verify_pv_exist(pv_name)
    assert delete_pv(pv_name)
    assert not verify_pv_exist(pv_name)
    utils.delete_file(TEMP_YAML_FILE)
    return StatusOfTest.PASSED
