"""
<<<<<<< HEAD
Helper functions file for OCS QE
"""
import os
import logging
import datetime
import ocs.ocp
import ocs.defaults as defaults
from utility import templating

logger = logging.getLogger(__name__)


OCP = ocs.ocp.OCP(
    kind='Service', namespace=defaults.ROOK_CLUSTER_NAMESPACE
)
PVC = ocs.ocp.OCP(
    kind='PersistentVolumeClaim', namespace=defaults.ROOK_CLUSTER_NAMESPACE
)

TEMP_YAML = os.path.join("templates/ocs-deployment", "temp.yaml")
TEMPLATES_DIR = "templates/ocs-deployment"
PROJECT_NAME = 'test-project'


def create_unique_resource_name(resource_description, resource_type):
    """
    Creates a unique object name by using the object_description
    and object_type, as well as the current date/time string.

    Args:
        resource_description (str): The user provided object description
        resource_type (str): The type of object for which the unique name
            will be created. For example: project, pvc, etc

    Returns:
        str: A unique name
    """
    current_date_time = (
        datetime.datetime.now().strftime("%d%H%M%S%f")
    )
    return f"{resource_type}_{resource_description[:23]}_{current_date_time[:10]}"


def run_io():
    """
    Run IO to a file within the pod

    """
    OCP.exec_oc_cmd(
        f"rsh -n openshift-storage ocsci-pod touch /var/lib/www/html/test && "
        f"dd if=/dev/urandom of=/var/lib/www/html/test bs=1M count=3000 &"
    )

def delete_pvc():
    """

    Returns:

    """
    template = os.path.join(TEMPLATES_DIR, "PersistentVolumeClaim.yaml")
    logger.info(f"Deleting PVC")
    templating.dump_to_temp_yaml(template, TEMP_YAML)
    assert PVC.delete(yaml_file=TEMP_YAML)
    open(TEMP_YAML, 'w').close()
