"""
<<<<<<< HEAD
Helper functions file for OCS QE
"""
import os
import logging
import datetime
from ocs import ocp
from utility import templating

logger = logging.getLogger(__name__)

PROJECT_NAME = 'test-project'

OCP = ocs.ocp.OCP(kind='Service', namespace=PROJECT_NAME)

TEMP_YAML = os.path.join("templates/ocs-deployment", "temp.yaml")
TEMPLATES_DIR = "templates/ocs-deployment"


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



def run_io(pod_name):
    """
    Run IO to a file within the pod

    Args:
        pod_name (str): The name of the pod

    """
    from ipdb import set_trace; set_trace()
    OCP.exec_oc_cmd(
        f"exec -t ocsci-pod -- bash -c \"touch /var/lib/www/html/test; while"
        f" true; do cp /var/lib/www/html/test /var/lib/www/html/test1; done\""
    )


def create_ceph_block_pool(cbp_name, project_name):
    """

    Args:
        cbp_name: The name of the new Ceph block pool
        project_name: The nam of the project/namespace of
            which the PVC belongs to

    Returns:
        bool: True for successful Ceph block pool creation,
            False otherwise

    """
    cbp_kwargs = {}
    cbp_kwargs['cephblockpool_name'] = cbp_name
    template = os.path.join(TEMPLATES_DIR, "CephBlockPool.yaml")
    logger.info(f'Creating a Ceph Block Pool')
    templating.dump_to_temp_yaml(template, TEMP_YAML, **cbp_kwargs)
    cbp = ocp.OCP(kind='CephBlockPool', namespace=project_name)
    assert cbp.create(yaml_file=TEMP_YAML), (
        f"Failed to create Ceph block pool {cbp_name}"
    )
    open(TEMP_YAML, 'w').close()
    assert cbp.wait_for_resource(
        condition='Available', resource_name=cbp_name
    ), f"Ceph block pool {cbp_name} failed to reach status Available'"


def delete_pvc(pvc_name, project_name):
    """
    Delete a PVC

    Args:
        pvc_name (str): The name of the PVC to delete
        project_name (str): The name of the project/namespace of
            which the PVC belongs to

    Returns:
        bool: True for successful PVC deletion, False otherwise

    """
    ocp_pvc = ocs.ocp.OCP(kind='PersistentVolumeClaim', namespace=project_name)
    template = os.path.join(TEMPLATES_DIR, "PersistentVolumeClaim.yaml")
    logger.info(f"Deleting PVC")
    templating.dump_to_temp_yaml(template, TEMP_YAML)
    assert ocp_pvc.delete(yaml_file=TEMP_YAML)
    open(TEMP_YAML, 'w').close()
