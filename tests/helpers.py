"""
Helper functions file for OCS QE
"""
import os
import logging
import datetime
from ocs import ocp
from utility import templating
from utility.utils import delete_file

logger = logging.getLogger(__name__)

CEPH_BLOCK_POOL = "CephBlockPool"
STORAGE_CLASS = "StorageClass"
PVC = "PersistentVolumeClaim"
POD = "pod"

DEFAULT_CBP_YAML = "CephBlockPool.yaml"
DEFAULT_SC_YAML = "StorageClass.yaml"
DEFAULT_POD_YAML = "Pod.yaml"
DEFAULT_PVC_YAML = "PersistentVolumeClaim.yaml"

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


def create_resource(
    resource_kind, resource_name, project_name, yaml_file, **kwargs
):
    """
    Create a resource

    Args:
        resource_kind (str): The kind of the resource (e.g. CephBlockPool)
        resource_name (str): The name of the resource
        project_name (str): The name of the project that the new resource
            should be associated with
        yaml_file (str): The yaml file to use (e.g. CephBlockPool.yaml)
        **kwargs: Keyword args to be set for the new created resource

    Raises:
        AssertionError: In case of any failure
    """
    template = os.path.join(TEMPLATES_DIR, yaml_file)
    templating.dump_to_temp_yaml(template, TEMP_YAML, **kwargs)
    ocp_obj = ocp.OCP(kind=resource_kind, namespace=project_name)
    logger.info(f"Creating a {resource_kind} {resource_name}")
    assert ocp_obj.create(yaml_file=TEMP_YAML), (
        f"Failed to create {resource_kind} {resource_name}"
    )
    assert ocp_obj.wait_for_resource(
        condition='Available', resource_name=resource_name
    ), f"Ceph block pool {resource_name} failed to reach status Available"
    delete_file(TEMP_YAML)


def delete_resource(
    resource_kind, resource_name, project_name, yaml_file, **kwargs
):
    """
    Delete a resource

    Args:
        resource_kind (str): The kind of the resource (e.g. CephBlockPool)
        resource_name (str): The name of the resource
        project_name (str): The name of the project that the resource
            is associated with
        yaml_file (str): The yaml file to use (e.g. CephBlockPool.yaml)

    Raises:
        AssertionError: In case of any failure
    """
    ocp_obj = ocp.OCP(kind=resource_kind, namespace=project_name)
    if ocp_obj.get(resource_name=resource_name):
        template = os.path.join(TEMPLATES_DIR, yaml_file)
        templating.dump_to_temp_yaml(template, TEMP_YAML, **kwargs)
        logger.info(f"Deleting {resource_kind} {resource_name}")
        assert ocp_obj.delete(yaml_file=TEMP_YAML), (
            f"Failed to delete {resource_kind} {resource_name}"
        )
        assert ocp_obj.wait_for_resource(
            condition='', resource_name=resource_name, to_delete=True
        ), f"{resource_kind} {resource_name} still exists'"
        delete_file(TEMP_YAML)


def create_ceph_block_pool(cbp_name, project_name, yaml_file=DEFAULT_CBP_YAML):
    """
    Create a Ceph block pool

    Args:
        cbp_name: The name of the new Ceph block pool
        project_name: The nam of the project/namespace of
            which the Ceph block pool belongs to
        yaml_file (str): The yaml file to use

    Raises:
        AssertionError: In case of any failure

    """
    cbp_kwargs = dict()
    cbp_kwargs['cephblockpool_name'] = cbp_name
    create_resource(CEPH_BLOCK_POOL, cbp_name, project_name, yaml_file, **cbp_kwargs)


def create_storageclass(sc_name, project_name, cbp_name, yaml_file=DEFAULT_SC_YAML):
    """
    Create a storage class

    Args:
        sc_name (str): The name of the storage class
        project_name (str): The nam of the project/namespace of
            which the storage class belongs to
        cbp_name (str): The Ceph block pool that the storage class should be associated with
        yaml_file (str): The yaml file to use

    Raises:
        AssertionError: In case of any failure
    """
    sc_kwargs = dict()
    sc_kwargs['sc_name'] = sc_name
    sc_kwargs['ceph_block_pool_name'] = cbp_name
    create_resource(
        STORAGE_CLASS, sc_name, project_name, yaml_file, **sc_kwargs
    )


def create_pvc(pvc_name, project_name, yaml_file=DEFAULT_PVC_YAML):
    """
    Create a persistent Volume Claim

    Args:
        pvc_name (str): The name of the PVC to create
        project_name (str): The name of the project/namespace of
            which the PVC belongs to
        yaml_file (str): The yaml file to use

    Raises:
        AssertionError: In case of any failure
    """
    pvc_kwargs = dict()
    pvc_kwargs['pvc_name'] = pvc_name
    create_resource(PVC, pvc_name, project_name, yaml_file, **pvc_kwargs)


def create_pod(pod_name, project_name, pvc_name, yaml_file=DEFAULT_POD_YAML):
    """
    Create a pod

    Args:
        pod_name (str): The name of the pod to create
        project_name (str): The name of the project/namespace of
            which the pod belongs to
        pvc_name (str): The name of the PVC for the pod
        yaml_file (str): The yaml file to use

    Raises:
        AssertionError: In case of any failure
    """
    pod_kwargs = dict()
    pod_kwargs['pod_name'] = pod_name
    pod_kwargs['pvc_name'] = pvc_name
    create_resource(PVC, pod_name, project_name, yaml_file, **pod_kwargs)


def create_project(project_name):
    """
    Create a project

    Args:
        project_name (str): The name of the project to create

    Raises:
        AssertionError: In case of any failure
    """
    ocp_project = ocp.OCP(kind='namespace')
    assert ocp_project.create(resource_name=project_name), (
        f"Failed to delete project {project_name}"
    )


def delete_project(project_name):
    """
    Delete a project

    Args:
        project_name (str): The name of the project to delete

    Raises:
        AssertionError: In case of any failure
    """
    ocp_project = ocp.OCP(kind='namespace')
    assert ocp_project.delete(resource_name=project_name), (
        f"Failed to create project {project_name}"
    )


def delete_ceph_block_pool(cbp_name, project_name, yaml_file=DEFAULT_CBP_YAML):
    """
    Delete a Ceph block pool

    Args:
        cbp_name (str): The name of the Ceph block pool to delete
        project_name (str): The name of the project/namespace of
            which the Ceph block pool belongs to
        yaml_file (str): The yaml file to use

    Raises:
        AssertionError: In case of any failure
    """
    cbp_kwargs = dict()
    cbp_kwargs['cephblockpool_name'] = cbp_name
    delete_resource(
        CEPH_BLOCK_POOL, cbp_name, project_name, yaml_file, **cbp_kwargs
    )


def delete_storage_class(sc_name, project_name, yaml_file=DEFAULT_SC_YAML):
    """
    Delete a storage class

    Args:
        sc_name (str): The name of the storage class to delete
        project_name (str): The name of the project/namespace of
            which the storage class belongs to
        yaml_file (str): The yaml file to use

    Raises:
        AssertionError: In case of any failure
    """
    sc_kwargs = dict()
    sc_kwargs['sc_name'] = sc_name
    delete_resource(
        STORAGE_CLASS, sc_name, project_name, yaml_file, **sc_kwargs
    )


def delete_pod(pod_name, project_name, yaml_file=DEFAULT_POD_YAML):
    """
    Delete a pod

    Args:
        pod_name (str): The name of the pod to delete
        project_name (str): The name of the project/namespace of
            which the pod belongs to
        yaml_file (str): The yaml file to use

    Raises:
        AssertionError: In case of any failure
    """
    pod_kwargs = dict()
    pod_kwargs['pod_name'] = pod_name
    delete_resource(POD, pod_name, project_name, yaml_file, **pod_kwargs)


def delete_pvc(pvc_name, project_name, yaml_file=DEFAULT_PVC_YAML):
    """
    Delete a persistent Volume Claim

    Args:
        pvc_name (str): The name of the PVC to delete
        project_name (str): The name of the project/namespace of
            which the PVC belongs to
        yaml_file (str): The yaml file to use

    Raises:
        AssertionError: In case of any failure
    """
    pvc_kwargs = dict()
    pvc_kwargs['pvc_name'] = pvc_name
    delete_resource(PVC, pvc_name, project_name, yaml_file, **pvc_kwargs)



def run_io(pod_name, project_name):
    """
    Run IO to a file within the pod

    Args:
        pod_name (str): The name of the pod
        project_name (str): The name of the project/namespace of
            which the PVC belongs to

    """
    pod = ocp.OCP(kind='pod', namespace=project_name)
    pod.exec_oc_cmd(
        f"rsh {pod_name} bash -c \"touch /var/lib/www/html/test; while"
        f" true; do cp /var/lib/www/html/test /var/lib/www/html/test1; done\""
    )
