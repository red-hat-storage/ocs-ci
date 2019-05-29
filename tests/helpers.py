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
    cbp_kwargs = {}
    cbp_kwargs['cephblockpool_name'] = cbp_name
    template = os.path.join(TEMPLATES_DIR, yaml_file)
    logger.info(f'Creating a Ceph Block Pool')
    templating.dump_to_temp_yaml(template, TEMP_YAML, **cbp_kwargs)
    cbp = ocp.OCP(kind='CephBlockPool', namespace=project_name)
    assert cbp.create(yaml_file=TEMP_YAML), (
        f"Failed to create Ceph block pool {cbp_name}"
    )
    assert cbp.wait_for_resource(
        condition='Available', resource_name=cbp_name
    ), f"Ceph block pool {cbp_name} failed to reach status Available"
    delete_file(TEMP_YAML)


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
    sc_kwargs = {}
    sc_kwargs['sc_name'] = sc_name
    sc_kwargs['ceph_block_pool_name'] = cbp_name
    template = os.path.join(TEMPLATES_DIR, yaml_file)
    logger.info(f'Creating a storage class')
    ocp_sc = ocp.OCP(kind='StorageClass', namespace=project_name)
    templating.dump_to_temp_yaml(template, TEMP_YAML, **sc_kwargs)
    assert ocp_sc.create(yaml_file=TEMP_YAML), (
        f"Failed to create storage class {sc_name}"
    )
    assert ocp_sc.wait_for_resource(
        condition='Available', resource_name=sc_name, to_delete=True
    ), f"Storage class {sc_name} failed to reach status Available"
    delete_file(TEMP_YAML)


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
    pvc_kwargs = {}
    pvc_kwargs['pvc_name'] = pvc_name
    template = os.path.join(TEMPLATES_DIR, yaml_file)
    logger.info(f'Creating PVC {pvc_name}')
    templating.dump_to_temp_yaml(template, TEMP_YAML, **pvc_kwargs)
    ocp_pvc = ocp.OCP(kind='PersistentVolumeClaim', namespace=project_name)
    assert ocp_pvc.create(yaml_file=TEMP_YAML), (
        f"Failed to create PVC {pvc_name}"
    )
    assert ocp_pvc.wait_for_resource(
        condition='Available', resource_name=pvc_name, to_delete=True
    ), f"PVC {pvc_name} failed to reach status Available"
    delete_file(TEMP_YAML)


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
    pod_kwargs = {}
    pod_kwargs['pod_name'] = pod_name
    pod_kwargs['pvc_name'] = pvc_name
    template = os.path.join(TEMPLATES_DIR, yaml_file)
    logger.info(f'Creating pod {pod_name}')
    templating.dump_to_temp_yaml(template, TEMP_YAML, **pod_kwargs)
    ocp_pod = ocp.OCP(kind='pod', namespace=project_name)
    assert ocp_pod.create(yaml_file=TEMP_YAML), (
        f"Failed to create pod {pod_name}"
    )
    assert ocp_pvc.wait_for_resource(
        condition='Available', resource_name=pod_name, to_delete=True
    ), f"Pod {pod_name} failed to reach status Available"
    delete_file(TEMP_YAML)


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
    cbp_kwargs = {}
    cbp_kwargs['cephblockpool_name'] = cbp_name
    ocp_cbp = ocp.OCP(kind='CephBlockPool', namespace=project_name)
    template = os.path.join(TEMPLATES_DIR, yaml_file)
    logger.info(f"Deleting Ceph block pool {cbp_name}")
    templating.dump_to_temp_yaml(template, TEMP_YAML, **cbp_kwargs)
    assert ocp_cbp.delete(yaml_file=TEMP_YAML), (
        f"Failed to delete Ceph block pool {cbp_name}"
    )
    assert ocp_cbp.wait_for_resource(
        condition='', resource_name=cbp_name, to_delete=True
    ), f"Ceph block pool {cbp_name} still exists'"
    delete_file(TEMP_YAML)


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
    sc_kwargs = {}
    sc_kwargs['sc_name'] = sc_name
    ocp_sc = ocp.OCP(kind='StorageClass', namespace=project_name)
    template = os.path.join(TEMPLATES_DIR, yaml_file)
    logger.info(f"Deleting storage class {sc_name}")
    templating.dump_to_temp_yaml(template, TEMP_YAML, **sc_kwargs)
    assert ocp_sc.delete(yaml_file=TEMP_YAML), (
        f"Failed to delete storage class {sc_name}"
    )
    assert ocp_sc.wait_for_resource(
        condition='', resource_name=sc_name, to_delete=True
    ), f"Storage class {sc_name} still exists'"
    delete_file(TEMP_YAML)


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
    pod_kwargs = {}
    pod_kwargs['pod_name'] = pod_name
    ocp_pod = ocp.OCP(kind='pod', namespace=project_name)
    template = os.path.join(TEMPLATES_DIR, yaml_file)
    logger.info(f"Deleting pod {pod_name}")
    templating.dump_to_temp_yaml(template, TEMP_YAML, **pod_kwargs)
    assert ocp_pod.delete(yaml_file=TEMP_YAML), (
        f"Failed to delete pod {pod_name}"
    )
    assert ocp_pod.wait_for_resource(
        condition='', resource_name=pod_name, to_delete=True
    ), f"Pod {pod_name} still exists'"
    delete_file(TEMP_YAML)


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
    pvc_kwargs = {}
    pvc_kwargs['pvc_name'] = pvc_name
    ocp_pvc = ocp.OCP(kind='PersistentVolumeClaim', namespace=project_name)
    if ocp_pvc.get(resource_name=pvc_name):
        template = os.path.join(TEMPLATES_DIR, yaml_file)
        logger.info(f"Deleting PVC {pvc_name}")
        templating.dump_to_temp_yaml(template, TEMP_YAML, **pvc_kwargs)
        assert ocp_pvc.delete(yaml_file=TEMP_YAML), (
            f"Failed to delete PVC {pvc_name}"
        )
        assert ocp_pvc.wait_for_resource(
            condition='', resource_name=pvc_name, to_delete=True
        ), f"PVC {pvc_name} still exists'"
        delete_file(TEMP_YAML)


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