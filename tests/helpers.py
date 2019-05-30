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

DEFAULT_CBP_YAML = "cephblockpool.yaml"
DEFAULT_SC_YAML = "storageclass.yaml"
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
    return (
        f"{resource_type}-{resource_description[:23]}-{current_date_time[:10]}"
    )


def create_resource(
    resource_kind, resource_name, project_name, template_yaml,
    desired_status='Available', wait=True, **kwargs
):
    """
    Create a resource

    Args:
        resource_kind (str): The kind of the resource (e.g. CephBlockPool)
        resource_name (str): The name of the resource
        project_name (str): The name of the project that the new resource
            should be associated with
        template_yaml (str): The yaml file to use (e.g. CephBlockPool.yaml)
        desired_status (str): The status of the resource to wait for
        wait (bool): True for waiting for the resource to be created
        **kwargs: Keyword args to be set for the new created resource

    Raises:
        AssertionError: In case of any failure
    """
    template = os.path.join(TEMPLATES_DIR, template_yaml)
    templating.dump_to_temp_yaml(template, TEMP_YAML, **kwargs)
    ocp_obj = ocp.OCP(kind=resource_kind, namespace=project_name)
    logger.info(f"Creating a {resource_kind} {resource_name}")
    assert ocp_obj.create(yaml_file=TEMP_YAML), (
        f"Failed to create {resource_kind} {resource_name}"
    )
    assert ocp_obj.get(resource_name=resource_name), (
        f"Failed to create {resource_kind} {resource_name}"
    )
    if wait:
        assert ocp_obj.wait_for_resource(
            condition=desired_status, resource_name=resource_name
        ), f"{resource_kind} {resource_name} failed to reach status {desired_status}"
        delete_file(TEMP_YAML)


def delete_resource(
    resource_kind, resource_name, project_name, wait=True
):
    """
    Delete a resource

    Args:
        resource_kind (str): The kind of the resource (e.g. CephBlockPool)
        resource_name (str): The name of the resource
        project_name (str): The name of the project that the resource
            is associated with
        wait (bool): True for wait, False otherwise

    Raises:
        AssertionError: In case of any failure
    """
    ocp_obj = ocp.OCP(kind=resource_kind, namespace=project_name)
    if ocp_obj.get(resource_name=resource_name):
        logger.info(f"Deleting {resource_kind} {resource_name}")
        assert ocp_obj.delete(resource_name=resource_name, wait=wait), (
            f"Failed to delete {resource_kind} {resource_name}"
        )
        if wait:
            assert ocp_obj.wait_for_resource(
                condition='', resource_name=resource_name, to_delete=True
            ), f"{resource_kind} {resource_name} still exists'"


def create_ceph_block_pool(
    cbp_name, project_name, failure_domain=None,
    replica_count=None, template_yaml=DEFAULT_CBP_YAML
):
    """
    Create a Ceph block pool

    Args:
        cbp_name (str): The name of the new Ceph block pool
        project_name (str): The nam of the project/namespace of
            which the Ceph block pool belongs to
        failure_domain (str): The failure domain
        replica_count (int): The replica count
        template_yaml (str): The yaml file to use

    Raises:
        AssertionError: In case of any failure

    """
    cbp_kwargs = dict()
    cbp_kwargs['cephblockpool_name'] = cbp_name
    cbp_kwargs['cluster_namespace'] = project_name
    cbp_kwargs['failureDomain'] = failure_domain
    cbp_kwargs['replica_count'] = replica_count
    create_resource(
        resource_kind=CEPH_BLOCK_POOL, resource_name=cbp_name,
        project_name=project_name, template_yaml=template_yaml,
        wait=False, **cbp_kwargs
    )


def create_storageclass(
    sc_name, project_name, sc_type, pool_name, template_yaml=DEFAULT_SC_YAML
):
    """
    Create a storage class

    Args:
        sc_name (str): The name of the storage class
        project_name (str): The nam of the project/namespace of
            which the storage class belongs to
        sc_type (str): CephBlockPool or CephFileSystem
        pool_name (str): The Ceph block pool or Ceph file system name
        that the storage class should be associated with
        template_yaml (str): The yaml file to use

    Raises:
        AssertionError: In case of any failure
    """
    sc_kwargs = dict()
    sc_kwargs['storageclass_name'] = sc_name
    if sc_type == CEPH_BLOCK_POOL:
        sc_kwargs['ceph_block_pool_name'] = pool_name
    else:
        sc_kwargs['fs_name'] = pool_name
    create_resource(
        resource_kind=STORAGE_CLASS, resource_name=sc_name,
        project_name=project_name, template_yaml=template_yaml,
        wait=False, **sc_kwargs
    )


def create_pvc(pvc_name, project_name, template_yaml=DEFAULT_PVC_YAML):
    """
    Create a persistent Volume Claim

    Args:
        pvc_name (str): The name of the PVC to create
        project_name (str): The name of the project/namespace of
            which the PVC belongs to
        template_yaml (str): The yaml file to use

    Raises:
        AssertionError: In case of any failure
    """
    pvc_kwargs = dict()
    pvc_kwargs['pvc_name'] = pvc_name
    pvc_kwargs['cluster_namespace'] = project_name
    create_resource(
        PVC, pvc_name, project_name, template_yaml,
        desired_status='Bound', **pvc_kwargs
    )


def create_pod(
    pod_name, project_name, pvc_name, template_yaml=DEFAULT_POD_YAML
):
    """
    Create a pod

    Args:
        pod_name (str): The name of the pod to create
        project_name (str): The name of the project/namespace of
            which the pod belongs to
        pvc_name (str): The name of the PVC for the pod
        template_yaml (str): The yaml file to use

    Raises:
        AssertionError: In case of any failure
    """
    pod_kwargs = dict()
    pod_kwargs['pod_name'] = pod_name
    pod_kwargs['pvc_name'] = pvc_name
    pod_kwargs['cluster_namespace'] = project_name
    create_resource(
        POD, pod_name, project_name, template_yaml,
        desired_status='Running', **pod_kwargs
    )


def create_project(project_name):
    """
    Create a project

    Args:
        project_name (str): The name of the project to create

    Raises:
        AssertionError: In case of any failure
    """
    ocp_project = ocp.OCP(kind='namespace')
    ocp_project.get()
    assert ocp_project.new_project(project_name=project_name), (
        f"Failed to create project {project_name}"
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
        f"Failed to delete project {project_name}"
    )


def delete_ceph_block_pool(cbp_name, project_name):
    """
    Delete a Ceph block pool

    Args:
        cbp_name (str): The name of the Ceph block pool to delete
        project_name (str): The name of the project/namespace of
            which the Ceph block pool belongs to

    Raises:
        AssertionError: In case of any failure
    """
    delete_resource(
        resource_kind=CEPH_BLOCK_POOL, resource_name=cbp_name,
        project_name=project_name
    )


def delete_storage_class(sc_name, project_name):
    """
    Delete a storage class

    Args:
        sc_name (str): The name of the storage class to delete
        project_name (str): The name of the project/namespace of
            which the storage class belongs to

    Raises:
        AssertionError: In case of any failure
    """
    delete_resource(STORAGE_CLASS, sc_name, project_name)


def delete_pod(pod_name, project_name):
    """
    Delete a pod

    Args:
        pod_name (str): The name of the pod to delete
        project_name (str): The name of the project/namespace of
            which the pod belongs to

    Raises:
        AssertionError: In case of any failure
    """
    delete_resource(POD, pod_name, project_name)


def delete_pvc(pvc_name, project_name):
    """
    Delete a persistent Volume Claim

    Args:
        pvc_name (str): The name of the PVC to delete
        project_name (str): The name of the project/namespace of
            which the PVC belongs to

    Raises:
        AssertionError: In case of any failure
    """
    # as a W/A for BZ1715627, deleting PVC with wait=False
    delete_resource(PVC, pvc_name, project_name, wait=False)



def run_io(pod_name, project_name):
    """
    Run IO to a file within the pod

    Args:
        pod_name (str): The name of the pod
        project_name (str): The name of the project/namespace of
            which the PVC belongs to

    """
    pod = ocp.OCP(kind='pod', namespace=project_name)
    logger.info(f"Running I/O on pod {pod_name}")
    pod.exec_oc_cmd(
        f"rsh {pod_name} bash -c \"echo \'123\' > /var/lib/www/html/test; while true; "
        f"do cp /var/lib/www/html/test /var/lib/www/html/test1; done\""
    )
