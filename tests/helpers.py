"""
Helper functions file for OCS QE
"""
import logging
import datetime
from ocs import defaults
from resources import ceph_file_system

logger = logging.getLogger(__name__)


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


def create_resource(resource_obj, desired_status=defaults.STATUS_AVAILABLE):
    """
    Create a resource

    Args:
        resource_obj (str): The resource object (e.g. CephBlockPool)
        desired_status (str): The status of the resource to wait for

    Raises:
        AssertionError: In case of any failure
    """
    created_resource = resource_obj.create()
    assert created_resource, (
        f"Failed to create resource {created_resource.metadata.name}"
    )
    assert resource_obj.ocp.wait_for_resource(
        condition=desired_status, resource_name=created_resource.metadata.name
    ), f"{resource_obj.kind} {resource_obj.metadata.name} failed to reach status Available"


def create_ceph_file_system(fs_name, project_name, **kwargs):
    """
    Create a Ceph file system

    Args:
        fs_name: The name of the new Ceph file system
        project_name: The nam of the project/namespace of
            which the Ceph file system belongs to

    Returns:
        CephFileSystem: A CephFileSystem object

    Raises:
        AssertionError: In case of any failure

    """
    fs_kwargs = defaults.CEPHFILESYSTEM_DICT.copy()
    fs_kwargs['metadata']['cephblockpool_name'] = fs_name
    fs_kwargs['metadata']['namespace'] = project_name
    fs_kwargs.update(kwargs)
    cephfs_obj = ceph_file_system.CephFileSystem(**fs_kwargs)
    create_resource(cephfs_obj)
    return cephfs_obj


def delete_ceph_file_system(cephfs_obj):
    """
    Delete a Ceph File System

    Args:
        cephfs_obj: A CephFileSystem object

    """
    cephfs_obj.delete()
