"""
Helper functions file for OCS QE
"""
import logging
import datetime
from ocs import defaults
from resources.ocs import OCS

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
    return f"{resource_type}-{resource_description[:23]}-{current_date_time[:10]}"


def create_resource(
    desired_status=defaults.STATUS_AVAILABLE, wait=True, **kwargs
):
    """
    Create a resource

    Args:
        desired_status (str): The status of the resource to wait for
        wait (bool): True for waiting for the resource to reach the desired
            status, False otherwise
        kwargs (dict): Dictionary of the OCS resource

    Returns:
        OCS: An OCS instance

    Raises:
        AssertionError: In case of any failure
    """
    ocs_obj = OCS(**kwargs)
    resource_name = kwargs.get('metadata').get('name')
    created_resource = ocs_obj.create()
    assert created_resource, (
        f"Failed to create resource {created_resource.metadata.name}"
    )
    if wait:
        assert ocs_obj.ocp.wait_for_resource(
            condition=desired_status, resource_name=resource_name
        ), f"{ocs_obj.kind} {resource_name} failed to reach"
        f"status {desired_status}"
        return ocs_obj
