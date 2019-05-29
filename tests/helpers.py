"""
Helper functions file for OCS QE
"""
import datetime


def create_unique_object_name(object_description, object_type):
    """
    Creates a unique object name by using the object_description
    and object_type, as well as the current date/time string.
    This can be used for any objects such as VMs, disks, clusters etc.

    Args:
        object_description (str): The user provided object description
        object_type (str): The type of object for which the unique name
            will be created. For example: project, pvc, etc

    Returns:
        str: A unique name
    """
    current_date_time = (
        datetime.datetime.now().strftime("%d%H%M%S%f")
    )
    return f"{object_type}_{object_description[:23]}_{current_date_time[:10]}"
