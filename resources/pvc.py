"""
General PVC helper functions
"""
import logging

from ocs.defaults import PVC, API_VERSION
from ocs.ocp import OCP
from resources.ocs import OCS

log = logging.getLogger(__name__)


def get_pvc_status(pvc_name, namespace):
    """
    Returns the PVC status

    Args:
         pvc_name (str): PVC name to check status
         namespace (str): The namespace this PVC lives in

    Returns:
        str: PVC status
    """
    oc_client = OCP(api_version=API_VERSION, kind=PVC, namespace=namespace)
    pvc = oc_client.get(resource_name=pvc_name)
    return pvc.get('status').get('phase')


def get_backed_pv(pvc_name, namespace):
    """
    Returns the backed PV name of pvc_name in namespace

    Args:
         pvc_name (str): PVC name to get its backed PV
         namespace (str): The namespace this PVC lives in

    Returns:
        str: PV name
    """
    oc_client = OCP(api_version=API_VERSION, kind=PVC, namespace=namespace)
    pvc = oc_client.get(resource_name=pvc_name)
    return pvc.get('spec').get('volumeName')


def get_pvc_size(pvc_name, namespace):
    """
    Returns the PVC size pvc_name in namespace

    Args:
         pvc_name (str): PVC name to get its size
         namespace (str): The namespace this PVC lives in

    Returns:
        int: PVC size
    """
    oc_client = OCP(api_version=API_VERSION, kind=PVC, namespace=namespace)
    pvc = oc_client.get(resource_name=pvc_name)
    #  [:-2] to remove the 'Gi' from the size (e.g. '5Gi --> '5')
    return int(pvc.get('status').get('capacity').get('storage')[:-2])


def resize_pvc(pvc_name, new_size, verify=False):
    """
    Returns the PVC size pvc_name in namespace

    Args:
         pvc_name (str): PVC name to get its size
         new_size (int): The new size for the PVC
         verify (bool): In order to check if the resize succeeded of not

    Returns:
        bool: True if operation succeeded, False otherwise
    """
    oc_client = OCP(api_version=API_VERSION, kind=PVC)
    pvc = oc_client.get(resource_name=pvc_name)
    pvc_obj = OCS(**pvc)
    pvc_obj.data['status']['capacity']['storage'] = f"{new_size}Gi"
    pvc_obj.apply(**pvc_obj.data)
    if verify:
        pvc_obj.reload()
        return int(
            pvc_obj.data.get('status').get('capacity').get('storage')[:-2]
        ) == new_size
    return True
