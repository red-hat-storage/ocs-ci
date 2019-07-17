"""
General PVC object
"""
import logging

from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.framework import config

log = logging.getLogger(__name__)


class PVC(OCS):
    """
    A basic PersistentVolumeClaim kind resource
    """

    def __init__(self, **kwargs):
        """
        Initializer function
        kwargs:
            See parent class for kwargs information
        """
        super(PVC, self).__init__(**kwargs)

    @property
    def size(self):
        """
        Returns the PVC size pvc_name in namespace

        Returns:
            int: PVC size
        """
        #  [:-2] -> to remove the 'Gi' from the size (e.g. '5Gi --> '5')
        return int(self.data.get('status').get('capacity').get('storage')[:-2])

    @property
    def status(self):
        """
        Returns the PVC status

        Returns:
            str: PVC status
        """
        return self.data.get('status').get('phase')

    @property
    def backed_pv(self):
        """
        Returns the backed PV name of pvc_name in namespace

        Returns:
            str: PV name
        """
        return self.data.get('spec').get('volumeName')

    def resize_pvc(self, new_size, verify=False):
        """
        Returns the PVC size pvc_name in namespace

        Returns:
            bool: True if operation succeeded, False otherwise
        """
        self.data['status']['capacity']['storage'] = f"{new_size}Gi"
        self.apply(**self.data)
        if verify:
            return self.size == new_size
        return True


def delete_pvcs(pvc_objs):
    """
    Deletes list of the pvc objects

    Args:
        pvc_objs (list): List of the pvc objects to be deleted

    Returns:
        bool: True if deletion is successful
    """
    for pvc in pvc_objs:
        pvc.delete()
    return True


def get_all_pvcs(namespace=None):
    """
    Gets all pvc in given namespace

    Args:
        namespace (str): Name of namespace

    Returns:
         dict: Dict of all pvc in namespaces
    """
    if not namespace:
        namespace = config.ENV_DATA['cluster_namespace']
    ocp_pvc_obj = OCP(
        kind=constants.PVC, namespace=namespace
    )
    out = ocp_pvc_obj.get()
    return out
