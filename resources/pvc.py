"""
General PVC object
"""
import logging

from ocs import constants
from tests import helpers
from ocs import defaults
from ocs.ocp import OCP
from resources.ocs import OCS
from ocsci import config

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
            return self.get_size() == new_size
        return True


def delete_all_pvcs():
    """
    Deletes all pvc in namespace

    Returns:
        bool: True if deletion is successful
    """
    ocp_pvc_obj = OCP(
        kind=constants.PVC, namespace=config.ENV_DATA['cluster_namespace']
    )
    ocp_pvc_list = get_all_pvcs()
    pvc_list = ocp_pvc_list['items']
    for item in pvc_list:
        ocp_pvc_obj.delete(resource_name=item.get('metadata').get('name'))

    return True


def get_all_pvcs():
    """
    Gets all pvc in given namespace

    Returns:
         dict: Dict of all pvc in namespaces
    """

    ocp_pvc_obj = OCP(
        kind=constants.PVC, namespace=config.ENV_DATA['cluster_namespace']
    )
    out = ocp_pvc_obj.get()
    return out


def create_multiple_pvc(number_of_pvc=1, pvc_data=None):
    """
    Create one or more PVC

    Args:
        number_of_pvc (int): Number of PVCs to be created
        pvc_data (dict): Parameters for PVC yaml

    Returns:
         list: List of PVC objects
    """
    if pvc_data is None:
        pvc_data = helpers.get_crd_dict(defaults.CSI_PVC_DICT)
    pvc_objs = []
    pvc_base_name = pvc_data['metadata']['name']

    for count in range(1, number_of_pvc + 1):
        if number_of_pvc != 1:
            pvc_name = f'{pvc_base_name}{count}'
            pvc_data['metadata']['name'] = pvc_name
        pvc_name = pvc_data['metadata']['name']
        log.info(f'Creating Persistent Volume Claim {pvc_name}')
        pvc_obj = PVC(**pvc_data)
        pvc_obj.create()
        pvc_objs.append(pvc_obj)
        log.info(f'Created Persistent Volume Claim {pvc_name}')
    return pvc_objs
