"""
General PVC object
"""
import logging

from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility.utils import run_cmd
from ocs_ci.ocs.resources import pod

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

    @property
    def backed_pv_obj(self):
        """
        Returns the backed PV object of pvc_name in namespace

        Returns:
            OCS: An OCS instance for PV
        """
        self.reload()
        data = dict()
        data['api_version'] = self.api_version
        data['kind'] = 'PersistentVolume'
        data['metadata'] = {
            'name': self.backed_pv, 'namespace': self.namespace
        }
        pv_obj = OCS(**data)
        pv_obj.reload()
        return pv_obj

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


def verify_pv_exists_in_backend(
        pv_name, pool_name, namespace=defaults.ROOK_CLUSTER_NAMESPACE
):
    """
    Verifies given pv exists in ceph backend

    Args:
        pvc_name (str): Name of the pvc
    Returns:
         bool: True if pv exists on backend, False otherwise
    """
    spec_volhandle = "'{.spec.csi.volumeHandle}'"
    cmd = f"oc get pv/{pv_name} -o jsonpath={spec_volhandle} -n {namespace}"
    out = run_cmd(cmd=cmd)
    image_uuid = "-"
    image_uuid = image_uuid.join(out.split('-')[5:10])
    cmd = f"rbd info -p {pool_name} csi-vol-{image_uuid}"
    ct_pod = pod.get_ceph_tools_pod()
    try:
        pv_info = ct_pod.exec_ceph_cmd(
            ceph_cmd=cmd, format='json'
        )
    except CommandFailed as ecf:
        assert (
            f"Error is rbd: error opening image csi-vol-{image_uuid}" in str(ecf),
            f"Failed to run the command {cmd}"
        )
        return False
    assert pv_info is not None, "Failed to get the pv information from ceph backend"
    return True
