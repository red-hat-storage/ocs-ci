"""
General PVC object
"""
import logging
from concurrent.futures import ThreadPoolExecutor

from ocs_ci.ocs import constants
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

    @property
    def image_uuid(self):
        """
        Fetch image uuid associated with PVC

        Returns:
            str: Image uuid associated with PVC
        """
        spec_volhandle = "'{.spec.csi.volumeHandle}'"
        cmd = f"oc get pv/{self.backed_pv} -o jsonpath={spec_volhandle}"
        out = run_cmd(cmd=cmd)
        image_uuid = "-".join(out.split('-')[-5:])
        return image_uuid

    def verify_pv_exists_in_backend(
            self, pool_name
    ):
        """
        Verifies given pv exists in ceph backend

        Args:
            pool_name (str): Name of the rbd-pool

        Returns:
             bool: True if pv exists on backend, False otherwise

        """
        spec_volhandle = "'{.spec.csi.volumeHandle}'"
        cmd = f"oc get pv/{self.backed_pv} -o jsonpath={spec_volhandle} -n {self.namespace}"
        out = run_cmd(cmd=cmd)
        image_uuid = "-".join(out.split('-')[5:10])
        cmd = f"rbd info -p {pool_name} csi-vol-{image_uuid}"
        ct_pod = pod.get_ceph_tools_pod()
        try:
            ct_pod.exec_ceph_cmd(
                ceph_cmd=cmd, format='json'
            )
        except CommandFailed as ecf:
            log.error(f"PV is not found on ceph backend: str{ecf}")
            return False
        return True

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


def delete_pvcs(pvc_objs, concurrent=False):
    """
    Deletes list of the pvc objects

    Args:
        pvc_objs (list): List of the pvc objects to be deleted
        concurrent (bool): Determines if the delete operation should be
            executed with multiple thread in parallel

    Returns:
        bool: True if deletion is successful
    """
    if concurrent:
        with ThreadPoolExecutor() as executor:
            for pvc in pvc_objs:
                executor.submit(pvc.delete)
    else:
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


def get_pvc_access_mode(pvc_obj):
    """
    Function to get pvc access_mode from given pvc_obj

    Args:
        pvc_obj (str): The pvc object

    Returns:
        access_mode (str): The accessModes on a given pvc_obj
    """
    return pvc_obj.get().get('spec').get('accessModes')[0]
