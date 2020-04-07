"""
General PVC object
"""
import logging
from concurrent.futures import ThreadPoolExecutor

from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.framework import config
from ocs_ci.utility.utils import run_cmd
from ocs_ci.utility.utils import TimeoutSampler

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
        unformatted_size = self.data.get('status').get('capacity').get('storage')
        units = unformatted_size[-2:]
        if units == 'Ti':
            return int(unformatted_size[:-2]) * 1024
        elif units == 'Gi':
            return int(unformatted_size[:-2])

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

    @property
    def get_pvc_access_mode(self):
        """
        Function to get pvc access_mode

        Returns:
            (str): The accessModes Value of pvc_obj
        """
        return self.data.get('spec').get('accessModes')[0]

    @property
    def backed_sc(self):
        """
        Returns the storage class of pvc object in namespace

        Returns:
            str: Storage class name
        """
        return self.data.get('spec').get('storageClassName')

    @property
    def reclaim_policy(self):
        """
        Get the reclaim policy of PV associated with the PVC

        Returns:
            str: Reclaim policy. eg: Reclaim, Delete
        """
        return self.backed_pv_obj.get().get('spec').get(
            'persistentVolumeReclaimPolicy'
        )

    def resize_pvc(self, new_size, verify=False):
        """
        Modify the capacity of PVC

        Args:
            new_size (int): New size of PVC in Gi
            verify (bool): True to verify the change is reflected on PVC,
                False otherwise

        Returns:
            bool: True if operation succeeded, False otherwise
        """
        patch_param = f'{{"spec": {{"resources": {{"requests": {{"storage": "{new_size}Gi"}}}}}}}}'

        # Modify size of PVC
        assert self.ocp.patch(resource_name=self.name, params=patch_param), (
            f"Patch command to modify size of PVC {self.name} has failed."
        )

        if verify:
            for pvc_data in TimeoutSampler(20, 2, self.get):
                capacity = pvc_data.get('status').get('capacity').get('storage')
                if capacity == f'{new_size}Gi':
                    break
                log.info(
                    f"Capacity of PVC {self.name} is not {new_size}Gi as "
                    f"expected, but {capacity}. Retrying."
                )
            log.info(
                f"Verified that the capacity of PVC {self.name} is changed to "
                f"{new_size}Gi."
            )
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


def get_all_pvcs(namespace=None, selector=None):
    """
    Gets all pvc in given namespace

    Args:
        namespace (str): Name of namespace
        selector (str): The label selector to look for

    Returns:
         dict: Dict of all pvc in namespaces
    """
    if not namespace:
        namespace = config.ENV_DATA['cluster_namespace']
    ocp_pvc_obj = OCP(
        kind=constants.PVC, namespace=namespace
    )
    out = ocp_pvc_obj.get(selector=selector)
    return out


def get_all_pvc_objs(namespace=None, selector=None):
    """
    Gets all PVCs objects in given namespace

    Args:
        namespace (str): Name of namespace
        selector (str): The label selector to look for

    Returns:
         list: Instances of PVC

    """
    all_pvcs = get_all_pvcs(namespace=namespace, selector=selector)
    err_msg = f"Failed to get the PVCs for namespace {namespace}"
    if selector:
        err_msg = err_msg + f" and selector {selector}"
    assert all_pvcs, err_msg
    return [PVC(**pvc) for pvc in all_pvcs['items']]


def get_deviceset_pvcs():
    """
    Get the deviceset PVCs

    Returns:
        list: The deviceset PVCs OCS objects

    Raises:
        AssertionError: In case the deviceset PVCs are not found

    """
    ocs_pvc_obj = get_all_pvc_objs(
        namespace=config.ENV_DATA['cluster_namespace']
    )
    deviceset_pvcs = []
    for pvc_obj in ocs_pvc_obj:
        if pvc_obj.name.startswith(constants.DEFAULT_DEVICESET_PVC_NAME):
            deviceset_pvcs.append(pvc_obj)
    assert deviceset_pvcs, "Failed to find the deviceset PVCs"
    return deviceset_pvcs


def get_deviceset_pvs():
    """
    Get the deviceset PVs

    Returns:
        list: the deviceset PVs OCS objects

    Raises:
        AssertionError: In case the deviceset PVCs are not found

    """
    deviceset_pvcs = get_deviceset_pvcs()
    return [pvc.backed_pv_obj for pvc in deviceset_pvcs]
