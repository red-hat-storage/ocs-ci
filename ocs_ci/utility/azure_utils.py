import logging

from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.compute import ComputeManagementClient
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.exceptions import TimeoutExpiredError

logger = logging.getLogger(name=__file__)


class AZURE:
    """
    wrapper for Azure
    """
    _compute_client = None
    _credentials = None

    def __init__(self, subscription_id, client_id, client_secret, tenant_id, resourcegroup):
        """
        Constructor for Azure class

        Args:
            subscription_id (str): Subscription ID
            client_id (str): Application (client) ID
            client_secret (): Client Secret
            tenant_id (str): Tenant ID
            resourcegroup (str): Resource Group
        """
        self._subscription_id = subscription_id
        self._client_id = client_id
        self._client_secret = client_secret
        self._tenant_id = tenant_id
        self._resourcegroup = resourcegroup

    @property
    def credentials(self):
        """ Property for azure service principle credentials used to authenticate the client

        Returns:
            credentials: service principle credentials
            subscription_id: Subscription ID
        """
        self._credentials = ServicePrincipalCredentials(
            client_id=self._client_id,
            secret=self._client_secret,
            tenant=self._tenant_id
        )
        return self._credentials, self._subscription_id

    @property
    def compute_client(self):
        """ Property for Azure vm resource

        Returns:
            ComputeManagementClient instance for managing Azure vm resource
        """
        if not self._compute_client:
            self._compute_client = ComputeManagementClient(*self.credentials)
        return self._compute_client

    def get_vm_instance(self, vm_name):
        """
        Get instance of Azure vm Instance

        Args:
            vm_name (str): The name of the Azure instance to get

        Returns:
            vm: instance of Azure vm instance resource

        """
        vm = self.compute_client.virtual_machines.get(
            self._resourcegroup,
            vm_name
        )
        return vm

    def get_node_by_attached_volume(self, volume):
        """
        Get the Azure Vm instance that has the volume attached to

        Args:
            volume (Disk): The disk object to get the Azure Vm according to

        Returns:
            vm: An Azure Vm instance

        """
        vm_list = self.compute_client.virtual_machines.list(self._resourcegroup)

        for vm in vm_list:
            for disk in vm.storage_profile.data_disks:
                if disk.name == volume.name:
                    return vm

    def detach_volume(self, volume, node, timeout=120):
        """
        Detach volume if attached

        Args:
            volume (disk): disk object required to delete a volume
            node (OCS): The OCS object representing the node
            timeout (int): Timeout in seconds for API calls

        """
        vm = self.get_vm_instance(node.name)
        data_disks = vm.storage_profile.data_disks
        data_disks[:] = [disk for disk in data_disks if disk.name != volume.name]
        logger.info(
            "Detaching volume: %s Instance: %s", volume.name, vm.name
        )
        result = self.compute_client.virtual_machines.create_or_update(
            self._resourcegroup,
            vm.name,
            vm)
        result.wait()
        try:
            for sample in TimeoutSampler(
                timeout, 3, self.get_disk_state, volume.name
            ):
                logger.info(
                    f"Volume id: {volume.name} has status: {sample}"
                )
                if sample == "Unattached":
                    break
        except TimeoutExpiredError:
            logger.error(
                f"Volume {volume.name} failed to be detached from an Azure Vm instance"
            )
            raise

    def restart_az_vm_instance(self, vm_name):
        """
        Restart an Azure vm instance

        Args:
            vm_name: Name of azure vm instance

        """
        result = self.compute_client.virtual_machines.restart(
            self._resourcegroup, vm_name)
        result.wait()

    def get_data_volumes(self, deviceset_pvs):
        """
        Get the instance data disk objects

        Args:
            deviceset_pvs (list): PVC objects of the deviceset PVs

        Returns:
            list: Azure Vm disk objects

        """
        volume_names = [pv.get()['spec']['azureDisk']['diskName'] for pv in deviceset_pvs]
        return [self.compute_client.disks.get(self._resourcegroup, volume_name) for volume_name in volume_names]

    def get_disk_state(self, volume_name):
        """
        Get the state of the disk

        Args:
            volume_name (str): Name of the volume/disk

        Returns:
            str: Azure Vm disk state

        """
        return self.compute_client.disks.get(self._resourcegroup, volume_name).disk_state
