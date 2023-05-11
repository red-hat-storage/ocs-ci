# -*- coding: utf8 -*-
"""
Module for interactions with OCP/OCS Cluster on Azure platform level.
"""

import json
import logging
import os

from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.resource import ResourceManagementClient


from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import TimeoutExpiredError, TerrafromFileNotFoundException
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(name=__file__)


# default location of files with necessary azure cluster details
SERVICE_PRINCIPAL_FILEPATH = os.path.expanduser("~/.azure/osServicePrincipal.json")
TERRRAFORM_FILENAME = "terraform.azure.auto.tfvars.json"
OLD_TERRRAFORM_FILENAME = "terraform.azure.auto.tfvars.json"


def load_cluster_resource_group():
    """
    Read terraform tfvars.json file created by ``openshift-installer`` in a
    cluster dir to get azure ``resource group`` of an OCP cluster. All Azure
    resources of the cluster are placed in this group.

    Returns:
        string with resource group name

    Raises:
        TerrafromFileNotFoundException : When the terraform tfvars file is not found
    """
    cluster_path = config.ENV_DATA["cluster_path"]
    terraform_files = [
        os.path.join(cluster_path, f)
        for f in [OLD_TERRRAFORM_FILENAME, TERRRAFORM_FILENAME]
    ]
    terraform_filename = None
    for tf_file in terraform_files:
        if os.path.exists(tf_file):
            terraform_filename = os.path.join(cluster_path, tf_file)

    if not terraform_filename:
        raise TerrafromFileNotFoundException(
            f"None of terraform file path from {','.join(terraform_files)} exists!"
        )

    with open(terraform_filename, "r") as tf_file:
        tf_dict = json.load(tf_file)
    resource_group = tf_dict.get("azure_network_resource_group_name")
    logger.debug(
        "fetching azure resource group (%s) from %s file",
        tf_dict.get("clientId"),
        terraform_filename,
    )
    return resource_group


def load_service_principal_dict(filepath=SERVICE_PRINCIPAL_FILEPATH):
    """
    Load Azure Service Principal from osServicePrincipal.json file and parse it
    into a dictionary.

    Args:
        filepath (str): path of the

    Returns:
        dictionary with the service principal details (3 IDs and 1 secret)
    """
    with open(filepath, "r") as sp_file:
        sp_dict = json.load(sp_file)
    logger.debug(
        "fetching azure service principal (clientId %s) from %s file",
        sp_dict.get("clientId"),
        filepath,
    )
    return sp_dict


# TODO: rename to AzureUtil
class AZURE:
    """
    Utility wrapper class for Azure OCP cluster. Design of the class follows
    similar AWS class.
    """

    _compute_client = None
    _resource_client = None
    _credentials = None
    _cluster_resource_group = None

    def __init__(
        self,
        subscription_id=None,
        tenant_id=None,
        client_id=None,
        client_secret=None,
        cluster_resource_group=None,
    ):
        """
        Constructor for Azure cluster util class.

        All arguments are optional. If cluster details are not specified via
        arguments, the this method will try to load the values from files in
        ~/.azure and openshift cluster directory.

        If you specify 'azure_cluster_resource_group' in ENV section of ocs-ci
        config file, value from ocs-ci config file will be used as a default
        instead of a terraform tfvars from openshift cluster dir. This is
        useful when the cluster wasn't deployed by ocs-ci, you don't have
        access to terraform files from it's cluster dir, but you know it's
        resource group.

        Args:
            subscription_id (str): Azure Subscription ID
            tenant_id (str): (Active) Directory (tenant) ID
            client_id (str): Application (client) ID of the service Principal
            client_secret (str): password of the Service Principal
            cluster_resource_group (str): Azure Resource Group of the cluster
        """
        self._subscription_id = subscription_id
        self._tenant_id = tenant_id
        self._client_id = client_id
        self._client_secret = client_secret
        self._cluster_resource_group = cluster_resource_group

    @property
    def cluster_resource_group(self):
        """
        Azure resource group of the OCP cluster. This group is created
        by openshift-installer during OCP deployment.

        If the value is not yet available and it's not specified anywhere, it
        returns None.
        """
        if self._cluster_resource_group is not None:
            return self._cluster_resource_group
        # we can override the resource group via ocs-ci config
        if "azure_cluster_resource_group" in config.ENV_DATA:
            self._cluster_resource_group = config.ENV_DATA[
                "azure_cluster_resource_group"
            ]
        elif "cluster_path" in config.ENV_DATA and os.path.exists(
            config.ENV_DATA["cluster_path"]
        ):
            try:
                self._cluster_resource_group = load_cluster_resource_group()
            except Exception as ex:
                logger.warning("failed to load azure resource group: %s", ex)
        return self._cluster_resource_group

    @property
    def credentials(self):
        """
        Property for azure service principle credentials used to authenticate
        the client.
        """
        if self._credentials:
            return self._credentials
        # tuple of private attributes which defines a service principal
        sp_attributes = (
            self._subscription_id,
            self._tenant_id,
            self._client_id,
            self._client_secret,
        )
        # load azure service principal file *only* if necessary
        if None in sp_attributes:
            sp_dict = load_service_principal_dict()
        if self._subscription_id is None:
            self._subscription_id = sp_dict["subscriptionId"]
        if self._tenant_id is None:
            self._tenant_id = sp_dict["tenantId"]
        if self._client_id is None:
            self._client_id = sp_dict["clientId"]
        if self._client_secret is None:
            self._client_secret = sp_dict["clientSecret"]
        # create azure SP Credentials object
        self._credentials = ServicePrincipalCredentials(
            client_id=self._client_id,
            secret=self._client_secret,
            tenant=self._tenant_id,
        )
        return self._credentials

    @property
    def compute_client(self):
        """Property for Azure vm resource

        Returns:
            ComputeManagementClient instance for managing Azure vm resource
        """
        if not self._compute_client:
            self._compute_client = ComputeManagementClient(
                credentials=self.credentials, subscription_id=self._subscription_id
            )
        return self._compute_client

    @property
    def resource_client(self):
        """
        Azure ResourceManagementClient instance
        """
        if not self._resource_client:
            self._resource_client = ResourceManagementClient(
                credentials=self.credentials, subscription_id=self._subscription_id
            )
        return self._resource_client

    def get_vm_instance(self, vm_name):
        """
        Get instance of Azure vm Instance

        Args:
            vm_name (str): The name of the Azure instance to get

        Returns:
            vm: instance of Azure vm instance resource

        """
        vm = self.compute_client.virtual_machines.get(
            self.cluster_resource_group, vm_name
        )
        return vm

    def get_vm_power_status(self, vm_name):
        """
        Get the power status of VM

        Args:
           vm_name (str): Azure VM name

        Returns :
           str: Power status of Azure VM

        """
        vm = self.compute_client.virtual_machines.get(
            self.cluster_resource_group, vm_name, expand="instanceView"
        )
        vm_statuses = vm.instance_view.statuses
        vm_power_state = len(vm_statuses) >= 2 and vm_statuses[1].code.split("/")[1]
        return vm_power_state

    def get_node_by_attached_volume(self, volume):
        """
        Get the Azure Vm instance that has the volume attached to

        Args:
            volume (Disk): The disk object to get the Azure Vm according to

        Returns:
            vm: An Azure Vm instance

        """
        vm_list = self.compute_client.virtual_machines.list(self.cluster_resource_group)

        for vm in vm_list:
            for disk in vm.storage_profile.data_disks:
                if disk.name == volume.name:
                    return vm

    def get_vm_names(self):
        """
        Get list of vms in azure resource group

        Returns:
           (list): list of Azure vm names

        """
        vm_list = self.compute_client.virtual_machines.list(self.cluster_resource_group)
        vm_names = [vm.id.split("/")[-1] for vm in vm_list]
        return vm_names

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
        logger.info("Detaching volume: %s Instance: %s", volume.name, vm.name)
        result = self.compute_client.virtual_machines.create_or_update(
            self.cluster_resource_group, vm.name, vm
        )
        result.wait()
        try:
            for sample in TimeoutSampler(timeout, 3, self.get_disk_state, volume.name):
                logger.info(f"Volume id: {volume.name} has status: {sample}")
                if sample == "Unattached":
                    break
        except TimeoutExpiredError:
            logger.error(
                f"Volume {volume.name} failed to be detached from an Azure Vm instance"
            )
            raise

    def restart_vm_instances(self, vm_names):
        """
        Restart Azure vm instances

        Args:
            vm_names (list): Names of azure vm instances

        """
        for vm_name in vm_names:
            result = self.compute_client.virtual_machines.restart(
                self.cluster_resource_group, vm_name
            )
            result.wait()

    def get_data_volumes(self, deviceset_pvs):
        """
        Get the instance data disk objects

        Args:
            deviceset_pvs (list): PVC objects of the deviceset PVs

        Returns:
            list: Azure Vm disk objects

        """
        volume_names = [
            pv.get()["spec"]["azureDisk"]["diskName"] for pv in deviceset_pvs
        ]
        return [
            self.compute_client.disks.get(self.cluster_resource_group, volume_name)
            for volume_name in volume_names
        ]

    def get_disk_state(self, volume_name):
        """
        Get the state of the disk

        Args:
            volume_name (str): Name of the volume/disk

        Returns:
            str: Azure Vm disk state

        """

        return self.compute_client.disks.get(
            self.cluster_resource_group, volume_name
        ).disk_state

    def start_vm_instances(self, vm_names):
        """
        Start Azure vm instances

        Args:
            vm_names (list): Names of azure vm instances

        """
        for vm_name in vm_names:
            result = self.compute_client.virtual_machines.start(
                self.cluster_resource_group, vm_name
            )
            result.wait()

    def stop_vm_instances(self, vm_names, force=False):
        """
        Stop Azure vm instances

        Args:
            vm_names (list): Names of azure vm instances
            force (bool): True for non-graceful VM shutdown, False for
                graceful VM shutdown

        """
        for vm_name in vm_names:
            result = self.compute_client.virtual_machines.power_off(
                self.cluster_resource_group, vm_name, skip_shutdown=force
            )
            result.wait()

    def restart_vm_instances_by_stop_and_start(self, vm_names, force=False):
        """
        Stop and Start Azure vm instances

        Args:
            vm_names (list): Names of azure vm instances
            force (bool): True for non-graceful VM shutdown, False for
                graceful VM shutdown

        """
        self.stop_vm_instances(vm_names, force=force)
        self.start_vm_instances(vm_names)
