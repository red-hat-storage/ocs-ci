# -*- coding: utf8 -*-
"""
Module for interactions with OCP/OCS Cluster on Azure platform level.
"""

import base64
import json
import logging
import os
import time
from datetime import datetime


from azure.identity import ClientSecretCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.storage import StorageManagementClient


from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    TimeoutExpiredError,
    TerrafromFileNotFoundException,
    UnsupportedPlatformVersionError,
)
from ocs_ci.utility import version as version_util
from ocs_ci.utility.utils import (
    exec_cmd,
    TimeoutSampler,
)
from ocs_ci.utility.ssl_certs import configure_ingress_and_api_certificates

logger = logging.getLogger(name=__file__)


# default location of files with necessary azure cluster details
SERVICE_PRINCIPAL_FILEPATH = os.path.expanduser("~/.azure/osServicePrincipal.json")
TERRRAFORM_FILENAME = "terraform.platform.auto.tfvars.json"
OLD_TERRRAFORM_FILENAME = "terraform.azure.auto.tfvars.json"


def load_cluster_resource_group(cluster_path, terraform_filename=TERRRAFORM_FILENAME):
    """
    Read terraform tfvars.json file created by ``openshift-installer`` in a
    cluster dir to get azure ``resource group`` of an OCP cluster. All Azure
    resources of the cluster are placed in this group.

    Args:
        cluster_path (str): full file path of the openshift cluster directory
        terraform_filename (str): name of azure terraform vars file, this is
            optional and you need to specify this only if you want to override
            the default

    Returns:
        string with resource group name
    """
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
    _storage_client = None
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
        azure_auth = config.AUTH.get("azure_auth", {})
        self._subscription_id = subscription_id or azure_auth.get("subscription_id")
        self._tenant_id = tenant_id or azure_auth.get("tenant_id")
        self._client_id = client_id or azure_auth.get("client_id")
        self._client_secret = client_secret or azure_auth.get("client_secret")
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
                self._cluster_resource_group = load_cluster_resource_group(
                    config.ENV_DATA["cluster_path"]
                )
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
        self._credentials = ClientSecretCredential(
            client_id=self._client_id,
            client_secret=self._client_secret,
            tenant_id=self._tenant_id,
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

    @property
    def storage_client(self):
        """
        Azure Stroage Management Client instance
        """
        if not self._storage_client:
            self._storage_client = StorageManagementClient(
                credential=self.credentials, subscription_id=self._subscription_id
            )
        return self._storage_client

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

    def get_storage_accounts(self):
        """
        Get list of storage accounts in azure resource group

        Returns:
           list: list of Azure storage accounts

        """
        storage_accounts_list = (
            self.storage_client.storage_accounts.list_by_resource_group(
                resource_group_name=self.cluster_resource_group
            )
        )
        return storage_accounts_list

    def get_storage_accounts_names(self):
        """
        Get list of names of storage accounts in azure resource group

        Returns:
           list: list of Azure storage accounts name

        """
        storage_accounts_list = (
            self.storage_client.storage_accounts.list_by_resource_group(
                resource_group_name=self.cluster_resource_group
            )
        )
        storage_accounts_name_list = [account.name for account in storage_accounts_list]
        return storage_accounts_name_list

    def get_storage_account_properties(self, storage_account_name):
        """
        Get the properties of the storage account whose name is passed.

        Args:
            storage_account_name (str): Name of the storage account

        Returns:
            str: Properties of the storage account in string format.
        """
        storage_account_properties = (
            self.storage_client.storage_accounts.get_properties(
                resource_group_name=self.cluster_resource_group,
                account_name=storage_account_name,
            )
        )
        return str(storage_account_properties)

    def az_login(self):
        login_cmd = (
            f"az login --service-principal --username {self._client_id} --password "
            f"{self._client_secret} --tenant {self._tenant_id}"
        )
        exec_cmd(login_cmd, secrets=[self._client_secret, self._tenant_id])


class AzureAroUtil(AZURE):
    """
    Utility wrapper class for Azure ARO OCP cluster.
    """

    def __init__(
        self,
        subscription_id=None,
        tenant_id=None,
        client_id=None,
        client_secret=None,
        cluster_resource_group=None,
    ):
        super(AzureAroUtil, self).__init__(
            subscription_id, tenant_id, client_id, client_secret, cluster_resource_group
        )
        self.az_login()

    def get_aro_ocp_version(self):
        """
        Get OCP version available in Azure ARO.

        Returns:
            str: version of ARO OCP currently available, matching the version from config.

        Raises:
            UnsupportedPlatformVersionError: In case the version is not supported yet available
                for the Azure ARO.

        """
        out = exec_cmd(
            f"az aro get-versions --location {config.ENV_DATA['region']}"
        ).stdout
        data = json.loads(out)
        ocp_config_version = version_util.get_semantic_ocp_version_from_config()
        versions = []
        for version in data:
            semantic_version = version_util.get_semantic_version(version, False)
            if (
                ocp_config_version.major == semantic_version.major
                and ocp_config_version.minor == semantic_version.minor
            ):
                versions.append(semantic_version)
        if versions:
            versions.sort()
            return str(versions[-1])
        raise UnsupportedPlatformVersionError(
            f"OCP version {ocp_config_version.major}.{ocp_config_version.minor} is not supported on Azure ARO platform!"
        )

    def create_cluster(self, cluster_name):
        """
        Create OCP cluster.

        Args:
            cluster_name (str): Cluster name.

        Raises:
            UnexpectedBehaviour: in the case, the cluster is not installed
                successfully.

        """
        worker_flavor = config.ENV_DATA["worker_instance_type"]
        master_flavor = config.ENV_DATA["master_instance_type"]
        worker_replicas = config.ENV_DATA["worker_replicas"]
        ocp_version = self.get_aro_ocp_version()
        resource_group = config.ENV_DATA.get("azure_base_domain_resource_group_name")
        cluster_resource_group = config.ENV_DATA.get(
            "azure_cluster_resource_group_name", f"aro-{cluster_name}"
        )
        vnet = config.ENV_DATA.get("aro_vnet", "aro-vnet")
        master_subnet = config.ENV_DATA.get(
            "aro_master_subnet", constants.ARO_MASTER_SUBNET
        )
        worker_subnet = config.ENV_DATA.get(
            "aro_worker_subnet", constants.ARO_WORKER_SUBNET
        )
        pull_secret_path = os.path.join(constants.DATA_DIR, "pull-secret")
        base_domain = config.ENV_DATA["base_domain"]
        self.create_network()
        cmd = (
            f"az aro create --resource-group {resource_group} --cluster-resource-group {cluster_resource_group} "
            f"--name {cluster_name} --version {ocp_version} --vnet {vnet} --master-subnet {master_subnet} "
            f"--worker-subnet {worker_subnet} --pull-secret @{pull_secret_path} "
            f"--worker-vm-size {worker_flavor} --master-vm-size {master_flavor}  "
            f"--worker-count {worker_replicas} --domain {cluster_name}.{base_domain}"
        )
        logger.info("Creating Azure ARO cluster.")
        out = exec_cmd(cmd, timeout=5400).stdout
        self.set_dns_records(cluster_name, resource_group, base_domain)
        logger.info(f"Cluster deployed: {out}")
        cluster_info = self.get_cluster_details(cluster_name)
        # Create metadata file to store the cluster name
        cluster_info["clusterName"] = cluster_name
        cluster_info["clusterID"] = cluster_info["id"]
        cluster_path = config.ENV_DATA["cluster_path"]
        metadata_file = os.path.join(cluster_path, "metadata.json")
        with open(metadata_file, "w+") as f:
            json.dump(cluster_info, f)
        installer_log_file = os.path.join(cluster_path, ".openshift_install.log")
        formatted_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        cluster_address = (
            f"http://console-openshift-console.apps.{cluster_name}.{base_domain}"
        )
        logger.info(f"Cluster URL: {cluster_address}")
        with open(installer_log_file, "w+") as f:
            f.writelines(
                [
                    "W/A for our CI to get URL to the cluster in jenkins job. "
                    "Cluster is deployed via az aro command!\n"
                    f'time="{formatted_time}" level=info msg="Access the OpenShift web-console here: '
                    f"{cluster_address}\"\n'",
                ]
            )
        self.get_kubeconfig(cluster_name, resource_group)
        self.write_kubeadmin_password(cluster_name, resource_group)
        configure_ingress_and_api_certificates(skip_tls_verify=True)
        attempts = 0
        maximum_attempts = 150
        successful_connections = 0
        successful_connections_in_row = 20
        while successful_connections != successful_connections_in_row:
            attempts += 1
            try:
                exec_cmd("oc cluster-info")
                successful_connections += 1
                logger.info(
                    f"{successful_connections}. successful connection to the cluster in row"
                )
                time.sleep(2)
                if successful_connections == successful_connections_in_row:
                    logger.info(
                        f"Reached {successful_connections_in_row} successful connection to the cluster!"
                    )
                    break
            except CommandFailed:
                logger.exception("Failed to connect to the cluster!")
                logger.warning(
                    "Waiting till TLS certificates will get propagated for ARO cluster!"
                    f"Attempt: {attempts} out of {maximum_attempts}."
                )
                time.sleep(5)
                successful_connections = 0
                if attempts >= maximum_attempts:
                    raise

    def create_network(self):
        """
        Create network related stuff for the cluster.
        """
        vnet = config.ENV_DATA.get("aro_vnet", "aro-vnet")
        vnet_address_prefixes = config.ENV_DATA.get(
            "aro_vnet_address_prefixes", constants.ARO_VNET_ADDRESS_PREFIXES
        )
        master_subnet = config.ENV_DATA.get(
            "aro_master_subnet", constants.ARO_MASTER_SUBNET
        )
        master_subnet_address_prefixes = config.ENV_DATA.get(
            "aro_master_subnet_address_prefixes",
            constants.ARO_MASTER_SUBNET_ADDRESS_PREFIXES,
        )
        worker_subnet = config.ENV_DATA.get(
            "aro_worker_subnet", constants.ARO_WORKER_SUBNET
        )
        worker_subnet_address_prefixes = config.ENV_DATA.get(
            "aro_worker_subnet_address_prefixes",
            constants.ARO_WORKER_SUBNET_ADDRESS_PREFIXES,
        )
        resource_group = config.ENV_DATA.get("azure_base_domain_resource_group_name")
        vnet_cmd = (
            f"az network vnet create --resource-group {resource_group} --name {vnet} "
            f"--address-prefixes {vnet_address_prefixes}"
        )
        exec_cmd(vnet_cmd)
        for subnet, prefixes_address in {
            master_subnet: master_subnet_address_prefixes,
            worker_subnet: worker_subnet_address_prefixes,
        }.items():
            subnet_prefixes_cmd = (
                f"az network vnet subnet create --resource-group {resource_group} --vnet-name {vnet} "
                f"--name {subnet} --address-prefixes {prefixes_address} --service-endpoints Microsoft.ContainerRegistry"
            )

            exec_cmd(subnet_prefixes_cmd)

    def get_cluster_details(self, cluster_name):
        """
        Returns info about the cluster which is taken from the az command.

        Args:
            cluster_name (str): Cluster name.

        Returns:
            dict: Cluster details

        """
        resource_group = config.ENV_DATA.get("azure_base_domain_resource_group_name")
        out = exec_cmd(
            f"az aro show --name {cluster_name} --resource-group {resource_group} -o json"
        ).stdout
        return json.loads(out)

    def write_kubeadmin_password(self, cluster_name, resource_group):
        """
        Get kubeadmin password for cluster
        """
        cmd = f"az aro list-credentials --name {cluster_name} --resource-group {resource_group}"
        password = json.loads(exec_cmd(cmd).stdout)["kubeadminPassword"]
        password_file = os.path.join(
            config.ENV_DATA["cluster_path"], config.RUN["password_location"]
        )
        with open(password_file, "w+") as fd:
            fd.write(password)

    def set_dns_records(self, cluster_name, resource_group, base_domain):
        """
        Set DNS records for Azure ARO cluster.

        Args:
            cluster_name (str): Cluster name.
            resource_group (str): Base resource group
            base_domain (str): Base domain for the ARO Cluster

        """
        cmd = (
            f"az aro show -n {cluster_name} -g {resource_group} --query "
            f"'{{api:apiserverProfile.ip, ingress:ingressProfiles[0].ip}}' --only-show-errors"
        )
        data = json.loads(exec_cmd(cmd).stdout)
        dns_data = {
            "api": data["api"],
            "*.apps": data["ingress"],
        }
        self.delete_dns_records(cluster_name, resource_group, base_domain)
        for entry, ip in dns_data.items():
            logger.debug("Creating DNS records")
            create_dns_record_cmd = (
                f"az network dns record-set a add-record -g {resource_group} "
                f"-z {base_domain} -n {entry}.{cluster_name} -a {ip}"
            )
            exec_cmd(create_dns_record_cmd)

    def delete_dns_records(self, cluster_name, resource_group, base_domain):
        """
        Delete DNS records for Azure ARO cluster

        Args:
            cluster_name (str): Cluster name.
            resource_group (str): Base resource group
            base_domain (str): Base domain for the ARO Cluster

        """
        for each in ["api", "*.apps"]:
            logger.debug("Deleting DNS records")
            delete_dns_record_cmd = (
                f"az network dns record-set a delete -g {resource_group} -z "
                f"{base_domain} --name {each}.{cluster_name} -y"
            )
            exec_cmd(delete_dns_record_cmd)

    def get_kubeconfig(self, cluster_name, resource_group, path=None):
        """
        Export kubeconfig to provided path.

        Args:
            cluster_name (str): Cluster name or ID.
            resource_group (str): Base resource group
            path (str): Path where to create kubeconfig file.

        """
        if path:
            path = os.path.expanduser(path)
        else:
            path = os.path.join(
                config.ENV_DATA["cluster_path"], config.RUN["kubeconfig_location"]
            )
        base_path = os.path.dirname(path)
        os.makedirs(base_path, exist_ok=True)
        cmd = (
            f"az rest --method post --url '/subscriptions/{self._subscription_id}/resourceGroups/{resource_group}/"
            f"providers/Microsoft.RedHatOpenShift/openShiftClusters/{cluster_name}/"
            "listAdminCredentials?api-version=2022-09-04'"
        )
        output_data = json.loads(exec_cmd(cmd).stdout)
        encoded_kubeconfig_data = output_data["kubeconfig"]
        decoded_kubeconfig_data = base64.b64decode(encoded_kubeconfig_data).decode(
            "utf-8"
        )
        with open(path, "w+") as fd:
            fd.write(decoded_kubeconfig_data)
        kubeconfig_path = os.path.join(
            config.ENV_DATA["cluster_path"], config.RUN["kubeconfig_location"]
        )
        os.environ["KUBECONFIG"] = kubeconfig_path

    def destroy_cluster(self, cluster_name, resource_group):
        """
        Destroy the cluster in Azure ARO.

        Args:
            cluster_name (str): Cluster name .

        """
        base_domain = config.ENV_DATA["base_domain"]
        self.delete_dns_records(cluster_name, resource_group, base_domain)
        cmd = f"az aro delete --resource-group {resource_group} --name {cluster_name} --yes"
        out = exec_cmd(cmd, timeout=3600).stdout
        logger.info(f"Destroy command output: {out}")


def azure_storageaccount_check():
    """
    Testing that Azure storage account, post deployment.

    Testing for property 'allow_blob_public_access' to be 'false'
    """
    logger.info(
        "Checking if the 'allow_blob_public_access property of storage account is 'false'"
    )
    azure = AZURE()
    storage_account_names = azure.get_storage_accounts_names()
    for storage in storage_account_names:
        if "noobaaaccount" in storage:
            property = azure.get_storage_account_properties(storage)
            pat = r"'allow_blob_public_access': (True|False),"

            from re import findall

            match = findall(pat, property)

            if match:
                assert (
                    match[0] == "False"
                ), "Property allow_blob_public_access is set to True"
            else:
                assert False, "Property allow_blob_public_access not found."
