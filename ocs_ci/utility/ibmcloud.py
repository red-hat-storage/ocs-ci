# -*- coding: utf8 -*-
"""
Module for interactions with IBM Cloud Cluster.

"""

import json
import logging
import os
import re
import requests
import time
import ipaddress
from copy import copy
from json import JSONDecodeError
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.defaults import IBM_CLOUD_REGIONS
from ocs_ci.ocs.exceptions import (
    APIRequestError,
    CommandFailed,
    UnsupportedPlatformVersionError,
    UnexpectedBehaviour,
    NodeHasNoAttachedVolume,
    TimeoutExpiredError,
)
from ocs_ci.utility import version as util_version
from ocs_ci.utility.utils import get_infra_id, get_ocp_version, run_cmd, TimeoutSampler
from ocs_ci.ocs.node import get_nodes


logger = logging.getLogger(name=__file__)
ibm_config = config.AUTH.get("ibmcloud", {})


def login(region=None):
    """
    Login to IBM Cloud cluster

    Args:
        region (str): region to log in, if not specified it will use one from config
    """
    api_key = ibm_config["api_key"]
    login_cmd = f"ibmcloud login --apikey {api_key}"
    account_id = ibm_config.get("account_id")
    if account_id:
        login_cmd += f" -c {account_id}"
    api_endpoint = ibm_config.get("api_endpoint")
    if api_endpoint:
        login_cmd += f" -a {api_endpoint}"
    if not region:
        region = config.ENV_DATA.get("region")
    if region:
        login_cmd += f" -r {region}"
    logger.info("Logging to IBM cloud")
    run_cmd(login_cmd, secrets=[api_key])
    logger.info("Successfully logged in to IBM cloud")
    config.RUN["ibmcloud_last_login"] = time.time()


def set_region(region=None):
    """
    Sets the cluster region to ENV_DATA when enable_region_dynamic_switching is
    enabled.

    Args:
        region (str): region to set, if not defined it will try to get from metadata.json

    """
    if not config.ENV_DATA.get("enable_region_dynamic_switching"):
        return
    if not region:
        region = get_region(config.ENV_DATA["cluster_path"])
    logger.info(f"cluster region is {region}")
    logger.info(f"updating region {region} to ENV_DATA ")
    config.ENV_DATA["region"] = region
    other_region = list(IBM_CLOUD_REGIONS - {region})[0]
    for node_type in ["master", "worker"]:
        for idx, zone in enumerate(
            copy(config.ENV_DATA.get(f"{node_type}_availability_zones", []))
        ):
            config.ENV_DATA[f"{node_type}_availability_zones"][idx] = zone.replace(
                other_region, region
            )
    # Make sure we are logged in proper region from config, once region changed!
    login()


def get_region(cluster_path):
    """
    Get region from metadata.json in given cluster_path

    Args:
        cluster_path: path to cluster install directory

    Returns:
        str: region where cluster is deployed

    """
    metadata_file = os.path.join(cluster_path, "metadata.json")
    with open(metadata_file) as f:
        metadata = json.load(f)
    return metadata["ibmcloud"]["region"]


def run_ibmcloud_cmd(cmd, secrets=None, timeout=600, ignore_error=False, **kwargs):
    """
    Wrapper function for `run_cmd` which if needed will perform IBM Cloud login
    command before running the ibmcloud command. In the case run_cmd will fail
    because the IBM cloud got disconnected, it will login and re-try.

    Args:
        cmd (str): command to run
        secrets (list): A list of secrets to be masked with asterisks
            This kwarg is popped in order to not interfere with
            subprocess.run(``**kwargs``)
        timeout (int): Timeout for the command, defaults to 600 seconds.
        ignore_error (bool): True if ignore non zero return code and do not
            raise the exception.
    """
    last_login = config.RUN.get("ibmcloud_last_login", 0)
    timeout_from_last_login = time.time() - last_login
    # Login if the timeout from last login is greater than 9.5 minutes.
    if not last_login or timeout_from_last_login > 570:
        login()
    try:
        return run_cmd(cmd, secrets, timeout, ignore_error, **kwargs)
    except CommandFailed as ex:
        if "Please login" in str(ex):
            login()
            return run_cmd(cmd, secrets, timeout, ignore_error, **kwargs)


def get_cluster_details(cluster):
    """
    Returns info about the cluster which is taken from the ibmcloud command.

    Args:
        cluster (str): Cluster name or ID

    """
    out = run_ibmcloud_cmd(f"ibmcloud ks cluster get --cluster {cluster} -json")
    return json.loads(out)


def list_clusters(provider=None):
    """
    Returns info about the cluster which is taken from the ibmcloud command.

    Args:
        provider (str): Provider type (classic, vpc-classic, vpc-gen2).

    """
    cmd = "ibmcloud ks clusters -s -json"
    if provider:
        cmd += f" --provider {provider}"
    out = run_ibmcloud_cmd(cmd)
    return json.loads(out)


def get_ibmcloud_ocp_version():
    """
    Get OCP version available in IBM Cloud.
    """
    out = run_ibmcloud_cmd("ibmcloud ks versions --json")
    data = json.loads(out)["openshift"]
    major, minor = get_ocp_version().split(".")
    for version in data:
        if major == str(version["major"]) and minor == str(version["minor"]):
            return f"{major}.{minor}.{version['patch']}_openshift"
    raise UnsupportedPlatformVersionError(
        f"OCP version {major}.{minor} is not supported on IBM Cloud!"
    )


def create_cluster(cluster_name):
    """
    Create OCP cluster.

    Args:
        cluster_name (str): Cluster name.

    Raises:
        UnexpectedBehaviour: in the case, the cluster is not installed
            successfully.

    """
    provider = config.ENV_DATA["provider"]
    zone = config.ENV_DATA["zone"]
    flavor = config.ENV_DATA["worker_instance_type"]
    worker_replicas = config.ENV_DATA["worker_replicas"]
    ocp_version = get_ibmcloud_ocp_version()

    cmd = (
        f"ibmcloud ks cluster create {provider} --name {cluster_name}"
        f" --flavor {flavor}  --workers {worker_replicas}"
        f" --kube-version {ocp_version}"
    )
    if provider == "vpc-gen2":
        vpc_id = config.ENV_DATA["vpc_id"]
        subnet_id = config.ENV_DATA["subnet_id"]
        cmd += f" --vpc-id {vpc_id} --subnet-id  {subnet_id} --zone {zone}"
        cos_instance = config.ENV_DATA["cos_instance"]
        cmd += f" --cos-instance {cos_instance}"
    out = run_ibmcloud_cmd(cmd)
    logger.info(f"Create cluster output: {out}")
    logger.info("Sleeping for 60 seconds before taking cluster info")
    time.sleep(60)
    cluster_info = get_cluster_details(cluster_name)
    # Create metadata file to store the cluster name
    cluster_info["clusterName"] = cluster_name
    cluster_info["clusterID"] = cluster_info["id"]
    cluster_path = config.ENV_DATA["cluster_path"]
    metadata_file = os.path.join(cluster_path, "metadata.json")
    with open(metadata_file, "w+") as f:
        json.dump(cluster_info, f)
    # Temporary increased timeout to 10 hours cause of issue with deployment on
    # IBM cloud
    timeout = 36000
    sampler = TimeoutSampler(
        timeout=timeout, sleep=30, func=is_cluster_installed, cluster=cluster_name
    )
    if not sampler.wait_for_func_status(True):
        logger.error(f"Cluster was not installed in the timeout {timeout} seconds!")
        raise UnexpectedBehaviour("Cluster didn't get to Normal state!")


def is_cluster_installed(cluster):
    """
    Check if cluster is installed and return True if so, False otherwise.

    Args:
        cluster (str): Cluster name or ID

    """
    cluster_info = get_cluster_details(cluster)
    if not cluster_info:
        return False
    logger.info(
        f"IBM cloud cluster: {cluster} has status: {cluster_info['state']} "
        f"and status: {cluster_info['status']}"
    )
    return cluster_info["state"] == "normal"


def get_kubeconfig(cluster, path):
    """
    Export kubeconfig to provided path.

    Args:
        cluster (str): Cluster name or ID.
        path (str): Path where to create kubeconfig file.

    """
    path = os.path.expanduser(path)
    basepath = os.path.dirname(path)
    os.makedirs(basepath, exist_ok=True)
    cmd = f"ibmcloud ks cluster config --cluster {cluster} --admin --output yaml"
    output = run_ibmcloud_cmd(cmd)
    with open(path, "w+") as fd:
        fd.write(output)


def destroy_cluster(cluster):
    """
    Destroy the cluster on IBM Cloud.

    Args:
        cluster (str): Cluster name or ID.

    """
    cmd = f"ibmcloud ks cluster rm -c {cluster} -f --force-delete-storage"
    out = run_ibmcloud_cmd(cmd)
    logger.info(f"Destroy command output: {out}")


def add_deployment_dependencies():
    """
    Adding dependencies for IBM Cloud deployment
    """
    ocp_version = util_version.get_semantic_ocp_version_from_config()
    if ocp_version >= util_version.VERSION_4_9:
        logger.info(
            "IBM Cloud dependencies like volumesnapshot CRs will not be created"
        )
        return
    cr_base_url = (
        "https://raw.githubusercontent.com/openshift/csi-external-snapshotter/"
        f"release-{ocp_version}/"
    )
    if ocp_version < util_version.VERSION_4_6:
        cr_base_url = f"{cr_base_url}config/crd/"
    else:
        cr_base_url = f"{cr_base_url}client/config/crd/"
    # This works only for OCP >= 4.6 and till IBM Cloud guys will resolve the issue
    wa_crs = [
        f"{cr_base_url}snapshot.storage.k8s.io_volumesnapshotclasses.yaml",
        f"{cr_base_url}snapshot.storage.k8s.io_volumesnapshotcontents.yaml",
        f"{cr_base_url}snapshot.storage.k8s.io_volumesnapshots.yaml",
    ]
    for cr in wa_crs:
        run_cmd(f"oc apply -f {cr}")


class IBMCloud(object):
    """
    Wrapper for Ibm Cloud
    """

    def restart_nodes(self, nodes, timeout=900, wait=True):
        """
        Reboot the nodes on IBM Cloud.

        Args:
            nodes (list): The worker node instance

        """
        logger.info("restarting nodes")
        provider_id = nodes[0].get()["spec"]["providerID"]
        cluster_id = provider_id.split("/")[5]

        for node in nodes:
            worker_id = node.get()["spec"]["providerID"].split("/")[-1]
            cmd = f"ibmcloud ks worker reboot --cluster {cluster_id} --worker {worker_id} -f"
            out = run_ibmcloud_cmd(cmd)
            logger.info(f"Node restart command output: {out}")

    def attach_volume(self, volume, node):
        """
        Attach volume to node on IBM Cloud.

        Args:
            volume (str): volume id.
            node (OCS): worker node id to attach.

        """
        logger.info(
            f"attach_volumes:{node[0].get()['metadata']['labels']['failure-domain.beta.kubernetes.io/zone']}"
        )
        provider_id = node[0].get()["spec"]["providerID"]
        cluster_id = provider_id.split("/")[5]
        worker_id = node[0].get()["metadata"]["labels"][
            "ibm-cloud.kubernetes.io/worker-id"
        ]

        logger.info(f"volume is : {volume}")

        cmd = f"ibmcloud is volume {volume} --output json"
        out = run_ibmcloud_cmd(cmd)
        out = json.loads(out)

        if len(out["volume_attachments"]) == 0:
            cmd = (
                f"ibmcloud ks storage attachment  create --cluster {cluster_id} --worker {worker_id}"
                f"  --volume {volume} --output json"
            )
            out = run_ibmcloud_cmd(cmd)
            out = json.loads(out)
            logger.info(f"attachment command output: {out}")
        else:
            logger.info(f"volume is already attached to node: {out}")

    def detach_volume(self, volume, node=None, delete_from_backend=True):
        """
        Detach volume from node on IBM Cloud.

        Args:
            volume (str): volume id.
            node (OCS): worker node id to detach.
            delete_from_backend (bool): True for deleting the disk from the
                storage backend, False otherwise

        """
        provider_id = node.get()["spec"]["providerID"]
        cluster_id = provider_id.split("/")[5]
        worker_id = node.get()["metadata"]["labels"][
            "ibm-cloud.kubernetes.io/worker-id"
        ]

        logger.info(f"volume is : {volume}")

        cmd = f"ibmcloud is volume {volume} --output json"
        out = run_ibmcloud_cmd(cmd)
        out = json.loads(out)

        if out["status"] == "available":
            attachment_id = out["volume_attachments"][0]["id"]

            cmd = (
                f"ibmcloud ks storage attachment  rm --cluster {cluster_id} --worker {worker_id}"
                f"  --attachment {attachment_id}"
            )
            out = run_ibmcloud_cmd(cmd)
            logger.info(f"detachment command output: {out}")

    def get_node_by_attached_volume(self, volume):
        """
        Get the node by attached volume on IBM Cloud.

        Args:
            volume (str): volume id.

        Raises:
            NodeHasNoAttachedVolume: In case the volume is not attached to node

        Returns:
            str: worker id

        """
        cmd = f"ibmcloud is volume {volume} --output json"
        out = run_ibmcloud_cmd(cmd)
        out = json.loads(out)

        if not out["volume_attachments"]:
            logger.info("volume is not attached to node")
            raise NodeHasNoAttachedVolume("volume not attached to node")
        else:
            worker_id = out["volume_attachments"][0]["instance"]["name"]
            logger.info(f"volume is  attached to node: {worker_id}")
            worker_nodes = get_nodes(node_type="worker")
            for worker_node in worker_nodes:
                logger.info(
                    f"worker node id is:{worker_node.get()['metadata']['labels']['ibm-cloud.kubernetes.io/worker-id']}"
                )
                if (
                    worker_node.get()["metadata"]["labels"][
                        "ibm-cloud.kubernetes.io/worker-id"
                    ]
                    == worker_id
                ):
                    logger.info(f"return worker node is:{worker_id}")
                    return worker_node

    def get_data_volumes(self):
        """
        Returns volumes in IBM Cloud for cluster.

        Returns:
            list: volumes in IBM Cloud for cluster.

        """
        logger.info("get data volumes")

        # get cluster ID
        cmd = f"ibmcloud ks cluster get --cluster {config.ENV_DATA['cluster_name']} --output json"
        out = run_ibmcloud_cmd(cmd)
        out = json.loads(out)
        cluster_id = out["id"]

        # get the volume list
        cmd = "ibmcloud is vols --output json"
        out = run_ibmcloud_cmd(cmd)
        out = json.loads(out)

        vol_ids = []
        for vol in out:
            if vol["volume_attachments"]:
                if cluster_id in vol["volume_attachments"][0]["instance"]["name"]:
                    vol_ids.append(vol["id"])

        logger.info(f"volume ids are : {vol_ids}")
        return vol_ids

    def is_volume_attached(self, volume):
        """
        Check if volume is attached to node or not.

        Args:
            volume (str): The volume to check for to attached

        Returns:
            bool: 'True' if volume is attached otherwise 'False'

        """
        logger.info("Checking volume attachment status")
        cmd = f"ibmcloud is volume {volume} --output json"
        out = run_ibmcloud_cmd(cmd)
        out = json.loads(out)
        return out["volume_attachments"]

    def wait_for_volume_attach(self, volume):
        """
        Checks volume is attached to node or not

        Args:
            volume (str): The volume to wait for to be attached

        Returns:
            bool: True if the volume has been attached to the
                instance, False otherwise

        """
        try:
            for sample in TimeoutSampler(300, 3, self.is_volume_attached, volume):
                if sample:
                    return True
        except TimeoutExpiredError:
            logger.info("Volume is not attached to node")
            return False

    def get_volume_id(self):
        """
        Returns Volumeid with the name taken from constants

        Returns:
            str: volume id if the volume exists otherwise create
                new volume

        """
        zone = config.ENV_DATA["zone"]

        cmd = "ibmcloud is vols --output json "
        out = run_ibmcloud_cmd(cmd)
        out = json.loads(out)

        vol_id = ""
        for vols in out:
            vol_name = str(vols["name"]).strip()
            if vol_name == constants.IBMCLOUD_VOLUME_NAME:
                vol_id = vols["id"]
                return vol_id

        cmd = f"ibmcloud is volume-create {constants.IBMCLOUD_VOLUME_NAME} general-purpose {zone} --output json"
        out = run_ibmcloud_cmd(cmd)
        out = json.loads(out)
        vol_id = out["id"]
        return vol_id

    def restart_nodes_by_stop_and_start(self, nodes, force=True):
        """
        Reboot the nodes which are not ready on IBM Cloud.

        Args:
            nodes (list): The OCS objects of the nodes
            force (bool): True for force VM stop, False otherwise

        """
        logger.info("restarting nodes by stop and start")
        provider_id = nodes[0].get()["spec"]["providerID"]
        cluster_id = provider_id.split("/")[5]

        cmd = f"ibmcloud ks workers --cluster {cluster_id} --output json"
        out = run_ibmcloud_cmd(cmd)
        worker_nodes = json.loads(out)

        worker_nodes_not_ready = []
        for worker_node in worker_nodes:
            if worker_node["health"]["message"] != "Ready":
                worker_nodes_not_ready.append(worker_node["id"])

        if len(worker_nodes_not_ready) > 0:
            for not_ready_node in worker_nodes_not_ready:
                cmd = f"ibmcloud ks worker reboot --cluster {cluster_id} --worker {not_ready_node} -f"
                out = run_ibmcloud_cmd(cmd)
                logger.info(f"Node restart command output: {out}")

    def delete_volume_id(self, volume):
        """
        Deletes Volumeid

        Args:
            volume (str): The volume to be deleted

        """
        cmd = f"ibmcloud is volume-delete {volume} --output json"
        out = run_ibmcloud_cmd(cmd)
        out = json.loads(out)
        logger.info("Sleeping for 60 seconds to delete the volume")
        time.sleep(60)

        if out[0]["result"]:
            logger.info(f"volume is deleted successfully: {volume}")
        else:
            logger.info("volume is not deleted")


def label_nodes_region():
    """
    Apply the region label to the worker nodes.
    Necessary for IBM COS-backed backingstore support.

    """
    logger.info("Applying region label to worker nodes")
    region = config.ENV_DATA.get("region")
    worker_nodes = get_nodes(node_type=constants.WORKER_MACHINE)
    for node in worker_nodes:
        node.add_label(rf"ibm-cloud\.kubernetes\.io/region={region}")


def get_cluster_service_ids(cluster_name, get_infra_id_from_metadata=True):
    """
    Get cluster service IDs

    Args:
        cluster_name (str): cluster name
        get_infra_id_from_metadata (bool): if set to true it will try to get
            infra ID from metadata.json file (Default: True)

    Returns:
        list: service IDs for cluster

    """
    cmd = "ibmcloud iam service-ids --output json"
    out = run_ibmcloud_cmd(cmd)
    infra_id = ""
    pattern = rf"{cluster_name}-[a-z0-9]{{5}}-.*"
    service_ids = []
    if get_infra_id_from_metadata:
        try:
            cluster_path = config.ENV_DATA["cluster_path"]
            infra_id = get_infra_id(cluster_path)
            pattern = rf"{infra_id}-.*"

        except (FileNotFoundError, JSONDecodeError, KeyError):
            logger.warning("Could not get infra ID")
    for service_id in json.loads(out):
        if re.match(pattern, service_id["name"]):
            service_ids.append(service_id)
    return service_ids


def get_cluster_account_policies(cluster_name, cluster_service_ids):
    """
    Get cluster account policies.

    Args:
        cluster_name (str): cluster name
        cluster_service_ids (list): list of service IDs, e.g. output from get_cluster_service_ids.

    Returns:
        list: Account policies for cluster

    """
    cmd = "ibmcloud iam access-policies --output json"
    out = run_ibmcloud_cmd(cmd)
    account_policies = json.loads(out)
    matched_account_policies = []
    if not cluster_service_ids:
        logger.warning(
            "No service ID provided, we cannot match any account policy without it!"
        )
        return matched_account_policies
        cluster_service_ids = get_cluster_service_ids(cluster_name)
    for account_policy in account_policies:
        for subject in account_policy.get("subjects", []):
            for attr in subject.get("attributes", []):
                for service_id in cluster_service_ids:
                    if attr.get("name") == "iam_id" and (
                        attr.get("value") == service_id.get("iam_id")
                    ):
                        matched_account_policies.append(account_policy)
    return matched_account_policies


def delete_service_id(service_id):
    """
    Delete service ID

    Args:
        service_id (str): ID of service ID to delete
    """
    logger.info(f"Deleting service ID: {service_id}")
    cmd = f"ibmcloud iam service-id-delete -f {service_id}"
    run_ibmcloud_cmd(cmd)


def get_api_token():
    """
    Get IBM Cloud API Token for API Calls authentication

    Returns:
        str: IBM Cloud API Token

    """
    token_cmd = "ibmcloud iam oauth-tokens --output json"
    out = run_ibmcloud_cmd(token_cmd)
    token = json.loads(out)
    return token["iam_token"].split()[1]


def delete_account_policy(policy_id, token=None):
    """
    Delete account policy

    Args:
        policy_id (str): policy ID
        token (str): IBM Cloud token to be used for API calls - if not provided it will
            create new one.

    Returns:
        bool: True in case it successfully deleted

    Raises:
        APIRequestError: in case API call didn't went well

    """
    if not token:
        token = get_api_token()
    url = f"https://iam.cloud.ibm.com/v1/policies/{policy_id}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.delete(url.format("YOUR_POLICY_ID_HERE"), headers=headers)
    if response.status_code == 204:  # 204 means success (No Content)
        logger.info(f"Policy id: {policy_id} deleted successfully.")
    else:
        raise APIRequestError(
            f"Failed to delete policy id: {policy_id}. Status code: {response.status_code}\n"
            f"Response content: {response.text}"
        )


def cleanup_policies_and_service_ids(cluster_name, get_infra_id_from_metadata=True):
    """
    Cleanup Account Policies and Service IDs for cluster.

    Args:
        cluster_name (str): cluster name
        get_infra_id_from_metadata (bool): if set to true it will try to get
            infra ID from metadata.json file (Default: True)

    """
    service_ids = get_cluster_service_ids(cluster_name, get_infra_id_from_metadata)
    if not service_ids:
        logger.info(f"No service ID found for cluster {cluster_name}")
        return
    account_policies = get_cluster_account_policies(cluster_name, service_ids)
    api_token = get_api_token()
    for policy in account_policies:
        delete_account_policy(policy["id"], api_token)
    for service_id in service_ids:
        delete_service_id(service_id["id"])


def create_resource_group(resource_group):
    """
    Create resource group.

    Args:
        resource_group (str): resource group name

    """
    run_ibmcloud_cmd(f"ibmcloud resource group-create {resource_group}")


def create_vpc(cluster_name, resource_group):
    """
    Create VPC.

    Args:
        cluster_name (str): cluster name
        resource_group (str): resource group name

    """
    run_ibmcloud_cmd(
        f"ibmcloud is vpc-create {cluster_name} --address-prefix-management manual"
        f" --resource-group-name {resource_group}"
    )


def get_used_subnets():
    """
    Get currently used subnets in IBM Cloud

    Returns:
        list: subnets

    """
    subnets_data = json.loads(run_ibmcloud_cmd("ibmcloud is subnets --output json"))
    return [subnet["ipv4_cidr_block"] for subnet in subnets_data]


def create_address_prefix(prefix_name, vpc, zone, cidr):
    """
    Create address prefix in VPC.

    Args:
        prefix_name (str): address prefix name to create
        vpc (str): VPC name
        zone (str): zone name
        cidr (str): CIDR for address prefix

    """
    run_ibmcloud_cmd(
        f"ibmcloud is vpc-address-prefix-create {prefix_name} {vpc} {zone} {cidr}"
    )


def create_subnet(subnet_name, vpc, zone, cidr, resource_group):
    """
    Create subnet in VPC.

    Args:
        subnet_name (str): address prefix name to create
        vpc (str): VPC name
        zone (str): zone name
        cidr (str): CIDR for address prefix
        resource_group (str): resource group name

    """
    run_ibmcloud_cmd(
        f"ibmcloud is subnet-create {subnet_name} {vpc} --zone {zone} --ipv4-cidr-block {cidr}"
        f" --resource-group-name {resource_group}"
    )


def create_public_gateway(gateway_name, vpc, zone, resource_group):
    """
    Create public gateway in VPC.

    Args:
        gateway_name (str): public gateway name
        vpc (str): VPC name
        zone (str): zone name
        resource_group (str): resource group name

    """
    run_ibmcloud_cmd(
        f"ibmcloud is public-gateway-create {gateway_name} {vpc} {zone} --resource-group-name {resource_group}"
    )


def attach_subnet_to_public_gateway(subnet_name, gateway_name, vpc):
    """
    Attach subnet to public gateway.

    Args:
        subnet_name (str): subnet name to attach to public gateway
        gateway_name (str): public gateway name
        vpc (str): VPC name

    """
    run_ibmcloud_cmd(
        f"ibmcloud is subnet-update {subnet_name} --pgw {gateway_name} --vpc {vpc}"
    )


def find_free_network_subnets(subnet_cidr, network_prefix=27):
    """
    This function will look for currently used subnet, and will try to find one which
    is not occupied by any other VPC.

    Args:
        subnet_cidr (str): subnet CIDR in which range to look for free subnet (e.g. 10.240.0.0/18)
        network_prefix (int): subnet prefix to look for

    Returns:
        tuple: (network_with_prefix, network_split1, network_split2), where
            network_with_prefix - is network CIDR which we are looking for.
            network_split1 - is first CIDR split of network_with_prefix
            network_split2 - is second CIDR split of network_with_prefix

    """
    network = ipaddress.ip_network(subnet_cidr)

    # Get all possible /network_prefix+1 networks within the /network_prefix network
    main_subnets = list(network.subnets(new_prefix=network_prefix))
    split_subnets = list(network.subnets(new_prefix=network_prefix + 1))
    zipped_subnets = [
        (main_subnets[i], split_subnets[2 * i], split_subnets[2 * i + 1])
        for i in range(len(main_subnets))
    ]

    for possible_subnets in zipped_subnets:
        is_free = True
        list_of_subnets = get_used_subnets()
        for subnet in list_of_subnets:
            for possible_subnet in possible_subnets:
                tested_network = ipaddress.ip_network(subnet)
                is_subnet = possible_subnet.subnet_of(tested_network)
                if is_subnet:
                    logger.debug(
                        f"Subnet {possible_subnet} is subnet of {tested_network} skipping it!"
                    )
                    is_free = False
                    break
        if is_free:
            logger.info(f"Free set of subnets found: {possible_subnets}")
            return possible_subnets
