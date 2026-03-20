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
import ibm_boto3
import ipaddress

from copy import copy
from ibm_botocore.client import Config as IBMBotocoreConfig, ClientError
from json import JSONDecodeError
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.defaults import IBM_CLOUD_REGIONS
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.exceptions import (
    APIRequestError,
    CommandFailed,
    UnsupportedPlatformVersionError,
    UnexpectedBehaviour,
    NodeHasNoAttachedVolume,
    TimeoutExpiredError,
    FloatingIPAssignException,
)
from ocs_ci.ocs.node import wait_for_nodes_status
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.utils import get_primary_cluster_config
from ocs_ci.utility import version as util_version
from ocs_ci.utility.utils import get_infra_id, get_ocp_version, run_cmd, TimeoutSampler
from ocs_ci.ocs.node import get_nodes


logger = logging.getLogger(__name__)


def login(region=None, resource_group=None):
    """
    Login to IBM Cloud cluster

    Args:
        region (str): region to log in, if not specified it will use one from config
        resource_group (str): resource group to log in, if not specified it will use one from config
            or nothing if not defined
    """
    platform = config.ENV_DATA["platform"]
    if platform != constants.IBMCLOUD_PLATFORM:
        logger.info(
            f"Skipping IBM Cloud login as platform: {platform} is not IBM Cloud"
        )
        return
    ibm_config = config.AUTH.get("ibmcloud", {})
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
    ibm_cloud_managed = (
        config.ENV_DATA["platform"] == constants.IBMCLOUD_PLATFORM
        and config.ENV_DATA["deployment_type"] == "managed"
    )
    if not resource_group and ibm_cloud_managed:
        resource_group = config.ENV_DATA.get("resource_group")
    if resource_group:
        login_cmd += f" -g {resource_group}"
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
    if not config.ENV_DATA.get("enable_region_dynamic_switching") or (
        config.ENV_DATA["platform"] != constants.IBMCLOUD_PLATFORM
    ):
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
    ibm_cloud_managed = (
        config.ENV_DATA["platform"] == constants.IBMCLOUD_PLATFORM
        and config.ENV_DATA["deployment_type"] == "managed"
    )
    if ibm_cloud_managed:
        return metadata["region"]
    return metadata["ibmcloud"]["region"]


def get_ibmcloud_cluster_region():
    """
    Get IBM Cloud region from the cluster's infrastructure object.

    This function queries the live cluster to retrieve the IBM Cloud region
    from the infrastructure status, which may differ from the metadata.json file.

    Returns:
        str: IBM Cloud region where the cluster is deployed

    Raises:
        CommandFailed: If the oc command fails
    """
    ocp_obj = OCP()
    region = ocp_obj.exec_oc_cmd(
        "get infrastructure cluster -o jsonpath='{.status.platformStatus.ibmcloud.location}'"
    )
    logger.info(f"IBM Cloud cluster region: {region}")
    return region.strip()


def get_resource_group_name(cluster_path):
    """
    Get resource group from metadata.json in given cluster_path

    Args:
        cluster_path: path to cluster install directory

    Returns:
        str: resource group name

    """
    metadata_file = os.path.join(cluster_path, "metadata.json")
    with open(metadata_file) as f:
        metadata = json.load(f)
    ibm_cloud_managed = (
        config.ENV_DATA["platform"] == constants.IBMCLOUD_PLATFORM
        and config.ENV_DATA["deployment_type"] == "managed"
    )
    if ibm_cloud_managed:
        return metadata["resourceGroupName"]
    return metadata["ibmcloud"]["resourceGroupName"]


def set_resource_group_name(resource_group_name=None):
    """
    Sets the resource group to ibmcloud cli

    Args:
        resource_group_name (str): Resource Group Name

    """
    if not resource_group_name:
        resource_group_name = get_resource_group_name(config.ENV_DATA["cluster_path"])
    run_ibmcloud_cmd(f"ibmcloud target -g {resource_group_name}")


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
        if config.multicluster:
            set_target_region()
        return run_cmd(cmd, secrets, timeout, ignore_error, **kwargs)
    except CommandFailed as ex:
        if "Please login" in str(ex):
            login()
            if config.multicluster:
                set_target_region()
            return run_cmd(cmd, secrets, timeout, ignore_error, **kwargs)
        else:
            if not ignore_error:
                raise


def set_target_region():
    """
    Set the target region for the IBM Cloud CLI.
    """
    region = get_region(config.ENV_DATA["cluster_path"])
    cmd = f"ibmcloud target -r {region}"
    run_cmd(cmd)


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
    worker_availability_zones = config.ENV_DATA.get("worker_availability_zones", [])
    worker_zones_number = len(worker_availability_zones)
    zone = config.ENV_DATA["worker_availability_zone"]
    flavor = config.ENV_DATA["worker_instance_type"]
    worker_replicas = config.ENV_DATA["worker_replicas"]
    if worker_zones_number > 1:
        worker_replicas = 2
    ocp_version = get_ibmcloud_ocp_version()
    cmd = (
        f"ibmcloud ks cluster create {provider} --name {cluster_name}"
        f" --flavor {flavor}  --workers {worker_replicas}"
        f" --kube-version {ocp_version}"
    )
    # Reloading correct number of worker replica for later usage.
    if worker_zones_number > 1:
        worker_replicas = int(config.ENV_DATA["worker_replicas"] / worker_zones_number)
    if provider == "vpc-gen2":
        semantic_ocp_version = util_version.get_semantic_ocp_version_from_config()
        if semantic_ocp_version >= util_version.VERSION_4_15:
            cmd += " --disable-outbound-traffic-protection"
        vpc_id = config.ENV_DATA["vpc_id"]
        subnet_id = config.ENV_DATA.get("subnet_id")
        subnet_ids_per_zone = config.ENV_DATA.get("subnet_ids_per_zone", {}).get(zone)
        if subnet_ids_per_zone:
            subnet_id = subnet_ids_per_zone
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
    cluster_id = cluster_info["id"]
    cluster_info["clusterID"] = cluster_id
    cluster_path = config.ENV_DATA["cluster_path"]
    metadata_file = os.path.join(cluster_path, "metadata.json")
    with open(metadata_file, "w+") as f:
        json.dump(cluster_info, f)
    for worker_zone in worker_availability_zones:
        if worker_zone == zone:
            continue
        subnet = config.ENV_DATA["subnet_ids_per_zone"][worker_zone]
        cmd = (
            f"ibmcloud oc zone add {provider} --subnet-id {subnet}  "
            f"--cluster {cluster_id} --zone {worker_zone} --worker-pool default"
        )
        run_ibmcloud_cmd(cmd)
    if worker_zones_number > 1:
        cmd = (
            f"ibmcloud ks worker-pool resize --cluster {cluster_name} --worker-pool "
            f"default --size-per-zone {worker_replicas}"
        )
        run_ibmcloud_cmd(cmd)
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


def is_ibm_platform():
    """
    Check if cluster is IBM or Not

    """
    return (
        get_primary_cluster_config().ENV_DATA.get("platform")
        == constants.IBMCLOUD_PLATFORM
    )


class IBMCloud(object):
    """
    Wrapper for Ibm Cloud
    """

    def start_nodes(self, nodes, wait=True):
        """
        Start nodes on IBM Cloud.

        Args:
            nodes (list): The OCS objects of the nodes
            wait (bool): True for waiting the instances to start, False otherwise

        Raises:
            ValueError: if the list of nodes is empty

        """
        if not nodes:
            raise ValueError("No nodes found to start")

        node_names = [n.name for n in nodes]
        self.restart_nodes(nodes)

        if wait:
            timeout = 300
            ibm_cloud_managed = (
                config.ENV_DATA["platform"] == constants.IBMCLOUD_PLATFORM
                and config.ENV_DATA["deployment_type"] == "managed"
            )
            if ibm_cloud_managed:
                timeout = 3000
            # When the node is reachable then the node reaches status Ready.
            logger.info(f"Waiting for nodes: {node_names} to reach ready state")
            wait_for_nodes_status(
                node_names=node_names,
                status=constants.NODE_READY,
                timeout=timeout,
                sleep=15,
            )

    def stop_nodes(self, nodes, wait=True):
        """
        Stop nodes on IBM Cloud

        Args:
            nodes (list): The OCS objects of the nodes
            wait (bool): True for waiting the instances to stop, False otherwise

        Raises:
            ValueError: if the list of nodes is empty

        """
        if not nodes:
            raise ValueError("No nodes found to stop")

        cmd = "oc debug node/{} --to-namespace=default -- chroot /host shutdown"
        node_names = [n.name for n in nodes]
        for node in node_names:
            run_cmd(cmd.format(node))

        if wait:
            timeout = 300
            ibm_cloud_managed = (
                config.ENV_DATA["platform"] == constants.IBMCLOUD_PLATFORM
                and config.ENV_DATA["deployment_type"] == "managed"
            )
            if ibm_cloud_managed:
                timeout = 900
            # When the node is reachable then the node reaches status Ready.
            logger.info(f"Waiting for nodes: {node_names} to reach not ready state")
            wait_for_nodes_status(
                node_names, constants.NODE_NOT_READY, timeout=timeout, sleep=5
            )

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


class IBMCloudIPI(object):
    """
    Wrapper for Ibm Cloud IPI

    """

    def restart_nodes(self, nodes, wait=True, timeout=900):
        """
        Reboot the nodes on IBM Cloud.
        Args:
            nodes (list): The worker node instance
            wait (bool): Wait for the VMs to stop
            timeout (int): Timeout for the command, defaults to 900 seconds.
        """
        logger.info("restarting nodes")

        for node in nodes:
            cmd = f"ibmcloud is instance-reboot {node.name} -f"
            out = run_ibmcloud_cmd(cmd)
            logger.info(f"Node restart command output: {out}")

        if wait:
            for node in nodes:
                sample = TimeoutSampler(
                    timeout=timeout,
                    sleep=10,
                    func=self.check_node_status,
                    node_name=node.name,
                    node_status=constants.STATUS_RUNNING.lower(),
                )
                sample.wait_for_func_status(result=True)

    def start_nodes(self, nodes):
        """
        Start the nodes on IBM Cloud
        Args:
            nodes (list): The OCS objects of the nodes
        """
        # logger.info(nodes)
        for node in nodes:
            # logger.info(node.get())
            cmd = f"ibmcloud is instance-start {node.name}"
            out = run_ibmcloud_cmd(cmd)
            logger.info(f"Node start command output: {out}")

    def stop_nodes(self, nodes, force=True, wait=True):
        """
        Stop the nodes on IBM Cloud
        Args:
            nodes (list): The OCS objects of the nodes
            force (bool): True for VM ungraceful power off, False for graceful VM shutdown
            wait (bool): Wait for the VMs to stop
        """
        for node in nodes:
            cmd = f"ibmcloud is instance-stop {node.name} --force={force}"
            out = run_ibmcloud_cmd(cmd)
            logger.info(f"Node Stop command output: {out}")

        if wait:
            for node in nodes:
                sample = TimeoutSampler(
                    timeout=300,
                    sleep=10,
                    func=self.check_node_status,
                    node_name=node.name,
                    node_status=constants.STATUS_STOPPED,
                )
                sample.wait_for_func_status(result=True)

    def restart_nodes_by_stop_and_start(
        self, nodes, wait=True, force=True, timeout=300
    ):
        """
        Restart nodes by stopping and starting VM in IBM Cloud
        Args:
            nodes (list): The OCS objects of the nodes
            wait (bool): True in case wait for status is needed, False otherwise
            force (bool): True for force instance stop, False otherwise
            timeout (int): Timeout for the command, defaults to 300 seconds.
        """
        logger.info(f"Stopping instances {list(node.name for node in nodes)}")
        self.stop_nodes(nodes=nodes, force=force)
        if wait:
            for node in nodes:
                sample = TimeoutSampler(
                    timeout=timeout,
                    sleep=10,
                    func=self.check_node_status,
                    node_name=node.name,
                    node_status=constants.STATUS_STOPPED,
                )
                sample.wait_for_func_status(result=True)
        logger.info(f"Starting instances {list(node.name for node in nodes)}")

        self.start_nodes(nodes=nodes)
        if wait:
            for node in nodes:
                sample = TimeoutSampler(
                    timeout=timeout,
                    sleep=10,
                    func=self.check_node_status,
                    node_name=node.name,
                    node_status=constants.STATUS_RUNNING.lower(),
                )
                sample.wait_for_func_status(result=True)

    def check_node_status(self, node_name, node_status):
        """
        Check the node status in IBM cloud

        Args:
            node_name (str): Node name
            node_status (str): Status of Node Running or Stopped

        Returns:
            bool: True if status matches else False
        """
        try:
            cmd = f"ibmcloud is instance {node_name} --output json"

            out = run_ibmcloud_cmd(cmd)
            out = json.loads(out)
            if out["status"] == node_status:
                return True
            else:
                return False
        except CommandFailed as cf:
            if "Instance not found" in str(cf):
                return True
        return False

    def restart_nodes_by_stop_and_start_force(self):
        """
        Make sure all nodes are up by the end of the test on IBM Cloud.
        """
        resource_group_name = get_resource_group_name(config.ENV_DATA["cluster_path"])
        stop_node_list = []
        cmd = (
            f"ibmcloud is ins --resource-group-name {resource_group_name} --output json"
        )
        out = run_ibmcloud_cmd(cmd)
        all_instance_output = json.loads(out)
        for instance in all_instance_output:
            if instance["status"] == constants.STATUS_STOPPED:
                node_obj = OCP(kind="Node", resource_name=instance["name"]).get()
                node_obj_ocs = OCS(**node_obj)
                stop_node_list.append(node_obj_ocs)
        logger.info("Force stopping node which are in stopping state")
        self.stop_nodes(nodes=stop_node_list, force=True, wait=True)
        logger.info("Starting Stopped Node")
        self.start_nodes(nodes=stop_node_list)

    def terminate_nodes(self, nodes, wait=True):
        """
        Terminate the Node in IBMCloud
        Args:
            nodes (list): The OCS objects of the nodes
            wait (bool): True in case wait for status is needed, False otherwise
        """
        for node in nodes:
            cmd = f"ibmcloud is instance-delete {node.name} -f"
            out = run_ibmcloud_cmd(cmd)
            logger.info(f"Node deletion command output: {out}")
            break

        if wait:
            for node in nodes:
                sample = TimeoutSampler(
                    timeout=300,
                    sleep=10,
                    func=self.check_node_status,
                    node_name=node.name,
                )
                sample.wait_for_func_status(result=True)
                break

    def detach_volume(self, volume, node=None):
        """
        Detach volume from node on IBM Cloud.
        Args:
            volume (str): volume id.
            node (OCS): worker node object to detach.
        """

        logger.info(f"volume is : {volume}")

        cmd = f"ibmcloud is volume {volume} --output json"
        out = run_ibmcloud_cmd(cmd)
        out = json.loads(out)

        if out["status"] == "available":
            attachment_id = out["volume_attachments"][0]["id"]
            cmd = (
                f"ibmcloud is instance-volume-attachment-update {node.name} {attachment_id} "
                f"--output json --auto-delete=false"
            )
            out = run_ibmcloud_cmd(cmd)
            out = json.loads(out)
            logger.info(f"Update command output: {out}")
            cmd = (
                f"ibmcloud is instance-volume-attachment-detach {node.name} {attachment_id} "
                f"--output=json --force"
            )
            out = run_ibmcloud_cmd(cmd)
            logger.info(f"detachment command output: {out}")

    def attach_volume(self, volume, node):
        """
        Attach volume to node on IBM Cloud.
        Args:
            volume (str): volume id.
            node (OCS): worker node object to attach.
        """
        logger.info(
            f"attach_volumes:{node.get()['metadata']['labels']['failure-domain.beta.kubernetes.io/zone']}"
        )

        logger.info(f"volume is : {volume}")

        cmd = f"ibmcloud is volume {volume} --output json"
        out = run_ibmcloud_cmd(cmd)
        out = json.loads(out)

        if len(out["volume_attachments"]) == 0:

            logger.info(f"attachment command output: {out}")
            cmd = f"ibmcloud is instance-volume-attachment-add data-vol-name {node.name} {volume} --output json"
            out = run_ibmcloud_cmd(cmd)
            out = json.loads(out)
            logger.info(f"attachment command output: {out}")
        else:
            logger.info(f"volume is already attached to node: {out}")

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
        return True if len(out["volume_attachments"]) > 0 else False

    def wait_for_volume_attach(self, volume):
        """
        Checks volume is attached to node or not
        Args:
            volume (str): The volume to wait for to be attached
        Returns:
            bool: True if the volume has been attached to the instance, False otherwise
        """
        try:
            for sample in TimeoutSampler(300, 3, self.is_volume_attached, volume):
                if sample:
                    return True
                else:
                    return False
        except TimeoutExpiredError:
            logger.info("Volume is not attached to node")
            return False


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
    response = requests.delete(
        url.format("YOUR_POLICY_ID_HERE"), headers=headers, timeout=120
    )
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


def get_used_subnets(vpc_id=""):
    """
    Get currently used subnets in IBM Cloud

    Args:
        vpc_id (str): VPC ID to filter subnets. Empty string means all VPCs.

    Returns:
        list: subnets

    """
    subnets_data = json.loads(
        run_ibmcloud_cmd(f"ibmcloud is subnets --vpc '{vpc_id}' --output json")
    )
    return [subnet["ipv4_cidr_block"] for subnet in subnets_data]


def get_security_groups(vpc_id="", resource_group_id="", resource_group_name=""):
    """
    Get security groups in IBM Cloud

    Args:
        vpc_id (str): VPC ID to filter security groups, if empty it will return security groups for all VPCs.
        resource_group_id (str): Resource group ID to filter security groups.
        resource_group_name (str): Resource group name to filter security groups.

    Returns:
        list: security groups

    """
    cmd = "ibmcloud is security-groups --output json"
    if vpc_id:
        cmd += f" --vpc '{vpc_id}'"
    if resource_group_id:
        cmd += f" --resource-group-id '{resource_group_id}'"
    if resource_group_name:
        cmd += f" --resource-group-name '{resource_group_name}'"
    sg_data = json.loads(run_ibmcloud_cmd(cmd))
    return sg_data


def get_security_group_id(
    sg_name, vpc_id="", resource_group_id="", resource_group_name=""
):
    """
    Get security group ID by name

    Args:
        sg_name (str): security group name
        vpc_id (str): VPC ID to filter security groups, if empty it will return security groups for all VPCs.
        resource_group_id (str): Resource group ID to filter security groups.
        resource_group_name (str): Resource group name to filter security groups

    Returns:
        str: security group ID or empty string if not found

    """
    sgs = get_security_groups(vpc_id, resource_group_id, resource_group_name)
    for sg in sgs:
        if sg["name"] == sg_name:
            return sg["id"]
    return ""


def add_security_group_rule(
    security_group, direction, protocol, port_min, port_max, **kwargs
):
    """
    Add security group rule

    Args:
        security_group (str): security group ID or Name
        direction (str): inbound or outbound
        protocol (str): protocol, e.g. tcp, udp, icmp, all
        port_min (int): minimum port number
        port_max (int): maximum port number
        **kwargs: other arguments to be passed to command, e.g.
            --vpc ID or name of the VPC. It is required to specify only the unique resource by name inside this VPC.

    """
    cmd = (
        f"ibmcloud is security-group-rule-add {security_group} {direction} {protocol} "
        f"--port-min {port_min} --port-max {port_max}"
    )
    for key, value in kwargs.items():
        cmd += f" {key} '{value}'"
    run_ibmcloud_cmd(cmd)


def get_security_group_name_by_pattern(
    sg_name_pattern, vpc_id="", resource_group_id="", resource_group_name=""
):
    """
    Get security group name by pattern

    Args:
        sg_name_pattern (str): security group name pattern (regular expression)
        vpc_id (str): VPC ID to filter security groups, if empty it will return security groups for all VPCs
        resource_group_id (str): Resource group ID to filter security groups
        resource_group_name (str): Resource group name to filter security groups

    Returns:
        str: security group name or empty string if not found

    """
    sgs = get_security_groups(vpc_id, resource_group_id, resource_group_name)
    for sg in sgs:
        if re.search(sg_name_pattern, sg["name"]):
            return sg["name"]
    return ""


def open_ports_on_ibmcloud_hub_cluster():
    """
    Add the inbound rules for these cluster security configs `-cluster-wide` and `-openshift-net` to open the
    following ports: 3300, 6789, 9283, 6800-7300 and 31659
    """
    rg_name = get_resource_group_name(config.ENV_DATA["cluster_path"])
    sg_names = [
        get_security_group_name_by_pattern(
            r"cluster-wide$", resource_group_name=rg_name
        ),
        get_security_group_name_by_pattern(
            r"openshift-net$", resource_group_name=rg_name
        ),
    ]

    for sg_name in sg_names:
        add_security_group_rule(sg_name, "inbound", "tcp", 3300, 3300)
        add_security_group_rule(sg_name, "inbound", "tcp", 6789, 6789)
        add_security_group_rule(sg_name, "inbound", "tcp", 9283, 9283)
        add_security_group_rule(sg_name, "inbound", "tcp", 6800, 7300)
        add_security_group_rule(sg_name, "inbound", "tcp", 31659, 31659)
    logger.info("Inbound rules added successfully")


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


def delete_dns_records(cluster_name):
    """
    Delete DNS records leftover from cluster destroy.

    Args:
        cluster_name (str): Name of the cluster, used to filter DNS records

    """
    dns_domain_id = config.ENV_DATA["base_domain_id"]
    cis_instance_name = config.ENV_DATA["cis_instance_name"]
    ids_to_delete = []
    page = 1

    logger.info(f"Setting cis instance to {cis_instance_name}")
    run_ibmcloud_cmd(f"ibmcloud cis instance-set {cis_instance_name}")

    while True:
        out = run_ibmcloud_cmd(
            f"ibmcloud cis dns-records {dns_domain_id} --per-page 1000 --page {page} --output json"
        )
        records = json.loads(out)
        if not records:
            logger.info("Reached end of pagination")
            break

        filter_string = f".{cluster_name}."
        logger.info(f"Searching for records with string: {filter_string}")
        for record in records:
            if filter_string in record["name"]:
                logger.info(f"Found {record['name']}, marking for deletion")
                ids_to_delete.append(record["id"])
        page += 1

    logger.info(f"Records to delete: {ids_to_delete}")
    for record_id in ids_to_delete:
        logger.info(f"Deleting DNS record: {record_id}")
        try:
            run_ibmcloud_cmd(
                f"ibmcloud cis dns-record-delete {dns_domain_id} {record_id}"
            )
        except CommandFailed:
            logger.exception("Failed to delete CIS leftovers")


class IBMCloudObjectStorage:
    """
    IBM Cloud Object Storage (COS) class
    """

    def __init__(self, api_key, service_instance_id, endpoint_url):
        """
        Initialize all necessary parameters

        Args:
            api_key (str): API key for IBM Cloud Object Storage (COS)
            service_instance_id (str): Service instance ID for COS
            endpoint_url (str): COS endpoint URL

        """
        self.cos_api_key_id = api_key
        self.cos_instance_crn = service_instance_id
        self.cos_endpoint = endpoint_url
        # create client
        self.cos_client = ibm_boto3.client(
            "s3",
            ibm_api_key_id=self.cos_api_key_id,
            ibm_service_instance_id=self.cos_instance_crn,
            config=IBMBotocoreConfig(signature_version="oauth"),
            endpoint_url=self.cos_endpoint,
        )

    def get_bucket_objects(self, bucket_name, prefix=None):
        """
        Fetches the objects in a bucket

        Args:
            bucket_name (str): Name of the bucket
            prefix (str): Prefix for the objects to fetch

        Returns:
            list: List of objects in a bucket

        """
        bucket_objects = []
        logger.info(f"Retrieving bucket contents from {bucket_name}")
        try:
            bucket_objects_info = []
            paginator = self.cos_client.get_paginator("list_objects_v2")
            operation_parameters = {"Bucket": bucket_name}
            if prefix:
                operation_parameters["Prefix"] = prefix
            page_iterator = paginator.paginate(**operation_parameters)
            for page in page_iterator:
                if "Contents" in page:
                    bucket_objects_info.extend(page["Contents"])
            for object_info in bucket_objects_info:
                bucket_objects.append(object_info["Key"])
        except ClientError as ce:
            logger.error(f"CLIENT ERROR when fetching objects: {ce}")
        except Exception as e:
            logger.error(f"Unable to retrieve bucket contents: {e}")
        logger.debug(f"bucket objects: {bucket_objects}")
        return bucket_objects

    def delete_objects(self, bucket_name):
        """
        Delete objects in a bucket

        Args:
            bucket_name (str): Name of the bucket

        """
        MAX_DELETE_OBJECTS = 1000
        objects = self.get_bucket_objects(bucket_name)
        if objects:
            try:
                total_objects = len(objects)
                logger.info(
                    f"Attempting to delete {total_objects} objects from {bucket_name}"
                )
                for i in range(0, total_objects, MAX_DELETE_OBJECTS):
                    batch = objects[i : i + MAX_DELETE_OBJECTS]
                    # Form the delete request
                    delete_request = {"Objects": [{"Key": obj} for obj in batch]}
                    response = self.cos_client.delete_objects(
                        Bucket=bucket_name, Delete=delete_request
                    )
                    deleted = response.get("Deleted", [])
                    errors = response.get("Errors", [])
                    logger.info(
                        f"Deleted {len(deleted)} objects in batch {i // MAX_DELETE_OBJECTS + 1}"
                    )
                    if errors:
                        logger.error(
                            f"Errors occurred during delete: {json.dumps(errors, indent=4)}"
                        )
                    logger.debug(json.dumps(deleted, indent=4))
                logger.info(f"Deleted objects for {bucket_name}")
            except ClientError as ce:
                logger.error(f"CLIENT ERROR during deleting objects: {ce}")
            except Exception as e:
                logger.error(f"Unable to delete objects: {e}")

    def delete_bucket(self, bucket_name):
        """
        Delete the bucket

        Args:
            bucket_name (str): Name of the bucket

        """
        logger.info(f"Deleting bucket: {bucket_name}")
        try:
            self.delete_objects(bucket_name=bucket_name)
            self.cos_client.delete_bucket(Bucket=bucket_name)
            logger.info(f"Bucket: {bucket_name} deleted!")
        except ClientError as ce:
            logger.error(f"CLIENT ERROR during deleting bucket {bucket_name}: {ce}")
        except Exception as e:
            logger.error(f"Unable to delete bucket: {e}")

    def get_buckets(self):
        """
        Fetches the buckets

        Returns:
            list: List of buckets

        """
        bucket_list = []
        logger.info("Retrieving list of buckets")
        try:
            buckets = self.cos_client.list_buckets()
            for bucket in buckets["Buckets"]:
                bucket_list.append(bucket["Name"])
        except ClientError as ce:
            logger.error(f"CLIENT ERROR: {ce}")
        except Exception as e:
            logger.error(f"Unable to retrieve list buckets: {e}")
        return bucket_list


def get_bucket_regions_map():
    """
    Fetches the buckets and their regions

    Returns:
        dict: Dictionary with bucket name as Key and region as value

    """
    bucket_region_map = {}
    api_key = config.AUTH["ibmcloud"]["api_key"]
    service_instance_id = config.AUTH["ibmcloud"]["cos_instance_crn"]
    endpoint_url = constants.IBM_COS_GEO_ENDPOINT_TEMPLATE.format("us-east")
    cos_client = IBMCloudObjectStorage(
        api_key=api_key,
        service_instance_id=service_instance_id,
        endpoint_url=endpoint_url,
    )

    # Fetch the buckets. It will list from all the regions
    buckets = cos_client.get_buckets()
    for region in constants.IBM_REGIONS:
        if not buckets:
            break
        logger.info(f"Initializing COS client for region {region}")
        endpoint_url = constants.IBM_COS_GEO_ENDPOINT_TEMPLATE.format(region)
        cos_client_region = ibm_boto3.client(
            "s3",
            ibm_api_key_id=api_key,
            ibm_service_instance_id=service_instance_id,
            config=IBMBotocoreConfig(signature_version="oauth"),
            endpoint_url=endpoint_url,
        )
        processed_buckets = []
        for each_bucket in buckets:
            logger.debug(f"Fetching bucket location for {each_bucket}")
            try:
                bucket_location = cos_client_region.get_bucket_location(
                    Bucket=each_bucket
                )
                bucket_region = bucket_location.get("LocationConstraint").split(
                    "-standard"
                )[0]
                bucket_region_map[each_bucket] = bucket_region
                processed_buckets.append(each_bucket)
            except Exception as e:
                logger.warning(
                    f"[Expected] Failed to get region for {each_bucket} in {region}: {e}"
                )

        # remove processed buckets from buckets list
        for bucket_name in processed_buckets:
            buckets.remove(bucket_name)

    return bucket_region_map


def get_worker_floating_ips():
    """
    Retrieve a mapping of worker node names to their associated floating IPs.

    Returns:
        dict: A dictionary mapping worker node names to their floating IP addresses.
    """
    logger.info("Fetching all VSIs...")
    try:
        instances_output = run_ibmcloud_cmd("ibmcloud is instances --output json")
        instances = json.loads(instances_output)
    except Exception as e:
        logger.error(f"Failed to retrieve instances: {e}")
        return {}

    logger.info("Fetching all floating IPs...")
    try:
        fips_output = run_ibmcloud_cmd("ibmcloud is floating-ips --output json")
        floating_ips = json.loads(fips_output)
    except Exception as e:
        logger.error(f"Failed to retrieve floating IPs: {e}")
        return {}

    # Filter instances with 'worker' in their name
    worker_instances = [
        inst for inst in instances if re.search("worker", inst["name"], re.IGNORECASE)
    ]
    if not worker_instances:
        logger.warning("No worker instances found.")
        return {}

    # Map instance IDs to names for quick lookup
    instance_id_to_name = {inst["id"]: inst["name"] for inst in worker_instances}

    # Build mapping of instance ID to floating IP address
    instance_id_to_fip = {}
    for fip in floating_ips:
        target = fip.get("target")
        if target and target.get("resource_id") in instance_id_to_name:
            instance_id = target["resource_id"]
            instance_id_to_fip[instance_id] = fip["address"]

    # Build final mapping of instance name to floating IP address
    fip_mapping = {}
    for instance_id, name in instance_id_to_name.items():
        fip_address = instance_id_to_fip.get(instance_id)
        if fip_address:
            fip_mapping[name] = fip_address
            logger.info(f"Worker '{name}' has floating IP: {fip_address}")
        else:
            logger.warning(f"No floating IP found for worker '{name}'.")

    return fip_mapping


def assign_floating_ips_to_workers():
    """
    Assigns floating IPs to all worker instances that do not already have one.

    Returns:
        dict: Mapping of worker VSI names to their assigned floating IPs.
    """
    logger.info("Assigning floating IPs to worker instances...")

    try:
        instances_output = run_ibmcloud_cmd("ibmcloud is instances --output json")
        instances = json.loads(instances_output)
    except Exception as e:
        logger.error(f"Failed to retrieve instances: {e}")
        raise FloatingIPAssignException(
            "Failed to retrieve instances from IBM Cloud VPC"
        ) from e

    # Filter for worker nodes
    workers = [
        inst for inst in instances if re.search("worker", inst["name"], re.IGNORECASE)
    ]
    if not workers:
        logger.warning("No worker instances found.")
        return {}

    fip_mapping = {}

    for inst in workers:
        name = inst["name"]
        instance_id = inst["id"]
        logger.info(f"Processing worker: {name} ({instance_id})")

        try:
            # Get NICs
            nics_output = run_ibmcloud_cmd(
                f"ibmcloud is instance-network-interfaces {instance_id} --output json"
            )
            nics = json.loads(nics_output)
            if not nics:
                logger.warning(f"No NICs found for instance: {name}")
                continue
            nic = nics[0]
            nic_name = nic["name"]
        except Exception as e:
            logger.error(f"Failed to retrieve NICs for {name}: {e}")
            continue

        # Construct Floating IP name
        fip_name = f"{name}-fip"
        try:
            logger.info(f"Reserving Floating IP '{fip_name}' on NIC '{nic_name}'...")
            run_ibmcloud_cmd(
                f"ibmcloud is floating-ip-reserve {fip_name} --nic {nic_name} --in {instance_id}"
            )
        except Exception as e:
            logger.warning(
                f"Failed to reserve floating IP for {name}, might already exist: {e}"
            )

        # Fetch the floating IP
        try:
            fips_output = run_ibmcloud_cmd("ibmcloud is floating-ips --output json")
            fips = json.loads(fips_output)
            fip = next((f for f in fips if f["name"] == fip_name), None)
            if fip:
                fip_mapping[name] = fip["address"]
                logger.debug(f"Floating IP assigned to {name}: {fip['address']}")
            else:
                logger.warning(f"Floating IP object not found for {fip_name}")
        except Exception as e:
            logger.error(
                f"Failed to fetch floating IPs after assignment for {name}: {e}"
            )

    return fip_mapping
