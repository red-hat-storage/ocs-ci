# -*- coding: utf8 -*-
"""
Module for interactions with IBM Cloud Cluster.

"""


import json
import logging
import os
import time

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import (
    UnsupportedPlatformVersionError,
    UnexpectedBehaviour,
    NodeHasNoAttachedVolume,
)
from ocs_ci.utility.utils import get_ocp_version, run_cmd, TimeoutSampler


logger = logging.getLogger(name=__file__)
ibm_config = config.AUTH.get("ibmcloud", {})


def login():
    """
    Login to IBM Cloud cluster
    """
    api_key = ibm_config["api_key"]
    login_cmd = f"ibmcloud login --apikey {api_key}"
    account_id = ibm_config.get("account_id")
    if account_id:
        login_cmd += f" -c {account_id}"
    region = config.ENV_DATA.get("region")
    if region:
        login_cmd += f" -r {region}"
    logger.info("Logging to IBM cloud")
    run_cmd(login_cmd, secrets=[api_key])
    logger.info("Successfully logged in to IBM cloud")


def get_cluster_details(cluster):
    """
    Returns info about the cluster which is taken from the ibmcloud command.

    Args:
        cluster (str): Cluster name or ID

    """
    out = run_cmd(f"ibmcloud ks cluster get --cluster {cluster} -json")
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
    out = run_cmd(cmd)
    return json.loads(out)


def get_ibmcloud_ocp_version():
    """
    Get OCP version available in IBM Cloud.
    """
    out = run_cmd("ibmcloud ks versions --json")
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
    out = run_cmd(cmd)
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
    output = run_cmd(cmd)
    with open(path, "w+") as fd:
        fd.write(output)


def destroy_cluster(cluster):
    """
    Destroy the cluster on IBM Cloud.

    Args:
        cluster (str): Cluster name or ID.

    """
    cmd = f"ibmcloud ks cluster rm -c {cluster} -f"
    out = run_cmd(cmd)
    logger.info(f"Destroy command output: {out}")


def add_deployment_dependencies():
    """
    Adding dependencies for IBM Cloud deployment
    """
    ocp_version = get_ocp_version()
    cr_base_url = (
        "https://raw.githubusercontent.com/openshift/csi-external-snapshotter/"
        f"release-{ocp_version}/"
    )
    if float(ocp_version) < 4.6:
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
        login()
        provider_id = nodes[0].get()["spec"]["providerID"]
        cluster_id = provider_id.split("/")[5]

        cmd = f"ibmcloud ks workers --cluster {cluster_id} --output json"
        out = run_cmd(cmd)
        worker_nodes = json.loads(out)

        if len(worker_nodes) > 0:
            for node in worker_nodes:
                cmd = f"ibmcloud ks worker reboot --cluster {cluster_id} --worker {node['id']} -f"
                out = run_cmd(cmd)
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
        out = run_cmd(cmd)
        out = json.loads(out)

        if len(out["volume_attachments"]) == 0:
            cmd = (
                f"ibmcloud ks storage attachment  create --cluster {cluster_id} --worker {worker_id}"
                f"  --volume {volume} --output json"
            )
            out = run_cmd(cmd)
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
        provider_id = node[0].get()["spec"]["providerID"]
        cluster_id = provider_id.split("/")[5]
        worker_id = node[0].get()["metadata"]["labels"][
            "ibm-cloud.kubernetes.io/worker-id"
        ]

        logger.info(f"volume is : {volume}")

        cmd = f"ibmcloud is volume {volume} --output json"
        out = run_cmd(cmd)
        out = json.loads(out)

        if out["status"] == "available":
            attachment_id = out["volume_attachments"][0]["id"]

            cmd = (
                f"ibmcloud ks storage attachment  rm --cluster {cluster_id} --worker {worker_id}"
                f"  --attachment {attachment_id}"
            )
            out = run_cmd(cmd)
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
        out = run_cmd(cmd)
        out = json.loads(out)

        if not out["volume_attachments"]:
            logger.info("volume is not attached to node")
            raise NodeHasNoAttachedVolume("volume not attached to node")
        else:
            worker_id = out["volume_attachments"][0]["instance"]["name"]
            logger.info(f"volume is  attached to node: {worker_id}")
            return worker_id

    def get_data_volumes(self):
        """
        Returns volumes in IBM Cloud.

        Returns:
            list: volumes in IBM Cloud.

        """
        logger.info("get data volumes")

        cmd = "ibmcloud is vols --output json"
        out = run_cmd(cmd)
        out = json.loads(out)

        vol_ids = []
        for vols in out:
            vol_ids.append(vols["id"])

        logger.info(f"volume ids are : {vol_ids}")
        return vol_ids

    def wait_for_volume_attach(self, volume):
        """
        Checks volume is attached to node or not

        Args:
            volume (str): The volume to wait for to be attached

        Returns:
            bool: True if the volume has been attached to the
                instance, False otherwise

        """
        cmd = f"ibmcloud is volume {volume} --output json"
        out = run_cmd(cmd)
        out = json.loads(out)

        if not out["volume_attachments"]:
            logger.info("volume is not attached to node")
            return False
        else:
            logger.info("volume is attached to node")
            return True

    def get_volume_id(self):
        """
        Returns Volumeid with the name taken from constants

        Returns:
            str: volume id if the volume exists otherwise create
                new volume

        """
        zone = config.ENV_DATA["zone"]

        cmd = "ibmcloud is vols --output json "
        out = run_cmd(cmd)
        out = json.loads(out)

        vol_id = ""
        for vols in out:
            vol_name = str(vols["name"]).strip()
            if vol_name == constants.IBMCLOUD_VOLUME_NAME:
                vol_id = vols["id"]
                return vol_id

        cmd = f"ibmcloud is volume-create {constants.IBMCLOUD_VOLUME_NAME} general-purpose {zone} --output json"
        out = run_cmd(cmd)
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
        login()
        provider_id = nodes[0].get()["spec"]["providerID"]
        cluster_id = provider_id.split("/")[5]

        cmd = f"ibmcloud ks workers --cluster {cluster_id} --output json"
        out = run_cmd(cmd)
        worker_nodes = json.loads(out)

        worker_nodes_not_ready = []
        for worker_node in worker_nodes:
            if worker_node["health"]["message"] != "Ready":
                worker_nodes_not_ready.append(worker_node["id"])

        if len(worker_nodes_not_ready) > 0:
            for not_ready_node in worker_nodes_not_ready:
                cmd = f"ibmcloud ks worker reboot --cluster {cluster_id} --worker {not_ready_node} -f"
                out = run_cmd(cmd)
                logger.info(f"Node restart command output: {out}")

    def delete_volume_id(self, volume):
        """
        Deletes Volumeid

        Args:
            volume (str): The volume to be deleted

        """
        cmd = f"ibmcloud is volume-delete {volume} --output json"
        out = run_cmd(cmd)
        out = json.loads(out)
        logger.info("Sleeping for 60 seconds to delete the volume")
        time.sleep(60)

        if out[0]["result"]:
            logger.info(f"volume is deleted successfully: {volume}")
        else:
            logger.info("volume is not deleted")
