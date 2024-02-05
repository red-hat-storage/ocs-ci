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
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    UnsupportedPlatformVersionError,
    UnexpectedBehaviour,
    NodeHasNoAttachedVolume,
    TimeoutExpiredError,
)
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility import version as util_version
from ocs_ci.utility.utils import get_ocp_version, run_cmd, TimeoutSampler
from ocs_ci.ocs.node import get_nodes


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
    api_endpoint = ibm_config.get("api_endpoint")
    if api_endpoint:
        login_cmd += f" -a {api_endpoint}"
    region = config.ENV_DATA.get("region")
    if region:
        login_cmd += f" -r {region}"
    logger.info("Logging to IBM cloud")
    run_cmd(login_cmd, secrets=[api_key])
    logger.info("Successfully logged in to IBM cloud")
    config.RUN["ibmcloud_last_login"] = time.time()


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
        if "Please login" in ex.message:
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


class IBMCloudIPI(object):
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
                )
                sample.wait_for_func_status(result=constants.STATUS_RUNNING.lower())

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
            force (bool): True for VM ungraceful power off, False for
                graceful VM shutdown
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
                )
                sample.wait_for_func_status(result=constants.STATUS_STOPPED)

    def restart_nodes_by_stop_and_start(
        self, nodes, wait=True, force=True, timeout=300
    ):
        """
        Restart nodes by stopping and starting VM in IBM Cloud
        Args:
            nodes (list): The OCS objects of the nodes
            wait (bool): True in case wait for status is needed,
                False otherwise
            force (bool): True for force instance stop, False otherwise
            timeout (int): Timeout for the command, defaults to 300 seconds.
        """
        logger.info(f"Stopping instances {list(nodes)}")
        self.stop_nodes(nodes=nodes, force=force)
        if wait:
            for node in nodes:
                sample = TimeoutSampler(
                    timeout=timeout,
                    sleep=10,
                    func=self.check_node_status,
                    node_name=node.name,
                )
                sample.wait_for_func_status(result=constants.STATUS_STOPPED)
        logger.info(f"Starting instances {list(nodes)}")

        self.start_nodes(nodes=nodes)
        if wait:
            for node in nodes:
                sample = TimeoutSampler(
                    timeout=timeout,
                    sleep=10,
                    func=self.check_node_status,
                    node_name=node.name,
                )
                sample.wait_for_func_status(result=constants.STATUS_RUNNING.lower())

    def check_node_status(self, node_name):
        """
        Check the node status in IBM cloud
        Args:
            node_name (str): Node name
        Returns:
            str: Status of node
        """
        try:
            cmd = f"ibmcloud is instance {node_name} --output json"

            out = run_ibmcloud_cmd(cmd)
            out = json.loads(out)
            return out["status"]
        except CommandFailed as cf:
            if "Instance not found" in str(cf):
                return True
        return False

    def restart_nodes_by_stop_and_start_force(self):
        """
        Make sure all nodes are up by the end of the test on IBM Cloud.
        """
        resource_name = None
        stop_node_list = []
        stopping_node_list = []
        cmd = "ibmcloud is ins --all-resource-groups --output json"
        out = run_ibmcloud_cmd(cmd)
        all_resource_grp = json.loads(out)
        cluster_name = config.ENV_DATA["cluster_name"]
        for resource_name in all_resource_grp:
            if cluster_name in resource_name["resource_group"]["name"]:
                resource_name = resource_name["resource_group"]["name"]
                break
        assert resource_name, "Resource Not found"
        cmd = f"ibmcloud is ins --resource-group-name {resource_name} --output json"
        out = run_ibmcloud_cmd(cmd)
        all_instance_output = json.loads(out)
        for instance_name in all_instance_output:
            if instance_name["status"] == constants.STATUS_STOPPED:
                node_obj = OCP(kind="Node", resource_name=instance_name["name"]).get()
                node_obj_ocs = OCS(**node_obj)
                stop_node_list.append(node_obj_ocs)
            if instance_name["status"] == constants.STATUS_STOPPED:
                node_obj = OCP(kind="Node", resource_name=instance_name["name"]).get()
                node_obj_ocs = OCS(**node_obj)
                stopping_node_list.append(node_obj_ocs)
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
            wait (bool): True in case wait for status is needed,
                False otherwise
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
            node (OCS): worker node id to detach.
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
            node (OCS): worker node id to attach.
        """
        logger.info(
            f"attach_volumes:{node[0].get()['metadata']['labels']['failure-domain.beta.kubernetes.io/zone']}"
        )

        logger.info(f"volume is : {volume}")

        cmd = f"ibmcloud is volume {volume} --output json"
        out = run_ibmcloud_cmd(cmd)
        out = json.loads(out)

        if len(out["volume_attachments"]) == 0:

            logger.info(f"attachment command output: {out}")
            cmd = f"ibmcloud is instance-volume-attachment-add data-vol-name {node[0].name} {volume} --output json"
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
