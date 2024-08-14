import json
import logging
import os
import re
import shutil
import time


import boto3
from botocore.exceptions import WaiterError
import yaml
import ovirtsdk4.types as types

from ocs_ci.deployment.terraform import Terraform
from ocs_ci.deployment.vmware import (
    clone_openshift_installer,
    comment_sensitive_var,
    get_ignition_provider_version,
    update_machine_conf,
)
from ocs_ci.ocs.exceptions import (
    TimeoutExpiredError,
    UnknownOperationForTerraformVariableUpdate,
    NotAllNodesCreated,
    RebootEventNotFoundException,
    ResourceWrongStatusException,
    VolumePathNotFoundException,
)
from ocs_ci.framework import config, merge_dict
from ocs_ci.utility import templating
from ocs_ci.utility.csr import approve_pending_csr
from ocs_ci.utility.load_balancer import LoadBalancer
from ocs_ci.utility.mirror_openshift import prepare_mirror_openshift_credential_files
from ocs_ci.utility.retry import retry
from ocs_ci.ocs import constants, ocp, exceptions, cluster
from ocs_ci.ocs.node import (
    get_node_objs,
    get_typed_worker_nodes,
    get_nodes,
)
from ocs_ci.ocs.resources.pvc import get_deviceset_pvs
from ocs_ci.ocs.resources import pod
from ocs_ci.utility import version as version_module
from ocs_ci.utility.csr import (
    get_nodes_csr,
    wait_for_all_nodes_csr_and_approve,
)
from ocs_ci.utility.utils import (
    get_cluster_name,
    get_infra_id,
    get_ocp_repo,
    create_rhelpod,
    replace_content_in_file,
    get_ocp_version,
    TimeoutSampler,
    delete_file,
    AZInfo,
    download_file_from_git_repo,
    set_aws_region,
    run_cmd,
    get_module_ip,
    get_terraform_ignition_provider,
)
from ocs_ci.ocs.node import (
    wait_for_nodes_status,
    get_nodes_in_statuses,
)
from ocs_ci.utility.vsphere_nodes import VSPHERENode
from paramiko.ssh_exception import NoValidConnectionsError, AuthenticationException
from semantic_version import Version
from ovirtsdk4.types import VmStatus
from ocs_ci.utility.ibmcloud import run_ibmcloud_cmd

logger = logging.getLogger(__name__)


class PlatformNodesFactory:
    """
    A factory class to get specific nodes platform object

    """

    def __init__(self):
        self.cls_map = {
            "AWS": AWSNodes,
            "vsphere": VMWareNodes,
            "aws": AWSNodes,
            "baremetal": BaremetalNodes,
            "azure": AZURENodes,
            "gcp": GCPNodes,
            "vsphere_lso": VMWareLSONodes,
            "powervs": IBMPowerNodes,
            "rhv": RHVNodes,
            "ibm_cloud": IBMCloud,
            "vsphere_ipi": VMWareIPINodes,
            "rosa": AWSNodes,
            "vsphere_upi": VMWareUPINodes,
            "fusion_aas": AWSNodes,
            "hci_baremetal": IBMCloudBMNodes,
        }

    def get_nodes_platform(self):
        platform = config.ENV_DATA["platform"]
        if platform == constants.VSPHERE_PLATFORM:
            deployment_type = config.ENV_DATA["deployment_type"]
            if cluster.is_lso_cluster():
                platform += "_lso"
            elif deployment_type in ("ipi", "upi"):
                platform += f"_{deployment_type}"

        return self.cls_map[platform]()


class NodesBase(object):
    """
    A base class for nodes related operations.
    Should be inherited by specific platform classes

    """

    def __init__(self):
        self.cluster_path = config.ENV_DATA["cluster_path"]
        self.platform = config.ENV_DATA["platform"]
        self.deployment_type = config.ENV_DATA["deployment_type"]
        self.nodes_map = {"AWSUPINode": AWSUPINode, "VSPHEREUPINode": VSPHEREUPINode}
        self.wait_time = 120

    def get_data_volumes(self):
        raise NotImplementedError("Get data volume functionality is not implemented")

    def get_node_by_attached_volume(self, volume):
        raise NotImplementedError(
            "Get node by attached volume functionality is not implemented"
        )

    def stop_nodes(self, nodes):
        raise NotImplementedError("Stop nodes functionality is not implemented")

    def start_nodes(self, nodes):
        raise NotImplementedError("Start nodes functionality is not implemented")

    def restart_nodes(self, nodes, wait=True):
        raise NotImplementedError("Restart nodes functionality is not implemented")

    def restart_nodes_by_stop_and_start(self, nodes, force=True):
        raise NotImplementedError(
            "Restart nodes by stop and start functionality is not implemented"
        )

    def detach_volume(self, volume, node=None, delete_from_backend=True):
        raise NotImplementedError("Detach volume functionality is not implemented")

    def attach_volume(self, volume, node):
        raise NotImplementedError("Attach volume functionality is not implemented")

    def wait_for_volume_attach(self, volume):
        raise NotImplementedError(
            "Wait for volume attach functionality is not implemented"
        )

    def restart_nodes_by_stop_and_start_teardown(self):
        raise NotImplementedError(
            "Restart nodes by stop and start teardown functionality is "
            "not implemented"
        )

    def create_and_attach_nodes_to_cluster(self, node_conf, node_type, num_nodes):
        """
        Create nodes and attach them to cluster
        Use this function if you want to do both creation/attachment in
        a single call

        Args:
            node_conf (dict): of node configuration
            node_type (str): type of node to be created RHCOS/RHEL
            num_nodes (int): Number of node instances to be created

        """
        node_list = self.create_nodes(node_conf, node_type, num_nodes)
        self.attach_nodes_to_cluster(node_list)

    def create_nodes(self, node_conf, node_type, num_nodes):
        raise NotImplementedError("Create nodes functionality not implemented")

    def attach_nodes_to_cluster(self, node_list):
        raise NotImplementedError(
            "attach nodes to cluster functionality is not implemented"
        )

    def read_default_config(self, default_config_path):
        """
        Commonly used function to read default config

        Args:
            default_config_path (str): Path to default config file

        Returns:
            dict: of default config loaded

        """
        assert os.path.exists(default_config_path), "Config file doesnt exists"

        with open(default_config_path) as f:
            default_config_dict = yaml.safe_load(f)

        return default_config_dict

    def terminate_nodes(self, nodes, wait=True):
        raise NotImplementedError("terminate nodes functionality is not implemented")

    def wait_for_nodes_to_stop(self, nodes):
        raise NotImplementedError(
            "wait for nodes to stop functionality is not implemented"
        )

    def wait_for_nodes_to_terminate(self, nodes):
        raise NotImplementedError(
            "wait for nodes to terminate functionality is not implemented"
        )

    def wait_for_nodes_to_stop_or_terminate(self, nodes):
        raise NotImplementedError(
            "wait for nodes to stop or terminate functionality is not implemented"
        )


class VMWareNodes(NodesBase):
    """
    VMWare nodes class

    """

    def __init__(self):
        super(VMWareNodes, self).__init__()
        from ocs_ci.utility import vsphere

        self.cluster_name = config.ENV_DATA.get("cluster_name")
        self.server = config.ENV_DATA["vsphere_server"]
        self.user = config.ENV_DATA["vsphere_user"]
        self.password = config.ENV_DATA["vsphere_password"]
        self.cluster = config.ENV_DATA["vsphere_cluster"]
        self.datacenter = config.ENV_DATA["vsphere_datacenter"]
        self.datastore = config.ENV_DATA["vsphere_datastore"]
        self.vsphere = vsphere.VSPHERE(self.server, self.user, self.password)

    def get_vms(self, nodes):
        """
        Get vSphere vm objects list

        Args:
            nodes (list): The OCS objects of the nodes

        Returns:
            list: vSphere vm objects list

        """
        vms_in_pool = self.vsphere.get_all_vms_in_pool(
            self.cluster_name, self.datacenter, self.cluster
        )
        node_names = [node.name for node in nodes]
        vms = []
        for node in node_names:
            node_vms = [vm for vm in vms_in_pool if vm.name in node]
            vms.extend(node_vms)
        return vms

    def get_data_volumes(self, pvs=None):
        """
        Get the data vSphere volumes

        Args:
            pvs (list): PV OCS objects

        Returns:
            list: vSphere volumes

        """
        if not pvs:
            pvs = get_deviceset_pvs()
        return [pv.get().get("spec").get("csi").get("volumeHandle") for pv in pvs]

    def get_node_by_attached_volume(self, volume):
        """
        Get node OCS object of the VM instance that has the volume attached to

        Args:
            volume (Volume): The volume to get the VM according to

        Returns:
            OCS: The OCS object of the VM instance

        """
        volume_kube_path = f"kubernetes.io/vsphere-volume/{volume}"
        all_nodes = get_node_objs()
        for node in all_nodes:
            for volume in node.data["status"]["volumesAttached"]:
                if volume_kube_path in volume.values():
                    return node

    def stop_nodes(self, nodes, force=True, wait=True):
        """
        Stop vSphere VMs

        Args:
            nodes (list): The OCS objects of the nodes
            force (bool): True for force VM stop, False otherwise
            wait (bool): Wait for the VMs to stop

        """
        vms = self.get_vms(nodes)
        assert vms, f"Failed to get VM objects for nodes {[n.name for n in nodes]}"
        self.vsphere.stop_vms(vms, force=force, wait=wait)

    def start_nodes(self, nodes, wait=True):
        """
        Start vSphere VMs

        Args:
            nodes (list): The OCS objects of the nodes
            wait (bool): Wait for the VMs to start

        """
        vms = self.get_vms(nodes)
        assert vms, f"Failed to get VM objects for nodes {[n.name for n in nodes]}"
        self.vsphere.start_vms(vms, wait=wait)

    def restart_nodes(self, nodes, force=True, timeout=300, wait=True):
        """
        Restart vSphere VMs

        Args:
            nodes (list): The OCS objects of the nodes
            force (bool): True for Hard reboot, False for Soft reboot
            timeout (int): time in seconds to wait for node to reach READY state
            wait (bool): True if need to wait till the restarted OCP node
                reaches READY state. False otherwise

        """
        vms = self.get_vms(nodes)
        assert vms, f"Failed to get VM objects for nodes {[n.name for n in nodes]}"

        num_events_pre_reboot = self.get_reboot_events(nodes)

        self.vsphere.restart_vms(vms, force=force)

        if wait:
            """
            When reboot is initiated on a VM from the VMware, the VM
            stays at "Running" state throughout the reboot operation.

            When the reboot operation is completed and the VM is reachable the
            OCP node reaches status Ready and a Reboot event is logged.
            """
            nodes_names = [n.name for n in nodes]

            wait_for_nodes_status(
                node_names=nodes_names, status=constants.NODE_READY, timeout=timeout
            )
            for node in nodes:
                reboot_events_cmd = (
                    "get events -A --field-selector involvedObject.name="
                    f"{node.name},reason=Rebooted -o yaml"
                )
                try:
                    for node_reboot_events in TimeoutSampler(
                        timeout=300, sleep=3, func=self.get_reboot_events, nodes=[node]
                    ):
                        if (
                            node_reboot_events[node.name]
                            != num_events_pre_reboot[node.name]
                        ):
                            break
                except TimeoutExpiredError:
                    logger.error(
                        f"{node.name}: reboot events before reboot are {num_events_pre_reboot[node.name]} and "
                        f"reboot events after reboot are {node_reboot_events[node.name]}"
                    )
                    raise RebootEventNotFoundException

                assert num_events_pre_reboot[node.name] < len(
                    node.ocp.exec_oc_cmd(reboot_events_cmd)["items"]
                ), f"Reboot event not found on node {node.name}"
                logger.info(f"Node {node.name} rebooted")

    def get_reboot_events(self, nodes):
        """
        Gets the number of reboot events

        Args:
            nodes (list): The OCS objects of the nodes

        Returns:
            dict: Dictionary which contains node names as key and value as number
                of reboot events
                e.g: {'compute-0': 11, 'compute-1': 9, 'compute-2': 9}

        """
        num_reboot_events = {}
        for node in nodes:
            reboot_events_cmd = (
                "get events -A --field-selector involvedObject.name="
                f"{node.name},reason=Rebooted -o yaml"
            )
            num_reboot_events[node.name] = len(
                node.ocp.exec_oc_cmd(reboot_events_cmd)["items"]
            )
        return num_reboot_events

    def restart_nodes_by_stop_and_start(self, nodes, force=True):
        """
        Restart vSphere VMs

        Args:
            nodes (list): The OCS objects of the nodes
            force (bool): True for force VM stop, False otherwise

        """
        vms = self.get_vms(nodes)
        assert vms, f"Failed to get VM objects for nodes {[n.name for n in nodes]}"
        self.vsphere.restart_vms_by_stop_and_start(vms, force=force)

    def detach_volume(self, volume, node=None, delete_from_backend=True):
        """
        Detach disk from a VM and delete from datastore if specified

        Args:
            volume (str): Volume path
            node (OCS): The OCS object representing the node
            delete_from_backend (bool): True for deleting the disk (vmdk)
                from backend datastore, False otherwise

        """
        vm = self.get_vms([node])[0]
        self.vsphere.remove_disk(
            vm=vm, identifier=volume, key="volume_path", datastore=delete_from_backend
        )

    def create_and_attach_volume(self, node, size):
        """
        Create a new volume and attach it to the given VM

        Args:
            node (OCS): The OCS object representing the node
            size (int): The size in GB for the new volume

        """
        vm = self.get_vms([node])[0]
        self.vsphere.add_disk(vm, size)

    def attach_volume(self, node, volume):
        raise NotImplementedError(
            "Attach volume functionality is not implemented for VMWare"
        )

    def wait_for_volume_attach(self, volume):
        logger.info("Not waiting for volume to get re-attached")
        pass

    def restart_nodes_by_stop_and_start_teardown(self):
        """
        Make sure all VMs are up by the end of the test

        """
        self.cluster_nodes = get_node_objs()
        vms = self.get_vms(self.cluster_nodes)
        assert (
            vms
        ), f"Failed to get VM objects for nodes {[n.name for n in self.cluster_nodes]}"
        stopped_vms = [
            vm
            for vm in vms
            if self.vsphere.get_vm_power_status(vm) == constants.VM_POWERED_OFF
        ]
        # Start the VMs
        if stopped_vms:
            logger.info(
                f"The following VMs are powered off: {[vm.name for vm in stopped_vms]}"
            )
            self.vsphere.start_vms(stopped_vms)

    def create_and_attach_nodes_to_cluster(self, node_conf, node_type, num_nodes):
        """
        Create nodes and attach them to cluster
        Use this function if you want to do both creation/attachment in
        a single call

        Args:
            node_conf (dict): of node configuration
            node_type (str): type of node to be created RHCOS/RHEL
            num_nodes (int): Number of node instances to be created

        """
        node_cls = self.nodes_map[
            f"{self.platform.upper()}{self.deployment_type.upper()}Node"
        ]
        node_cls_obj = node_cls(node_conf, node_type, num_nodes)
        node_cls_obj.add_node()

    def terminate_nodes(self, nodes, wait=True):
        """
        Terminate the VMs.
        The VMs will be deleted only from the inventory and not from the disk.

        Args:
            nodes (list): The OCS objects of the nodes
            wait (bool): True for waiting the VMs to terminate,
            False otherwise

        """
        vms = self.get_vms(nodes)
        self.vsphere.remove_vms_from_inventory(vms)
        if wait:
            for vm in vms:
                self.vsphere.wait_for_vm_delete(vm)

    def get_vm_from_ips(self, node_ips, dc):
        """
        Fetches VM objects from given IP's

        Args:
            node_ips (list): List of node IP's
            dc (str): Datacenter name

        Returns:
            list: List of VM objects

        """
        return [self.vsphere.get_vm_by_ip(ip, dc) for ip in node_ips]

    def get_volume_path(self, volume_handle, node_name=None):
        """
        Fetches the volume path for the volumeHandle

        Args:
            volume_handle (str): volumeHandle which exists in PV
            node_name (str): Node name where PV exists.

        Returns:
            str: volume path of PV

        """
        if not node_name:
            return self.vsphere.get_volume_path(
                volume_id=volume_handle,
                datastore_name=self.datastore,
                datacenter_name=self.datacenter,
            )


class AWSNodes(NodesBase):
    """
    AWS EC2 instances class

    """

    def __init__(self):
        super(AWSNodes, self).__init__()
        from ocs_ci.utility import aws as aws_utility

        self.aws_utility = aws_utility
        self.aws = aws_utility.AWS()
        self.az = AZInfo()

    def get_ec2_instances(self, nodes):
        """
        Get the EC2 instances dicts

        Args:
            nodes (list): The OCS objects of the nodes

        Returns:
            dict: The EC2 instances dicts (IDs and names)

        """
        return self.aws_utility.get_instances_ids_and_names(nodes)

    def get_data_volumes(self):
        """
        Get the data EBS volumes

        Returns:
            list: EBS Volume instances

        """
        pvs = get_deviceset_pvs()
        return self.aws_utility.get_data_volumes(pvs)

    def get_node_by_attached_volume(self, volume):
        """
        Get node OCS object of the EC2 instance that has the volume attached to

        Args:
            volume (Volume): The volume to get the EC2 according to

        Returns:
            OCS: The OCS object of the EC2 instance

        """
        instance_ids = [at.get("InstanceId") for at in volume.attachments]
        assert (
            instance_ids
        ), f"EBS Volume {volume.id} is not attached to any EC2 instance"
        instance_id = instance_ids[0]
        all_nodes = get_node_objs()
        nodes = [
            n for n in all_nodes if instance_id in n.get().get("spec").get("providerID")
        ]
        assert nodes, f"Failed to find the OCS object for EC2 instance {instance_id}"
        return nodes[0]

    def stop_nodes(self, nodes, wait=True, force=True):
        """
        Stop EC2 instances

        Args:
            nodes (list): The OCS objects of the nodes
            wait (bool): True for waiting the instances to stop, False otherwise
            force (bool): True for force stopping the instances abruptly, False otherwise

        """

        instances = self.get_ec2_instances(nodes)
        assert (
            instances
        ), f"Failed to get the EC2 instances for nodes {[n.name for n in nodes]}"
        self.aws.stop_ec2_instances(instances=instances, wait=wait, force=force)

    def start_nodes(self, instances=None, nodes=None, wait=True):
        """
        Start EC2 instances

        Args:
            nodes (list): The OCS objects of the nodes
            instances (dict): instance-id and name dict
            wait (bool): True for waiting the instances to start, False otherwise

        """
        instances = instances or self.get_ec2_instances(nodes)
        assert (
            instances
        ), f"Failed to get the EC2 instances for nodes {[n.name for n in nodes]}"
        self.aws.start_ec2_instances(instances=instances, wait=wait)

    def restart_nodes(self, nodes, timeout=300, wait=True):
        """
        Restart EC2 instances

        Args:
            nodes (list): The OCS objects of the nodes
            wait (bool): True if need to wait till the restarted node reaches
                READY state. False otherwise
            timeout (int): time in seconds to wait for node to reach 'not ready' state,
                and 'ready' state.

        """
        num_events_pre_reboot = {}
        instances = self.get_ec2_instances(nodes)
        assert instances, (
            f"Failed to get the EC2 instances for " f"nodes {[n.name for n in nodes]}"
        )

        for node in nodes:
            reboot_events_cmd = (
                "get events -A --field-selector involvedObject.name="
                f"{node.name},reason=Rebooted -o yaml"
            )
            num_events_pre_reboot[node.name] = len(
                node.ocp.exec_oc_cmd(reboot_events_cmd)["items"]
            )

        self.aws.restart_ec2_instances(instances=instances)

        if wait:
            """
            When reboot is initiated on an instance from the AWS, the
            instance stays at "Running" state throughout the reboot operation.

            When the reboot operation is complete and the instance is reachable
            the OCP node reaches status Ready and a Reboot event is logged.
            """
            logger.info("Waiting for 60 seconds for reboot to complete...")
            time.sleep(60)

            nodes_names = [n.name for n in nodes]
            logger.info(f"Waiting for nodes: {nodes_names} to reach ready state")
            wait_for_nodes_status(
                node_names=nodes_names, status=constants.NODE_READY, timeout=timeout
            )
            for node in nodes:
                reboot_events_cmd = (
                    "get events -A --field-selector involvedObject.name="
                    f"{node.name},reason=Rebooted -o yaml"
                )
                assert num_events_pre_reboot[node.name] < len(
                    node.ocp.exec_oc_cmd(reboot_events_cmd)["items"]
                ), f"Reboot event not found on node {node.name}"
                logger.info(f"Node {node.name} rebooted")

    def restart_nodes_by_stop_and_start(self, nodes, wait=True, force=True):
        """
        Restart nodes by stopping and starting EC2 instances

        Args:
            nodes (list): The OCS objects of the nodes
            wait (bool): True in case wait for status is needed,
                False otherwise
            force (bool): True for force instance stop, False otherwise
        Returns:
        """
        instances = self.get_ec2_instances(nodes)
        assert (
            instances
        ), f"Failed to get the EC2 instances for nodes {[n.name for n in nodes]}"
        self.aws.restart_ec2_instances_by_stop_and_start(
            instances=instances, wait=wait, force=force
        )

    def terminate_nodes(self, nodes, wait=True):
        """
        Terminate EC2 instances

        Args:
            nodes (list): The OCS objects of the nodes
            wait (bool): True for waiting the instances to terminate,
            False otherwise

        """
        instances = self.get_ec2_instances(nodes)
        assert instances, (
            f"Failed to terminate the "
            f"EC2 instances for nodes {[n.name for n in nodes]}"
        )
        self.aws.terminate_ec2_instances(instances=instances, wait=wait)

    def detach_volume(self, volume, node=None, delete_from_backend=True):
        """
        Detach a volume from an EC2 instance

        Args:
            volume (Volume): The volume to delete
            node (OCS): The OCS object representing the node
            delete_from_backend (bool): True for deleting the disk from the
                storage backend, False otherwise


        """
        self.aws.detach_volume(volume)

    def attach_volume(self, volume, node):
        """
        Attach a data volume to an instance

        Args:
            volume (Volume): The volume to delete
            node (OCS): The EC2 instance to attach the volume to

        """
        volume.load()
        volume_attachments = [at.get("InstanceId") for at in volume.attachments]
        if not volume_attachments:
            instance = self.get_ec2_instances([node])
            assert instance, f"Failed to get the EC2 instance for nodes {node.name}"
            self.aws.attach_volume(volume, [*instance][0])
        else:
            logger.warning(
                f"Volume {volume.id} is already attached to EC2 "
                f"instance/s {volume_attachments}"
            )

    def wait_for_volume_attach(self, volume):
        """
        Wait for an EBS volume to be attached to an EC2 instance.
        This is used as when detaching the EBS volume from the EC2 instance,
        re-attach should take place automatically

        Args:
            volume (Volume): The volume to wait for to be attached

        Returns:
            bool: True if the volume has been attached to the
                instance, False otherwise

        """

        def get_volume_attachments(ebs_volume):
            ebs_volume.reload()
            return ebs_volume.attachments

        try:

            for sample in TimeoutSampler(300, 3, get_volume_attachments, volume):
                logger.info(f"EBS volume {volume.id} attachments are: {sample}")
                if sample:
                    return True
        except TimeoutExpiredError:
            logger.error(f"Volume {volume.id} failed to be attached to an EC2 instance")
            return False

    def restart_nodes_by_stop_and_start_teardown(self):
        """
        Make sure all EC2 instances are up. To be used in the test teardown

        """
        # Get the cluster nodes ec2 instances
        self.cluster_nodes = get_node_objs()
        ec2_instances = self.get_ec2_instances(self.cluster_nodes)
        assert (
            ec2_instances
        ), f"Failed to get ec2 instances for nodes {[n.name for n in self.cluster_nodes]}"

        logger.info(
            "Getting the instances that are in status 'stopping' (if there are any), "
            "and wait for them to get to status 'stopped', "
            "so it will be possible to start them"
        )
        stopping_instances = {
            key: val
            for key, val in ec2_instances.items()
            if self.aws.get_instances_status_by_id(key) == constants.INSTANCE_STOPPING
        }

        logger.info(
            "Waiting fot the instances that are in status 'stopping' "
            "(if there are any) to reach 'stopped'"
        )
        if stopping_instances:
            for stopping_instance in stopping_instances:
                instance = self.aws.get_ec2_instance(stopping_instance.key())
                instance.wait_until_stopped()
        stopped_instances = {
            key: val
            for key, val in ec2_instances.items()
            if self.aws.get_instances_status_by_id(key) == constants.INSTANCE_STOPPED
        }

        # Start the instances
        if stopped_instances:
            self.aws.start_ec2_instances(instances=stopped_instances, wait=True)

    def create_nodes(self, node_conf, node_type, num_nodes):
        """
        create aws instances of nodes

        Args:
            node_conf (dict): of node configuration
            node_type (str): type of node to be created RHCOS/RHEL
            num_nodes (int): Number of node instances to be created

        Returns:
           list: of AWSUPINode objects

        """
        node_list = []
        node_cls = self.nodes_map[
            f"{self.platform.upper()}{self.deployment_type.upper()}Node"
        ]

        if node_type.upper() == "RHCOS":
            workers_stacks = self.aws.get_worker_stacks()
            logger.info(f"Existing worker stacks: {workers_stacks}")
            existing_indexes = self.get_existing_indexes(workers_stacks)
            logger.info(f"Existing indexes: {existing_indexes}")
            slots_available = self.get_available_slots(existing_indexes, num_nodes)
            logger.info(f"Available indexes: {slots_available}")
            for slot in slots_available:
                node_conf["zone"] = self.az.get_zone_number()
                node_id = slot
                node_list.append(node_cls(node_conf, node_type))
                node_list[-1]._prepare_node(node_id)
        elif node_type.upper() == "RHEL":
            rhel_workers = len(get_typed_worker_nodes("rhel"))
            for i in range(num_nodes):
                node_conf["zone"] = self.az.get_zone_number()
                node_id = i + rhel_workers
                node_list.append(node_cls(node_conf, node_type))
                node_list[-1]._prepare_node(node_id)

        # Make sure that csr is approved for all the nodes
        # not making use of csr.py functions as aws rhcos has long
        # delays for csr to appear
        if node_type.upper() == "RHCOS":
            self.approve_all_nodes_csr(node_list)

        return node_list

    @retry(
        (exceptions.PendingCSRException, exceptions.TimeoutExpiredError),
        tries=4,
        delay=10,
        backoff=1,
    )
    def approve_all_nodes_csr(self, node_list):
        """
        Make sure that all the newly added nodes are in approved csr state

        Args:
            node_list (list): of AWSUPINode/AWSIPINode objects

        Raises:
             PendingCSRException: If any pending csrs exists

        """
        node_names = [node.aws_instance_obj.private_dns_name for node in node_list]

        sample = TimeoutSampler(
            timeout=600, sleep=3, func=self.all_nodes_found, node_names=node_names
        )
        if not sample.wait_for_func_status(result=True):
            raise exceptions.PendingCSRException("All nodes csr not approved")

    def all_nodes_found(self, node_names):
        """
        Relying on oc get nodes -o wide to confirm that
        node is added to cluster

        Args:
            node_names (list): of node names as string

        Returns:
            bool: 'True' if all the node names appeared in 'get nodes'
            else 'False'

        """
        approve_pending_csr()
        get_nodes_cmd = "get nodes -o wide"
        oc_obj = ocp.OCP()
        nodes_wide_out = oc_obj.exec_oc_cmd(get_nodes_cmd, out_yaml_format=False)
        for line in nodes_wide_out.splitlines():
            for node in node_names:
                if node in line:
                    node_names.remove(node)
                    break
        if node_names:
            logger.warning("Some of the nodes have not appeared in nodes list")
        return not node_names

    def get_available_slots(self, existing_indexes, required_slots):
        """
        Get indexes which are free

        Args:
            existing_indexes (list): of integers
            required_slots (int): required number of integers

        Returns:
            list: of integers (available slots)

        """
        slots_available = []
        count = 0
        index = 0

        while count < required_slots:
            if index not in existing_indexes:
                slots_available.append(index)
                count = count + 1
            index = index + 1
        return slots_available

    def get_existing_indexes(self, index_list):
        """
        Extract index suffixes from index_list

        Args:
            index_list (list): of stack names in the form of
                'clustername-no$i'

        Returns:
            list: sorted list of Integers

        """
        temp = []
        for index in index_list:
            temp.append(int(re.findall(r"\d+", index.split("-")[-1])[-1]))
        temp.sort()
        return temp

    def attach_nodes_to_cluster(self, node_list):
        """
        Attach nodes in the list to the cluster

        Args:
            node_list (list): of AWSUPINode/AWSIPINode objects

        """
        if self.deployment_type.lower() == "upi":
            self.attach_nodes_to_upi_cluster(node_list)

    def attach_nodes_to_upi_cluster(self, node_list):
        """
        Attach node to upi cluster
        Note: For RHCOS nodes, create function itself would have
        attached the nodes to cluster so nothing to do here

        Args:
            node_list (list): of AWSUPINode objects

        """
        if node_list[0].node_type == "RHEL":
            self.attach_rhel_nodes_to_upi_cluster(node_list)

    def attach_rhel_nodes_to_upi_cluster(self, node_list):
        """
        Attach RHEL nodes to upi cluster

        Args:
            node_list (list): of AWSUPINode objects with RHEL os

        """
        rhel_pod_name = "rhel-ansible"
        # TODO: This method is creating only RHEL 7 pod. Once we would like to use
        # different version of RHEL for running openshift ansible playbook, we need
        # to update this method!
        rhel_pod_obj = create_rhelpod(
            constants.DEFAULT_NAMESPACE, rhel_pod_name, timeout=600
        )
        timeout = 4000  # For ansible-playbook

        # copy openshift-dev.pem to RHEL ansible pod
        pem_src_path = "~/.ssh/openshift-dev.pem"
        pem_dst_path = "/openshift-dev.pem"
        pod.upload(rhel_pod_obj.name, pem_src_path, pem_dst_path)
        repo_dst_path = constants.YUM_REPOS_PATH
        # Ansible playbook and dependency is described in the documentation to run
        # on RHEL7 node
        # https://docs.openshift.com/container-platform/4.9/machine_management/adding-rhel-compute.html
        repo_rhel_ansible = get_ocp_repo(
            rhel_major_version=config.ENV_DATA["rhel_version_for_ansible"]
        )
        repo = get_ocp_repo()
        diff_rhel = repo != repo_rhel_ansible
        pod.upload(rhel_pod_obj.name, repo_rhel_ansible, repo_dst_path)
        if diff_rhel:
            repo_dst_path = constants.POD_UPLOADPATH
            pod.upload(rhel_pod_obj.name, repo, repo_dst_path)
            repo_file = os.path.basename(repo)
        else:
            repo_file = os.path.basename(repo_rhel_ansible)
        # prepare credential files for mirror.openshift.com
        with prepare_mirror_openshift_credential_files() as (
            mirror_user_file,
            mirror_password_file,
        ):
            pod.upload(rhel_pod_obj.name, mirror_user_file, constants.YUM_VARS_PATH)
            pod.upload(rhel_pod_obj.name, mirror_password_file, constants.YUM_VARS_PATH)
        # Install scp on pod
        rhel_pod_obj.install_packages("openssh-clients")
        # distribute repo file to all RHEL workers
        hosts = [node.aws_instance_obj.private_dns_name for node in node_list]
        # Check whether every host is acceptin ssh connections
        for host in hosts:
            self.check_connection(rhel_pod_obj, host, pem_dst_path)

        for host in hosts:
            disable = "sudo yum-config-manager --disable *"
            rhel_pod_obj.exec_cmd_on_node(
                host, pem_dst_path, disable, user=constants.EC2_USER
            )
            rhel_pod_obj.copy_to_server(
                host,
                pem_dst_path,
                os.path.join(repo_dst_path, repo_file),
                os.path.join("/tmp", repo_file),
                user=constants.EC2_USER,
            )
            rhel_pod_obj.exec_cmd_on_node(
                host,
                pem_dst_path,
                f"sudo mv {os.path.join(constants.RHEL_TMP_PATH, repo_file)} {constants.YUM_REPOS_PATH}",
                user=constants.EC2_USER,
            )
            for file_name in (
                constants.MIRROR_OPENSHIFT_USER_FILE,
                constants.MIRROR_OPENSHIFT_PASSWORD_FILE,
            ):
                rhel_pod_obj.copy_to_server(
                    host,
                    pem_dst_path,
                    os.path.join(constants.YUM_VARS_PATH, file_name),
                    os.path.join(constants.RHEL_TMP_PATH, file_name),
                    user=constants.EC2_USER,
                )
                rhel_pod_obj.exec_cmd_on_node(
                    host,
                    pem_dst_path,
                    f"sudo mv "
                    f"{os.path.join(constants.RHEL_TMP_PATH, file_name)} "
                    f"{constants.YUM_VARS_PATH}",
                    user=constants.EC2_USER,
                )
        # copy kubeconfig to pod
        kubeconfig = os.path.join(
            self.cluster_path, config.RUN.get("kubeconfig_location")
        )
        pod.upload(rhel_pod_obj.name, kubeconfig, "/")
        pull_secret_path = os.path.join(constants.DATA_DIR, "pull-secret")
        pod.upload(rhel_pod_obj.name, pull_secret_path, "/tmp/")
        host_file = self.build_ansible_inventory(hosts)
        pod.upload(rhel_pod_obj.name, host_file, "/")
        # install pod packages
        rhel_pod_obj.install_packages(constants.RHEL_POD_PACKAGES)
        # run ansible
        try:
            cmd = (
                f"ansible-playbook -i /hosts --private-key={pem_dst_path} "
                f"{constants.SCALEUP_ANSIBLE_PLAYBOOK}"
            )

            rhel_pod_obj.exec_cmd_on_pod(cmd, out_yaml_format=False, timeout=timeout)
            self.verify_nodes_added(hosts)
        finally:
            rhel_pod_obj.delete(force=True)

    def verify_nodes_added(self, hosts):
        """
        Verify RHEL workers are added

        Args:
             hosts (list): list of aws private hostnames

        Raises:
            FailedToAddNodeException: if node addition failed

        """
        timeout = 600
        ocp_obj = ocp.OCP(kind="node")
        node_info = ocp_obj.get()
        for i in range(len(hosts)):
            for entry in node_info["items"]:
                for each in entry["status"]["addresses"]:
                    if each["type"] == "Hostname":
                        if each["address"] in hosts:
                            logger.info(f"Checking status for {each['address']}")
                            sample = TimeoutSampler(
                                timeout, 3, self.get_ready_status, entry
                            )
                            try:
                                assert sample.wait_for_func_status(result=True)
                            except AssertionError:
                                raise exceptions.FailedToAddNodeException(
                                    "Failed to add RHEL node"
                                )

    def get_ready_status(self, node_info):
        """
        Get the node 'Ready' status

        Args:
            node_info (dict): Node info which includes details

        Returns:
            bool: True if node is Ready else False

        """
        for cond in node_info["status"]["conditions"]:
            if cond["type"] == "Ready":
                if not cond["status"] == "True":
                    return False
                else:
                    return True

    def build_ansible_inventory(self, hosts):
        """
        Build the ansible hosts file from jinja template

        Args:
            hosts (list): list of private host names

        Returns:
            str: path of the ansible file created

        """
        _templating = templating.Templating()
        ansible_host_file = dict()
        ansible_host_file["ansible_user"] = constants.EC2_USER
        ansible_host_file["ansible_become"] = "True"
        ansible_host_file["pod_kubeconfig"] = "/kubeconfig"
        ansible_host_file["pod_pull_secret"] = "/tmp/pull-secret"
        ansible_host_file["rhel_worker_nodes"] = hosts

        logger.info(ansible_host_file)
        data = _templating.render_template(
            constants.ANSIBLE_INVENTORY_YAML,
            ansible_host_file,
        )
        logger.debug("Ansible hosts file:%s", data)
        host_file_path = "/tmp/hosts"
        with open(host_file_path, "w") as f:
            f.write(data)
        return host_file_path

    @retry(exceptions.CommandFailed, tries=15, delay=30, backoff=1)
    def check_connection(self, rhel_pod_obj, host, pem_dst_path):
        """
        Check whether newly brought up RHEL instances are accepting
        ssh connections

        Args:
            rhel_pod_obj (Pod): object for handling ansible pod
            host (str): Node to which we want to try ssh
            pem_dst_path (str): path to private key for ssh

        """
        cmd = "ls"
        rhel_pod_obj.exec_cmd_on_node(host, pem_dst_path, cmd, user=constants.EC2_USER)

    def get_stack_name_of_node(self, node_name):
        """
        Get the stack name of a given node

        Args:
            node_name (str): the name of the node

        Returns:
            str: The stack name of the given node
        """
        instance_id = self.aws.get_instance_id_from_private_dns_name(node_name)
        stack_name = self.aws.get_stack_name_by_instance_id(instance_id)
        return stack_name

    def wait_for_nodes_to_stop(self, nodes):
        """
        Wait for the nodes to reach status stopped

        Args:
            nodes (list): The OCS objects of the nodes

        Raises:
            ResourceWrongStatusException: In case of the nodes didn't reach the expected status stopped.

        """
        instances = self.get_ec2_instances(nodes)
        if not instances:
            raise ValueError(
                f"Failed to get the EC2 instances for nodes {[n.name for n in nodes]}"
            )
        try:
            self.aws.wait_for_instances_to_stop(instances=instances)
        except WaiterError as e:
            logger.info("Failed to reach the expected status stopped")
            raise ResourceWrongStatusException(e)

    def wait_for_nodes_to_terminate(self, nodes):
        """
        Wait for the nodes to reach status terminated

        Args:
            nodes (list): The OCS objects of the nodes

        Raises:
            ResourceWrongStatusException: In case of the nodes didn't reach the expected status terminated.

        """
        instances = self.get_ec2_instances(nodes)
        if not instances:
            raise ValueError(
                f"Failed to get the EC2 instances for nodes {[n.name for n in nodes]}"
            )
        try:
            self.aws.wait_for_instances_to_terminate(instances=instances)
        except WaiterError as e:
            logger.info("Failed to reach the expected status terminated")
            raise ResourceWrongStatusException(e)

    def wait_for_nodes_to_stop_or_terminate(self, nodes):
        """
        Wait for the nodes to reach status stopped or terminated

        Args:
            nodes (list): The OCS objects of the nodes

        Raises:
            ResourceWrongStatusException: In case of the nodes didn't reach the expected
                status stopped or terminated.

        """
        instances = self.get_ec2_instances(nodes)
        if not instances:
            raise ValueError(
                f"Failed to get the EC2 instances for nodes {[n.name for n in nodes]}"
            )
        try:
            self.aws.wait_for_instances_to_stop_or_terminate(instances=instances)
        except WaiterError as e:
            logger.info("Failed to reach the expected status stopped or terminated")
            raise ResourceWrongStatusException(e)


class AWSUPINode(AWSNodes):
    """
    Node object representing AWS upi nodes

    """

    def __init__(self, node_conf, node_type):
        super(AWSUPINode, self).__init__()
        self.node_conf = node_conf
        #  RHEL/RHCOS
        self.node_type = node_type
        #  This variable will hold the AWS instance object
        self.aws_instance_obj = None
        self.region = config.ENV_DATA["region"]
        self.cluster_name = get_cluster_name(self.cluster_path)
        self.client = boto3.client("ec2", region_name=self.region)
        # cloudformation
        self.cf = self.aws.cf_client
        self.infra_id = get_infra_id(self.cluster_path)

    def _prepare_node(self, node_id):
        """
        Create AWS instance of the node

        Args:
            node_id (int): Unique integer id for node

        """
        if self.node_type == "RHEL":
            conf = self._prepare_rhel_node_conf()
            conf["node_id"] = node_id
            try:
                self.aws_instance_obj = self._prepare_upi_rhel_node(conf)
            except Exception:
                logger.error("Failed to create RHEL node")
                raise
        elif self.node_type == "RHCOS":
            conf = self._prepare_rhcos_node_conf()
            conf["node_id"] = node_id
            try:
                self.aws_instance_obj = self._prepare_upi_rhcos_node(conf)
            except Exception:
                logger.error("Failed to create RHCOS node")
                raise
            approve_pending_csr()

    def _prepare_rhcos_node_conf(self):
        """
        Merge default RHCOS node configuration for rhcos node
        along with the user provided config

        Returns:
            dict: A dictionary of merged user and default values

        """
        conf = self.read_default_config(constants.RHCOS_WORKER_CONF)
        default_conf = conf.get("ENV_DATA")
        merge_dict(default_conf, self.node_conf)
        logger.info(f"Config after merge is {default_conf}")
        return default_conf

    def _prepare_upi_rhcos_node(self, conf):
        """
        Handle RHCOS worker instance creation using cloudformation template,
        Create RHCOS instance with ami same as master

        Args:
            conf (dict): configuration for node

        Returns:
            boto3.Instance: instance of ec2 instance resource

        """
        logger.info(f"new rhcos node conf = {conf}")
        stack_name = conf.get("stack_name")
        if conf.get("stack_name"):
            suffix = stack_name.split("-")[-1]
        else:
            suffix = f"no{conf.get('zone')}"

        self.gather_worker_data(suffix)
        worker_template_path = self.get_rhcos_worker_template()
        self.bucket_name = constants.AWS_S3_UPI_BUCKET
        self.template_obj_key = f"{self.cluster_name}-workertemplate"
        self.add_cert_to_template(worker_template_path)
        self.aws.upload_file_to_s3_bucket(
            self.bucket_name, self.template_obj_key, worker_template_path
        )
        s3_url = self.aws.get_s3_bucket_object_url(
            self.bucket_name, self.template_obj_key
        )
        params_list = self.build_stack_params(conf["node_id"], conf)
        capabilities = ["CAPABILITY_NAMED_IAM"]
        self.stack_name, self.stack_id = self.aws.create_stack(
            s3_url, conf["node_id"], params_list, capabilities
        )
        instance_id = self.aws.get_stack_instance_id(
            self.stack_name, constants.AWS_WORKER_LOGICAL_RESOURCE_ID
        )

        delete_file(worker_template_path)
        self.aws.delete_s3_object(self.bucket_name, self.template_obj_key)
        return self.aws.get_ec2_instance(instance_id)

    def build_stack_params(self, index, conf):
        """
        Build all the params required for a stack creation

        Args:
            index (int): An integer index for this stack
            conf (dict): Node config

        Returns:
            list: of param dicts

        """
        param_list = []
        pk = "ParameterKey"
        pv = "ParameterValue"

        param_list.append({pk: "Index", pv: str(index)})
        param_list.append({pk: "InfrastructureName", pv: self.infra_id})
        param_list.append({pk: "RhcosAmi", pv: self.worker_image_id})
        param_list.append({pk: "IgnitionLocation", pv: self.worker_ignition_location})
        param_list.append({pk: "Subnet", pv: self.worker_subnet})
        param_list.append(
            {
                pk: "WorkerSecurityGroupId",
                pv: self.worker_security_group[0].get("GroupId"),
            }
        )
        param_list.append(
            {pk: "WorkerInstanceProfileName", pv: self.worker_instance_profile}
        )
        param_list.append({pk: "WorkerInstanceType", pv: conf["worker_instance_type"]})

        return param_list

    def add_cert_to_template(self, worker_template_path):
        """
        Add cert to worker template

        Args:
            worker_template_path (str): Path where template file is located

        """
        worker_ignition_path = os.path.join(self.cluster_path, constants.WORKER_IGN)
        cert = self.get_cert_content(worker_ignition_path)
        self.update_template_with_cert(worker_template_path, cert)

    def update_template_with_cert(self, worker_template_path, cert):
        """
        Update the template file with cert provided

        Args:
            worker_template_path (str): template file path
            cert (str): Certificate body

        """
        search_str = "ABC...xYz=="
        temp = "/tmp/worker_temp.yaml"
        with open(worker_template_path, "r") as fp:
            orig_content = fp.read()
            logger.info("=====ORIGINAL=====")
            logger.info(orig_content)
            final_content = re.sub(
                r"{}".format(search_str), r"{}".format(cert), orig_content
            )
        with open(temp, "w") as wfp:
            logger.info(final_content)
            wfp.write(final_content)
        os.rename(temp, worker_template_path)

    def get_cert_content(self, worker_ignition_path):
        """
        Get the certificate content from worker ignition file

        Args:
            worker_ignition_path (str): Path of the worker ignition file

        Returns:
            formatted_cert (str): certificate content

        """
        assert os.path.exists(worker_ignition_path)
        with open(worker_ignition_path, "r") as fp:
            content = json.loads(fp.read())
            tls_data = content.get("ignition").get("security").get("tls")
            cert_content = tls_data.get("certificateAuthorities")[0].get("source")
            formatted_cert = cert_content.split(",")[1]
        return formatted_cert

    def get_rhcos_worker_template(self):
        """
        Download template and keep it locally

        Returns:
            path (str): local path to template file

        """
        common_base = "functionality-testing"
        ocp_version = get_ocp_version("_")
        relative_template_path = os.path.join(
            f"aos-{ocp_version}", "hosts/upi_on_aws-cloudformation-templates"
        )

        path_to_file = os.path.join(
            f"{common_base}",
            f"{relative_template_path}",
            f"{constants.AWS_WORKER_NODE_TEMPLATE}",
        )
        logger.info(
            f"Getting file '{path_to_file}' from "
            f"git repository {constants.OCP_QE_MISC_REPO}"
        )
        tmp_file = os.path.join("/tmp", constants.AWS_WORKER_NODE_TEMPLATE)
        download_file_from_git_repo(constants.OCP_QE_MISC_REPO, path_to_file, tmp_file)
        return tmp_file

    def _prepare_rhel_node_conf(self):
        """
        Merge default RHEL node config with the user provided
        config

        """
        # Expand RHEL Version in the file name below!
        rhel_version = Version.coerce(config.ENV_DATA["rhel_version"])
        conf = self.read_default_config(
            constants.RHEL_WORKERS_CONF.format(version=rhel_version.major)
        )
        default_conf = conf.get("ENV_DATA")
        merge_dict(default_conf, self.node_conf)
        logger.info(f"Merged dict is {default_conf}")
        return default_conf

    def _prepare_upi_rhel_node(self, node_conf):
        """
        Handle RHEL worker instance creation
        1. Create RHEL worker instance , copy required AWS tags from existing
        worker instances to new RHEL instance
        2. Copy IAM role from existing worker to new RHEL worker

        """
        cluster_id = get_infra_id(self.cluster_path)
        node_id = node_conf["node_id"]
        zone = node_conf.get("zone")
        logger.info("Creating RHEL worker node")
        self.gather_worker_data(f"no{zone}")
        rhel_version = config.ENV_DATA["rhel_version"]
        rhel_worker_ami = config.ENV_DATA[f"rhel{rhel_version}_worker_ami"]
        response = self.client.run_instances(
            BlockDeviceMappings=[
                {
                    "DeviceName": node_conf["root_disk"],
                    "Ebs": {
                        "DeleteOnTermination": True,
                        "VolumeSize": node_conf["root_disk_size"],
                        "VolumeType": "gp2",
                    },
                },
            ],
            ImageId=rhel_worker_ami,
            SubnetId=self.worker_subnet,
            InstanceType=node_conf["rhel_worker_instance_type"],
            MaxCount=1,
            MinCount=1,
            Monitoring={"Enabled": False},
            SecurityGroupIds=[
                self.worker_security_group[0]["GroupId"],
            ],
            KeyName="openshift-dev",
        )
        inst_id = response["Instances"][0]["InstanceId"]
        worker_ec2 = boto3.resource("ec2", region_name=self.region)
        worker_instance = worker_ec2.Instance(inst_id)
        worker_instance.wait_until_running()
        worker_name = f"{cluster_id}-rhel-worker-{node_id}"
        worker_ec2.create_tags(
            Resources=[inst_id],
            Tags=[
                {"Key": "Name", "Value": f"{worker_name}"},
                {"Key": self.worker_tag[0], "Value": self.worker_tag[1]},
            ],
        )
        logger.info(self.worker_iam_role)
        self.client.associate_iam_instance_profile(
            IamInstanceProfile=self.worker_iam_role,
            InstanceId=inst_id,
        )
        return worker_instance

    def gather_worker_data(self, suffix="no0"):
        """
        Gather various info like vpc, iam role, subnet,security group,
        cluster tag from existing RHCOS workers

        Args:
            suffix (str): suffix to get resource of worker node, 'no0' by default

        """
        stack_name = f"{self.cluster_name}-{suffix}"
        resource = self.cf.list_stack_resources(StackName=stack_name)
        worker_id = self.get_worker_resource_id(resource)
        ec2 = boto3.resource("ec2", region_name=self.region)
        worker_instance = ec2.Instance(worker_id)
        self.worker_vpc = worker_instance.vpc.id
        self.worker_subnet = worker_instance.subnet.id
        self.worker_security_group = worker_instance.security_groups
        self.worker_iam_role = worker_instance.iam_instance_profile
        self.worker_tag = self.get_kube_tag(worker_instance.tags)
        self.worker_image_id = worker_instance.image.id  # AMI id
        self.worker_instance_profile = self.aws.get_worker_instance_profile_name(
            stack_name
        )
        self.worker_ignition_location = self.aws.get_worker_ignition_location(
            stack_name
        )
        del self.worker_iam_role["Id"]

    def get_kube_tag(self, tags):
        """
        Fetch kubernets.io tag from worker instance

        Args:
            tags (dict): AWS tags from existing worker

        Returns:
            tuple: key looks like
                "kubernetes.io/cluster/<cluster-name>" and value looks like
                "share" OR "owned"

        """
        for each in tags:
            if "kubernetes" in each["Key"]:
                return each["Key"], each["Value"]

    def get_worker_resource_id(self, resource):
        """
        Get the resource ID

        Args:
            resource (dict): a dictionary of stack resource

        Returns:
            str: ID of worker stack resource

        """
        return resource["StackResourceSummaries"][0]["PhysicalResourceId"]


class VSPHEREUPINode(VMWareNodes):
    """
    Node object representing VMWARE UPI nodes
    """

    def __init__(self, node_conf, node_type, compute_count):
        """
        Initialize necessary variables

        Args:
            node_conf (dict): of node configuration
            node_type (str): type of node to be created RHCOS/RHEL
            compute_count (int): number of nodes to add to existing cluster

        """
        super(VSPHEREUPINode, self).__init__()
        self.node_conf = node_conf
        self.node_type = node_type
        self.compute_count = compute_count
        self.current_compute_count = len(get_nodes())
        self.terraform_var = os.path.join(
            self.cluster_path, constants.TERRAFORM_DATA_DIR, "terraform.tfvars"
        )
        with open(self.terraform_var, "r") as fd:
            compute_count_line = [
                line.rstrip("\n") for line in fd.readlines() if "compute_count" in line
            ][0]
        self.current_count_in_tfvars = int(
            compute_count_line.split("=")[1].strip().strip('"')
        )

        self.target_compute_count = self.current_count_in_tfvars + self.compute_count

        # update the terraform installer path in ENV_DATA
        # DON'T download terraform again since we need to use the same
        # version as deployment
        bin_dir = os.path.expanduser(config.RUN["bin_dir"])
        terraform_filename = "terraform"
        terraform_binary_path = os.path.join(bin_dir, terraform_filename)
        config.ENV_DATA["terraform_installer"] = terraform_binary_path

        self.folder_structure = False
        if (
            version_module.get_semantic_ocp_running_version()
            >= version_module.VERSION_4_5
        ):
            set_aws_region()
            self.folder_structure = True
            config.ENV_DATA["folder_structure"] = True

        # Initialize Terraform
        self.previous_dir = os.getcwd()
        self.terraform_data_dir = os.path.join(
            self.cluster_path, constants.TERRAFORM_DATA_DIR
        )
        self.terraform_work_dir = constants.VSPHERE_DIR
        self.terraform = Terraform(self.terraform_work_dir)
        self.upi_repo_path = os.path.join(
            constants.EXTERNAL_DIR,
            "installer",
        )

    def _update_terraform(self):
        """
        Update terraform variables
        """
        self.update_terraform_tfvars_compute_count(type="add", count=self.compute_count)

    def _update_machine_conf(self):
        """
        Update the machine config for vsphere
        """
        to_change = "clone {"
        add_file_block = f"{constants.LIFECYCLE}\n  {to_change}"
        vm_machine_conf = (
            constants.VM_MAIN
            if self.folder_structure
            else constants.INSTALLER_MACHINE_CONF
        )
        logger.debug(f"Adding {constants.LIFECYCLE} to {vm_machine_conf}")
        replace_content_in_file(vm_machine_conf, to_change, add_file_block)

        # update the machine configurations
        update_machine_conf(self.folder_structure)

    def update_terraform_tfvars_compute_count(self, type, count):
        """
        Update terraform tfvars file for compute count

        Args:
             type (str): Type of operation. Either add or remove
             count (int): Number to add or remove to the exiting compute count

        """
        logger.debug("Updating terraform variables")
        compute_str = "compute_count ="
        if type == "add":
            target_compute_count = self.current_count_in_tfvars + count
        elif type == "remove":
            target_compute_count = self.current_count_in_tfvars - count
        else:
            raise UnknownOperationForTerraformVariableUpdate
        updated_compute_str = f'{compute_str} "{target_compute_count}"'
        logger.debug(f"Updating {updated_compute_str} in {self.terraform_var}")

        # backup the terraform variable file
        original_file = f"{self.terraform_var}_{int(time.time())}"
        shutil.copyfile(self.terraform_var, original_file)
        logger.info(f"original terraform file: {original_file}")

        replace_content_in_file(
            self.terraform_var,
            compute_str,
            updated_compute_str,
            match_and_replace_line=True,
        )

    @retry(
        (NoValidConnectionsError, AuthenticationException),
        tries=20,
        delay=30,
        backoff=1,
    )
    def wait_for_connection_and_set_host_name(self, ip, host_name):
        """
        Waits for connection to establish to the node and sets the hostname

        Args:
            ip (str): IP of the node
            host_name (str): Host name to set for the node

        Raises:
            NoValidConnectionsError: Raises if connection is not established
            AuthenticationException: Raises if credentials are not correct

        """
        vmnode = VSPHERENode(ip)
        vmnode.set_host_name(host_name)
        vmnode.reboot()

    def add_node(self, use_terraform=True):
        """
        Add nodes to the current cluster

        Args:
            use_terraform (bool): if True use terraform to add nodes,
                otherwise use manual steps to add nodes

        """
        if self.node_type == constants.RHCOS:
            logger.info(f"Adding Nodes of type {self.node_type}")
            logger.info(
                f"Existing worker nodes: {self.current_compute_count}, "
                f"New nodes to add: {self.compute_count}"
            )

            # Gets the existing CSR data
            existing_csr_data = get_nodes_csr()
            pre_count_csr = len(existing_csr_data)
            logger.debug(f"Existing CSR count before adding nodes: {pre_count_csr}")

            # get VM names from vSphere before adding node
            compute_vms = self.vsphere.get_compute_vms_in_pool(
                self.cluster_name, self.datacenter, self.cluster
            )
            compute_node_names = [compute_vm.name for compute_vm in compute_vms]
            compute_node_names.sort()
            logger.info(f"VM names before adding nodes: {compute_node_names}")

            if use_terraform:
                self.add_nodes_with_terraform()
            else:
                self.add_nodes_without_terraform()

            # give some time to settle down the newly added nodes
            time.sleep(self.wait_time)

            # approve pending CSRs
            if constants.CSR_BOOTSTRAPPER_NODE in existing_csr_data:
                nodes_approve_csr_num = pre_count_csr + self.compute_count
            else:
                nodes_approve_csr_num = pre_count_csr + self.compute_count + 1

            # get vm names from vSphere after adding node
            compute_vms_after_adding_node = self.vsphere.get_compute_vms_in_pool(
                self.cluster_name, self.datacenter, self.cluster
            )
            compute_node_names_after_adding_node = [
                compute_vm.name for compute_vm in compute_vms_after_adding_node
            ]
            compute_node_names_after_adding_node.sort()
            logger.info(
                f"VM names after adding node: {compute_node_names_after_adding_node}"
            )

            # get newly added VM name
            new_node = list(
                set(compute_node_names_after_adding_node) - set(compute_node_names)
            )[0]

            # If CSR exists for new node, create dictionary with the csr info
            # e.g: {'compute-1': ['csr-64vkw']}
            ignore_existing_csr = None
            if new_node in existing_csr_data:
                nodes_approve_csr_num -= 1
                ignore_existing_csr = {new_node: existing_csr_data[new_node]}

            wait_for_all_nodes_csr_and_approve(
                expected_node_num=nodes_approve_csr_num,
                ignore_existing_csr=ignore_existing_csr,
            )

    def add_nodes_with_terraform(self):
        """
        Add nodes using terraform
        """
        terraform_state_file = os.path.join(
            self.terraform_data_dir, "terraform.tfstate"
        )
        ips_before_adding_nodes = get_module_ip(
            terraform_state_file, constants.COMPUTE_MODULE
        )
        logger.debug(f"Compute IP's before adding new nodes: {ips_before_adding_nodes}")

        # clone openshift installer
        clone_openshift_installer()
        self._update_terraform()
        self._update_machine_conf()

        # comment sensitive variable as current terraform version doesn't support
        if (
            version_module.get_semantic_ocp_running_version()
            >= version_module.VERSION_4_11
        ):
            comment_sensitive_var()
            ignition_provider_version = get_ignition_provider_version()
            terraform_plugins_path = ".terraform/plugins/linux_amd64/"
            terraform_ignition_provider_path = os.path.join(
                self.terraform_data_dir,
                terraform_plugins_path,
                f"terraform-provider-ignition_{ignition_provider_version}",
            )
            if not os.path.isfile(terraform_ignition_provider_path):
                get_terraform_ignition_provider(
                    self.terraform_data_dir, version=ignition_provider_version
                )

        # initialize terraform and apply
        os.chdir(self.terraform_data_dir)
        self.terraform.initialize()
        self.terraform.apply(
            self.terraform_var, module=constants.COMPUTE_MODULE, refresh=False
        )
        self.terraform.apply(
            self.terraform_var, module=constants.COMPUTE_MODULE_VM, refresh=False
        )
        os.chdir(self.previous_dir)

        # get the newly added compute IPs
        ips_after_adding_nodes = get_module_ip(
            terraform_state_file, constants.COMPUTE_MODULE
        )
        logger.debug(f"Compute IP's after adding new nodes: {ips_after_adding_nodes}")
        new_node_ips = list(set(ips_after_adding_nodes) - set(ips_before_adding_nodes))
        logger.info(f"Newly added compute IP's: {new_node_ips}")

        # inform load balancer regarding newly added nodes
        lb = LoadBalancer()
        lb.update_haproxy_with_nodes(new_node_ips)
        lb.restart_haproxy()

    def add_nodes_without_terraform(self):
        """
        Add nodes without terraform
        """
        # generate new node names
        new_nodes_names = self.generate_node_names_for_vsphere(self.compute_count)
        logger.info(f"New node names: {new_nodes_names}")

        # get the worker ignition
        worker_ignition_path = os.path.join(self.cluster_path, constants.WORKER_IGN)
        worker_ignition_base64 = run_cmd(f"base64 -w0 {worker_ignition_path}")
        data = {
            "disk.EnableUUID": config.ENV_DATA["disk_enable_uuid"],
            "guestinfo.ignition.config.data": worker_ignition_base64,
            "guestinfo.ignition.config.data.encoding": config.ENV_DATA[
                "ignition_data_encoding"
            ],
        }

        # clone VM
        for node_name in new_nodes_names:
            self.vsphere.clone_vm(
                node_name,
                config.ENV_DATA["vm_template"],
                self.datacenter,
                self.cluster_name,
                self.datastore,
                config.ENV_DATA["vsphere_cluster"],
                int(config.ENV_DATA["worker_num_cpus"]),
                int(config.ENV_DATA["compute_memory"]),
                125829120,
                config.ENV_DATA["vm_network"],
                power_on=True,
                **data,
            )
        logger.info(f"Sleeping for {self.wait_time} sec to settle down the VMs")
        time.sleep(self.wait_time)

        # set hostname
        for node_name in new_nodes_names:
            for ip in TimeoutSampler(
                600,
                60,
                self.vsphere.find_ip_by_vm,
                node_name,
                self.datacenter,
                config.ENV_DATA["vsphere_cluster"],
                self.cluster_name,
            ):
                if not ("<unset>" in ip or "127.0.0.1" in ip):
                    logger.info("setting host name")
                    self.wait_for_connection_and_set_host_name(ip, node_name)
                    break

    def generate_node_names_for_vsphere(self, count, prefix="compute-"):
        """
        Generate the node names for vsphere platform

        Args:
            count (int): Number of node names to generate
            prefix (str): Prefix for node name

        Returns:
            list: List of node names

        """
        compute_vms = self.vsphere.get_compute_vms_in_pool(
            self.cluster_name, self.datacenter, self.cluster
        )
        compute_node_names = [compute_vm.name for compute_vm in compute_vms]
        logger.info(f"Current node names: {compute_node_names}")
        compute_node_names.sort()

        current_compute_suffix = int(compute_node_names[-1].split("-")[-1])
        return [
            f"{prefix}{current_compute_suffix + node_count}"
            for node_count in range(1, count + 1)
        ]

    def change_terraform_statefile_after_remove_vm(self, vm_name):
        """
        Remove the records from the state file, so that terraform will no longer be tracking the
        corresponding remote objects of the vSphere VM object we removed.

        Args:
            vm_name (str): The VM name

        """
        if vm_name.startswith("compute-"):
            module = "compute_vm"
        else:
            module = "control_plane_vm"
        instance = f"{vm_name}.{config.ENV_DATA.get('cluster_name')}.{config.ENV_DATA.get('base_domain')}"

        os.chdir(self.terraform_data_dir)
        logger.info(f"Modifying terraform state file of the removed vm {vm_name}")
        self.terraform.change_statefile(
            module=module,
            resource_type="vsphere_virtual_machine",
            resource_name="vm",
            instance=instance,
        )
        os.chdir(self.previous_dir)

    def change_terraform_tfvars_after_remove_vm(self, num_nodes_removed=1):
        """
        Update the compute count after removing node from cluster

        Args:
             num_nodes_removed (int): Number of nodes removed from cluster

        """
        self.update_terraform_tfvars_compute_count(
            type="remove", count=num_nodes_removed
        )


class BaremetalNodes(NodesBase):
    """
    Baremetal Nodes class
    """

    def __init__(self):
        super(BaremetalNodes, self).__init__()
        from ocs_ci.utility import baremetal

        self.baremetal = baremetal.BAREMETAL()

    def stop_nodes(self, nodes, force=True):
        """
        Stop Baremetal Machine

        Args:
            nodes (list): The OCS objects of the nodes
            force (bool): True for force nodes stop, False otherwise

        """
        self.baremetal.stop_baremetal_machines(nodes, force=force)

    def start_nodes(self, nodes, wait=True):
        """
        Start Baremetal Machine

        Args:
            nodes (list): The OCS objects of the nodes
            wait (bool): Wait for node status

        """
        self.baremetal.start_baremetal_machines(nodes, wait=wait)

    def restart_nodes(self, nodes, force=True):
        """
        Restart Baremetal Machine

        Args:
            nodes (list): The OCS objects of the nodes
            force (bool): True for force BM stop, False otherwise

        """
        self.baremetal.restart_baremetal_machines(nodes, force=force)

    def restart_nodes_by_stop_and_start_teardown(self):
        """
        Make sure all BMs are up by the end of the test

        """
        self.cluster_nodes = get_node_objs()
        bms = self.baremetal.get_nodes_ipmi_ctx(self.cluster_nodes)
        stopped_bms = [
            bm
            for bm in bms
            if self.baremetal.get_power_status(bm) == constants.VM_POWERED_OFF
        ]

        if stopped_bms:
            logger.info(f"The following BMs are powered off: {stopped_bms}")
            self.baremetal.start_baremetal_machines_with_ipmi_ctx(stopped_bms)
        for bm in bms:
            bm.session.close()

    def get_data_volumes(self):
        raise NotImplementedError("Get data volume functionality is not implemented")

    def get_node_by_attached_volume(self, volume):
        raise NotImplementedError(
            "Get node by attached volume functionality is not implemented"
        )

    def detach_volume(self, volume, node=None, delete_from_backend=True):
        raise NotImplementedError("Detach volume functionality is not implemented")

    def attach_volume(self, volume, node):
        raise NotImplementedError("Attach volume functionality is not implemented")

    def wait_for_volume_attach(self, volume):
        raise NotImplementedError(
            "Wait for volume attach functionality is not implemented"
        )

    def create_and_attach_nodes_to_cluster(self, node_conf, node_type, num_nodes):
        raise NotImplementedError(
            "attach nodes to cluster functionality is not implemented"
        )

    def create_nodes(self, node_conf, node_type, num_nodes):
        raise NotImplementedError(
            "attach nodes to cluster functionality is not implemented"
        )

    def attach_nodes_to_cluster(self, node_list):
        raise NotImplementedError(
            "attach nodes to cluster functionality is not implemented"
        )

    def read_default_config(self, default_config_path):
        """
        Commonly used function to read default config

        Args:
            default_config_path (str): Path to default config file

        Returns:
            dict: of default config loaded

        """
        assert os.path.exists(default_config_path), "Config file doesnt exists"

        with open(default_config_path) as f:
            default_config_dict = yaml.safe_load(f)

        return default_config_dict


class IBMPowerNodes(NodesBase):
    """
    IBM Power Nodes class
    """

    def __init__(self):
        super(IBMPowerNodes, self).__init__()
        from ocs_ci.utility import powernodes

        self.powernodes = powernodes.PowerNodes()

    def stop_nodes(self, nodes, force=True):
        """
        Stop PowerNode

        Args:
            nodes (list): The OCS objects of the nodes
            force (bool): True for force nodes stop, False otherwise

        """
        if self.powernodes.iskvm():
            self.powernodes.stop_powernodes_machines(
                nodes, timeout=900, wait=True, force=force
            )
        else:
            self.powernodes.stop_powernodes_machines_powervs(
                nodes, timeout=900, wait=True
            )

    def start_nodes(self, nodes, force=True):
        """
        Start PowerNode

        Args:
            nodes (list): The OCS objects of the nodes
            wait (bool): Wait for node status

        """
        if self.powernodes.iskvm():
            self.powernodes.start_powernodes_machines(
                nodes, timeout=900, wait=True, force=force
            )
        else:
            self.powernodes.start_powernodes_machines_powervs(
                nodes, timeout=900, wait=True
            )

    def restart_nodes(self, nodes, timeout=540, wait=True, force=True):
        """
        Restart PowerNode

        Args:
            nodes (list): The OCS objects of the nodes
            timeout (int): time in seconds to wait for node to reach 'not ready' state,
                and 'ready' state.
            wait (bool): True if need to wait till the restarted node reaches timeout
            force (bool): True for force BM stop, False otherwise

        """
        if self.powernodes.iskvm():
            self.powernodes.restart_powernodes_machines(
                nodes, timeout=900, wait=True, force=force
            )
        else:
            self.powernodes.restart_powernodes_machines_powervs(
                nodes, timeout=900, wait=True
            )

    def restart_nodes_by_stop_and_start(self, nodes, force=True):
        """
        Restart PowerNodes with stop and start

        Args:
            nodes (list): The OCS objects of the nodes
            force (bool): True for force node stop, False otherwise

        """
        if self.powernodes.iskvm():
            self.powernodes.restart_powernodes_machines(
                nodes, timeout=900, wait=True, force=force
            )
        else:
            self.powernodes.restart_powernodes_machines_powervs(
                nodes, timeout=900, wait=True
            )

    def restart_nodes_by_stop_and_start_teardown(self):
        """
        Make sure all PowerNodes are up by the end of the test
        """
        self.cluster_nodes = get_node_objs()
        if self.powernodes.iskvm():
            stopped_powernodes = [
                powernode
                for powernode in self.cluster_nodes
                if self.powernodes.verify_machine_is_down(powernode) is True
            ]
        else:
            stopped_powernodes = [
                powernode
                for powernode in self.cluster_nodes
                if powernode.ocp.get_resource_status(powernode.name)
                == constants.NODE_NOT_READY
            ]

        if stopped_powernodes:
            logger.info(
                f"The following PowerNodes are powered off: {stopped_powernodes}"
            )
            if self.powernodes.iskvm():
                self.powernodes.start_powernodes_machines(stopped_powernodes)
            else:
                self.powernodes.start_powernodes_machines_powervs(stopped_powernodes)


class AZURENodes(NodesBase):
    """
    Azure Nodes class
    """

    def __init__(self):
        super(AZURENodes, self).__init__()
        from ocs_ci.utility import azure_utils

        self.azure = azure_utils.AZURE()

    def stop_nodes(self, nodes, timeout=540, wait=True, force=True):
        """
        Stop Azure vm instances

        Args:
            nodes (list): The OCS objects of the nodes
            wait (bool): True for waiting the instances to stop, False otherwise
            timeout (int): time in seconds to wait for node to reach 'not ready' state.
            force (bool): True for force VM stop, False otherwise

        """
        if not nodes:
            raise ValueError("No nodes found to stop")

        node_names = [n.name for n in nodes]
        self.azure.stop_vm_instances(node_names, force=force)

        if wait:
            # When the node is not reachable then the node reaches status NotReady.
            logger.info(f"Waiting for nodes: {node_names} to reach not ready state")
            wait_for_nodes_status(
                node_names=node_names, status=constants.NODE_NOT_READY, timeout=timeout
            )

    def start_nodes(self, nodes, timeout=540, wait=True):
        """
        Start Azure vm instances

        Args:
            nodes (list): The OCS objects of the nodes
            wait (bool): True for waiting the instances to start, False otherwise
            timeout (int): time in seconds to wait for node to reach 'ready' state.

        """
        if not nodes:
            raise ValueError("No nodes found to start")
        node_names = [n.name for n in nodes]
        self.azure.start_vm_instances(node_names)

        if wait:
            # When the node is reachable then the node reaches status Ready.
            logger.info(f"Waiting for nodes: {node_names} to reach ready state")
            wait_for_nodes_status(
                node_names=node_names, status=constants.NODE_READY, timeout=timeout
            )

    def restart_nodes(self, nodes, timeout=900, wait=True):
        """
        Restart Azure vm instances

        Args:
            nodes (list): The OCS objects of the nodes / Azure Vm instance
            wait (bool): True if need to wait till the restarted node reaches
                READY state. False otherwise
            timeout (int): time in seconds to wait for node to reach 'not ready' state,
                and 'ready' state.

        """
        if not nodes:
            logger.error("No nodes found for restarting")
            raise ValueError
        node_names = [n.name for n in nodes]
        self.azure.restart_vm_instances(node_names)

        if wait:
            """
            When reboot is initiated on an instance from the Azure, the
            instance stays at "Running" state throughout the reboot operation.

            Once the OCP node detects that the node is not reachable then the
            node reaches status NotReady.
            When the reboot operation is completed and the instance is
            reachable the OCP node reaches status Ready.
            """
            logger.info(f"Waiting for nodes: {node_names} to reach not ready state")
            wait_for_nodes_status(
                node_names=node_names, status=constants.NODE_NOT_READY, timeout=timeout
            )
            logger.info(f"Waiting for nodes: {node_names} to reach ready state")
            wait_for_nodes_status(
                node_names=node_names, status=constants.NODE_READY, timeout=timeout
            )

    def restart_nodes_by_stop_and_start(
        self, nodes, timeout=540, wait=True, force=True
    ):
        """
        Restart Azure vm instances by stop and start

        Args:
            nodes (list): The OCS objects of the nodes / Azure Vm instance
            wait (bool): True if need to wait till the restarted node reaches
                READY state. False otherwise
            timeout (int): time in seconds to wait for node to reach 'not ready' state,
                and 'ready' state.
            force (bool): True for force VM stop, False otherwise

        """
        if not nodes:
            raise ValueError("No nodes found for restarting")
        node_names = [n.name for n in nodes]
        self.azure.restart_vm_instances_by_stop_and_start(node_names, force=force)

        if wait:
            logger.info(f"Waiting for nodes: {node_names} to reach ready state")
            wait_for_nodes_status(
                node_names=node_names, status=constants.NODE_READY, timeout=timeout
            )

    def get_data_volumes(self):
        """
        Get the data Azure disk objects

        Returns:
            list: azure disk objects

        """
        pvs = get_deviceset_pvs()
        return self.azure.get_data_volumes(pvs)

    def get_node_by_attached_volume(self, volume):
        """
        Get node OCS object of the Azure vm instance that has the volume attached to

        Args:
            volume (Disk): The disk object to get the Azure Vm according to

        Returns:
            OCS: The OCS object of the Azure Vm instance

        """
        vm = self.azure.get_node_by_attached_volume(volume)
        all_nodes = get_node_objs()
        nodes = [n for n in all_nodes if n.name == vm.name]
        assert nodes, f"Failed to find the OCS object for Azure Vm instance {vm.name}"
        return nodes[0]

    def detach_volume(self, volume, node=None, delete_from_backend=True):
        """
        Detach a volume from an Azure Vm instance

        Args:
            volume (Disk): The disk object required to delete a volume
            node (OCS): The OCS object representing the node
            delete_from_backend (bool): True for deleting the disk from the
                storage backend, False otherwise

        """
        self.azure.detach_volume(volume, node)

    def attach_volume(self, volume, node):
        raise NotImplementedError("Attach volume functionality is not implemented")

    def wait_for_volume_attach(self, volume):
        """
        Wait for a Disk to be attached to an Azure Vm instance.
        This is used as when detaching the Disk from the Azure Vm instance,
        re-attach should take place automatically

        Args:
            volume (Disk): The Disk to wait for to be attached

        Returns:
            bool: True if the volume has been attached to the
                instance, False otherwise

        """
        try:
            for sample in TimeoutSampler(
                300, 3, self.azure.get_disk_state, volume.name
            ):
                logger.info(f"Volume id: {volume.name} has status: {sample}")
                if sample == "Attached":
                    return True
        except TimeoutExpiredError:
            logger.error(
                f"Volume {volume.name} failed to be attached to an Azure Vm instance"
            )
            return False

    def create_and_attach_nodes_to_cluster(self, node_conf, node_type, num_nodes):
        raise NotImplementedError(
            "attach nodes to cluster functionality is not implemented"
        )

    def create_nodes(self, node_conf, node_type, num_nodes):
        raise NotImplementedError(
            "attach nodes to cluster functionality is not implemented"
        )

    def attach_nodes_to_cluster(self, node_list):
        raise NotImplementedError(
            "attach nodes to cluster functionality is not implemented"
        )

    def restart_nodes_by_stop_and_start_teardown(self):
        """
        Make sure all VM instances up by the end of the test

        """
        self.cluster_nodes = get_node_objs()
        vms = self.azure.get_vm_names()
        assert (
            vms
        ), f"Failed to get VM objects for nodes {[n.name for n in self.cluster_nodes]}"

        stopped_vms = [
            vm
            for vm in vms
            if self.azure.get_vm_power_status(vm) == constants.VM_STOPPED
            or self.azure.get_vm_power_status(vm) == constants.VM_STOPPING
        ]
        # Start the VMs
        if stopped_vms:
            logger.info(f"The following VMs are powered off: {stopped_vms}")
            self.azure.start_vm_instances(stopped_vms)


class VMWareLSONodes(VMWareNodes):
    """
    VMWare LSO nodes class

    """

    def __init__(self):
        super().__init__()

    def get_data_volumes(self, pvs=None):
        """
        Get the data vSphere volumes

        Args:
            pvs (list): PV OCS objects

        Returns:
            list: vSphere volumes

        """
        if not pvs:
            pvs = get_deviceset_pvs()
        return [pv.get().get("spec").get("local").get("path") for pv in pvs]

    def detach_volume(self, volume, node=None, delete_from_backend=True):
        """
        Detach disk from a VM and delete from datastore if specified

        Args:
            volume (str): Volume path
            node (OCS): The OCS object representing the node
            delete_from_backend (bool): True for deleting the disk (vmdk)
                from backend datastore, False otherwise

        """
        vm = self.get_vms([node])[0]
        self.vsphere.remove_disk(
            vm=vm, identifier=volume, key="volume_path", datastore=delete_from_backend
        )

    def get_volume_path(self, volume_handle, node_name=None):
        """
        Fetches the volume path for the volumeHandle

        Args:
            volume_handle (str): volumeHandle which exists in PV
            node_name (str): Node name where PV exists

        Returns:
            str: volume path of PV

        """
        volume_path = None
        vm = self.vsphere.get_vm_in_pool_by_name(
            name=node_name,
            dc=config.ENV_DATA["vsphere_datacenter"],
            cluster=config.ENV_DATA["vsphere_cluster"],
            pool=config.ENV_DATA["cluster_name"],
        )
        disks = self.vsphere.get_disks(vm)
        for each_disk in disks:
            disk_wwn = each_disk["wwn"].replace("-", "")
            if disk_wwn.lower() in volume_handle:
                volume_path = each_disk["fileName"]
                logger.info(
                    f"Volume path for {volume_handle} is `{volume_path}` on node {node_name}"
                )
                break
        if volume_path:
            return volume_path
        else:
            raise VolumePathNotFoundException


class RHVNodes(NodesBase):
    """
    RHV Nodes  class
    """

    def __init__(self):
        super(RHVNodes, self).__init__()
        from ocs_ci.utility import rhv

        self.rhv = rhv.RHV()

    def get_rhv_vm_instances(self, nodes):
        """
        Get the RHV VM instaces list

        Args:
           nodes (list): The OCS objects of the nodes

        Returns:
            list: The RHV vm instances list

        """
        return [self.rhv.get_rhv_vm_instance(n.name) for n in nodes]

    def stop_nodes(self, nodes, timeout=600, wait=True, force=True):
        """
        Shutdown RHV VM

        Args:
            nodes (list): The OCS objects of the nodes
            wait (bool): True for waiting the instances to stop, False otherwise
            timeout (int): time in seconds to wait for node to reach 'not ready' state.
            force (bool): True for force VM stop, False otherwise

        """
        if not nodes:
            raise ValueError("No nodes found to stop")

        vms = self.get_rhv_vm_instances(nodes)
        node_names = [n.name for n in nodes]
        self.rhv.stop_rhv_vms(vms, timeout=timeout, force=force)
        logger.info(f"node names are: {node_names} ")
        if wait:
            # When the node is not reachable then the node reaches status NotReady.
            logger.info(f"Waiting for nodes: {node_names} to reach not ready state")
            wait_for_nodes_status(
                node_names=node_names, status=constants.NODE_NOT_READY, timeout=timeout
            )

    def restart_nodes(self, nodes, timeout=900, wait=True, force=True):
        """
        Restart RHV VM

        Args:
            nodes (list): The OCS objects of the nodes
            timeout (int): time in seconds to wait for node to reach 'ready' state
            wait (bool): True if need to wait till the restarted node reaches
                READY state. False otherwise
            force (bool): True for force VM reboot, False otherwise

        Raises:
            ValueError: Raises if No nodes found for restarting

        """
        if not nodes:
            raise ValueError("No nodes found for restarting")
        vms = self.get_rhv_vm_instances(nodes)
        node_names = [n.name for n in nodes]
        self.rhv.reboot_rhv_vms(vms, timeout=timeout, wait=wait, force=force)

        if wait:
            # When the node is reachable then the node reaches status Ready.
            logger.info(f"Waiting for nodes: {node_names} to reach ready state")
            wait_for_nodes_status(
                node_names=node_names, status=constants.NODE_READY, timeout=timeout
            )

    def start_nodes(self, nodes, timeout=600, wait=True):
        """
        Start RHV VM

        Args:
            nodes (list): The OCS objects of the nodes
            wait (bool): True for waiting the instances to start, False otherwise
            timeout (int): time in seconds to wait for node to reach 'ready' state.

        """
        if not nodes:
            raise ValueError("No nodes found to start")
        vms = self.get_rhv_vm_instances(nodes)
        node_names = [n.name for n in nodes]
        self.rhv.start_rhv_vms(vms, wait=wait, timeout=timeout)

        if wait:
            # When the node is reachable then the node reaches status Ready.
            logger.info(f"Waiting for nodes: {node_names} to reach ready state")
            wait_for_nodes_status(
                node_names=node_names, status=constants.NODE_READY, timeout=timeout
            )

    def restart_nodes_by_stop_and_start(
        self, nodes, timeout=900, wait=True, force=True
    ):
        """
        Restart RHV vms by stop and start

        Args:
            nodes (list): The OCS objects of the nodes
            wait (bool): True if need to wait till the restarted node reaches
                READY state. False otherwise
            timeout (int): time in seconds to wait for node to reach 'not ready' state,
                and 'ready' state.
            force (bool): True for force VM stop, False otherwise

        """
        if not nodes:
            raise ValueError("No nodes found for restarting")
        node_names = [n.name for n in nodes]
        vms = self.get_rhv_vm_instances(nodes)
        self.rhv.restart_rhv_vms_by_stop_and_start(vms, wait=wait, force=force)

        if wait:
            logger.info(f"Waiting for nodes: {node_names} to reach ready state")
            wait_for_nodes_status(
                node_names=node_names, status=constants.NODE_READY, timeout=timeout
            )

    def restart_nodes_by_stop_and_start_teardown(self):
        """
        Make sure all RHV VMs are up by the end of the test

        """
        vm_names = self.rhv.get_vm_names()
        assert vm_names, "Failed to get VM list"
        vms = [self.rhv.get_rhv_vm_instance(vm) for vm in vm_names]

        stopping_vms = [
            vm for vm in vms if self.rhv.get_vm_status(vm) == VmStatus.POWERING_DOWN
        ]
        for vm in stopping_vms:
            # wait untill VM with powering down status changed to down status
            for status in TimeoutSampler(600, 5, self.rhv.get_vm_status, vm):
                logger.info(
                    f"Waiting for RHV Machine {vm.name} to shutdown "
                    f"Current status is : {status}"
                )
                if status == types.VmStatus.DOWN:
                    logger.info(f"RHV Machine {vm.name} reached down status")
                    break
        # Get all down Vms
        stopped_vms = [vm for vm in vms if self.rhv.get_vm_status(vm) == VmStatus.DOWN]

        # Start the VMs
        if stopped_vms:
            logger.info(
                f"The following VMs are powered off: {[vm.name for vm in stopped_vms]}"
            )
            self.rhv.start_rhv_vms(stopped_vms)


class IBMCloud(NodesBase):
    """
    IBM Cloud class
    """

    def __init__(self):
        from ocs_ci.utility import ibmcloud

        super(IBMCloud, self).__init__()
        self.ibmcloud = ibmcloud.IBMCloud()

    def restart_nodes(self, nodes, timeout=900, wait=True):
        """
        Restart all the ibmcloud vm instances

        Args:
            nodes (list): The OCS objects of the nodes instance
            timeout (int): time in seconds to wait for node to reach 'not ready' state,
                and 'ready' state.
            wait (bool): True if need to wait till the restarted node reaches
                READY state. False otherwise

        """
        self.ibmcloud.restart_nodes(nodes, timeout=900, wait=True)

    def restart_nodes_by_stop_and_start(self, nodes, force=True):
        """
        Make sure all the nodes which are not ready on IBM Cloud

        Args:
            nodes (list): The OCS objects of the nodes instance
            force (bool): True for force node stop, False otherwise

        """
        self.ibmcloud.restart_nodes_by_stop_and_start(nodes, force=True)

    def attach_volume(self, volume, node):
        self.ibmcloud.attach_volume(volume, node)

    def detach_volume(self, volume, node=None, delete_from_backend=True):
        self.ibmcloud.detach_volume(volume, node)

    def get_node_by_attached_volume(self, volume):
        return self.ibmcloud.get_node_by_attached_volume(volume)

    def get_data_volumes(self):
        return self.ibmcloud.get_data_volumes()

    def wait_for_volume_attach(self, volume):
        self.ibmcloud.wait_for_volume_attach(volume)

    def get_volume_id(self):
        return self.ibmcloud.get_volume_id()

    def delete_volume_id(self, volume):
        self.ibmcloud.delete_volume_id(volume)

    def restart_nodes_by_stop_and_start_teardown(self):
        """
        Make sure all nodes are up by the end of the test on IBM Cloud.

        """
        logger.info("restarting nodes by stop and start teardown")
        worker_nodes = get_nodes(node_type="worker")
        provider_id = worker_nodes[0].get()["spec"]["providerID"]
        cluster_id = provider_id.split("/")[5]

        worker_nodes_not_ready = []
        for worker_node in worker_nodes:
            logger.info(f"status is : {worker_node.status()}")
            if worker_node.status() != "Ready":
                worker_nodes_not_ready.append(
                    worker_node.get()["metadata"]["labels"][
                        "ibm-cloud.kubernetes.io/worker-id"
                    ]
                )

        if len(worker_nodes_not_ready) > 0:
            for not_ready_node in worker_nodes_not_ready:
                cmd = f"ibmcloud ks worker reboot --cluster {cluster_id} --worker {not_ready_node} -f"
                out = run_ibmcloud_cmd(cmd)
                logger.info(f"Node restart command output: {out}")

    def check_workers_ready_state(self, cmd):
        """
        Check if all worker nodes are in Ready state.

        Args:
            cmd (str): command to get the workers

        Returns:
            bool: 'True' if all the node names appeared in 'Ready'
            else 'False'

        """
        logger.info("Getting all workers status")
        out = run_ibmcloud_cmd(cmd)
        worker_nodes = json.loads(out)
        for worker_node in worker_nodes:
            node_id = worker_node["id"]
            logger.info(f"{node_id} status is : {worker_node['health']['message']}")
            if worker_node["health"]["message"] != "Ready":
                return False

        return True

    def create_nodes(self, node_conf, node_type, num_nodes):
        """
        Creates new node on IBM Cloud.

        Args:
            node_conf (dict): of node configuration
            node_type (str): type of node to be created
            num_nodes (int): Number of node instances to be created

        Returns:
           list: of IBMCloudNode objects

        Raises:
           NotAllNodesCreated: In case all nodes are not created
           TimeoutExpiredError: In case node is not created in time

        """
        logger.info("creating new node")

        worker_nodes = get_nodes(node_type="worker")
        provider_id = worker_nodes[0].get()["spec"]["providerID"]
        cluster_id = provider_id.split("/")[5]

        cmd = f"ibmcloud ks worker-pool get --cluster {cluster_id} --worker-pool default --output json"
        out = run_ibmcloud_cmd(cmd)
        cluster_zones = json.loads(out)
        workers_per_zone = cluster_zones["zones"][0]["workerCount"]
        logger.info(f"workers_per_zone value is:{workers_per_zone}")

        no_of_nodes = workers_per_zone + int(num_nodes)
        logger.info(f"number of nodes going to be add in each zone are : {num_nodes}")

        cmd = (
            f"ibmcloud ks worker-pool resize --cluster {cluster_id} --worker-pool default"
            f"  --size-per-zone {no_of_nodes}"
        )
        run_ibmcloud_cmd(cmd)

        logger.info(
            "Waiting for 60 seconds to execute above command to create new node"
        )
        time.sleep(60)

        cmd = f"ibmcloud ks workers --cluster {cluster_id} --output json"
        worker_nodes_not_ready = []

        sample = TimeoutSampler(
            timeout=1800,
            sleep=3,
            func=self.check_workers_ready_state,
            cmd=cmd,
        )

        if not sample.wait_for_func_status(result=True):
            logger.error("Failed to create nodes")
            raise TimeoutExpiredError("Failed to create nodes")

        cmd = f"ibmcloud ks workers --cluster {cluster_id} --output json"
        out = run_ibmcloud_cmd(cmd)
        worker_nodes = json.loads(out)

        cmd = f"ibmcloud ks worker-pool zones --cluster {cluster_id} --worker-pool default --output json"
        out = run_ibmcloud_cmd(cmd)
        cluster_zones = json.loads(out)
        workers_per_zone = cluster_zones["zones"]
        no_of_zones = len(workers_per_zone)
        total_no_of_nodes = no_of_nodes * no_of_zones
        logger.info(f"total_no_nodes values is:{total_no_of_nodes}")

        if len(worker_nodes) != total_no_of_nodes:
            logger.info("Expected nodes are not created")
            raise NotAllNodesCreated(
                f"Expected number of nodes is {no_of_nodes} but created during deployment is {len(worker_nodes)}"
            )

        nodes_list = []
        for worker_node in worker_nodes:
            node_id = worker_node["id"]
            if worker_node["health"]["message"] != "Ready":
                worker_nodes_not_ready.append(node_id)
            nodes_list.append(node_id)

        if len(worker_nodes_not_ready) > 0:
            logger.info("Expected nodes are not created")
            raise NotAllNodesCreated("Nodes are not created successfully")
        return nodes_list

    def create_and_attach_nodes_to_cluster(self, node_conf, node_type, num_nodes):
        """
        Create nodes and attach them to cluster
        Use this function if you want to do both creation/attachment in
        a single call

        Args:
            node_conf (dict): of node configuration
            node_type (str): type of node to be created
            num_nodes (int): Number of node instances to be created

        """
        self.create_nodes(node_conf, node_type, num_nodes)


class VMWareIPINodes(VMWareNodes):
    """
    VMWare IPI nodes class

    """

    def __init__(self):
        super().__init__()

    def get_vms(self, nodes):
        """
        Get vSphere vm objects list in the Datacenter(and not just in the cluster scope).
        Note: If one of the nodes failed with an exception, it will not return his
        corresponding VM object.

        Args:
            nodes (list): The OCS objects of the nodes

        Returns:
            list: vSphere vm objects list in the Datacenter

        """
        vms_in_dc = self.vsphere.get_all_vms_in_dc(self.datacenter)
        node_names = set([node.name for node in nodes])
        vms = []
        for vm in vms_in_dc:
            try:
                vm_name = vm.name
                if vm_name in node_names:
                    vms.append(vm)
            except Exception as e:
                logger.info(f"Failed to get the vm name due to exception: {e}")

        if len(vms) < len(nodes):
            logger.warning("Didn't find all the VM objects for all the nodes")

        return vms


class VMWareUPINodes(VMWareNodes):
    """
    VMWare UPI nodes class

    """

    def __init__(self):
        super().__init__()

    def terminate_nodes(self, nodes, wait=True):
        """
        Terminate the VMs.
        The VMs will be deleted only from the inventory and not from the disk.
        After deleting the VMs, it will also modify terraform state file of the removed VMs

        Args:
            nodes (list): The OCS objects of the nodes
            wait (bool): True for waiting the VMs to terminate,
            False otherwise

        """
        # Save the names of the VMs before removing them
        vms = self.get_vms(nodes)
        vm_names = [vm.name for vm in vms]

        logger.info(f"Terminating nodes: {vm_names}")
        super().terminate_nodes(nodes, wait)

        if config.ENV_DATA.get("rhel_user"):
            node_type = constants.RHEL_OS
        else:
            node_type = constants.RHCOS
        node_cls_obj = VSPHEREUPINode(
            node_conf={}, node_type=node_type, compute_count=0
        )
        logger.info(f"Modifying terraform state file of the removed VMs {vm_names}")
        for vm_name in vm_names:
            node_cls_obj.change_terraform_statefile_after_remove_vm(vm_name)
            node_cls_obj.change_terraform_tfvars_after_remove_vm()


class GCPNodes(NodesBase):
    """
    Google Cloud Platform Nodes class

    """

    def __init__(self):
        super(GCPNodes, self).__init__()
        from ocs_ci.utility import gcp

        self.gcp = gcp.GoogleCloud()

    def stop_nodes(self, nodes, wait=True):
        """
        Stop nodes

        Args:
            nodes (list): The OCS objects of the nodes
            wait (bool): True for waiting the instances to stop, False otherwise

        Raises:
            OperationFailedToCompleteException: In case that not all the operations completed successfully

        """
        node_names = [n.name for n in nodes]
        self.gcp.stop_instances(node_names)

    def start_nodes(self, nodes, wait=True):
        """
        Start nodes

        Args:
            nodes (list): The OCS objects of the nodes
            wait (bool): True for waiting the instances to start, False otherwise

        Raises:
            OperationFailedToCompleteException: In case that not all the operations completed successfully

        """
        node_names = [n.name for n in nodes]
        self.gcp.start_instances(node_names)

    def restart_nodes(self, nodes, wait=True):
        """
        Restart nodes. This is a hard reset - the instance does not do a graceful shutdown

        Args:
            nodes (list): The OCS objects of the nodes
            wait (bool): True for waiting the instances to start, False otherwise

        Raises:
            OperationFailedToCompleteException: In case that not all the operations completed successfully

        """
        node_names = [n.name for n in nodes]
        self.gcp.restart_instances(node_names, wait)

    def terminate_nodes(self, nodes, wait=True):
        """
        Terminate nodes

        Args:
            nodes (list): The OCS objects of the nodes
            wait (bool): True for waiting the instances to terminate, False otherwise

        Raises:
            OperationFailedToCompleteException: In case that not all the operations completed successfully

        """
        node_names = [n.name for n in nodes]
        self.gcp.terminate_instances(node_names, wait)

    def restart_nodes_by_stop_and_start(self, nodes, wait=True, force=True):
        """
        Restart nodes by stop and start

        Args:
            nodes (list): The OCS objects of the nodes
            wait (bool): True for waiting the instances to stop, False otherwise
            force (bool): True for force node stop, False otherwise

        Raises:
            OperationFailedToCompleteException: In case that not all the operations completed successfully

        """
        node_names = [n.name for n in nodes]
        # In the Google Compute Engine instance, the stop operation is a clean shutdown without force.
        # To perform a force stop and start, we need to use the GCP restart method, which performs a hard reset.
        if force:
            self.gcp.restart_instances(node_names, wait)
        else:
            self.gcp.restart_instances_by_stop_and_start(node_names, wait)

    def restart_nodes_by_stop_and_start_teardown(self):
        """
        Start the nodes in a NotReady state

        Raises:
            OperationFailedToCompleteException: In case that not all the operations completed successfully

        """
        not_ready_nodes = get_nodes_in_statuses([constants.NODE_NOT_READY])
        node_names = [n.name for n in not_ready_nodes]
        if node_names:
            self.gcp.start_instances(node_names)


class IBMCloudBMNodes(NodesBase):
    """
    IBM Cloud for Bare metal machines class

    """

    def __init__(self):
        super(IBMCloudBMNodes, self).__init__()
        from ocs_ci.utility import ibmcloud_bm

        self.ibmcloud_bm = ibmcloud_bm.IBMCloudBM()

    def get_machines(self, nodes):
        """
        Get the machines associated with the given nodes

        Args:
            nodes (list): The OCS objects of the nodes

        Returns:
            list: List of dictionaries. List of the machines associated with the given nodes

        """
        node_names = [n.name for n in nodes]
        return self.ibmcloud_bm.get_machines_by_names(node_names)

    def stop_nodes(self, nodes, wait=True):
        """
        Stop nodes

        Args:
            nodes (list): The OCS objects of the nodes
            wait (bool): If True, wait for the nodes to be in a NotReady state. False, otherwise

        """
        machines = self.get_machines(nodes)
        self.ibmcloud_bm.stop_machines(machines)
        if wait:
            node_names = [n.name for n in nodes]
            wait_for_nodes_status(
                node_names, constants.NODE_NOT_READY, timeout=180, sleep=5
            )

    def start_nodes(self, nodes, wait=True):
        """
        Start nodes

        Args:
            nodes (list): The OCS objects of the nodes
            wait (bool): If True, wait for the nodes to be ready. False, otherwise

        """
        machines = self.get_machines(nodes)
        self.ibmcloud_bm.start_machines(machines)
        if wait:
            node_names = [n.name for n in nodes]
            wait_for_nodes_status(
                node_names, constants.NODE_READY, timeout=720, sleep=20
            )

    def restart_nodes(self, nodes, wait=True, force=False):
        """
        Restart nodes

        Args:
            nodes (list): The OCS objects of the nodes
            wait (bool): If True, wait for the nodes to be ready. False, otherwise
            force (bool): If True, it will force restarting the nodes. False, otherwise.
                Default value is False.

        """
        machines = self.get_machines(nodes)
        self.ibmcloud_bm.restart_machines(machines, force=force)
        if wait:
            node_names = [n.name for n in nodes]
            logger.info(
                f"Wait for the nodes {node_names} to reach the status {constants.NODE_NOT_READY}"
            )
            wait_for_nodes_status(
                node_names, constants.NODE_NOT_READY, timeout=180, sleep=5
            )
            logger.info(
                f"Wait for the nodes {node_names} to be in a Ready status again"
            )
            wait_for_nodes_status(
                node_names, constants.NODE_READY, timeout=720, sleep=20
            )

    def restart_nodes_by_stop_and_start(self, nodes, wait=True):
        """
        Restart the nodes by stop and start

        Args:
            nodes (list): The OCS objects of the nodes
            wait (bool): If True, wait for the nodes to be ready. False, otherwise

        """
        self.stop_nodes(nodes, wait=True)
        self.start_nodes(nodes, wait=wait)

    def restart_nodes_by_stop_and_start_teardown(self):
        """
        Start the nodes in a NotReady state

        """
        nodes_not_ready = get_nodes_in_statuses([constants.NODE_NOT_READY])
        machines = self.get_machines(nodes_not_ready)
        self.ibmcloud_bm.start_machines(machines)

    def create_nodes(self, node_conf, node_type, num_nodes):
        """
        Create nodes

        """
        raise NotImplementedError("Create nodes functionality not implemented")

    def terminate_nodes(self, nodes, wait=True):
        """
        Terminate nodes

        """
        raise NotImplementedError("terminate nodes functionality is not implemented")
