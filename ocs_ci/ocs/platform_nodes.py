import json
import logging
import os
import re
import shutil
import time


import boto3
import yaml

from ocs_ci.deployment.terraform import Terraform
from ocs_ci.deployment.vmware import (
    clone_openshift_installer,
    update_machine_conf,
)
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.framework import config, merge_dict
from ocs_ci.utility import aws, vsphere, templating, baremetal, azure_utils
from ocs_ci.utility.retry import retry
from ocs_ci.utility.csr import approve_pending_csr
from ocs_ci.ocs import constants, ocp, exceptions
from ocs_ci.ocs.node import (
    get_node_objs, get_typed_worker_nodes, get_typed_nodes,
)
from ocs_ci.ocs.resources.pvc import get_deviceset_pvs
from ocs_ci.ocs.resources import pod
from ocs_ci.utility.csr import (
    get_nodes_csr, wait_for_all_nodes_csr_and_approve,
)
from ocs_ci.utility.utils import (
    get_cluster_name, get_infra_id, create_rhelpod,
    replace_content_in_file,
    get_ocp_version, TimeoutSampler,
    delete_file, AZInfo, download_file_from_git_repo,
)
from ocs_ci.ocs.node import wait_for_nodes_status

logger = logging.getLogger(__name__)


class PlatformNodesFactory:
    """
    A factory class to get specific nodes platform object

    """
    def __init__(self):
        self.cls_map = {
            'AWS': AWSNodes,
            'vsphere': VMWareNodes,
            'aws': AWSNodes,
            'baremetal': BaremetalNodes,
            'azure': AZURENodes,
            'gcp': NodesBase
        }

    def get_nodes_platform(self):
        platform = config.ENV_DATA['platform']
        return self.cls_map[platform]()


class NodesBase(object):
    """
    A base class for nodes related operations.
    Should be inherited by specific platform classes

    """
    def __init__(self):
        self.cluster_path = config.ENV_DATA['cluster_path']
        self.platform = config.ENV_DATA['platform']
        self.deployment_type = config.ENV_DATA['deployment_type']
        self.nodes_map = {
            'AWSUPINode': AWSUPINode, 'VSPHEREUPINode': VSPHEREUPINode
        }
        self.wait_time = 120

    def get_data_volumes(self):
        raise NotImplementedError(
            "Get data volume functionality is not implemented"
        )

    def get_node_by_attached_volume(self, volume):
        raise NotImplementedError(
            "Get node by attached volume functionality is not implemented"
        )

    def stop_nodes(self, nodes):
        raise NotImplementedError(
            "Stop nodes functionality is not implemented"
        )

    def start_nodes(self, nodes):
        raise NotImplementedError(
            "Start nodes functionality is not implemented"
        )

    def restart_nodes(self, nodes, wait=True):
        raise NotImplementedError(
            "Restart nodes functionality is not implemented"
        )

    def restart_nodes_by_stop_and_start(self, nodes, force=True):
        raise NotImplementedError(
            "Restart nodes by stop and start functionality is not implemented"
        )

    def detach_volume(self, volume, node=None, delete_from_backend=True):
        raise NotImplementedError(
            "Detach volume functionality is not implemented"
        )

    def attach_volume(self, volume, node):
        raise NotImplementedError(
            "Attach volume functionality is not implemented"
        )

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
        raise NotImplementedError(
            "Create nodes functionality not implemented"
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
        assert os.path.exists(default_config_path), (
            'Config file doesnt exists'
        )

        with open(default_config_path) as f:
            default_config_dict = yaml.safe_load(f)

        return default_config_dict


class VMWareNodes(NodesBase):
    """
    VMWare nodes class

    """
    def __init__(self):
        super(VMWareNodes, self).__init__()
        self.cluster_name = config.ENV_DATA.get("cluster_name")
        self.server = config.ENV_DATA['vsphere_server']
        self.user = config.ENV_DATA['vsphere_user']
        self.password = config.ENV_DATA['vsphere_password']
        self.cluster = config.ENV_DATA['vsphere_cluster']
        self.datacenter = config.ENV_DATA['vsphere_datacenter']
        self.datastore = config.ENV_DATA['vsphere_datastore']
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
        node_names = [node.get().get('metadata').get('name') for node in nodes]
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
        return [
            pv.get().get('spec').get('vsphereVolume').get('volumePath') for pv in pvs
        ]

    def get_node_by_attached_volume(self, volume):
        raise NotImplementedError(
            "get node by attached volume functionality is not implemented"
        )

    def stop_nodes(self, nodes, force=True):
        """
        Stop vSphere VMs

        Args:
            nodes (list): The OCS objects of the nodes
            force (bool): True for force VM stop, False otherwise

        """
        vms = self.get_vms(nodes)
        assert vms, (
            f"Failed to get VM objects for nodes {[n.name for n in nodes]}"
        )
        self.vsphere.stop_vms(vms, force=force)

    def start_nodes(self, nodes, wait=True):
        """
        Start vSphere VMs

        Args:
            nodes (list): The OCS objects of the nodes

        """
        vms = self.get_vms(nodes)
        assert vms, (
            f"Failed to get VM objects for nodes {[n.name for n in nodes]}"
        )
        self.vsphere.start_vms(vms)

    def restart_nodes(self, nodes, force=True, timeout=300, wait=True):
        """
        Restart vSphere VMs

        Args:
            nodes (list): The OCS objects of the nodes
            force (bool): True for Hard reboot, False for Soft reboot
            timeout (int): time in seconds to wait for node to reach 'not ready' state,
                and 'ready' state.
            wait (bool): True if need to wait till the restarted OCP node
                reaches READY state. False otherwise

        """
        vms = self.get_vms(nodes)
        assert vms, (
            f"Failed to get VM objects for nodes {[n.name for n in nodes]}"
        )
        self.vsphere.restart_vms(vms, force=force)

        if wait:
            """
            When reboot is initiated on a VM from the VMware, the VM
            stays at "Running" state throughout the reboot operation.

            Once the OCP node detects that the node is not reachable then the
            node reaches status NotReady.
            When the reboot operation is completed and the VM is reachable the
            OCP node reaches status Ready.
            """
            nodes_names = [n.name for n in nodes]
            wait_for_nodes_status(
                node_names=nodes_names, status=constants.NODE_NOT_READY,
                timeout=timeout
            )
            wait_for_nodes_status(
                node_names=nodes_names, status=constants.NODE_READY,
                timeout=timeout
            )

    def restart_nodes_by_stop_and_start(self, nodes, force=True):
        """
        Restart vSphere VMs

        Args:
            nodes (list): The OCS objects of the nodes
            force (bool): True for force VM stop, False otherwise

        """
        vms = self.get_vms(nodes)
        assert vms, (
            f"Failed to get VM objects for nodes {[n.name for n in nodes]}"
        )
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
            vm=vm, identifier=volume, key='volume_path',
            datastore=delete_from_backend
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
        assert vms, (
            f"Failed to get VM objects for nodes {[n.name for n in self.cluster_nodes]}"
        )
        stopped_vms = [
            vm for vm in vms if self.vsphere.get_vm_power_status(vm) == constants.VM_POWERED_OFF
        ]
        # Start the VMs
        if stopped_vms:
            logger.info(f"The following VMs are powered off: {stopped_vms}")
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
            f'{self.platform.upper()}{self.deployment_type.upper()}Node'
        ]
        node_cls_obj = node_cls(node_conf, node_type, num_nodes)
        node_cls_obj.add_node()


class AWSNodes(NodesBase):
    """
    AWS EC2 instances class

    """
    def __init__(self):
        super(AWSNodes, self).__init__()
        self.aws = aws.AWS()
        self.az = AZInfo()

    def get_ec2_instances(self, nodes):
        """
        Get the EC2 instances dicts

        Args:
            nodes (list): The OCS objects of the nodes

        Returns:
            dict: The EC2 instances dicts (IDs and names)

        """
        return aws.get_instances_ids_and_names(nodes)

    def get_data_volumes(self):
        """
        Get the data EBS volumes

        Returns:
            list: EBS Volume instances

        """
        pvs = get_deviceset_pvs()
        return aws.get_data_volumes(pvs)

    def get_node_by_attached_volume(self, volume):
        """
        Get node OCS object of the EC2 instance that has the volume attached to

        Args:
            volume (Volume): The volume to get the EC2 according to

        Returns:
            OCS: The OCS object of the EC2 instance

        """
        instance_ids = [
            at.get('InstanceId') for at in volume.attachments
        ]
        assert instance_ids, (
            f"EBS Volume {volume.id} is not attached to any EC2 instance"
        )
        instance_id = instance_ids[0]
        all_nodes = get_node_objs()
        nodes = [
            n for n in all_nodes if instance_id in n.get()
            .get('spec').get('providerID')
        ]
        assert nodes, (
            f"Failed to find the OCS object for EC2 instance {instance_id}"
        )
        return nodes[0]

    def stop_nodes(self, nodes, wait=True):
        """
        Stop EC2 instances

        Args:
            nodes (list): The OCS objects of the nodes
            wait (bool): True for waiting the instances to stop, False otherwise


        """
        instances = self.get_ec2_instances(nodes)
        assert instances, (
            f"Failed to get the EC2 instances for nodes {[n.name for n in nodes]}"
        )
        self.aws.stop_ec2_instances(instances=instances, wait=wait)

    def start_nodes(self, instances=None, nodes=None, wait=True):
        """
        Start EC2 instances

        Args:
            nodes (list): The OCS objects of the nodes
            instances (dict): instance-id and name dict
            wait (bool): True for waiting the instances to start, False otherwise

        """
        instances = instances or self.get_ec2_instances(nodes)
        assert instances, (
            f"Failed to get the EC2 instances for nodes {[n.name for n in nodes]}"
        )
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
        instances = self.get_ec2_instances(nodes)
        assert instances, (
            f"Failed to get the EC2 instances for "
            f"nodes {[n.name for n in nodes]}"
        )
        self.aws.restart_ec2_instances(instances=instances)
        if wait:
            """
            When reboot is initiated on an instance from the AWS, the
            instance stays at "Running" state throughout the reboot operation.

            Once the OCP node detects that the node is not reachable then the
            node reaches status NotReady.
            When the reboot operation is completed and the instance is
            reachable the OCP node reaches status Ready.
            """
            nodes_names = [n.name for n in nodes]
            logger.info(
                f"Waiting for nodes: {nodes_names} to reach not ready state"
            )
            wait_for_nodes_status(
                node_names=nodes_names, status=constants.NODE_NOT_READY,
                timeout=timeout
            )
            logger.info(
                f"Waiting for nodes: {nodes_names} to reach ready state"
            )
            wait_for_nodes_status(
                node_names=nodes_names, status=constants.NODE_READY,
                timeout=timeout
            )

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
        assert instances, (
            f"Failed to get the EC2 instances for nodes {[n.name for n in nodes]}"
        )
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
        volume_attachments = [
            at.get('InstanceId') for at in volume.attachments
        ]
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
            for sample in TimeoutSampler(
                300, 3, get_volume_attachments, volume
            ):
                logger.info(
                    f"EBS volume {volume.id} attachments are: {sample}"
                )
                if sample:
                    return True
        except TimeoutExpiredError:
            logger.error(
                f"Volume {volume.id} failed to be attached to an EC2 instance"
            )
            return False

    def restart_nodes_by_stop_and_start_teardown(self):
        """
        Make sure all EC2 instances are up. To be used in the test teardown

        """
        # Get the cluster nodes ec2 instances
        self.cluster_nodes = get_node_objs()
        ec2_instances = self.get_ec2_instances(self.cluster_nodes)
        assert ec2_instances, (
            f"Failed to get ec2 instances for nodes {[n.name for n in self.cluster_nodes]}"
        )

        logger.info(
            "Getting the instances that are in status 'stopping' (if there are any), "
            "and wait for them to get to status 'stopped', "
            "so it will be possible to start them"
        )
        stopping_instances = {
            key: val for key, val in ec2_instances.items() if
            self.aws.get_instances_status_by_id(key) == constants.INSTANCE_STOPPING
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
            key: val for key, val in ec2_instances.items() if
            self.aws.get_instances_status_by_id(key) == constants.INSTANCE_STOPPED
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
            f'{self.platform.upper()}{self.deployment_type.upper()}Node'
        ]

        if node_type.upper() == 'RHCOS':
            workers_stacks = self.aws.get_worker_stacks()
            logger.info(f"Existing worker stacks: {workers_stacks}")
            existing_indexes = self.get_existing_indexes(workers_stacks)
            logger.info(f"Existing indexes: {existing_indexes}")
            slots_available = self.get_available_slots(existing_indexes, num_nodes)
            logger.info(f"Available indexes: {slots_available}")
            for slot in slots_available:
                node_conf['zone'] = self.az.get_zone_number()
                node_id = slot
                node_list.append(node_cls(node_conf, node_type))
                node_list[-1]._prepare_node(node_id)
        elif node_type.upper() == 'RHEL':
            rhel_workers = len(get_typed_worker_nodes('rhel'))
            for i in range(num_nodes):
                node_conf['zone'] = self.az.get_zone_number()
                node_id = i + rhel_workers
                node_list.append(node_cls(node_conf, node_type))
                node_list[-1]._prepare_node(node_id)

        # Make sure that csr is approved for all the nodes
        # not making use of csr.py functions as aws rhcos has long
        # delays for csr to appear
        if node_type.upper() == 'RHCOS':
            self.approve_all_nodes_csr(node_list)

        return node_list

    @retry(
        (exceptions.PendingCSRException, exceptions.TimeoutExpiredError),
        tries=4,
        delay=10,
        backoff=1
    )
    def approve_all_nodes_csr(self, node_list):
        """
        Make sure that all the newly added nodes are in approved csr state

        Args:
            node_list (list): of AWSUPINode/AWSIPINode objects

        Raises:
             PendingCSRException: If any pending csrs exists

        """
        node_names = [
            node.aws_instance_obj.private_dns_name for node in node_list
        ]

        sample = TimeoutSampler(
            timeout=600, sleep=3, func=self.all_nodes_found,
            node_names=node_names
        )
        if not sample.wait_for_func_status(result=True):
            raise exceptions.PendingCSRException(
                "All nodes csr not approved"
            )

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
        nodes_wide_out = oc_obj.exec_oc_cmd(
            get_nodes_cmd, out_yaml_format=False
        )
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
            temp.append(int(re.findall(r'\d+', index.split('-')[-1])[-1]))
        temp.sort()
        return temp

    def attach_nodes_to_cluster(self, node_list):
        """
        Attach nodes in the list to the cluster

        Args:
            node_list (list): of AWSUPINode/AWSIPINode objects

        """
        if self.deployment_type.lower() == 'upi':
            self.attach_nodes_to_upi_cluster(node_list)

    def attach_nodes_to_upi_cluster(self, node_list):
        """
        Attach node to upi cluster
        Note: For RHCOS nodes, create function itself would have
        attached the nodes to cluster so nothing to do here

        Args:
            node_list (list): of AWSUPINode objects

        """
        if node_list[0].node_type == 'RHEL':
            self.attach_rhel_nodes_to_upi_cluster(node_list)

    def attach_rhel_nodes_to_upi_cluster(self, node_list):
        """
        Attach RHEL nodes to upi cluster

        Args:
            node_list (list): of AWSUPINode objects with RHEL os

        """
        rhel_pod_name = "rhel-ansible"
        rhel_pod_obj = create_rhelpod(
            constants.DEFAULT_NAMESPACE, rhel_pod_name, 600
        )
        timeout = 4000  # For ansible-playbook

        # copy openshift-dev.pem to RHEL ansible pod
        pem_src_path = "~/.ssh/openshift-dev.pem"
        pem_dst_path = "/openshift-dev.pem"
        pod.upload(rhel_pod_obj.name, pem_src_path, pem_dst_path)
        repo_dst_path = constants.YUM_REPOS_PATH
        repo = os.path.join(
            constants.REPO_DIR, f"ocp_{get_ocp_version('_')}.repo"
        )
        assert os.path.exists(repo), f"Required repo file {repo} doesn't exist!"
        repo_file = os.path.basename(repo)
        pod.upload(
            rhel_pod_obj.name, repo, repo_dst_path
        )
        # copy the .pem file for our internal repo on all nodes
        # including ansible pod
        # get it from URL
        mirror_pem_file_path = os.path.join(
            constants.DATA_DIR,
            constants.INTERNAL_MIRROR_PEM_FILE
        )
        dst = constants.PEM_PATH
        pod.upload(rhel_pod_obj.name, mirror_pem_file_path, dst)
        # Install scp on pod
        rhel_pod_obj.install_packages("openssh-clients")
        # distribute repo file to all RHEL workers
        hosts = [
            node.aws_instance_obj.private_dns_name for node in
            node_list
        ]
        # Check whether every host is acceptin ssh connections
        for host in hosts:
            self.check_connection(rhel_pod_obj, host, pem_dst_path)

        for host in hosts:
            disable = "sudo yum-config-manager --disable *"
            rhel_pod_obj.exec_cmd_on_node(
                host, pem_dst_path, disable, user=constants.EC2_USER
            )
            rhel_pod_obj.copy_to_server(
                host, pem_dst_path,
                os.path.join(repo_dst_path, repo_file),
                os.path.join('/tmp', repo_file),
                user=constants.EC2_USER
            )
            rhel_pod_obj.exec_cmd_on_node(
                host, pem_dst_path,
                f'sudo mv {os.path.join("/tmp", repo_file)} {repo_dst_path}',
                user=constants.EC2_USER
            )
            rhel_pod_obj.copy_to_server(
                host, pem_dst_path,
                os.path.join(dst, constants.INTERNAL_MIRROR_PEM_FILE),
                os.path.join('/tmp', constants.INTERNAL_MIRROR_PEM_FILE),
                user=constants.EC2_USER,
            )
            cmd = (
                f'sudo mv '
                f'{os.path.join("/tmp/", constants.INTERNAL_MIRROR_PEM_FILE)} '
                f'{dst}'
            )
            rhel_pod_obj.exec_cmd_on_node(
                host, pem_dst_path,
                cmd, user=constants.EC2_USER
            )
        # copy kubeconfig to pod
        kubeconfig = os.path.join(
            self.cluster_path, config.RUN.get('kubeconfig_location')
        )
        pod.upload(rhel_pod_obj.name, kubeconfig, "/")
        pull_secret_path = os.path.join(
            constants.TOP_DIR,
            "data",
            "pull-secret"
        )
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

            rhel_pod_obj.exec_cmd_on_pod(
                cmd, out_yaml_format=False, timeout=timeout
            )
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
        ocp_obj = ocp.OCP(kind='node')
        node_info = ocp_obj.get()
        for i in range(len(hosts)):
            for entry in node_info['items']:
                for each in entry['status']['addresses']:
                    if each['type'] == 'Hostname':
                        if each['address'] in hosts:
                            logging.info(
                                f"Checking status for {each['address']}"
                            )
                            sample = TimeoutSampler(
                                timeout, 3,
                                self.get_ready_status, entry
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
        for cond in node_info['status']['conditions']:
            if cond['type'] == 'Ready':
                if not cond['status'] == "True":
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
        ansible_host_file['ansible_user'] = constants.EC2_USER
        ansible_host_file['ansible_become'] = 'True'
        ansible_host_file['pod_kubeconfig'] = '/kubeconfig'
        ansible_host_file['pod_pull_secret'] = '/tmp/pull-secret'
        ansible_host_file['rhel_worker_nodes'] = hosts

        logging.info(ansible_host_file)
        data = _templating.render_template(
            constants.ANSIBLE_INVENTORY_YAML,
            ansible_host_file,
        )
        logging.debug("Ansible hosts file:%s", data)
        host_file_path = "/tmp/hosts"
        with open(host_file_path, 'w') as f:
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
        cmd = 'ls'
        rhel_pod_obj.exec_cmd_on_node(
            host, pem_dst_path, cmd, user=constants.EC2_USER
        )

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
        self.region = config.ENV_DATA['region']
        self.cluster_name = get_cluster_name(self.cluster_path)
        self.client = boto3.client(
            'ec2', region_name=self.region
        )
        # cloudformation
        self.cf = self.aws.cf_client
        self.infra_id = get_infra_id(self.cluster_path)

    def _prepare_node(self, node_id):
        """
        Create AWS instance of the node

        Args:
            node_id (int): Unique integer id for node

        """
        if self.node_type == 'RHEL':
            conf = self._prepare_rhel_node_conf()
            conf['node_id'] = node_id
            try:
                self.aws_instance_obj = self._prepare_upi_rhel_node(conf)
            except Exception:
                logger.error("Failed to create RHEL node")
                raise
        elif self.node_type == 'RHCOS':
            conf = self._prepare_rhcos_node_conf()
            conf['node_id'] = node_id
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
        default_conf = conf.get('ENV_DATA')
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
        stack_name = conf.get('stack_name')
        if conf.get('stack_name'):
            suffix = stack_name.split('-')[-1]
        else:
            suffix = f"no{conf.get('zone')}"

        self.gather_worker_data(suffix)
        worker_template_path = self.get_rhcos_worker_template()
        self.bucket_name = constants.AWS_S3_UPI_BUCKET
        self.template_obj_key = f'{self.cluster_name}-workertemplate'
        self.add_cert_to_template(worker_template_path)
        self.aws.upload_file_to_s3_bucket(
            self.bucket_name, self.template_obj_key, worker_template_path
        )
        s3_url = self.aws.get_s3_bucket_object_url(
            self.bucket_name, self.template_obj_key
        )
        params_list = self.build_stack_params(
            conf['node_id'], conf
        )
        capabilities = ['CAPABILITY_NAMED_IAM']
        self.stack_name, self.stack_id = self.aws.create_stack(
            s3_url, conf['node_id'], params_list, capabilities
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
        pk = 'ParameterKey'
        pv = 'ParameterValue'

        param_list.append({pk: 'Index', pv: str(index)})
        param_list.append({pk: 'InfrastructureName', pv: self.infra_id})
        param_list.append({pk: 'RhcosAmi', pv: self.worker_image_id})
        param_list.append(
            {
                pk: 'IgnitionLocation', pv: self.worker_ignition_location
            }
        )
        param_list.append({pk: 'Subnet', pv: self.worker_subnet})
        param_list.append(
            {
                pk: 'WorkerSecurityGroupId',
                pv: self.worker_security_group[0].get('GroupId')
            }
        )
        param_list.append(
            {
                pk: 'WorkerInstanceProfileName', pv: self.worker_instance_profile
            }
        )
        param_list.append(
            {
                pk: 'WorkerInstanceType', pv: conf['worker_instance_type']
            }
        )

        return param_list

    def add_cert_to_template(self, worker_template_path):
        """
        Add cert to worker template

        Args:
            worker_template_path (str): Path where template file is located

        """
        worker_ignition_path = os.path.join(
            self.cluster_path,
            constants.WORKER_IGN
        )
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
                r'{}'.format(search_str),
                r'{}'.format(cert),
                orig_content
            )
        with open(temp, 'w') as wfp:
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
            tls_data = content.get('ignition').get('security').get('tls')
            cert_content = tls_data.get('certificateAuthorities')[0].get(
                'source'
            )
            formatted_cert = cert_content.split(',')[1]
        return formatted_cert

    def get_rhcos_worker_template(self):
        """
        Download template and keep it locally

        Returns:
            path (str): local path to template file

        """
        common_base = 'functionality-testing'
        ocp_version = get_ocp_version('_')
        relative_template_path = os.path.join(
            f'aos-{ocp_version}',
            'hosts/upi_on_aws-cloudformation-templates'
        )

        path_to_file = os.path.join(
            f'{common_base}',
            f'{relative_template_path}',
            f'{constants.AWS_WORKER_NODE_TEMPLATE}'
        )
        logger.info(
            f"Getting file '{path_to_file}' from "
            f"git repository {constants.OCP_QE_MISC_REPO}"
        )
        tmp_file = os.path.join(
            '/tmp', constants.AWS_WORKER_NODE_TEMPLATE
        )
        download_file_from_git_repo(constants.OCP_QE_MISC_REPO, path_to_file, tmp_file)
        return tmp_file

    def _prepare_rhel_node_conf(self):
        """
        Merge default RHEL node config with the user provided
        config

        """
        conf = self.read_default_config(constants.RHEL_WORKERS_CONF)
        default_conf = conf.get('ENV_DATA')
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
        node_id = node_conf['node_id']
        zone = node_conf.get('zone')
        logger.info("Creating RHEL worker node")
        self.gather_worker_data(f'no{zone}')
        response = self.client.run_instances(
            BlockDeviceMappings=[
                {
                    'DeviceName': node_conf['root_disk'],
                    'Ebs': {
                        'DeleteOnTermination': True,
                        'VolumeSize': node_conf['root_disk_size'],
                        'VolumeType': 'gp2'
                    },
                },
            ],
            ImageId=node_conf['rhel_worker_ami'],
            SubnetId=self.worker_subnet,
            InstanceType=node_conf['rhel_worker_instance_type'],
            MaxCount=1,
            MinCount=1,
            Monitoring={
                'Enabled': False
            },
            SecurityGroupIds=[
                self.worker_security_group[0]['GroupId'],
            ],
            KeyName='openshift-dev'
        )
        inst_id = response['Instances'][0]['InstanceId']
        worker_ec2 = boto3.resource('ec2', region_name=self.region)
        worker_instance = worker_ec2.Instance(inst_id)
        worker_instance.wait_until_running()
        worker_name = f'{cluster_id}-rhel-worker-{node_id}'
        worker_ec2.create_tags(
            Resources=[inst_id],
            Tags=[
                {'Key': 'Name', 'Value': f'{worker_name}'},
                {'Key': self.worker_tag[0], 'Value': self.worker_tag[1]}
            ]
        )
        logging.info(self.worker_iam_role)
        self.client.associate_iam_instance_profile(
            IamInstanceProfile=self.worker_iam_role,
            InstanceId=inst_id,
        )
        return worker_instance

    def gather_worker_data(self, suffix='no0'):
        """
        Gather various info like vpc, iam role, subnet,security group,
        cluster tag from existing RHCOS workers

        Args:
            suffix (str): suffix to get resource of worker node, 'no0' by default

        """
        stack_name = f'{self.cluster_name}-{suffix}'
        resource = self.cf.list_stack_resources(StackName=stack_name)
        worker_id = self.get_worker_resource_id(resource)
        ec2 = boto3.resource('ec2', region_name=self.region)
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
        del self.worker_iam_role['Id']

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
            if 'kubernetes' in each['Key']:
                return each['Key'], each['Value']

    def get_worker_resource_id(self, resource):
        """
        Get the resource ID

        Args:
            resource (dict): a dictionary of stack resource

        Returns:
            str: ID of worker stack resource

        """
        return resource['StackResourceSummaries'][0]['PhysicalResourceId']


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
        self.current_compute_count = len(get_typed_nodes())
        self.target_compute_count = (
            self.current_compute_count + self.compute_count
        )
        self.previous_dir = os.getcwd()
        self.terraform_data_dir = os.path.join(
            self.cluster_path,
            constants.TERRAFORM_DATA_DIR
        )
        self.terraform_work_dir = constants.VSPHERE_DIR
        self.terraform = Terraform(self.terraform_work_dir)
        self.upi_repo_path = os.path.join(
            constants.EXTERNAL_DIR, 'installer',
        )

    def _update_terraform(self):
        """
        Update terraform variables
        """
        logger.debug("Updating terraform variables")
        self.terraform_var = os.path.join(
            self.cluster_path,
            constants.TERRAFORM_DATA_DIR,
            "terraform.tfvars"
        )
        compute_str = 'compute_count ='
        to_change = f"{compute_str} \"{self.current_compute_count}\""
        updated_compute_str = f"{compute_str} \"{self.target_compute_count}\""
        logging.debug(f"Updating {updated_compute_str} in {self.terraform_var}")

        # backup the terraform variable file
        original_file = f"{self.terraform_var}_{int(time.time())}"
        shutil.copyfile(self.terraform_var, original_file)
        logging.info(f"original terraform file: {original_file}")

        replace_content_in_file(
            self.terraform_var,
            to_change,
            updated_compute_str
        )

    def _update_machine_conf(self):
        """
        Update the machine config for vsphere
        """
        to_change = "clone {"
        add_file_block = f"{constants.LIFECYCLE}\n  {to_change}"
        logging.debug(
            f"Adding {constants.LIFECYCLE} to"
            f" {constants.INSTALLER_MACHINE_CONF}"
        )
        replace_content_in_file(
            constants.INSTALLER_MACHINE_CONF,
            to_change,
            add_file_block
        )

        # update the machine configurations
        update_machine_conf()

    def add_node(self):
        """
        Add nodes to the current cluster
        """
        if self.node_type == constants.RHCOS:
            logger.info(f"Adding Nodes of type {self.node_type}")
            logger.info(
                f"Existing worker nodes: {self.current_compute_count}, "
                f"New nodes to add: {self.compute_count}"
            )
            clone_openshift_installer()
            self._update_terraform()
            self._update_machine_conf()

            # Gets the existing CSR data
            existing_csr_data = get_nodes_csr()
            pre_count_csr = len(existing_csr_data)
            logger.debug(f"Existing CSR count before adding nodes: {pre_count_csr}")

            os.chdir(self.terraform_data_dir)
            self.terraform.initialize()
            self.terraform.apply(self.terraform_var)
            os.chdir(self.previous_dir)
            time.sleep(self.wait_time)

            if constants.CSR_BOOTSTRAPPER_NODE in existing_csr_data:
                nodes_approve_csr_num = pre_count_csr + self.compute_count
            else:
                nodes_approve_csr_num = pre_count_csr + self.compute_count + 1

            wait_for_all_nodes_csr_and_approve(
                expected_node_num=nodes_approve_csr_num
            )


class BaremetalNodes(NodesBase):
    """
    Baremetal Nodes class
    """
    def __init__(self):
        super(BaremetalNodes, self).__init__()
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

    def restart_nodes_teardown(self):
        """
        Make sure all BMs are up by the end of the test

        """
        self.cluster_nodes = get_node_objs()
        bms = self.baremetal.get_nodes_ipmi_ctx(self.cluster_nodes)
        stopped_bms = [
            bm for bm in bms if self.baremetal.get_power_status(bm) == constants.VM_POWERED_OFF
        ]

        if stopped_bms:
            logger.info(f"The following BMs are powered off: {stopped_bms}")
            self.baremetal.start_baremetal_machines_with_ipmi_ctx(stopped_bms)
        for bm in bms:
            bm.session.close()

    def get_data_volumes(self):
        raise NotImplementedError(
            "Get data volume functionality is not implemented"
        )

    def get_node_by_attached_volume(self, volume):
        raise NotImplementedError(
            "Get node by attached volume functionality is not implemented"
        )

    def detach_volume(self, volume, node=None, delete_from_backend=True):
        raise NotImplementedError(
            "Detach volume functionality is not implemented"
        )

    def attach_volume(self, volume, node):
        raise NotImplementedError(
            "Attach volume functionality is not implemented"
        )

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
        assert os.path.exists(default_config_path), (
            'Config file doesnt exists'
        )

        with open(default_config_path) as f:
            default_config_dict = yaml.safe_load(f)

        return default_config_dict


class AZURENodes(NodesBase):
    """
    Azure Nodes class
    """
    def __init__(self):
        super(AZURENodes, self).__init__()
        self.azure = azure_utils.AZURE()

    def stop_nodes(self, nodes):
        raise NotImplementedError(
            "Stop nodes functionality is not implemented"
        )

    def start_nodes(self, nodes):
        raise NotImplementedError(
            "Start nodes functionality is not implemented"
        )

    def restart_nodes(self, nodes, timeout=540, wait=True):
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
        for node_name in node_names:
            self.azure.restart_az_vm_instance(node_name)

        if wait:
            """
            When reboot is initiated on an instance from the Azure, the
            instance stays at "Running" state throughout the reboot operation.

            Once the OCP node detects that the node is not reachable then the
            node reaches status NotReady.
            When the reboot operation is completed and the instance is
            reachable the OCP node reaches status Ready.
            """
            logger.info(
                f"Waiting for nodes: {node_names} to reach not ready state"
            )
            wait_for_nodes_status(
                node_names=node_names, status=constants.NODE_NOT_READY,
                timeout=timeout
            )
            logger.info(
                f"Waiting for nodes: {node_names} to reach ready state"
            )
            wait_for_nodes_status(
                node_names=node_names, status=constants.NODE_READY,
                timeout=timeout
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
        nodes = [
            n for n in all_nodes if n.name == vm.name
        ]
        assert nodes, (
            f"Failed to find the OCS object for Azure Vm instance {vm.name}"
        )
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
        raise NotImplementedError(
            "Attach volume functionality is not implemented"
        )

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
                logger.info(
                    f"Volume id: {volume.name} has status: {sample}"
                )
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
