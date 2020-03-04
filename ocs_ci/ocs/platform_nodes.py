import logging

from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.framework import config
from ocs_ci.utility import aws
from ocs_ci.utility import vsphere
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import get_node_objs
from ocs_ci.ocs.resources.pvc import get_deviceset_pvs


logger = logging.getLogger(__name__)


class PlatformNodesFactory:
    """
    A factory class to get specific nodes platform object

    """
    def __init__(self):
        self.cls_map = {'AWS': AWSNodes, 'vsphere': VMWareNodes, 'aws': AWSNodes}

    def get_nodes_platform(self):
        platform = config.ENV_DATA['platform']
        return self.cls_map[platform]()


class NodesBase(object):
    """
    A base class for nodes related operations.
    Should be inherited by specific platform classes

    """
    def __init__(self):
        self.cluster_nodes = get_node_objs()

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

    def restart_nodes(self, nodes, force=True):
        raise NotImplementedError(
            "Restart nodes functionality is not implemented"
        )

    def detach_volume(self, node):
        raise NotImplementedError(
            "Detach volume functionality is not implemented"
        )

    def attach_volume(self, node, volume):
        raise NotImplementedError(
            "Attach volume functionality is not implemented"
        )

    def wait_for_volume_attach(self, volume):
        raise NotImplementedError(
            "Wait for volume attach functionality is not implemented"
        )

    def restart_nodes_teardown(self):
        raise NotImplementedError(
            "Restart nodes teardown functionality is not implemented"
        )


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
        return [vm for vm in vms_in_pool if vm.name in node_names]

    def get_data_volumes(self):
        raise NotImplementedError(
            "Get data volume functionality is not implemented for VMWare"
        )

    def get_node_by_attached_volume(self, volume):
        raise NotImplementedError(
            "Get node by attached volume functionality is not "
            "implemented for VMWare"
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

    def restart_nodes(self, nodes, force=True):
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
        self.vsphere.restart_vms(vms, force=force)

    def detach_volume(self, node):
        raise NotImplementedError(
            "Detach volume functionality is not implemented for VMWare"
        )

    def attach_volume(self, node, volume):
        raise NotImplementedError(
            "Attach volume functionality is not implemented for VMWare"
        )

    def wait_for_volume_attach(self, volume):
        raise NotImplementedError(
            "Wait for volume attach functionality is not implemented for VMWare"
        )

    def restart_nodes_teardown(self):
        """
        Make sure all VMs are up by the end of the test

        """
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


class AWSNodes(NodesBase):
    """
    AWS EC2 instances class

    """
    def __init__(self):
        super(AWSNodes, self).__init__()
        self.aws = aws.AWS()

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

    def start_nodes(self, nodes, wait=True):
        """
        Start EC2 instances

        Args:
            nodes (list): The OCS objects of the nodes
            wait (bool): True for waiting the instances to start, False otherwise

        """
        instances = self.get_ec2_instances(nodes)
        assert instances, (
            f"Failed to get the EC2 instances for nodes {[n.name for n in nodes]}"
        )
        self.aws.start_ec2_instances(instances=instances, wait=wait)

    def restart_nodes(self, nodes, wait=True, force=True):
        """
        Restart EC2 instances

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
        self.aws.restart_ec2_instances(instances=instances, wait=wait, force=force)

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

    def detach_volume(self, volume):
        """
        Detach a volume from an EC2 instance

        Args:
            volume (Volume): The volume to delete

        """
        self.aws.detach_volume(volume)

    def attach_volume(self, node, volume):
        """
        Attach a data volume to an instance

        Args:
            node (OCS): The EC2 instance to attach the volume to
            volume (Volume): The volume to delete

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

    def restart_nodes_teardown(self):
        """
        Make sure all EC2 instances are up. To be used in the test teardown

        """
        # Get the cluster nodes ec2 instances
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
