import os
import logging
import time
import boto3
import random
import traceback
import re

from datetime import datetime, timezone
from botocore.exceptions import ClientError, NoCredentialsError, WaiterError

from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import get_infra_id
from ocs_ci.framework import config
from ocs_ci.ocs import constants, defaults, exceptions
from ocs_ci.ocs.parallel import parallel
from ocs_ci.utility.templating import load_yaml
from tempfile import NamedTemporaryFile

logger = logging.getLogger(name=__file__)

TIMEOUT = 90
SLEEP = 3


class AWSTimeoutException(Exception):
    pass


class StackStatusError(Exception):
    pass


class AWS(object):
    """
    This is wrapper class for AWS
    """

    _ec2_client = None
    _ec2_resource = None
    _region_name = None
    _s3_client = None
    _s3_resource = None
    _route53_client = None
    _elb_client = None

    def __init__(self, region_name=None):
        """
        Constructor for AWS class

        Args:
            region_name (str): Name of AWS region (default: us-east-2)
        """
        self._region_name = region_name or config.ENV_DATA["region"]

    @property
    def ec2_client(self):
        """Property for ec2 client

        Returns:
            boto3.client: instance of ec2
        """
        if not self._ec2_client:
            self._ec2_client = boto3.client(
                "ec2",
                region_name=self._region_name,
            )
        return self._ec2_client

    @property
    def ec2_resource(self):
        """Property for ec2 resource

        Returns:
            boto3.resource instance of ec2 resource
        """
        if not self._ec2_resource:
            self._ec2_resource = boto3.resource(
                "ec2",
                region_name=self._region_name,
            )
        return self._ec2_resource

    @property
    def s3_resource(self):
        """
        Property for s3 resource

        Returns:
            boto3.resource instance of s3

        """
        if not self._s3_resource:
            self._s3_resource = boto3.resource(
                "s3",
                region_name=self._region_name,
            )
        return self._s3_resource

    @property
    def s3_client(self):
        """
        Property for s3 client

        Returns:
            boto3.client instance of s3

        """
        if not self._s3_client:
            self._s3_client = boto3.client(
                "s3",
                region_name=self._region_name,
            )
        return self._s3_client

    @property
    def route53_client(self):
        """
        Property for route53 client

        Returns:
            boto3.client: instance of route53 client

        """
        if not self._route53_client:
            self._route53_client = boto3.client(
                "route53",
                region_name=self._region_name,
            )
        return self._route53_client

    @property
    def elb_client(self):
        """
        Property for elb client

        Returns:
            boto3.client: instance of elb client

        """
        if not self._elb_client:
            self._elb_client = boto3.client(
                "elb",
                region_name=self._region_name,
            )
        return self._elb_client

    def get_ec2_instance(self, instance_id):
        """
        Get instance of ec2 Instance

        Args:
            instance_id (str): The ID of the instance to get

        Returns:
            boto3.Instance: instance of ec2 instance resource

        """
        return self.ec2_resource.Instance(instance_id)

    def get_instances_response_by_name_pattern(
        self, pattern=None, filter_by_cluster_name=True
    ):
        """
        Get the instances by name tag pattern. If not specified it will return
        all the instances, or will return the instances
        filtered by the cluster name.

        Args:
            pattern (str): Pattern of tag name like:
                pbalogh-testing-cluster-55jx2-worker*
            filter_by_cluster_name: Will be used only if the 'pattern' param
                not specified. If True it filters the instances
                by the cluster name, else if False it returns all instances.

        Returns:
            list: list of instances dictionaries.
        """
        if not pattern:
            if filter_by_cluster_name:
                pattern = f"{config.ENV_DATA['cluster_name']}*"
            else:
                pattern = "*"

        instances_response = self.ec2_client.describe_instances(
            Filters=[
                {
                    "Name": "tag:Name",
                    "Values": [pattern],
                },
            ],
        )["Reservations"]

        return instances_response

    def get_instances_by_name_pattern(self, pattern):
        """Get instances by Name tag pattern

        The instance details do not contain all the values but just those we
        are consuming.

        Those parameters we are storing for instance are:
        * id: id of instance
        * avz: Availability Zone
        * name: The value of Tag Name if define otherwise None
        * vpc_id: VPC ID
        * security_groups: Security groups of the instance

        Args:
            pattern (str): Pattern of tag name like:
                pbalogh-testing-cluster-55jx2-worker*

        Returns:
            list: contains dictionaries with instance details mentioned above
        """
        instances_response = self.get_instances_response_by_name_pattern(
            pattern=pattern
        )
        instances = []
        for instance in instances_response:
            instance = instance["Instances"][0]
            id = instance["InstanceId"]
            avz = instance["Placement"]["AvailabilityZone"]
            name = None
            for tag in instance["Tags"]:
                if tag["Key"] == "Name":
                    name = tag["Value"]
                    break
            instance_data = dict(
                id=id,
                avz=avz,
                name=name,
                vpc_id=instance.get("VpcId"),
                security_groups=instance.get("SecurityGroups", []),
            )
            instances.append(instance_data)
        logger.debug("All found instances: %s", instances)
        return instances

    def get_instances_status_by_id(self, instance_id):
        """
        Get instances by ID

        Args:
            instance_id (str): ID of the instance

        Returns:
            str: The instance status
        """
        return (
            self.ec2_client.describe_instances(
                InstanceIds=[instance_id],
            )
            .get("Reservations")[0]
            .get("Instances")[0]
            .get("State")
            .get("Code")
        )

    def get_vpc_id_by_instance_id(self, instance_id):
        """
        Fetch vpc id out of ec2 node (EC2.Instances.vpc_id)

        Args:
            instance_id (str): ID of the instance - to get vpc id info from ec2 node

        Returns:
            str: vpc_id: The vpc id

        """
        instance = self.get_ec2_instance(instance_id)

        return instance.vpc_id

    def get_availability_zone_id_by_instance_id(self, instance_id):
        """
        Fetch availability zone out of ec2 node (EC2.Instances.placement)

        Args:
            instance_id (str): ID of the instance - to get availability zone info from ec2 node

        Returns:
            str: availability_zone: The availability zone name

        """
        instance = self.get_ec2_instance(instance_id)

        return instance.placement.get("AvailabilityZone")

    def create_volume(
        self,
        availability_zone,
        name,
        encrypted=False,
        size=100,
        timeout=20,
        volume_type="gp2",
    ):
        """
        Create volume

        Args:
            availability_zone (str): The availability zone e.g.: us-west-1b
            name (str): The name of the volume
            encrypted (boolean): True if encrypted, False otherwise
                (default: False)
            size (int): The size in GB (default: 100)
            timeout (int): The timeout in seconds for volume creation (default: 20)
            volume_type (str): 'standard'|'io1'|'gp2'|'sc1'|'st1'
                (default: gp2)

        Returns:
            Volume: AWS Resource instance of the newly created volume

        """
        volume_response = self.ec2_client.create_volume(
            AvailabilityZone=availability_zone,
            Encrypted=encrypted,
            Size=size,
            VolumeType=volume_type,
            TagSpecifications=[
                {
                    "ResourceType": "volume",
                    "Tags": [
                        {
                            "Key": "Name",
                            "Value": name,
                        },
                    ],
                },
            ],
        )
        logger.debug("Response of volume creation: %s", volume_response)
        volume = self.ec2_resource.Volume(volume_response["VolumeId"])
        for x in range(timeout):
            volume.reload()
            logger.debug("Volume id: %s has status: %s", volume.volume_id, volume.state)
            if volume.state == "available":
                break
            if x == timeout - 1:
                raise AWSTimeoutException(
                    f"Reached timeout {timeout} for volume creation, volume "
                    f"state is still: {volume.state} for volume ID: "
                    f"{volume.volume_id}"
                )
            time.sleep(1)
        return volume

    def attach_volume(self, volume, instance_id, device="/dev/sdx"):
        """
        Attach volume to an ec2 instance

        Args:
            volume (Volume): Volume instance
            instance_id (str): id of instance where to attach the volume
            device (str): The name of the device where to attach (default: /dev/sdx)

        """
        logger.info(f"Attaching volume: {volume.volume_id} Instance: {instance_id}")
        attach_response = volume.attach_to_instance(
            Device=device,
            InstanceId=instance_id,
        )
        logger.debug("Response of attaching volume: %s", attach_response)

    def create_volume_and_attach(
        self,
        availability_zone,
        instance_id,
        name,
        device="/dev/sdx",
        encrypted=False,
        size=100,
        timeout=20,
        volume_type="gp2",
    ):
        """
        Create volume and attach to instance

        Args:
            availability_zone (str): The availability zone e.g.: us-west-1b
            instance_id (str): The id of the instance where to attach the volume
            name (str): The name of volume
            device (str): The name of device where to attach (default: /dev/sdx)
            encrypted (boolean): True if encrypted, False otherwise
                (default: False)
            size (int): The size in GB (default: 100)
            timeout (int): The timeout in seconds for volume creation (default: 20)
            volume_type (str): 'standard'|'io1'|'gp2'|'sc1'|'st1'
                (default: gp2)

        """
        volume = self.create_volume(
            availability_zone, name, encrypted, size, timeout, volume_type
        )
        self.attach_volume(volume, instance_id, device)

    def get_volumes_by_tag_pattern(self, tag, pattern):
        """
        Get volumes by tag pattern

        Args:
            tag (str): Tag name
            pattern (str): Pattern of tag value (e.g. '*cl-vol-*')

        Returns:
            list: Volume information like id and attachments
        """
        volumes_response = self.ec2_client.describe_volumes(
            Filters=[
                {
                    "Name": f"tag:{tag}",
                    "Values": [pattern],
                },
            ],
        )
        volumes = []
        for volume in volumes_response["Volumes"]:
            volumes.append(
                dict(
                    id=volume["VolumeId"],
                    attachments=volume["Attachments"],
                )
            )
        return volumes

    def get_volume_data(self, volume_id):
        """
        Get volume information

        Args:
            volume_id(str): ID of the volume

        Returns:
            dict: complete volume information
        """
        volumes_response = self.ec2_client.describe_volumes(
            VolumeIds=[
                volume_id,
            ],
        )
        return volumes_response["Volumes"][0]

    def get_volume_tag_value(self, volume_data, tag_name):
        """
        Get the value of the volume's tag

        Args:
            volume_data(dict): complete volume information
            tag_name(str): name of the tag
        Returns:
            str: value of the tag or None if there's no such tag
        """
        tags = volume_data["Tags"]
        for tag in tags:
            if tag["Key"] == tag_name:
                return tag["Value"]
        return None

    def get_volumes_by_name_pattern(self, pattern):
        """
        Get volumes by pattern

        Args:
            pattern (str): Pattern of volume name (e.g. '*cl-vol-*')

        Returns:
            list: Volume information like id and attachments
        """
        return self.get_volumes_by_tag_pattern("Name", pattern)

    def check_volume_attributes(
        self,
        volume_id,
        name_end=None,
        size=None,
        iops=None,
        throughput=None,
        namespace=None,
    ):
        """
        Verify aws volume attributes
        Primarily used for faas

        Args:
            volume_id(str): id of the volume to be checked
            name_end(str): expected ending of Name tag
            size(int): expected value of volume's size
            iops(int): expected value of IOPS
            throughput(int): expected value of Throughput
            namespace(str): expected value of kubernetes.io/created-for/pvc/namespace tag

        Raises:
            ValueError if the actual value differs from the expected one
        """
        volume_data = self.get_volume_data(volume_id)
        volume_name = self.get_volume_tag_value(
            volume_data,
            "Name",
        )
        logger.info(
            f"Verifying that volume name {volume_name} starts with cluster name"
        )
        if not volume_name.startswith(config.ENV_DATA["cluster_name"]):
            raise ValueError(
                f"Volume name should start with cluster name {config.ENV_DATA['cluster_name']}"
            )
        if name_end:
            logger.info(f"Verifying that volume name ends with {name_end}")
            if not volume_name.endswith(name_end):
                raise ValueError(f"Volume name should end with {name_end}")
        if size:
            logger.info(f"Verifying that volume size is {size}")
            if volume_data["Size"] != size:
                raise ValueError(
                    f"Volume size should be {size} but it's {volume_data['Size']}"
                )
        if iops:
            logger.info(f"Verifying that volume IOPS is {iops}")
            if volume_data["Iops"] != iops:
                raise ValueError(
                    f"Volume IOPS should be {iops} but it's {volume_data['Iops']}"
                )
        if throughput:
            logger.info(f"Verifying that volume throughput is {throughput}")
            if volume_data["Throughput"] != throughput:
                raise ValueError(
                    f"Volume size should be {throughput} but it's {volume_data['Throughput']}"
                )
        if namespace:
            logger.info(f"Verifying that namespace is {namespace}")
            volume_namespace = self.get_volume_tag_value(
                volume_data,
                constants.AWS_VOL_PVC_NAMESPACE,
            )
            if volume_namespace != namespace:
                raise ValueError(
                    "Namespace in kubernetes.io/created-for/pvc/namespace tag "
                    f"should be {namespace} but it's {volume_namespace}"
                )

    def detach_volume(self, volume, timeout=120):
        """
        Detach volume if attached

        Args:
            volume (Volume): The volume to delete
            timeout (int): Timeout in seconds for API calls

        Returns:
            Volume: ec2 Volume instance

        """
        if volume.attachments:
            attachment = volume.attachments[0]
            logger.info(
                "Detaching volume: %s Instance: %s",
                volume.volume_id,
                attachment.get("InstanceId"),
            )
            response_detach = volume.detach_from_instance(
                Device=attachment["Device"],
                InstanceId=attachment["InstanceId"],
                Force=True,
            )
            logger.debug("Detach response: %s", response_detach)
        for x in range(timeout):
            volume.reload()
            logger.debug("Volume id: %s has status: %s", volume.volume_id, volume.state)
            if volume.state == "available":
                break
            if x == timeout - 1:
                raise AWSTimeoutException(
                    f"Reached timeout {timeout}s for volume detach/delete for "
                    f"volume ID: {volume.volume_id}, Volume state: "
                    f"{volume.state}"
                )
            time.sleep(1)

    def delete_volume(self, volume):
        """
        Delete an ec2 volume from AWS

        Args:
            volume (Volume): The volume to delete

        """
        logger.info("Deleting volume: %s", volume.volume_id)
        delete_response = volume.delete()
        logger.debug(
            "Delete response for volume: %s is: %s", volume.volume_id, delete_response
        )

    def get_cluster_subnet_ids(self, cluster_name):
        """
        Get the cluster's subnet ids of existing cluster

        Args:
            cluster_name (str): Cluster name

        Returns:
            string of space separated subnet ids

        """
        subnets = self.ec2_client.describe_subnets(
            Filters=[{"Name": "tag:Name", "Values": [f"{cluster_name}*"]}]
        )
        subnet_ids = [subnet["SubnetId"] for subnet in subnets["Subnets"]]
        return subnet_ids

    def detach_and_delete_volume(self, volume, timeout=120):
        """
        Detach volume if attached and then delete it from AWS

        Args:
            volume (Volume): The volume to delete
            timeout (int): Timeout in seconds for API calls

        """
        self.detach_volume(volume, timeout)
        self.delete_volume(volume)

    def stop_ec2_instances(self, instances, wait=False, force=True):
        """
        Stopping an instance

        Args:
            instances (dict): A dictionary of instance IDs and names to stop
            wait (bool): True in case wait for status is needed,
                False otherwise
            force (bool): True for force instance stop, False otherwise

        """
        instance_ids, instance_names = zip(*instances.items())
        logger.info(f"Stopping instances {instance_names} with Force={force}")
        ret = self.ec2_client.stop_instances(InstanceIds=instance_ids, Force=force)
        stopping_instances = ret.get("StoppingInstances")
        for instance in stopping_instances:
            assert instance.get("CurrentState").get("Code") in [
                constants.INSTANCE_STOPPED,
                constants.INSTANCE_STOPPING,
                constants.INSTANCE_SHUTTING_DOWN,
            ], (
                f"Instance {instance.get('InstanceId')} status "
                f"is {instance.get('CurrentState').get('Code')}"
            )
        if wait:
            for instance_id, instance_name in instances.items():
                logger.info(
                    f"Waiting for instance {instance_name} to reach status stopped"
                )
                instance = self.get_ec2_instance(instance_id)
                instance.wait_until_stopped()

    def start_ec2_instances(self, instances, wait=False):
        """
        Starting an instance

        Args:
            instances (dict): A dictionary of instance IDs and names to start
            wait (bool): True in case wait for status is needed,
                False otherwise

        """
        instance_ids, instance_names = zip(*instances.items())
        logger.info(f"Starting instances {instance_names}")
        ret = self.ec2_client.start_instances(InstanceIds=instance_ids)
        starting_instances = ret.get("StartingInstances")
        for instance in starting_instances:
            assert instance.get("CurrentState").get("Code") in [
                constants.INSTANCE_RUNNING,
                constants.INSTANCE_PENDING,
            ], (
                f"Instance {instance.get('InstanceId')} status "
                f"is {instance.get('CurrentState').get('Code')}"
            )
        if wait:
            for instance_id, instance_name in instances.items():
                logger.info(
                    f"Waiting for instance {instance_name} to reach status running"
                )
                instance = self.get_ec2_instance(instance_id)
                instance.wait_until_running()

    def restart_ec2_instances_by_stop_and_start(
        self, instances, wait=False, force=True
    ):
        """
        Restart EC2 instances by stop and start

        Args:
            instances (dict): A dictionary of instance IDs and names to stop
                & start
            wait (bool): True in case wait for status is needed,
                False otherwise
            force (bool): True for force instance stop, False otherwise

        """
        logger.info(f"Restarting instances {list(instances.values())} by stop & start")
        self.stop_ec2_instances(instances=instances, wait=wait, force=force)
        self.start_ec2_instances(instances=instances, wait=wait)

    def restart_ec2_instances(self, instances):
        """
        Restart ec2 instances

        Args:
            instances (dict): A dictionary of instance IDs and names to restart

        """
        instance_ids, instance_names = zip(*instances.items())
        logger.info(f"Rebooting instances {instance_names}")
        self.ec2_client.reboot_instances(InstanceIds=instance_ids)

    def terminate_ec2_instances(self, instances, wait=True):
        """
        Terminate an instance

        Args:
            instances (dict): A dictionary of instance IDs and names
            wait (bool): True in case wait for status is needed,
                False otherwise

        """
        instance_ids, instance_names = zip(*instances.items())
        logger.info(f"Terminating instances {list(instances.values())}")
        ret = self.ec2_client.terminate_instances(InstanceIds=instance_ids)
        terminating_instances = ret.get("TerminatingInstances")
        for instance in terminating_instances:
            assert instance.get("CurrentState").get("Code") in [
                constants.INSTANCE_SHUTTING_DOWN,
                constants.INSTANCE_TERMINATED,
            ], (
                f"Instance {instance.get('InstanceId')} status "
                f"is {instance.get('CurrentState').get('Code')}"
            )
        if wait:
            for instance_id, instance_name in instances.items():
                logger.info(
                    f"Waiting for instance {instance_name} to reach status "
                    f"terminated"
                )
                instance = self.get_ec2_instance(instance_id)
                instance.wait_until_terminated()

    def get_ec2_instance_volumes(self, instance_id):
        """
        Get all volumes attached to an ec2 instance

        Args:
            instance_id (str): The ec2 instance ID

        Returns:
            list: ec2 Volume instances

        """
        instance = self.get_ec2_instance(instance_id)
        volumes = instance.volumes.all()
        return [vol for vol in volumes]

    def get_all_security_groups(self):
        """
        Get all security groups in AWS region

        Returns:
            list: All security groups

        """
        all_security_groups = list()

        security_groups_dict = self.ec2_client.describe_security_groups()
        security_groups = security_groups_dict["SecurityGroups"]
        for group_object in security_groups:
            all_security_groups.append(group_object["GroupId"])

        return all_security_groups

    def get_security_groups_by_instance_id(self, instance_id):
        """
        Get all attached security groups of ec2 instance

        Args:
            instance_id (str): Required instance to get security groups from it

        Returns:
            list: all_sg_ids: all attached security groups id.

        """
        ec2_instance = self.get_ec2_instance(instance_id)
        all_sg_ids = [sg.get("GroupId") for sg in ec2_instance.security_groups]

        return all_sg_ids

    def create_security_group(self, group_name, dict_permissions, vpc_id):
        """
        Create security group with predefined group name and permissions

        Args:
            group_name (str): Group name (aws tag: "Group Name")
            dict_permissions (dict): The security group's inbound/outbound permissions
            vpc_id(str): For group to be attached

        Returns:
            str: newly created security group id

        """
        instance_response = self.ec2_client.create_security_group(
            GroupName=group_name,
            Description="This group created by method:aws.create_security_group",
            VpcId=vpc_id,
        )

        security_group_id = instance_response["GroupId"]
        logger.info(f"Security Group Created {security_group_id} in vpc {vpc_id}")

        data = self.ec2_client.authorize_security_group_ingress(
            GroupId=security_group_id, IpPermissions=[dict_permissions]
        )
        logger.info(f"Ingress Successfully Set {data}")
        return security_group_id

    def append_security_group(self, security_group_id, instance_id):
        """
        Append security group to selected ec2 nodes

        Args:
            instance_id (str): Instances to attach security group
            security_group_id(str): Security group to attach

            print out: security group <id> added to selected nodes

        """
        ec2_instance = self.get_ec2_instance(instance_id)
        logger.info(f"ec2_instance id is: {ec2_instance.id}")
        all_sg_ids = [sg.get("GroupId") for sg in ec2_instance.security_groups]
        if security_group_id not in all_sg_ids:
            all_sg_ids.append(security_group_id)
            ec2_instance.modify_attribute(Groups=all_sg_ids)

        logger.info(f"Security Group {security_group_id} added to selected node")

    def remove_security_group(self, security_group_id, instance_id):
        """
        Remove security group from selected ec2 instance (by instance id)
        print out: security group <id> removed from selected nodes

        Args:
            security_group_id (str): Security group to be removed
            instance_id (str): Instance attached with selected security group

        """
        ec2_instance = self.get_ec2_instance(instance_id)
        logger.info(f"ec2_instance id is: {ec2_instance.id}")
        all_sg_ids = self.get_security_groups_by_instance_id(instance_id)
        for sg in all_sg_ids:
            if sg == security_group_id:
                all_sg_ids.remove(security_group_id)
                ec2_instance.modify_attribute(Groups=all_sg_ids)
                logger.info(
                    f"Security Group {security_group_id} removed from selected node"
                )

    def delete_security_group(self, security_group_id):
        """
        Delete selected security group
        print out: Security group <id> deleted

        Args:
            security_group_id (str): Id of selected security group

        """
        self.ec2_client.delete_security_group(GroupId=security_group_id)
        logger.info(f"Security group {security_group_id} deleted")

    def store_security_groups_for_instances(self, instances_id):
        """
        Stored all security groups attached to selected ec2 instances

        Args:
            instances_id (list): ec2 instance_id

        Returns:
            dict: security_group_dict: keys: blocked instances: ec2_instances ids
                values: list of original security groups of each instance

        """
        sg_list = list()
        for instance in instances_id:
            sg_list.append(self.get_security_groups_by_instance_id(instance))

        return dict(zip(instances_id, sg_list))

    def block_instances_access(self, security_group_id, instances_id):
        """
        Block ec2 instances by:

        - Append security group without access permissions
        - Remove original security groups

        Args:
            security_group_id (str): security group without access permissions
            instances_id (list): list of ec2 instances ids

        """

        for instance in instances_id:
            original_sgs = self.get_security_groups_by_instance_id(instance)
            self.append_security_group(security_group_id, instance)
            for sg_grp in original_sgs:
                self.remove_security_group(sg_grp, instance)

    def restore_instances_access(
        self, security_group_id_to_remove, original_security_group_dict
    ):
        """
        Restore access to instances by removing blocking security group and
        append original security group.

        Args:
            security_group_id_to_remove (str): id of the security group
            original_security_group_dict (dict): dict with:
                keys: blocked instances: ec2 instances id
                values: list of original security groups


        """
        for instance in original_security_group_dict.keys():
            org_sg_grp_of_instance = list(original_security_group_dict.get(instance))
            for sg in org_sg_grp_of_instance:
                self.append_security_group(sg, instance)
                self.remove_security_group(security_group_id_to_remove, instance)

    @property
    def cf_client(self):
        """
        Property for cloudformation client

        Returns:
            boto3.client: instance of cloudformation

        """
        return boto3.client("cloudformation", region_name=self._region_name)

    def get_cloudformation_stacks(self, pattern):
        """
        Get cloudformation stacks

        Args:
            pattern (str): The pattern of the stack name

        """
        result = self.cf_client.describe_stacks(StackName=pattern)
        return result["Stacks"]

    def delete_cloudformation_stacks(self, stack_names):
        """
        Delete cloudformation stacks

        Args:
            stack_names (list): List of cloudformation stacks

        """

        @retry(StackStatusError, tries=20, delay=30, backoff=1)
        def verify_stack_deleted(stack_name):
            try:
                stacks = self.get_cloudformation_stacks(stack_name)
                for stack in stacks:
                    status = stack["StackStatus"]
                    raise StackStatusError(
                        f"{stack_name} not deleted yet, current status: {status}."
                    )
            except ClientError as e:
                assert f"Stack with id {stack_name} does not exist" in str(e)
                logger.info("Received expected ClientError, stack successfully deleted")

        for stack_name in stack_names:
            logger.info("Destroying stack: %s", stack_name)
            self.cf_client.delete_stack(StackName=stack_name)
        for stack_name in stack_names:
            verify_stack_deleted(stack_name)

    def upload_file_to_s3_bucket(self, bucket_name, object_key, file_path):
        """
        Upload objects to s3 bucket

        Args:
            bucket_name (str): Name of a valid s3 bucket
            object_key (str): the key for the s3 object
            file_path (str): path for the file to be uploaded

        """
        self.s3_resource.meta.client.upload_file(
            file_path, bucket_name, object_key, ExtraArgs={"ACL": "public-read"}
        )

    def delete_s3_object(self, bucket_name, object_key):
        """
        Delete an object from s3 bucket

        Args:
            bucket_name (str): name of a valid s3 bucket
            object_key (str): the key for s3 object

        """
        self.s3_resource.meta.client.delete_object(Bucket=bucket_name, Key=object_key)

    def get_s3_bucket_object_url(self, bucket_name, object_key):
        """
        Get s3 bucket object url

        Args:
            bucket_name (str): Name of a valid s3 bucket
            object_key (str): Name of the key for s3 object

        Returns:
            s3_url (str): An s3 url

        """
        s3_url = os.path.join(
            f"https://s3.{self._region_name}.amazonaws.com/{bucket_name}",
            f"{object_key}",
        )
        return s3_url

    def get_stack_instance_id(self, stack_name, logical_id):
        """
        Get the instance id associated with the cloudformation stack

        Args:
            stack_name (str): Name of the cloudformation stack
            logical_id (str):  LogicalResourceId of the resource
                ex: "Worker0"

        Returns:
            instance_id (str): Id of the instance

        """
        resource = self.cf_client.describe_stack_resource(
            StackName=stack_name, LogicalResourceId=logical_id
        )
        return resource.get("StackResourceDetail").get("PhysicalResourceId")

    def get_stack_params(self, stack_name, param_name):
        """
        Get value of a particular param

        Args:
            stack_name (str): AWS cloudformation stack name
            param_name (str): Stack parameter name

        Returns:
            str: Parameter value

        """
        stack_description = self.cf_client.describe_stacks(StackName=stack_name)
        params = stack_description.get("Stacks")[0].get("Parameters")
        for param_dict in params:
            if param_dict.get("ParameterKey") == param_name:
                return param_dict.get("ParameterValue")

    def get_worker_ignition_location(self, stack_name):
        """
        Get the ignition location from given stack

        Args:
            stack_name (str): AWS cloudformation stack name

        Returns:
            ignition_location (str): An AWS URL ignition location

        """
        param_name = "IgnitionLocation"
        ignition_loction = self.get_stack_params(stack_name, param_name)
        return ignition_loction

    def get_worker_instance_profile_name(self, stack_name):
        """
        Get the worker instance profile name

        Args:
            stack_name (str): AWS cloudformation stack name

        Returns:
            worker_instance_profile_name (str): instance profile name

        """
        param_name = "WorkerInstanceProfileName"
        worker_instance_profile_name = self.get_stack_params(stack_name, param_name)
        return worker_instance_profile_name

    def get_worker_stacks(self):
        """
        Get the cloudformation stacks only for workers of this cluster

        Returns:
            list : of worker stacks

        """
        worker_pattern = r"{}-no[0-9]+".format(config.ENV_DATA["cluster_name"])
        return self.get_matching_stacks(worker_pattern)

    def get_matching_stacks(self, pattern):
        """
        Get only the stacks which matches the pattern

        Args:
            pattern (str): A raw string which is re compliant

        Returns:
            list : of strings which are matching stack name

        """
        all_stacks = self.get_all_stacks()
        matching_stacks = []
        for stack in all_stacks:
            matching = re.match(pattern, stack)
            if matching:
                matching_stacks.append(matching.group())
        return matching_stacks

    def get_all_stacks(self):
        """
        Get all the cloudformation stacks

        Returns:
            list : of all cloudformation stacks

        """
        all_stacks = []
        stack_description = self.cf_client.describe_stacks()
        for stack in stack_description["Stacks"]:
            all_stacks.append(stack["StackName"])
        return all_stacks

    def create_stack(self, s3_url, index, params_list, capabilities):
        """
        Create a new cloudformation stack for worker creation

        Args:
            s3_url (str): An aws url for accessing s3 object
            index (int): Integer index for stack name
            params_list (list): of parameters (k,v) for create_stack
            capabilities (list): of valid AWS capabilities like
                CAPABILITY_NAMED_IAM etc

        Returns:
            tuple : of (stack_name, stack_id)

        """
        stack_name = f"{config.ENV_DATA['cluster_name']}-no{index}"
        response = self.cf_client.create_stack(
            StackName=stack_name,
            TemplateURL=s3_url,
            Parameters=params_list,
            Capabilities=capabilities,
        )
        self.cf_client.get_waiter("stack_create_complete").wait(StackName=stack_name)
        logger.info(f"Stack {stack_name} created successfuly")
        stack_id = response["StackId"]
        logger.info(f"Stackid = {stack_id}")
        return stack_name, stack_id

    def delete_apps_record_set(self, cluster_name=None, from_base_domain=False):
        """
        Delete apps record set that sometimes blocks sg stack deletion.
            https://github.com/red-hat-storage/ocs-ci/issues/2549

        Args:
            cluster_name (str): Name of the cluster
            from_base_domain (bool): Delete apps record set from base domain
                created by Flexy

        """
        cluster_name = cluster_name or config.ENV_DATA["cluster_name"]
        base_domain = config.ENV_DATA["base_domain"]
        if from_base_domain:
            hosted_zone_name = f"{base_domain}."
        else:
            hosted_zone_name = f"{cluster_name}.{base_domain}."
        record_set_name = f"\\052.apps.{cluster_name}.{base_domain}."

        hosted_zones = self.route53_client.list_hosted_zones_by_name(
            DNSName=hosted_zone_name, MaxItems="1"
        )["HostedZones"]
        hosted_zone_ids = [
            zone["Id"] for zone in hosted_zones if zone["Name"] == hosted_zone_name
        ]
        if hosted_zone_ids:
            hosted_zone_id = hosted_zone_ids[0]
        else:
            logger.info(f"hosted zone {hosted_zone_name} not found")
            return
        record_sets = self.route53_client.list_resource_record_sets(
            HostedZoneId=hosted_zone_id
        )["ResourceRecordSets"]
        apps_record_sets = [
            record_set
            for record_set in record_sets
            if record_set["Name"] == record_set_name
        ]
        if apps_record_sets:
            apps_record_set = apps_record_sets[0]
        else:
            logger.info(f"app record set not found for record {record_set_name}")
            return
        logger.info(f"Deleting hosted zone: {record_set_name}")
        self.route53_client.change_resource_record_sets(
            HostedZoneId=hosted_zone_id,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "DELETE",
                        "ResourceRecordSet": {
                            "Name": record_set_name,
                            "Type": apps_record_set["Type"],
                            "AliasTarget": apps_record_set["AliasTarget"],
                        },
                    }
                ]
            },
        )

    def get_instance_id_from_private_dns_name(self, private_dns_name):
        """
        Get the instance id from the private dns name of the instance

        Args:
            private_dns_name (str): The private DNS name of the instance

        Returns:
            str: The instance id associated to the private DNS name.
                 If not found returns None
        """
        instances_response = self.get_instances_response_by_name_pattern()
        for instance in instances_response:
            instance_dict = instance["Instances"][0]
            if instance_dict["PrivateDnsName"] == private_dns_name:
                return instance_dict["InstanceId"]

        return None

    def get_stack_name_by_instance_id(self, instance_id):
        """
        Get the stack name by the instance id

        Args:
            instance_id (str): The instance id

        Returns:
            str: The stack name associated to the instance id.
                 If not found returns None
        """
        stack_name = None
        instances_response = self.get_instances_response_by_name_pattern()
        for instance in instances_response:
            instance_dict = instance["Instances"][0]
            if instance_dict["InstanceId"] == instance_id:
                stack_name = get_stack_name_from_instance_dict(instance_dict)

        return stack_name

    @retry(StackStatusError, tries=3, delay=10, backoff=2)
    def delete_cf_stack_including_dependencies(self, cfs_name):
        """
        Delete cloudformation stack including dependencies.

        Some of the depending resources are not deletable, so related errors
        are ignored and only logged.
        Thsi method is mainly used as a WORKAROUND for folowing Flexy issue:
        https://issues.redhat.com/browse/OCPQE-1521

        Args:
            cfs_name (str): CloudFormation stack name to cleanup

        """
        # get all VPCs related to the CloudFormation Stack
        vpcs = self.ec2_client.describe_vpcs(
            Filters=[
                {
                    "Name": "tag:aws:cloudformation:stack-name",
                    "Values": [f"{cfs_name}"],
                }
            ]
        )["Vpcs"]

        for vpc in vpcs:
            # get all NetworkInterfaces related to the particular VPC
            nis = self.ec2_client.describe_network_interfaces(
                Filters=[
                    {
                        "Name": "vpc-id",
                        "Values": [vpc["VpcId"]],
                    }
                ]
            )["NetworkInterfaces"]
            for ni in nis:
                # delete LoadBalancer related to the NetworkInterface
                if ni["Description"].split(" ")[0] == "ELB":
                    elb = ni["Description"].split(" ")[1]
                    logger.info(f"Deleting LoadBalancer: {elb}")
                    try:
                        self.elb_client.delete_load_balancer(LoadBalancerName=elb)
                    except ClientError as err:
                        logger.warning(err)

                logger.info(f"Deleting NetworkInterface: {ni['NetworkInterfaceId']}")
                try:
                    self.ec2_client.delete_network_interface(
                        NetworkInterfaceId=ni["NetworkInterfaceId"]
                    )
                except ClientError as err:
                    logger.warning(err)

            # get all InternetGateways related to the particular VPC
            igs = self.ec2_client.describe_internet_gateways(
                Filters=[
                    {
                        "Name": "attachment.vpc-id",
                        "Values": [vpc["VpcId"]],
                    }
                ]
            )["InternetGateways"]
            for ig in igs:
                logger.info(f"Deleting InternetGateway: {ig['InternetGatewayId']}")
                try:
                    self.ec2_client.delete_internet_gateway(
                        InternetGatewayId=ig["InternetGatewayId"]
                    )
                except ClientError as err:
                    logger.warning(err)

            # get all Subnets related to the particular VPC
            subnets = self.ec2_client.describe_subnets(
                Filters=[
                    {
                        "Name": "vpc-id",
                        "Values": [vpc["VpcId"]],
                    }
                ]
            )["Subnets"]
            for subnet in subnets:
                logger.info(f"Deleting Subnet: {subnet['SubnetId']}")
                try:
                    self.ec2_client.delete_subnet(SubnetId=subnet["SubnetId"])
                except ClientError as err:
                    logger.warning(err)

            # get all RouteTables related to the particular VPC
            rts = self.ec2_client.describe_route_tables(
                Filters=[
                    {
                        "Name": "vpc-id",
                        "Values": [vpc["VpcId"]],
                    }
                ]
            )["RouteTables"]
            for rt in rts:
                logger.info(f"Deleting RouteTable: {rt['RouteTableId']}")
                try:
                    self.ec2_client.delete_route_table(RouteTableId=rt["RouteTableId"])
                except ClientError as err:
                    logger.warning(err)

            # get all NetworkAcls related to the particular VPC
            nas = self.ec2_client.describe_network_acls(
                Filters=[
                    {
                        "Name": "vpc-id",
                        "Values": [vpc["VpcId"]],
                    }
                ]
            )["NetworkAcls"]
            for na in nas:
                logger.info(f"Deleting NetworkAcl: {na['NetworkAclId']}")
                try:
                    self.ec2_client.delete_network_acl(NetworkAclId=na["NetworkAclId"])
                except ClientError as err:
                    logger.warning(err)

            # get all VpcPeeringConnections related to the particular VPC
            vpc_pcs = self.ec2_client.describe_vpc_peering_connections(
                Filters=[
                    {
                        "Name": "requester-vpc-info.vpc-id",
                        "Values": [vpc["VpcId"]],
                    }
                ]
            )["VpcPeeringConnections"]
            for vpc_pc in vpc_pcs:
                logger.info(
                    f"Deleting VpcPeeringConnection: {vpc_pc['VpcPeeringConnectionId']}"
                )
                try:
                    self.ec2_client.delete_vpc_peering_connections(
                        VpcPeeringConnectionId=vpc_pc["VpcPeeringConnectionId"]
                    )
                except ClientError as err:
                    logger.warning(err)

            # get all VpcEndpoints related to the particular VPC
            vpc_es = self.ec2_client.describe_vpc_endpoints(
                Filters=[
                    {
                        "Name": "vpc-id",
                        "Values": [vpc["VpcId"]],
                    }
                ]
            )["VpcEndpoints"]
            for vpc_e in vpc_es:
                logger.info(f"Deleting VpcEndpoint: {vpc_e['VpcEndpointId']}")
                try:
                    self.ec2_client.delete_vpc_endpoints(
                        VpcEndpointIds=[vpc_e["VpcEndpointId"]]
                    )
                except ClientError as err:
                    logger.warning(err)

            # get all NatGateways related to the particular VPC
            ngs = self.ec2_client.describe_nat_gateways(
                Filters=[
                    {
                        "Name": "vpc-id",
                        "Values": [vpc["VpcId"]],
                    }
                ]
            )["NatGateways"]
            for ng in ngs:
                logger.info(f"Deleting NatGateway: {ng['NatGatewayId']}")
                try:
                    self.ec2_client.delete_nat_gateways(NatGatewayId=ng["NatGatewayId"])
                except ClientError as err:
                    logger.warning(err)

            # get all VpnConnections related to the particular VPC
            vcs = self.ec2_client.describe_vpn_connections(
                Filters=[
                    {
                        "Name": "attachment.vpc-id",
                        "Values": [vpc["VpcId"]],
                    }
                ]
            )["VpnConnections"]
            for vc in vcs:
                logger.info(f"Deleting VpnConnection: {vc['VpnConnectionId']}")
                try:
                    self.ec2_client.delete_vpn_connection(
                        VpnConnectionId=vc["VpnConnectionId"]
                    )
                except ClientError as err:
                    logger.warning(err)

            # get all VpnGateways related to the particular VPC
            vgs = self.ec2_client.describe_vpn_gateways(
                Filters=[
                    {
                        "Name": "vpc-id",
                        "Values": [vpc["VpcId"]],
                    }
                ]
            )["VpnGateways"]
            for vg in vgs:
                logger.info(f"Deleting VpnGateway: {vg['VpnGatewayId']}")
                try:
                    self.ec2_client.delete_vpn_gateway(VpnGatewayId=vg["VpnGatewayId"])
                except ClientError as err:
                    logger.warning(err)

            # get all SecurityGroups related to the particular VPC
            sgs = self.ec2_client.describe_security_groups(
                Filters=[
                    {
                        "Name": "vpc-id",
                        "Values": [vpc["VpcId"]],
                    }
                ]
            )["SecurityGroups"]
            for sg in sgs:
                logger.info(f"Deleting SecurityGroup: {sg['GroupId']}")
                try:
                    self.ec2_client.delete_security_group(GroupId=sg["GroupId"])
                except ClientError as err:
                    logger.warning(err)

            logger.info(f"Deleting VPC: {vpc['VpcId']}")
            try:
                self.ec2_client.delete_vpc(VpcId=vpc["VpcId"], DryRun=False)
            except ClientError as err:
                logger.warning(err)

        logger.info(f"Deleting CloudFormation Stack: {cfs_name}")
        self.delete_cloudformation_stacks([cfs_name])

    def delete_hosted_zone(
        self, cluster_name, delete_zone=True, delete_from_base_domain=False
    ):
        """
        Deletes the hosted zone

        Args:
            cluster_name (str): Name of the cluster
            delete_zone (bool): Whether to delete complete zone
            delete_from_base_domain (bool): Whether to delete record from base domain

        """
        cluster_name = cluster_name or config.ENV_DATA["cluster_name"]
        base_domain = config.ENV_DATA["base_domain"]
        hosted_zone_name = f"{cluster_name}.{base_domain}."

        hosted_zones = self.route53_client.list_hosted_zones_by_name(
            DNSName=hosted_zone_name, MaxItems="50"
        )["HostedZones"]
        hosted_zone_ids = [
            zone["Id"] for zone in hosted_zones if zone["Name"] == hosted_zone_name
        ]

        if hosted_zone_ids:
            for hosted_zone_id in hosted_zone_ids:
                logger.info(
                    f"Deleting domain name {hosted_zone_name} with "
                    f"hosted zone ID {hosted_zone_id}"
                )
                self.delete_all_record_sets(hosted_zone_id)
                if delete_zone:
                    self.route53_client.delete_hosted_zone(Id=hosted_zone_id)
                if delete_from_base_domain:
                    self.delete_record_from_base_domain(
                        cluster_name=cluster_name, base_domain=base_domain
                    )

        else:
            logger.info(f"hosted zone {hosted_zone_name} not found")
            return

    def delete_all_record_sets(self, hosted_zone_id):
        """
        Deletes all record sets in a hosted zone

        Args:
            hosted_zone_id (str): Hosted Zone ID
                example: /hostedzone/Z91022921MMOZDVPPC8D6

        """
        record_types_exclude = ["NS", "SOA"]
        record_sets = self.route53_client.list_resource_record_sets(
            HostedZoneId=hosted_zone_id
        )["ResourceRecordSets"]

        for each_record in record_sets:
            record_set_type = each_record["Type"]
            if record_set_type not in record_types_exclude:
                self.delete_record(each_record, hosted_zone_id)
        logger.info("Successfully deleted all record sets")

    def delete_record_from_base_domain(self, cluster_name, base_domain=None):
        """
        Deletes the record for cluster name in base domain

        Args:
            cluster_name (str): Name of the cluster
            base_domain (str): Base domain name

        """
        base_domain = base_domain or config.ENV_DATA["base_domain"]
        record_name = f"{cluster_name}.{base_domain}."
        hosted_zone_id = self.get_hosted_zone_id_for_domain(domain=base_domain)
        record_sets_in_base_domain = self.get_record_sets(domain=base_domain)

        for record in record_sets_in_base_domain:
            if record["Name"] == record_name:
                logger.info(f"Deleting record {record_name} from {base_domain}")
                self.delete_record(record, hosted_zone_id)
                # breaking here since we will have single record in
                # base domain and deleting is destructive action
                break

    def get_record_sets(self, domain=None):
        """
        Get all the record sets in domain

        Args:
            domain (str): Domain name to fetch the records

        Returns:
            list: list of record sets

        """
        domain = domain or config.ENV_DATA["base_domain"]
        hosted_zone_id = self.get_hosted_zone_id_for_domain(domain=domain)
        record_sets = []
        record_sets_data = self.route53_client.list_resource_record_sets(
            HostedZoneId=hosted_zone_id
        )
        record_sets.extend(record_sets_data["ResourceRecordSets"])
        # If a ListResourceRecordSets command returns more than one page of results,
        # the value of IsTruncated is true. To display the next page of results,
        # get the values of NextRecordName, NextRecordType, and NextRecordIdentifier (if any)
        # from the response.
        # Then submit another ListResourceRecordSets request, and specify those values for StartRecordName,
        # StartRecordType, and StartRecordIdentifier.
        is_truncated = record_sets_data["IsTruncated"]
        while is_truncated:
            start_record_name = record_sets_data["NextRecordName"]
            start_record_type = record_sets_data["NextRecordType"]

            record_sets_data = self.route53_client.list_resource_record_sets(
                HostedZoneId=hosted_zone_id,
                StartRecordName=start_record_name,
                StartRecordType=start_record_type,
            )
            record_sets.extend(record_sets_data["ResourceRecordSets"])
            is_truncated = record_sets_data["IsTruncated"]

        return record_sets

    def delete_record(self, record, hosted_zone_id):
        """
        Deletes the record from Hosted Zone

        Args:
            record (dict): record details to delete
                e.g:{
                'Name': 'vavuthu-eco1.qe.rh-ocs.com.',
                'Type': 'NS',
                'TTL': 300,
                'ResourceRecords':[
                {'Value': 'ns-1389.awsdns-45.org'},
                {'Value': 'ns-639.awsdns-15.net'},
                {'Value': 'ns-1656.awsdns-15.co.uk'},
                {'Value': 'ns-183.awsdns-22.com'}
                ]
                }
            hosted_zone_id (str): Hosted Zone ID
                example: /hostedzone/Z91022921MMOZDVPPC8D6

        """
        record_set_name = record["Name"]
        record_set_type = record["Type"]
        record_set_ttl = record["TTL"]
        record_set_resource_records = record["ResourceRecords"]
        logger.info(f"deleting record set: {record_set_name}")
        resource_record_set = {
            "Name": record_set_name,
            "Type": record_set_type,
            "TTL": record_set_ttl,
            "ResourceRecords": record_set_resource_records,
        }
        # Weight and SetIdentifier is needed for
        # deleting api-int.cls-vavuthu-eco1.qe.rh-ocs.com. and
        # api.cls-vavuthu-eco1.qe.rh-ocs.com.
        if record.get("Weight"):
            resource_record_set["Weight"] = record.get("Weight")
            resource_record_set["SetIdentifier"] = record.get("SetIdentifier")
        self.route53_client.change_resource_record_sets(
            HostedZoneId=hosted_zone_id,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "DELETE",
                        "ResourceRecordSet": resource_record_set,
                    }
                ]
            },
        )

    def get_hosted_zone_id(self, cluster_name):
        """
        Get Zone id from given cluster_name

        Args:
            cluster_name (str): Name of cluster

        Returns:
            str: Zone id
        """
        cluster_name = cluster_name or config.ENV_DATA["cluster_name"]
        base_domain = config.ENV_DATA["base_domain"]
        hosted_zone_name = f"{cluster_name}.{base_domain}."
        hosted_zones_output = self.route53_client.list_hosted_zones_by_name(
            DNSName=hosted_zone_name
        )
        full_hosted_zone_id = hosted_zones_output["HostedZones"][0]["Id"]
        return full_hosted_zone_id.strip("/hostedzone/")

    def update_hosted_zone_record(
        self, zone_id, record_name, data, type, operation_type, ttl=60, raw_data=None
    ):
        """
        Update Route53 DNS record

        Args:
            zone_id (str): Zone id of DNS record
            record_name (str): Record Name without domain
                eg: api.apps.ocp-baremetal-auto
            data (str): Data to be added for DNS Record
            type (str): DNS record type
            operation_type (str): Operation Type (Allowed Values:- Add, Delete)
            ttl (int): Default set to 60 sec
            raw_data (list): Data to be added as a record

        Returns:
            dict: The response from change_resource_record_sets
        """
        base_domain = config.ENV_DATA["base_domain"]
        record_name = f"{record_name}.{base_domain}."
        if "*" in record_name:
            trim_record_name = record_name.strip("*.")
        else:
            trim_record_name = record_name
        old_resource_record_list = []
        res = self.route53_client.list_resource_record_sets(HostedZoneId=zone_id)
        for records in res.get("ResourceRecordSets"):
            if trim_record_name in records.get("Name"):
                old_resource_record_list = records.get("ResourceRecords")

        if operation_type == "Add":
            old_resource_record_list.append({"Value": data})
        elif operation_type == "Delete":
            old_resource_record_list.remove({"Value": data})
        if raw_data:
            old_resource_record_list = data
        response = self.route53_client.change_resource_record_sets(
            HostedZoneId=zone_id,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "UPSERT",
                        "ResourceRecordSet": {
                            "Name": record_name,
                            "Type": type,
                            "TTL": ttl,
                            "ResourceRecords": old_resource_record_list,
                        },
                    }
                ],
            },
        )
        logger.debug(f"Record Created with {record_name} for {data}")
        return response

    def wait_for_record_set(self, response_list, max_attempts=10):
        """
        Wait for Record to be created

        Args:
            max_attempts (int): Max Attempt's for Waiting
            response_list (list): List of response

        """

        waiter = self.route53_client.get_waiter("resource_record_sets_changed")
        for response in response_list:
            logger.debug(f"Waiting for Response {response['ChangeInfo']['Id']}")
            waiter.wait(
                Id=response["ChangeInfo"]["Id"],
                WaiterConfig={"MaxAttempts": max_attempts},
            )

    def get_hosted_zone_id_for_domain(self, domain=None):
        """
        Get Zone id for domain

        Args:
            domain (str): Name of the domain.

        Returns:
            str: Zone id

        """
        domain = domain or config.ENV_DATA["base_domain"]
        hosted_zones = self.route53_client.list_hosted_zones_by_name(
            DNSName=domain, MaxItems="1"
        )["HostedZones"]
        hosted_zone_ids = [
            zone["Id"] for zone in hosted_zones if zone["Name"] == f"{domain}."
        ]
        return hosted_zone_ids[0]

    def create_hosted_zone(self, cluster_name):
        """
        Create Hosted Zone

        Args:
            cluster_name (str): Name of cluster

        Returns:
            str: Hosted Zone id

        """
        ts = time.time()
        domain = config.ENV_DATA["base_domain"]
        full_cluster_name = f"{cluster_name}.{domain}."
        response = self.route53_client.create_hosted_zone(
            Name=full_cluster_name, CallerReference=str(ts)
        )

        full_hosted_zone_id = response["HostedZone"]["Id"]
        hosted_zone_id = full_hosted_zone_id.strip("/hostedzone/")
        logger.info(
            f"Hosted zone Created with id {hosted_zone_id} and name is {response['HostedZone']['Name']}"
        )
        return hosted_zone_id

    def get_hosted_zone_details(self, zone_id):
        """
        Get Hosted zone Details

        Args:
            zone_id (str): Zone Id of cluster_name

        Returns:
            dict: Response

        """
        return self.route53_client.get_hosted_zone(Id=zone_id)

    def get_ns_for_hosted_zone(self, zone_id):
        """
        Get NameServers Details from Hosted Zone

        Args:
            zone_id (str): Zone Id of cluster_name

        Returns:
            list: NameServers

        """
        return self.get_hosted_zone_details(zone_id)["DelegationSet"]["NameServers"]

    def wait_for_instances_to_stop(self, instances):
        """
        Wait for the instances to reach status stopped

        Args:
            instances: A dictionary of instance IDs and names

        Raises:
            botocore.exceptions.WaiterError: If it failed to reach the expected status stopped

        """
        for instance_id, instance_name in instances.items():
            logger.info(f"Waiting for instance {instance_name} to reach status stopped")
            instance = self.get_ec2_instance(instance_id)
            instance.wait_until_stopped()

    def wait_for_instances_to_terminate(self, instances):
        """
        Wait for the instances to reach status terminated

        Args:
            instances: A dictionary of instance IDs and names

        Raises:
            botocore.exceptions.WaiterError: If it failed to reach the expected status terminated

        """
        for instance_id, instance_name in instances.items():
            logger.info(
                f"Waiting for instance {instance_name} to reach status terminated"
            )
            instance = self.get_ec2_instance(instance_id)
            instance.wait_until_terminated()

    def wait_for_instances_to_stop_or_terminate(self, instances):
        """
        Wait for the instances to reach statuses stopped or terminated

        Args:
            instances: A dictionary of instance IDs and names

        Raises:
            botocore.exceptions.WaiterError: If it failed to reach the expected statuses stopped or terminated

        """
        for instance_id, instance_name in instances.items():
            logger.info(
                f"Waiting for instance {instance_name} to reach status stopped or terminated"
            )
            instance = self.get_ec2_instance(instance_id)
            try:
                instance.wait_until_stopped()
            except WaiterError as e:
                logger.warning(
                    f"Failed to reach the status stopped due to the error {str(e)}"
                )
                logger.info(
                    f"Waiting for instance {instance_name} to reach status terminated"
                )
                instance.wait_until_terminated()

    def list_buckets(self):
        """
        List the buckets

        Returns:
            list: List of dictionaries which contains bucket name and creation date as keys
               e.g: [
               {'Name': '214qpg-oidc', 'CreationDate': datetime.datetime(2023, 1, 9, 11, 27, 48, tzinfo=tzutc())},
               {'Name': '214rmh4-oidc', 'CreationDate': datetime.datetime(2023, 1, 9, 12, 32, 8, tzinfo=tzutc())}
               ]

        """
        return self.s3_client.list_buckets()["Buckets"]

    def get_buckets_to_delete(self, bucket_prefix, hours):
        """
        Get the bucket with prefix which are older than given days

        Args:
            bucket_prefix (str): prefix for the buckets to fetch
            hours (int): fetch buckets that are older than to the specified number of hours

        """
        buckets_to_delete = []
        # Get the current date in UTC
        current_date = datetime.now(timezone.utc)
        all_buckets = self.list_buckets()
        for bucket in all_buckets:
            bucket_name = bucket["Name"]

            bucket_delete_time = self.get_bucket_time_based_rules(
                bucket_prefix, bucket_name, hours
            )
            # Get the creation date of the bucket in UTC
            bucket_creation_date = bucket["CreationDate"].replace(tzinfo=timezone.utc)

            # Calculate the age of the bucket
            age_of_bucket = current_date - bucket_creation_date

            # Check if the bucket is older than given days
            if (age_of_bucket.days) * 24 >= bucket_delete_time:
                logger.info(
                    f"{bucket_name} (Created on {bucket_creation_date} and age is {age_of_bucket}) can be deleted"
                )
                buckets_to_delete.append(bucket_name)
        return buckets_to_delete

    def get_bucket_time_based_rules(self, bucket_prefixes, bucket_name, hours):
        """
        Get the time bucket based prefix and hours

        Args:
            bucket_prefixes (dict): The rules according to them determine the number of hours the bucket can exist
            bucket_name (str): bucket name
            hours (int): The number of hours bucket can exist if there is no compliance with one of the rules

        Returns:
            int: The number of hours bucket can exist

        """
        for bucket_prefix in bucket_prefixes:
            if bool(re.match(bucket_prefix, bucket_name, re.I)):
                return bucket_prefixes[bucket_prefix]
        return hours

    def delete_objects_in_bucket(self, bucket):
        """
        Delete objects in a bucket

        Args:
            bucket (str): Name of the bucket to delete objects

        """
        # List all objects within the bucket
        response = self.s3_client.list_objects_v2(Bucket=bucket)

        # Delete each object within the bucket
        if "Contents" in response:
            for obj in response["Contents"]:
                object_key = obj["Key"]
                self.s3_client.delete_object(Bucket=bucket, Key=object_key)
                logger.info(f"Deleted object: {object_key}")
        else:
            logger.info(f"No objects found in bucket {bucket}")

    def delete_bucket(self, bucket):
        """
        Delete the bucket

        Args:
            bucket (str): Name of the bucket to delete

        """
        logger.info(f"Deleting bucket {bucket}")
        self.delete_objects_in_bucket(bucket=bucket)

        # Delete the empty bucket
        self.s3_client.delete_bucket(Bucket=bucket)
        logger.info(f"Deleted bucket {bucket}")

    def delete_buckets(self, buckets):
        """
        Delete the buckets

        Args:
            buckets (list): List of buckets to delete

        """
        for each_bucket in buckets:
            self.delete_bucket(bucket=each_bucket)


def get_instances_ids_and_names(instances):
    """
    Get the instances IDs and names according to nodes dictionary

    Args:
        instances (list): Nodes dictionaries, returned by 'oc get node -o yaml'

    Returns:
        dict: The ID keys and the name values of the instances

    """
    return {
        "i-"
        + instance.get()
        .get("spec")
        .get("providerID")
        .partition("i-")[-1]: instance.get()
        .get("metadata")
        .get("name")
        for instance in instances
    }


def get_data_volumes(deviceset_pvs):
    """
    Get the instance data volumes (which doesn't include root FS)

    Args:
        deviceset_pvs (list): PVC objects of the deviceset PVs

    Returns:
        list: ec2 Volume instances

    """
    aws = AWS()

    volume_ids = [
        "vol-"
        + pv.get()
        .get("spec")
        .get("awsElasticBlockStore")
        .get("volumeID")
        .partition("vol-")[-1]
        for pv in deviceset_pvs
    ]
    return [aws.ec2_resource.Volume(vol_id) for vol_id in volume_ids]


def get_vpc_id_by_node_obj(aws_obj, instances):
    """
    This function getting vpc id by randomly selecting instances out of user aws deployment

    Args:
        aws_obj (obj): AWS() object
        instances (dict): cluster ec2 instances objects

    Returns:
        str: vpc_id: The vpc id

    """

    instance_id = random.choice(list(instances.keys()))
    vpc_id = aws_obj.get_vpc_id_by_instance_id(instance_id)

    return vpc_id


def get_rhel_worker_instances(cluster_path):
    """
    Get list of rhel worker instance IDs

    Args:
        cluster_path (str): The cluster path

    Returns:
        list: list of instance IDs of rhel workers

    """
    aws = AWS()
    rhel_workers = []
    worker_pattern = get_infra_id(cluster_path) + "*rhel-worker*"
    worker_filter = [{"Name": "tag:Name", "Values": [worker_pattern]}]

    response = aws.ec2_client.describe_instances(Filters=worker_filter)
    if not response["Reservations"]:
        return
    for worker in response["Reservations"]:
        rhel_workers.append(worker["Instances"][0]["InstanceId"])
    return rhel_workers


def terminate_rhel_workers(worker_list):
    """
    Terminate the RHEL worker EC2 instances

    Args:
        worker_list (list): Instance IDs of rhel workers

    Raises:
        exceptions.FailedToDeleteInstance: if failed to terminate

    """
    aws = AWS()
    if not worker_list:
        logger.info("No workers in list, skipping termination of RHEL workers")
        return

    logger.info(f"Terminating RHEL workers {worker_list}")
    # Do a dry run of instance termination
    try:
        aws.ec2_client.terminate_instances(InstanceIds=worker_list, DryRun=True)
    except aws.ec2_client.exceptions.ClientError as err:
        if "DryRunOperation" in str(err):
            logger.info("Instances can be deleted")
        else:
            logger.error("Some of the Instances can't be deleted")
            raise exceptions.FailedToDeleteInstance()
    # Actual termination call here
    aws.ec2_client.terminate_instances(InstanceIds=worker_list, DryRun=False)
    try:
        waiter = aws.ec2_client.get_waiter("instance_terminated")
        waiter.wait(InstanceIds=worker_list)
        logger.info("Instances are terminated")
    except aws.ec2_client.exceptions.WaiterError as ex:
        logger.error(f"Failed to terminate instances {ex}")
        raise exceptions.FailedToDeleteInstance()


def destroy_volumes(cluster_name):
    """
    Destroy cluster volumes

    Args:
        cluster_name (str): The name of the cluster

    """
    aws = AWS()
    try:
        volume_pattern = f"{cluster_name}*"
        logger.debug(f"Finding volumes with pattern: {volume_pattern}")
        volumes = aws.get_volumes_by_name_pattern(volume_pattern)
        logger.debug(f"Found volumes: \n {volumes}")
        for volume in volumes:
            # skip root devices for deletion
            # EBS root device volumes are automatically deleted when
            # the instance terminates
            if not check_root_volume(volume):
                aws.detach_and_delete_volume(aws.ec2_resource.Volume(volume["id"]))
    except Exception:
        logger.error(traceback.format_exc())


def check_root_volume(volume):
    """
    Checks whether given EBS volume is root device or not

    Args:
         volume (dict): EBS volume dictionary

    Returns:
        bool: True if EBS volume is root device, False otherwise

    """
    return True if volume["attachments"][0]["DeleteOnTermination"] else False


def update_config_from_s3(
    bucket_name=constants.OCSCI_DATA_BUCKET, filename=constants.AUTHYAML
):
    """
    Get the config file that has secrets/configs from the S3 and update the config

    Args:
        bucket_name (string): name of the bucket
        filename (string): name of the file in bucket

    Returns:
        dict: returns the updated file contents as python dict
        None: In case the private bucket could not be accessed

    """
    try:
        logger.info("Fetching authentication credentials from ocs-ci-data")
        s3 = boto3.resource("s3")
        with NamedTemporaryFile(mode="w", prefix="config", delete=True) as auth:
            s3.meta.client.download_file(bucket_name, filename, auth.name)
            config_yaml = load_yaml(auth.name)
        config.update(config_yaml)
        return config_yaml
    except NoCredentialsError:
        logger.warning("Failed to fetch auth.yaml from ocs-ci-data")
        return None
    except ClientError:
        logger.warning(f"Permission denied to access bucket {bucket_name}")
        return None


def delete_cluster_buckets(cluster_name):
    """
    Delete s3 buckets corresponding to a particular OCS cluster

    Args:
        cluster_name (str): name of the cluster the buckets belong to

    """
    region = config.ENV_DATA["region"]
    base_domain = config.ENV_DATA["base_domain"]
    s3_client = boto3.client("s3", region_name=region)
    buckets = s3_client.list_buckets()["Buckets"]
    bucket_names = [bucket["Name"] for bucket in buckets]
    logger.debug("Found buckets: %s", bucket_names)

    # patterns for mcg target bucket, image-registry buckets and bucket created
    # durring installation via Flexy (for installation files)
    patterns = [
        f"nb.(\\d+).apps.{cluster_name}.{base_domain}",
        f"{cluster_name}-(\\w+)-image-registry-{region}-(\\w+)",
        f"{cluster_name}-(\\d{{4}})-(\\d{{2}})-(\\d{{2}})-(\\d{{2}})-(\\d{{2}})-(\\d{{2}})",
        f"{cluster_name}-(\\w+)-oidc",
        f"{cluster_name}-(\\d{{8}})",
    ]
    for pattern in patterns:
        r = re.compile(pattern)
        filtered_buckets = list(filter(r.search, bucket_names))
        logger.info(f"Found buckets: {filtered_buckets}")
        s3_resource = boto3.resource("s3", region_name=region)
        for bucket_name in filtered_buckets:
            logger.info("Deleting all files in bucket %s", bucket_name)
            try:
                bucket = s3_resource.Bucket(bucket_name)
                bucket.objects.delete()
                logger.info("Deleting bucket %s", bucket_name)
                bucket.delete()
            except ClientError as e:
                logger.error(e)


def get_stack_name_from_instance_dict(instance_dict):
    """
    Get the stack name by the given instance dictionary from AWS

    Args:
        instance_dict (dict): The instance dictionary from AWS

    Returns:
        str: The stack name of the given instance dictionary from AWS.
             If not found returns None
    """
    tags = instance_dict.get("Tags", [])
    stack_name = None

    for tag in tags:
        if tag.get("Key") == constants.AWS_CLOUDFORMATION_TAG:
            stack_name = tag.get("Value")

    return stack_name


def create_and_attach_ebs_volumes(
    worker_pattern, size=100, count=1, device_names=("sdx",)
):
    """
    Create volumes on workers

    Args:
        worker_pattern (string): Worker name pattern e.g.:
            cluster-55jx2-worker*
        size (int): Size in GB (default: 100)
        count (int): number of EBS volumes to attach to worker node, if it's
        device_names (list): list of the devices like ["sda", "sdb"]. Length of list needs
            to match count!

    Raises:
        UnexpectedInput: In case the device_names length doesn't match count.

    """
    region = config.ENV_DATA["region"]
    if len(device_names) != count:
        raise exceptions.UnexpectedInput(
            "The device_names doesn't contain the same number of devices as the "
            f"count, which is: {count}! If count is 2, the device_names should be for example ['sdc', 'sdx']!"
        )
    aws = AWS(region)
    worker_instances = aws.get_instances_by_name_pattern(worker_pattern)
    with parallel() as p:
        for worker in worker_instances:
            for number in range(1, count + 1):
                logger.info(
                    f"Creating and attaching {number}. {size} GB volume to {worker['name']}"
                )
                p.spawn(
                    aws.create_volume_and_attach,
                    availability_zone=worker["avz"],
                    instance_id=worker["id"],
                    name=f"{worker['name']}_extra_volume_{number}",
                    size=size,
                    device=f"/dev/{device_names[number - 1]}",
                )


def create_and_attach_volume_for_all_workers(
    device_size=None,
    worker_suffix="worker",
    count=1,
    device_letters="ghijklmnopxyz",
):
    """
    Create volumes on workers

    Args:
        device_size (int): Size in GB, if not specified value from:
            config.ENV_DATA["device_size"] will be used
        worker_suffix (str): Worker name suffix (default: worker)
        count (int): number of EBS volumes to attach to worker node
        device_letters (str): device letters from which generate device names.
            e.g. for "abc" and if count=2 it will generate ["sda", "sdb"]

    """
    device_size = device_size or int(
        config.ENV_DATA.get("device_size", defaults.DEVICE_SIZE)
    )
    device_letters = "ghijklmnopxyz"
    device_names = [f"sd{letter}" for letter in device_letters[:count]]
    infra_id = get_infra_id(config.ENV_DATA["cluster_path"])
    create_and_attach_ebs_volumes(
        f"{infra_id}-{worker_suffix}*",
        device_size,
        count,
        device_names,
    )
