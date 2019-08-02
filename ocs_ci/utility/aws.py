import logging
import time

import boto3

from ocs_ci.framework import config
from ocs_ci.ocs import constants

logger = logging.getLogger(name=__file__)

TIMEOUT = 90
SLEEP = 3


class AWSTimeoutException(Exception):
    pass


class AWS(object):
    """
    This is wrapper class for AWS
    """

    _ec2_client = None
    _ec2_resource = None
    _region_name = None

    def __init__(self, region_name=None):
        """
        Constructor for AWS class

        Args:
            region_name (str): Name of AWS region (default: us-east-2)
        """
        self._region_name = region_name or config.ENV_DATA['region']

    @property
    def ec2_client(self):
        """ Property for ec2 client

        Returns:
            boto3.client: instance of ec2
        """
        if not self._ec2_client:
            self._ec2_client = boto3.client(
                'ec2',
                region_name=self._region_name,
            )
        return self._ec2_client

    @property
    def ec2_resource(self):
        """ Property for ec2 resource

        Returns:
            boto3.resource instance of ec2 resource
        """
        if not self._ec2_resource:
            self._ec2_resource = boto3.resource(
                'ec2',
                region_name=self._region_name,
            )
        return self._ec2_resource

    def get_ec2_instance(self, instance_id):
        """
        Get instance of ec2 Instance

        Args:
            instance_id (str): The ID of the instance to get

        Returns:
            boto3.Instance instance of ec2 instance resource

        """
        return self.ec2_resource.Instance(instance_id)

    def get_instances_by_name_pattern(self, pattern):
        """ Get instances by Name tag pattern

        The instance details do not contain all the values but just those we
        are consuming.

        Those parameters we are storing for instance are:
        * id: id of instance
        * avz: Availability Zone
        * name: The value of Tag Name if define otherwise None

        Args:
            pattern (str): Pattern of tag name like:
                pbalogh-testing-cluster-55jx2-worker*

        Returns:
            list: contains dictionaries with instance details mentioned above
        """
        instances_response = self.ec2_client.describe_instances(
            Filters=[
                {
                    'Name': 'tag:Name',
                    'Values': [pattern],
                },
            ],
        )['Reservations']
        instances = []
        for instance in instances_response:
            instance = instance['Instances'][0]
            id = instance['InstanceId']
            avz = instance['Placement']['AvailabilityZone']
            name = None
            for tag in instance['Tags']:
                if tag['Key'] == 'Name':
                    name = tag['Value']
                    break
            instance_data = dict(
                id=id,
                avz=avz,
                name=name,
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
        return self.ec2_client.describe_instances(
            InstanceIds=[instance_id],
        ).get('Reservations')[0].get('Instances')[0].get('State').get('Code')

    def create_volume_and_attach(
        self,
        availability_zone,
        instance_id,
        name,
        device='/dev/sdx',
        encrypted=False,
        size=100,
        timeout=20,
        volume_type='gp2',
    ):
        """ Create volume and attach to instance

        Args:
            availability_zone (str): availability zone e.g.: us-west-1b
            instance_id (str): id of instance where to attach the volume
            name (str): name of volume
            device (str): name of device where to attach (default: /dev/sdx)
            encrypted (boolean): True if encrypted False otherwise
                (default: False)
            size (int): size in GB (default: 100)
            timeout (int): timeout in seconds for volume creation (default: 20)
            volume_type (str): 'standard'|'io1'|'gp2'|'sc1'|'st1'
                (default: gp2)
        """
        volume_response = self.ec2_client.create_volume(
            AvailabilityZone=availability_zone,
            Encrypted=encrypted,
            Size=size,
            VolumeType=volume_type,
            TagSpecifications=[
                {
                    'ResourceType': 'volume',
                    'Tags': [
                        {
                            'Key': 'Name',
                            'Value': name,
                        },
                    ],
                },
            ],
        )
        logger.debug("Response of volume creation: %s", volume_response)
        volume = self.ec2_resource.Volume(volume_response['VolumeId'])
        for x in range(timeout):
            volume.reload()
            logger.debug(
                "Volume id: %s has status: %s", volume.volume_id, volume.state
            )
            if volume.state == 'available':
                break
            if x == timeout - 1:
                raise AWSTimeoutException(
                    f"Reached timeout {timeout} for volume creation, volume "
                    f"state is still: {volume.state} for volume ID: "
                    f"{volume.volume_id}"
                )
            time.sleep(1)
        attach_response = volume.attach_to_instance(
            Device=device,
            InstanceId=instance_id,
        )
        logger.debug("Response of attaching volume: %s", attach_response)

    def get_volumes_by_name_pattern(self, pattern):
        """
        Get volumes by pattern

        Args:
            pattern (str): Pattern of volume name (e.g. '*cl-vol-*')

        Returns:
            list: Volume information like id and attachments
        """
        volumes_response = self.ec2_client.describe_volumes(
            Filters=[
                {
                    'Name': 'tag:Name',
                    'Values': [pattern],
                },
            ],
        )
        volumes = []
        for volume in volumes_response['Volumes']:
            volumes.append(
                dict(
                    id=volume['VolumeId'],
                    attachments=volume['Attachments'],
                )
            )
        return volumes

    def detach_and_delete_volume(self, volume, timeout=120):
        """
        Detach volume if attached and then delete it from AWS

        Args:
            volume (dict): Dict of volume details
            timeout (int): Timeout in seconds for API calls
        """
        ec2_volume = self.ec2_resource.Volume(volume['id'])
        if volume['attachments']:
            attachment = volume['attachments'][0]
            logger.info(
                "Detaching volume: %s Instance: %s", volume['id'],
                attachment['InstanceId']
            )
            response_detach = ec2_volume.detach_from_instance(
                Device=attachment['Device'],
                InstanceId=attachment['InstanceId'],
                Force=True,
            )
            logger.debug("Detach response: %s", response_detach)
        for x in range(timeout):
            ec2_volume.reload()
            logger.debug(
                "Volume id: %s has status: %s", ec2_volume.volume_id,
                ec2_volume.state
            )
            if ec2_volume.state == 'available':
                break
            if x == timeout - 1:
                raise AWSTimeoutException(
                    f"Reached timeout {timeout}s for volume detach/delete for "
                    f"volume ID: {volume['id']}, Volume state: "
                    f"{ec2_volume.state}"
                )
            time.sleep(1)
        logger.info("Deleting volume: %s", ec2_volume.volume_id)
        delete_response = ec2_volume.delete()
        logger.debug(
            "Delete response for volume: %s is: %s", ec2_volume.volume_id,
            delete_response
        )

    def stop_ec2_instance(self, instance_id, wait=False):
        """
        Stopping an instance

        Args:
            instance_id (str): ID of the instance to stop
            wait (bool): True in case wait for status is needed,
                False otherwise

        Returns:
            bool: True in case operation succeeded, False otherwise
        """
        res = self.ec2_client.stop_instances(
            InstanceIds=[instance_id], Force=True
        )
        if wait:
            instance = self.get_ec2_instance(instance_id)
            instance.wait_until_stopped()
        state = res.get('StoppingInstances')[0].get('CurrentState').get('Code')
        return state == constants.INSTANCE_STOPPING

    def start_ec2_instance(self, instance_id, wait=False):
        """
        Starting an instance

        Args:
            instance_id (str): ID of the instance to start
            wait (bool): True in case wait for status is needed,
                False otherwise

        Returns:
            bool: True in case operation succeeded, False otherwise
        """
        res = self.ec2_client.start_instances(InstanceIds=[instance_id])
        if wait:
            instance = self.get_ec2_instance(instance_id)
            instance.wait_until_running()
        state = res.get('StartingInstances')[0].get('CurrentState').get('Code')
        return state == constants.INSTANCE_PENDING


def get_instances_ids_and_names(instances):
    """
    Get the instances IDs and names according to nodes dictionary

    Args:
        instances (list): Nodes dictionaries, returned by 'oc get node -o yaml'

    Returns:
        tuple: lists of node IDs and names

    """
    instance_names = [node.get('metadata').get('name') for node in instances]
    instance_ids = [
        'i-' + node.get('spec').get(
            'providerID'
        ).partition('i-')[-1] for node in instances
    ]
    return instance_ids, instance_names


def stop_instances(instances):
    """
    Stop instances

    Args:
        instances (list): Dictionaries of instances (nodes returned
            by 'oc get node -o yaml') to stop

    """
    aws = AWS()
    instance_ids, instance_names = get_instances_ids_and_names(instances)

    for instance_id, instance_name in zip(instance_ids, instance_names):
        if aws.get_instances_status_by_id(instance_id) == constants.INSTANCE_RUNNING:
            logger.info(f"Stopping instance {instance_name}")
            aws.stop_ec2_instance(instance_id)

    for instance_id, instance_name in zip(instance_ids, instance_names):
        logger.info(f"Waiting for instance {instance_name} to reach status stopped")
        instance = aws.get_ec2_instance(instance_id)
        instance.wait_until_stopped()


def start_instances(instances):
    """
    Start instances

    Args:
        instances (list): Dictionaries of instances (nodes returned
            by 'oc get node -o yaml') to start

    """
    aws = AWS()
    instance_ids, instance_names = get_instances_ids_and_names(instances)

    for instance_id, instance_name in zip(instance_ids, instance_names):
        if aws.get_instances_status_by_id(instance_id) == constants.INSTANCE_STOPPED:
            logger.info(f"Starting instance {instance_name}")
            aws.start_ec2_instance(instance_id)

    for instance_id, instance_name in zip(instance_ids, instance_names):
        logger.info(f"Waiting for instance {instance_name} to reach status running")
        instance = aws.get_ec2_instance(instance_id)
        instance.wait_until_running()
