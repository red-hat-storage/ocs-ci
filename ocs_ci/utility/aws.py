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
            boto3.Instance: instance of ec2 instance resource

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

    def create_volume(
        self,
        availability_zone,
        name,
        encrypted=False,
        size=100,
        timeout=20,
        volume_type='gp2'
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
        return volume

    def attach_volume(self, volume, instance_id, device='/dev/sdx'):
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
        device='/dev/sdx',
        encrypted=False,
        size=100,
        timeout=20,
        volume_type='gp2',
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
                "Detaching volume: %s Instance: %s", volume.volume_id,
                attachment.get('InstanceId')
            )
            response_detach = volume.detach_from_instance(
                Device=attachment['Device'],
                InstanceId=attachment['InstanceId'],
                Force=True,
            )
            logger.debug("Detach response: %s", response_detach)
        for x in range(timeout):
            volume.reload()
            logger.debug(
                "Volume id: %s has status: %s", volume.volume_id,
                volume.state
            )
            if volume.state == 'available':
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
            "Delete response for volume: %s is: %s", volume.volume_id,
            delete_response
        )

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
        stopping_instances = ret.get('StoppingInstances')
        for instance in stopping_instances:
            assert instance.get('CurrentState').get('Code') in [
                constants.INSTANCE_STOPPED, constants.INSTANCE_STOPPING,
                constants.INSTANCE_SHUTTING_DOWN
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
        starting_instances = ret.get('StartingInstances')
        for instance in starting_instances:
            assert instance.get('CurrentState').get('Code') in [
                constants.INSTANCE_RUNNING, constants.INSTANCE_PENDING
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

    def restart_ec2_instances(self, instances, wait=False, force=True):
        """
        Stop and start ec2 instances

        Args:
            instances (dict): A dictionary of instance IDs and names to restart
            wait (bool): True in case wait for status is needed,
                False otherwise
            force (bool): True for force instance stop, False otherwise

        """
        self.stop_ec2_instances(instances=instances, wait=wait, force=force)
        self.start_ec2_instances(instances=instances, wait=wait)

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


def get_instances_ids_and_names(instances):
    """
    Get the instances IDs and names according to nodes dictionary

    Args:
        instances (list): Nodes dictionaries, returned by 'oc get node -o yaml'

    Returns:
        dict: The ID keys and the name values of the instances

    """
    return {
        'i-' + instance.get().get('spec').get('providerID').partition('i-')[-1]:
        instance.get().get('metadata').get('name') for instance in instances
    }


def get_data_volumes(instance_id):
    """
    Get the instance data volumes (which doesn't include root FS)

    Args:
        instance_id (str): The ID of the instance

    Returns:
        list: ec2 Volume instances

    """
    aws = AWS()
    volumes = aws.get_ec2_instance_volumes(instance_id)

    # Get the data volume according to DeleteOnTermination
    return [
        vol for vol in volumes if vol.attachments[0]
        .get('DeleteOnTermination') is False
    ]
