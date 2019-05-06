import logging
import time

import ocs.defaults as default

import boto3


logger = logging.getLogger(name=__file__)


class AWSTimeoutException(Exception):
    pass


class AWS(object):
    """
    This is wrapper class for AWS
    """

    _ec2_client = None
    _ec2_resource = None
    _region_name = None

    def __init__(self, region_name=default.AWS_REGION):
        """
        Constructor for AWS class

        Args:
            region_name (str): Name of AWS region (default: us-east-2)
        """
        self._region_name = region_name

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
            dict: Volume information like id and attachments
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
