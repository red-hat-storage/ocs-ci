import logging
import time

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

    @property
    def ec2_client(self):
        """ Property for ec2 client

        Returns:
            boto3.client: instance of ec2
        """
        if not self._ec2_client:
            self._ec2_client = boto3.client('ec2')
        return self._ec2_client

    @property
    def ec2_resource(self):
        """ Property for ec2 resource

        Returns:
            boto3.resource instance of ec2 resource
        """
        if not self._ec2_resource:
            self._ec2_resource = boto3.resource('ec2')
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
                    'Values': [pattern]
                }
            ]
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
                    ]
                },
            ]
        )
        logger.debug("Response of volume creation: %s", volume_response)
        volume = self.ec2_resource.Volume(volume_response['VolumeId'])
        for x in range(timeout):
            volume.reload()
            if volume.state == 'available':
                break
            if x == timeout - 1:
                raise AWSTimeoutException(
                    "Reached timeout %s for volume creation, volume state is "
                    "still: %s for volume ID: %s", timeout, volume.state,
                    volume_response['VolumeId']
                )
            time.sleep(1)
        attach_response = volume.attach_to_instance(
            Device=device,
            InstanceId=instance_id,
        )
        logger.debug("Response of attaching volume: %s", attach_response)
