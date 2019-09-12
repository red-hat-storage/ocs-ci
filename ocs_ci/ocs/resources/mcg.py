import base64
import logging

import boto3
from botocore.client import ClientError

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility import templating
from ocs_ci.utility.utils import run_mcg_cmd
from tests.helpers import create_unique_resource_name, create_resource

logger = logging.getLogger(name=__file__)


class MCG(object):
    """
    Wrapper class for the Multi Cloud Gateway's S3 service
    """

    s3_resource, ocp_resource, endpoint, region, access_key_id, access_key = (None,) * 6

    def __init__(self):
        """
        Constructor for the MCG class
        """
        ocp_obj = OCP(kind='noobaa', namespace='openshift-storage')
        results = ocp_obj.get()
        self.endpoint = 'http:' + (
            results.get('items')[0].get('status').get('services')
            .get('serviceS3').get('externalDNS')[0].split(':')[1]
        )
        self.region = self.endpoint.split('.')[1]
        creds_secret_name = (
            results.get('items')[0].get('status').get('accounts')
            .get('admin').get('secretRef').get('name')
        )
        secret_ocp_obj = OCP(kind='secret', namespace='openshift-storage')
        results2 = secret_ocp_obj.get(creds_secret_name)

        self.access_key_id = base64.b64decode(
            results2.get('data').get('AWS_ACCESS_KEY_ID')
        ).decode('utf-8')
        self.access_key = base64.b64decode(
            results2.get('data').get('AWS_SECRET_ACCESS_KEY')
        ).decode('utf-8')

        self._ocp_resource = ocp_obj
        self.s3_resource = boto3.resource(
            's3', verify=False, endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.access_key
        )

    def s3_create_bucket(self, bucketname, region=config.ENV_DATA['region']):
        """
        Args:
            bucketname: Name of the bucket to be created
            region: Name of the region for the bucket to be created on

        Returns:
            s3.Bucket object

        """
        return self.s3_resource.create_bucket(
            Bucket=bucketname,
            CreateBucketConfiguration={
                'LocationConstraint': region
            }
        ).name
    # Method alias to support the getattr abstraction in bucket_factory
    s3_create_obc = s3_create_bucket

    def oc_create_obc(self, bucketname):
        """
        Args:
            bucketname: Name of bucket to be created

        Returns:
            OCS: An OCS object representing the created bucket

        """
        obc_data = templating.load_yaml_to_dict(constants.MCG_OBC_YAML)
        if bucketname is None:
            bucketname = create_unique_resource_name('oc', 'obc')
        obc_data['metadata']['name'] = bucketname
        obc_data['spec']['bucketName'] = bucketname
        obc_obj = create_resource(**obc_data)
        return obc_obj

    def cli_create_obc(self, bucketname):
        """
        Args:
            bucketname: Name of bucket to be created

        """
        run_mcg_cmd(f'obc create --exact {bucketname}')

    def s3_delete_bucket(self, bucketname):
        """
        Args:
            bucketname: Name of bucket to be deleted

        """
        logger.info(f"Deleting bucket: {bucketname}")
        self.s3_resource.Bucket(bucketname).object_versions.delete()
        self.s3_resource.Bucket(bucketname).delete()
    # Method alias to support the getattr abstraction in bucket_factory
    s3_delete_obc = s3_delete_bucket

    def oc_delete_obc(self, bucketname):
        """
        Args:
            bucketname: Name of bucket to be deleted

        """
        logger.info(f"Deleting bucket: {bucketname}")
        OCP(kind='obc', namespace='openshift-storage').delete(resource_name=bucketname)

    def cli_delete_obc(self, bucketname):
        """
        Args:
            bucketname: Name of bucket to be deleted

        """
        logger.info(f"Deleting bucket: {bucketname}")
        run_mcg_cmd(f'obc delete {bucketname}')

    def s3_list_all_bucket_names(self):
        """
        Returns:
            list: A list of all bucket names

        """
        return [bucket.name for bucket in self.s3_resource.buckets.all()]

    def oc_list_all_bucket_names(self):
        """
        Returns:
            list: A list of all bucket names

        """
        return [bucket.get('spec').get('bucketName')
                for bucket
                in OCP(namespace='openshift-storage', kind='obc').get().get('items')]

    def cli_list_all_bucket_names(self):
        """
        Returns:
            list: A list of all bucket names

        """
        return [row.split()[1] for row in run_mcg_cmd('obc list').split('\n')[1:-1]]

    def s3_list_all_objects_in_bucket(self, bucketname):
        """
        Returns:
            list: A list of all bucket objects
        """
        return [obj for obj in self.s3_resource.Bucket(bucketname).objects.all()]

    def s3_get_all_buckets(self):
        """
        Returns:
            list: A list of all s3.Bucket objects

        """
        return [bucket for bucket in self.s3_resource.buckets.all()]

    def s3_verify_bucket_exists(self, bucketname):
        """
        Verifies whether a bucket with the given bucketname exists
        Args:
            bucketname : The bucket name to be verified

        Returns:
              bool: True if bucket exists, False otherwise

        """
        try:
            self.s3_resource.meta.client.head_bucket(Bucket=bucketname)
            logger.info(f"{bucketname} exists")
            return True
        except ClientError:
            logger.info(f"{bucketname} does not exist")
            return False

    def oc_verify_bucket_exists(self, bucketname):
        """
        Verifies whether a bucket with the given bucketname exists
        Args:
            bucketname : The bucket name to be verified

        Returns:
              bool: True if bucket exists, False otherwise

        """
        try:
            OCP(namespace='openshift-storage', kind='obc').get(bucketname)
            logger.info(f"{bucketname} exists")
            return True
        except CommandFailed as e:
            if 'NotFound' in repr(e):
                logger.info(f"{bucketname} does not exist")
                return False
            raise e

    def cli_verify_bucket_exists(self, bucketname):
        """
        Verifies whether a bucket with the given bucketname exists
        Args:
            bucketname : The bucket name to be verified

        Returns:
              bool: True if bucket exists, False otherwise

        """
        return bucketname in self.cli_list_all_bucket_names()
