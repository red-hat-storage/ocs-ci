import base64
import logging
import shlex

import boto3
from botocore.client import ClientError
from ocs_ci.framework import config
from ocs_ci.ocs.ocp import OCP


logger = logging.getLogger(name=__file__)


class MCG(object):
    """
    Wrapper class for the Multi Cloud Gateway's S3 service
    """

    s3_resource, ocp_resource, endpoint, region, access_key_id, access_key, namespace = (None,) * 7

    def __init__(self):
        """
        Constructor for the MCG class
        """
        self.namespace = config.ENV_DATA['cluster_namespace']
        ocp_obj = OCP(kind='noobaa', namespace=self.namespace)
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
        secret_ocp_obj = OCP(kind='secret', namespace=self.namespace)
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
        )

    def s3_delete_bucket(self, bucket):
        """
        Args:
            bucket: The bucket object to be deleted

        """
        bucket.delete()

    def s3_list_all_bucket_names(self):
        """
        Returns:
            list: A list of all bucket names

        """
        return [bucket.name for bucket in self.s3_resource.buckets.all()]

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

    def s3_verify_bucket_exists(self, bucket):
        """
        Verifies whether the Bucket exists
        Args:
            bucket(S3 object) : The bucket object to be verified

        Returns:
              bool: True if bucket exists, False otherwise

        """
        try:
            self.s3_resource.meta.client.head_bucket(Bucket=bucket.name)
            logger.info(f"{bucket.name} exists")
            return True
        except ClientError:
            logger.info(f"{bucket.name} does not exist")
            return False

    def verify_s3_object_integrity(self, original_object_path, result_object_path, awscli_pod):
        """
        Verifies checksum between orignial object and result object on an awscli pod

        Args:
            original_object_path (str): The Object that is uploaded to the s3 bucket
            result_object_path (str):  The Object that is downloaded from the s3 bucket
            awscli_pod (pod): A pod running the AWSCLI tools

        Returns:
              bool: True if checksum matches, False otherwise

        """
        md5sum = shlex.split(awscli_pod.exec_cmd_on_pod(command=f'md5sum {original_object_path} {result_object_path}'))
        if md5sum[0] == md5sum[2]:
            logger.info(f'Passed: MD5 comparison for {original_object_path} and {result_object_path}')
            return True
        else:
            logger.error(f'Failed: MD5 comparison of {original_object_path} and {result_object_path} - '
                         f'{md5sum[0]} ≠ {md5sum[2]}')
            return False

    def oc_create_bucket(self, bucketname):
        """
        Todo: Design and implement
        """
        raise NotImplementedError()

    def oc_delete_bucket(self, bucketname):
        """
        Todo: Design and implement
        """
        raise NotImplementedError()

    def oc_list_all_buckets(self):
        """
        Todo: Design and implement
        """
        raise NotImplementedError()
