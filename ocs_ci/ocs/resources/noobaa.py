import base64
import logging
import boto3

from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(name=__file__)


class NooBaa(object):
    """
    Wrapper class for NooBaa's S3 service
    """

    _s3_resource = None
    _ocp_resource = None

    def __init__(self):
        """
        Constructor for the NooBaa class
        """
        ocp_obj = OCP(kind='noobaa', namespace='noobaa')
        results = ocp_obj.get()
        endpoint = (
            results.get('items')[0].get('status').get('services')
            .get('serviceS3').get('externalDNS')[0]
        )
        creds_secret_name = (
            results.get('items')[0].get('status').get('accounts')
            .get('admin').get('secretRef').get('name')
        )
        secret_ocp_obj = OCP(kind='secret', namespace='noobaa')
        results2 = secret_ocp_obj.get(creds_secret_name)

        noobaa_access_key = base64.b64decode(
            results2.get('data').get('AWS_ACCESS_KEY_ID')
        ).decode('utf-8')
        noobaa_secret_key = base64.b64decode(
            results2.get('data').get('AWS_SECRET_ACCESS_KEY')
        ).decode('utf-8')

        self._ocp_resource = ocp_obj
        self._s3_resource = boto3.resource(
            's3', verify=False, endpoint_url=endpoint,
            aws_access_key_id=noobaa_access_key,
            aws_secret_access_key=noobaa_secret_key
        )

    def s3_create_bucket(self, bucketname):
        """
        Args:
            bucketname: Name of the bucket to be created

        Returns:
            s3.Bucket object

        """
        return self._s3_resource.create_bucket(Bucket=bucketname)

    def s3_delete_bucket(self, bucket):
        """
        Args:
            bucket: The bucket object to be deleted

        """
        bucket.delete()

    def s3_list_all_bucket_names(self):
        """
        Returns:
            A list of all bucket names

        """
        return [bucket.name for bucket in self._s3_resource.buckets.all()]

    def s3_get_all_bucket_objects(self):
        """
        Returns:
            A list of all  s3.Bucket objects

        """
        return [bucket for bucket in self._s3_resource.buckets.all()]

    def s3_verify_bucket_exists(self, bucket):
        """
        Args:
            bucket: The bucket object to be verified

        """
        if bucket in self._s3_resource.buckets.all():
            logger.info(f"{bucket.name} exits")
        else:
            logger.info(f"{bucket.name} does not exist")

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
