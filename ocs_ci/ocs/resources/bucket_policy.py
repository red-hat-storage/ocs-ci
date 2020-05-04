import logging

import boto3
import datetime
import base64

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)


class OBC(object):
    """
    Wrapper class for Object Bucket Claim credentials
    """

    (
        s3_resource, s3_endpoint, obc_name,
        ob_name, bucket_name, obc_account,
        access_key_id, access_key, namespace
    ) = (None,) * 9

    def __init__(self, mcg, obc):
        """
        Initializer function

        Args:
            mcg (obj): Multi cloud gateway object
            obc (str): Name of the Object Bucket Claim
        """
        self.obc_name = obc
        self.namespace = config.ENV_DATA['cluster_namespace']
        obc_obj = OCP(namespace=self.namespace, kind='ObjectBucketClaim')
        assert obc_obj.wait_for_resource(
            condition=constants.STATUS_BOUND,
            resource_name=self.obc_name,
            column='PHASE',
            resource_count=1,
            timeout=60
        ), "OBC did not reach BOUND Phase, cannot initialize OBC credentials"
        obc_resource = OCP(namespace=self.namespace, kind='ObjectBucketClaim', resource_name=self.obc_name)
        obc_results = obc_resource.get()
        self.ob_name = obc_results.get('spec').get('ObjectBucketName')
        self.bucket_name = obc_results.get('spec').get('bucketName')
        ob_obj = OCP(namespace=self.namespace, kind='ObjectBucket', resource_name=self.ob_name).get()
        self.obc_account = ob_obj.get('spec').get('additionalState').get('account')
        secret_obc_obj = OCP(kind='secret', namespace=self.namespace, resource_name=self.obc_name).get()

        self.access_key_id = base64.b64decode(
            secret_obc_obj.get('data').get('AWS_ACCESS_KEY_ID')
        ).decode('utf-8')
        self.access_key = base64.b64decode(
            secret_obc_obj.get('data').get('AWS_SECRET_ACCESS_KEY')
        ).decode('utf-8')
        self.s3_endpoint = mcg.s3_endpoint

        self.s3_resource = boto3.resource(
            's3', verify=False, endpoint_url=self.s3_endpoint,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.access_key
        )

        self.s3_client = boto3.client(
            's3', verify=False, endpoint_url=self.s3_endpoint,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.access_key
        )


class HttpResponseParser(object):
    """
    A simple class for parsing HTTP responses
    """
    def __init__(self, http_response):
        """
        Initializer function

        Args:
            http_response (dict): HTTP response
        """
        logger.info('http response:\n%s' % http_response)

        self.metadata = http_response['ResponseMetadata']
        logger.info(f'metadata: {self.metadata}')

        self.headers = self.metadata['HTTPHeaders']
        logger.info(f'headers: {self.headers}')

        self.status_code = self.metadata['HTTPStatusCode']
        logger.info(f'status code: {self.status_code}')

        self.error = http_response.get('Error', None)
        logger.info(f'Error: {self.error}')


class NoobaaAccount(object):
    """
    Class for Noobaa account
    """
    (
        s3_resource, s3_endpoint, account_name,
        email_id, token, access_key_id, access_key
    ) = (None,) * 7

    def __init__(
        self, mcg, name, email, buckets, admin_access=False, s3_access=True,
        full_bucket_access=True, backingstore_name=constants.DEFAULT_NOOBAA_BACKINGSTORE
    ):
        """
        Initializer function

        Args:
            mcg (obj): Multi cloud gateway object
            name (str): Name of noobaa account
            email (str): Email id to be assigned to noobaa account
            buckets (list): list of bucket names to be given permission
            admin_access (bool): True for admin privilege, otherwise False. Default (False)
            s3_access (bool): True for S3 access, otherwise False. Default (True)
            backingstore_name (str): Backingstore name on which buckets created
                using this account to be placed by default. Default("noobaa-default-backing-store")
            full_bucket_access (bool): True for future bucket access, otherwise False. Default (False)
        """
        self.account_name = name
        self.email_id = email
        response = mcg.send_rpc_query(
            api="account_api",
            method="create_account",
            params={
                "email": email,
                "name": name,
                "has_login": admin_access,
                "s3_access": s3_access,
                "default_pool": backingstore_name,
                "allowed_buckets": {
                    "full_permission": full_bucket_access,
                    "permission_list": buckets
                }
            }
        ).json()
        self.access_key_id = response['reply']['access_keys'][0]['access_key']
        self.access_key = response['reply']['access_keys'][0]['secret_key']
        self.s3_endpoint = mcg.s3_endpoint
        self.token = response['reply']['token']

        self.s3_resource = boto3.resource(
            's3', verify=False, endpoint_url=self.s3_endpoint,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.access_key
        )

        self.s3_client = boto3.client(
            's3', verify=False, endpoint_url=self.s3_endpoint,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.access_key
        )


def gen_bucket_policy(user_list, actions_list, resources_list, effect="Allow", sid="statement"):
    """
    Function prepares bucket policy parameters in syntax and format provided by AWS bucket policy

    Args:
        user_list (list): List of user accounts to access bucket policy
        actions_list (list): List of actions in bucket policy eg: Get, Put objects etc
        resources_list (list): List of resources. Eg: Bucket name, specific object in a bucket etc
        effect (str): Permission given to the bucket policy ie: Allow(default) or Deny
        sid (str): Statement name. Can be any string. Default: "Statement"

    Returns:
        dict: Bucket policy in json format
    """
    principals = user_list
    actions = list(map(lambda action: "s3:%s" % action, actions_list))
    resources = list(map(lambda bucket_name: "arn:aws:s3:::%s" % bucket_name, resources_list))
    version = datetime.date.today().strftime("%Y-%m-%d")

    logger.info(f'version: {version}')
    logger.info(f'principal_list: {principals}')
    logger.info(f'actions_list: {actions_list}')
    logger.info(f'resource: {resources_list}')
    logger.info(f'effect: {effect}')
    logger.info(f'sid: {sid}')
    bucket_policy = {"Version": version,
                     "Statement": [{
                         "Action": actions,
                         "Principal": principals,
                         "Resource": resources,
                         "Effect": effect,
                         "Sid": sid}]}

    logger.info(f'bucket_policy: {bucket_policy}')
    return bucket_policy
