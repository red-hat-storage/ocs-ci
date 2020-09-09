import base64
import json
import logging
import os
from abc import ABC, abstractmethod
from time import sleep

import boto3
from botocore.exceptions import ClientError
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import storage

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.utility import templating
from ocs_ci.utility.aws import update_config_from_s3
from ocs_ci.utility.utils import TimeoutSampler, load_auth_config
from tests.helpers import create_resource

logger = logging.getLogger(name=__file__)


class CloudManager(ABC):
    """
    Class containing all client types

    """
    def __init__(self):
        cloud_map = {
            'AWS': S3Client,
            'GOOGLE': GoogleClient,
            'AZURE': AzureClient,
            # TODO: Implement - 'IBMCOS': S3Client
        }
        try:
            logger.info('Trying to load credentials from ocs-ci-data')
            cred_dict = update_config_from_s3().get('AUTH')
        except AttributeError:
            logger.warn(
                'Failed to load credentials from ocs-ci-data. '
                'Loading from local auth.yaml'
            )
            cred_dict = load_auth_config().get('AUTH')
        for cloud_name in cred_dict:
            if cloud_name in cloud_map:
                try:
                    setattr(
                        self, f'{cloud_name.lower()}_client',
                        cloud_map[cloud_name](auth_dict=cred_dict[cloud_name])
                    )
                except DefaultCredentialsError:
                    setattr(
                        self, f'{cloud_name.lower()}_client',
                        None
                    )


class CloudClient(ABC):
    """
    Base abstract class for Cloud based API calls

    """
    client = None

    def __init__(self, *args, **kwargs):
        pass

    def create_uls(self, name, region):
        """
        Super method that first logs the Underlying Storage creation and then calls
        the appropriate implementation

        """
        logger.info(f"Creating Underlying Storage {name} in {region}")
        self.internal_create_uls(name, region)

    def delete_uls(self, name):
        """
        Super method that first logs the Underlying Storage deletion and then calls
        the appropriate implementation

        """
        logger.info(f"Deleting ULS: {name}")
        self.internal_delete_uls(name)

    def get_all_uls_names(self):
        pass

    def verify_uls_exists(self, uls_name):
        pass

    @abstractmethod
    def internal_create_uls(self, name, region):
        pass

    @abstractmethod
    def internal_delete_uls(self, name):
        pass


class S3Client(CloudClient):
    """
    Implementation of a S3 Client using the S3 API

    """
    def __init__(self, key_id=None, access_key=None, endpoint="https://s3.amazonaws.com",
                 verify=True, auth_dict=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if auth_dict:
            key_id = auth_dict.get('AWS_ACCESS_KEY_ID')
            access_key = auth_dict.get('AWS_SECRET_ACCESS_KEY')
            endpoint = endpoint or auth_dict.get('ENDPOINT')
            s3_comp = auth_dict.get('S3_COMP')
        if key_id and access_key:
            self.client = boto3.resource(
                's3', verify=verify, endpoint_url=endpoint,
                aws_access_key_id=key_id,
                aws_secret_access_key=access_key
            )
            self.access_key = key_id
            self.secret_key = access_key
        elif s3_comp:
            logger.warning("Did not find S3 compatible credentials")
            raise DefaultCredentialsError
        else:
            try:
                self.client = boto3.resource('s3', endpoint_url=endpoint)
                # create a secret for the underlying storage to use
                session = boto3.Session()
                # Retrieving the credentials of the existing session
                credentials = session.get_credentials().get_frozen_credentials()
                self.access_key = credentials.access_key
                self.secret_key = credentials.secret_key
            except AttributeError:
                logger.warning("Failed to find default AWS credentials")
                raise DefaultCredentialsError

        self.secret = self.create_aws_secret()

    def internal_create_uls(self, name, region=None):
        """
        Creates the Underlying Storage using the S3 API

        Args:
           name (str): The Underlying Storage name to be created
           region (str): The region to create the Underlying Storage, if none will create at
           `us-east-1` IMPORTANT!!! note that `us-east-1` will cause an error if used since it is
           the default region for aws

        """
        if region is None:
            self.client.create_bucket(Bucket=name)
        else:
            self.client.create_bucket(
                Bucket=name,
                CreateBucketConfiguration={
                    'LocationConstraint': region
                }
            )

    def internal_delete_uls(self, name):
        """
        Deletes the Underlying Storage using the S3 API

        Args:
           name (str): The Underlying Storage name to be deleted

        """
        assert self.exec_uls_deletion(name)
        # Todo: rename client to resource (or find an alternative)
        sample = TimeoutSampler(
            timeout=180, sleep=15, func=self.verify_uls_exists,
            uls_name=name
        )
        if not sample.wait_for_func_status(result=False):
            logger.error(
                f'Deletion of Underlying Storage {name} timed out. Unable to delete {name}'
            )
            logger.warning(
                f'AWS S3 bucket {name} still found after 3 minutes, and might require manual removal.'
            )
        else:
            logger.info(f'Underlying Storage {name} deleted successfully.')

    def exec_uls_deletion(self, name):
        """
        Try to delete Underlying Storage by name if exists

        Args:
            name (str): the Underlying Storage name

        Returns:
            bool: True if deleted successfully

        """
        if self.verify_uls_exists(name):
            try:
                self.client.meta.client.delete_bucket_policy(
                    Bucket=name
                )
                self.client.Bucket(name).objects.all().delete()
                self.client.Bucket(name).delete()
                return True
            except ClientError:
                logger.info(f'Deletion of Underlying Storage {name} failed.')
                return False
        else:
            logger.warning(f'Underlying Storage {name} does not exist, and was not deleted.')
            return True

    def get_all_uls_names(self):
        return {bucket.name for bucket in self.client.buckets.all()}

    def verify_uls_exists(self, uls_name):
        """
       Verifies whether a Underlying Storage with the given uls_name exists

       Args:
           uls_name (str): The Underlying Storage name to be verified

       Returns:
             bool: True if Underlying Storage exists, False otherwise

        """
        try:
            # Todo: rename client to resource (or find an alternative)
            self.client.meta.client.head_bucket(Bucket=uls_name)
            logger.info(f"{uls_name} exists")
            return True
        except ClientError:
            logger.info(f"{uls_name} does not exist")
            return False

    def toggle_aws_bucket_readwrite(self, aws_bucket_name, block=True):
        """
        Toggles a bucket's IO using a bucket policy

        Args:
            aws_bucket_name: The name of the bucket that should be manipulated
            block: Whether to block RW or un-block. True | False

        """
        if block:
            bucket_policy = {
                "Version": "2012-10-17",
                "Id": "DenyReadWrite",
                "Statement": [
                    {
                        "Effect": "Deny",
                        "Principal": {
                            "AWS": "*"
                        },
                        "Action": [
                            "s3:GetObject",
                            "s3:PutObject",
                            "s3:ListBucket"
                        ],
                        "Resource": [
                            f"arn:aws:s3:::{aws_bucket_name}/*",
                            f"arn:aws:s3:::{aws_bucket_name}"
                        ]
                    }
                ]
            }
            bucket_policy = json.dumps(bucket_policy)
            self.client.meta.client.put_bucket_policy(
                Bucket=aws_bucket_name, Policy=bucket_policy
            )
        else:
            self.client.meta.client.delete_bucket_policy(
                Bucket=aws_bucket_name
            )

    def create_aws_secret(self):
        bs_secret_data = templating.load_yaml(constants.MCG_BACKINGSTORE_SECRET_YAML)
        bs_secret_data['metadata']['name'] = 'cldmgr-aws-secret'
        bs_secret_data['metadata']['namespace'] = config.ENV_DATA['cluster_namespace']
        bs_secret_data['data']['AWS_ACCESS_KEY_ID'] = base64.urlsafe_b64encode(
            self.access_key.encode('UTF-8')
        ).decode('ascii')
        bs_secret_data['data']['AWS_SECRET_ACCESS_KEY'] = base64.urlsafe_b64encode(
            self.secret_key.encode('UTF-8')
        ).decode('ascii')

        return create_resource(**bs_secret_data)


class GoogleClient(CloudClient):
    """
    Implementation of a Google Client using the Google API

    """

    def __init__(self, creds=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if creds:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds
        try:
            self.client = storage.Client()
        except DefaultCredentialsError:
            raise

    def internal_create_uls(self, name, region=None):
        """
        Creates the Underlying Storage using the Google API

        Args:
           name (str): The Underlying Storage name to be created
           region (str): The region to create the Underlying Storage

        """
        if region is None:
            self.client.create_bucket(name)
        else:
            self.client.create_bucket(name, location=region)

    def internal_delete_uls(self, name):
        """
        Deletes the Underlying Storage using the Google API

        Args:
           name (str): The Underlying Storage name to be deleted

        """
        # Todo: Replace with a TimeoutSampler
        for _ in range(10):
            try:
                bucket = self.client.get_bucket(name)
                bucket.delete_blobs(bucket.list_blobs())
                bucket.delete()
                break
            except ClientError:  # TODO: Find relevant exception
                logger.info(f'Deletion of Underlying Storage {name} failed. Retrying...')
                sleep(3)

    def get_all_uls_names(self):
        pass

    def verify_uls_exists(self, uls_name):
        pass


class AzureClient(CloudClient):
    """
    Implementation of a Azure Client using the Azure API

    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # TODO: Implement
        raise DefaultCredentialsError

    def internal_create_uls(self, name, region=None):
        """
        Creates the Underlying Storage using the Azure API

        Args:
           name (str): The Underlying Storage name to be created
           region (str): The region to create the Underlying Storage,

        """
        pass

    def internal_delete_uls(self, name):
        """
        Deletes the Underlying Storage using the Azure API

        Args:
           name (str): The Underlying Storage name to be deleted

        """
        pass

    def get_all_uls_names(self):
        pass

    def verify_uls_exists(self, uls_name):
        pass
