import base64
import logging
import os
from abc import ABC, abstractmethod
import boto3
from botocore.exceptions import ClientError
from time import sleep
from google.cloud import storage
from google.auth.exceptions import DefaultCredentialsError

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.utility import templating
from ocs_ci.utility.utils import TimeoutSampler
from tests.helpers import create_resource

logger = logging.getLogger(name=__file__)


class CloudManager(ABC):
    """
    Class containing all client types

    """
    aws_client, google_client, azure_client, s3comp_client = (None,) * 4

    def __init__(self):
        # TODO: solve credentials for clients (working with local creds for now)
        self.aws_client = S3Client()
        # TODO Need credentials to check
        self.google_client = None
        self.azure_client = None
        self.s3comp_client = None


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
                 verify=True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if key_id and access_key:
            self.client = boto3.resource(
                's3', verify=verify, endpoint_url=endpoint,
                aws_access_key_id=key_id,
                aws_secret_access_key=access_key
            )
            self.access_key = key_id
            self.secret_key = access_key
        else:
            self.client = boto3.resource('s3', endpoint_url=endpoint)
            # create a secret for the underlying storage to use
            session = boto3.Session()
            credentials = session.get_credentials()
            credentials = credentials.get_frozen_credentials()
            self.access_key = credentials.access_key
            self.secret_key = credentials.secret_key
        bs_secret_data = templating.load_yaml(constants.MCG_BACKINGSTORE_SECRET_YAML)
        bs_secret_data['metadata']['name'] += f'-client-secret'
        bs_secret_data['metadata']['namespace'] = config.ENV_DATA['cluster_namespace']
        bs_secret_data['data']['AWS_ACCESS_KEY_ID'] = base64.urlsafe_b64encode(
            self.access_key.encode('UTF-8')
        ).decode('ascii')
        bs_secret_data['data']['AWS_SECRET_ACCESS_KEY'] = base64.urlsafe_b64encode(
            self.secret_key.encode('UTF-8')
        ).decode('ascii')
        self.secret = create_resource(**bs_secret_data)

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
        # Todo: rename client to resource (or find an alternative)
        self.client.meta.client.delete_bucket_policy(
            Bucket=name
        )
        sample = TimeoutSampler(
            timeout=30, sleep=3, func=self.wait_for_delete_uls,
            name=name
        )
        if not sample.wait_for_func_status(result=True):
            logger.error(
                f'Deletion of Underlying Storage {name} timed out. Unable to delete {name}'
            )
            raise TimeoutExpiredError
        else:
            logger.info(f'Underlying Storage {name} deleted successfully')

    def wait_for_delete_uls(self, name):
        """
        Try to delete Underlying Storage by name
        Args:
            name (str): the Underlying Storage name
        Returns:
            bool: True if deleted successfully

        """
        if self.verify_uls_exists(name):
            try:
                self.client.Bucket(name).objects.all().delete()
                self.client.Bucket(name).delete()
                return True
            except ClientError:
                logger.info(f'Deletion of Underlying Storage {name} failed. Retrying...')
                return False
        else:
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

    def get_oc_secret(self):
        return self.secret.name

    def get_aws_key(self):
        return self.access_key

    def get_aws_secret(self):
        return self.secret_key


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
            logger.info(f'No credentials found failing test')

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
        pass

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
