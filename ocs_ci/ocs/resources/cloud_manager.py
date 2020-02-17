import logging
import os
from abc import ABC, abstractmethod
import boto3
from botocore.exceptions import ClientError
from time import sleep
from google.cloud import storage
from google.auth.exceptions import DefaultCredentialsError
import ibm_boto3

logger = logging.getLogger(name=__file__)


class CloudManager(ABC):
    aws_client, google_client, azure_client, s3comp_client = (None,) * 4

    def __init__(self):
        self.aws_client = S3Client()
        self.google_client = GoogleClient()
        self.azure_client = AzureClient()
        self.s3comp_client = S3Client()


class CloudClient(ABC):
    """
        Base abstract class for Cloud based Underlying Storage
    """
    client = None

    def __init__(self, *args, **kwargs):
        pass

    def create_uls(self, name):
        """
            Super method that first logs the ULS creation and then calls
            the appropriate implementation
        """
        logger.info(f"Creating ULS: {name}")
        self.internal_create_uls(name)

    def delete_uls(self, name):
        """
            Super method that first logs the ULS deletion and then calls
            the appropriate implementation
        """
        logger.info(f"Deleting ULS: {name}")
        self.internal_delete_uls(name)

    @abstractmethod
    def internal_create_uls(self, name):
        pass

    @abstractmethod
    def internal_delete_uls(self, name):
        pass


class S3Client(CloudClient):
    """
        Implementation of a S3 Client using the S3 API
    """

    def __init__(self, key_id, access_key, endpoint="https://s3.amazonaws.com", verify=True,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = boto3.resource(
            's3', verify=verify, endpoint_url=endpoint,
            aws_access_key_id=key_id,
            aws_secret_access_key=access_key
        )

    def internal_create_uls(self, name, region=None):
        """
            Creates the Underlying Storage using the S3 API
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
        """
        self.client.meta.client.delete_bucket_policy(
            Bucket=name
        )
        for _ in range(10):
            try:
                self.client.Bucket(name).objects.all().delete()
                self.client.Bucket(name).delete()
                break
            except ClientError:
                logger.info(f'Deletion of ULS {name} failed. Retrying...')
                sleep(3)


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

    def internal_create_uls(self, name, location=None):
        """
            Creates the Underlying Storage using the Google API
        """
        if location is None:
            self.client.create_bucket(name)
        else:
            self.client.create_bucket(name, location=location)

    def internal_delete_uls(self, name):
        """
            Deletes the Underlying Storage using the Google API
        """
        for _ in range(10):
            try:
                bucket = self.client.get_bucket(name)
                bucket.delete_blobs(bucket.list_blobs())
                bucket.delete()
                break
            except ClientError:  # TODO: Find relevant exception
                logger.info(f'Deletion of ULS {name} failed. Retrying...')
                sleep(3)


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
        """
        pass

    def internal_delete_uls(self, name):
        """
            Deletes the Underlying Storage using the Azure API
        """
        pass


class IBMClient(CloudClient):
    """
        Implementation of a IBM Client using the IBM API
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        pass

    def internal_create_uls(self, name, region=None):
        """
            Creates the Underlying Storage using the IBM API
        """
        pass

    def internal_delete_uls(self, name):
        """
            Deletes the Underlying Storage using the IBM API
        """
        pass
