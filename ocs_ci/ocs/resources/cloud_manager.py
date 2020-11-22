import base64
import json
import logging
from abc import ABC, abstractmethod
from time import sleep

import boto3
import google.api_core.exceptions as GoogleExceptions
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient
from botocore.exceptions import ClientError
from google.auth.exceptions import DefaultCredentialsError
from google.cloud.storage import Client as GCPStorageClient
from google.cloud.storage.bucket import Bucket as GCPBucket
from google.oauth2 import service_account

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.utility import templating
from ocs_ci.utility.aws import update_config_from_s3
from ocs_ci.utility.utils import TimeoutSampler, load_auth_config
from ocs_ci.helpers.helpers import create_resource

logger = logging.getLogger(name=__file__)


class CloudManager(ABC):
    """
    Class containing all client types

    """

    def __init__(self):
        cloud_map = {
            "AWS": S3Client,
            "GCP": GoogleClient,
            "AZURE": AzureClient,
            "IBMCOS": S3Client,
        }
        try:
            logger.info(
                "Trying to load credentials from ocs-ci-data. "
                "This flow is only relevant when running under OCS-QE environments."
            )
            cred_dict = update_config_from_s3().get("AUTH")
        except AttributeError:
            logger.warn(
                "Failed to load credentials from ocs-ci-data. "
                "Loading from local auth.yaml"
            )
            cred_dict = load_auth_config().get("AUTH", {})

        if not cred_dict:
            logger.warn(
                "Local auth.yaml not found, or failed to load. "
                "Instantiating default clients as None."
            )
            for cloud_name in constants.cld_mgr_platforms:
                setattr(self, f"{cloud_name.lower()}_client", None)

        else:
            for cloud_name in cred_dict:
                if cloud_name in cloud_map:
                    if any(value is None for value in cred_dict[cloud_name].values()):
                        logger.warn(
                            f"{cloud_name} credentials not found "
                            "no client will be instantiated"
                        )
                        setattr(self, f"{cloud_name.lower()}_client", None)
                    else:
                        setattr(
                            self,
                            f"{cloud_name.lower()}_client",
                            cloud_map[cloud_name](auth_dict=cred_dict[cloud_name]),
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

    def __init__(
        self,
        auth_dict,
        verify=True,
        endpoint="https://s3.amazonaws.com",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        secret_prefix = auth_dict.get("SECRET_PREFIX", "AWS")
        key_id = auth_dict.get(f"{secret_prefix}_ACCESS_KEY_ID")
        access_key = auth_dict.get(f"{secret_prefix}_SECRET_ACCESS_KEY")
        self.endpoint = auth_dict.get("ENDPOINT") or endpoint
        self.region = auth_dict.get("REGION")
        self.access_key = key_id
        self.secret_key = access_key

        self.client = boto3.resource(
            "s3",
            verify=verify,
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )

        self.secret = self.create_s3_secret(secret_prefix)

    def internal_create_uls(self, name, region=None):
        """
        Creates the Underlying Storage using the S3 API

        Args:
           name (str): The Underlying Storage name to be created
           region (str): The region to create the Underlying Storage,
           if none will be created on `us-east-1`
           **IMPORTANT**
           Passing `us-east-1` as the region will cause an error if used since it is
           the default region for AWS

        """
        if region is None:
            self.client.create_bucket(Bucket=name)
        else:
            self.client.create_bucket(
                Bucket=name, CreateBucketConfiguration={"LocationConstraint": region}
            )

    def internal_delete_uls(self, name):
        """
        Deletes the Underlying Storage using the S3 API

        Args:
           name (str): The Underlying Storage name to be deleted

        """

        def _exec_uls_deletion(self, name):
            """
            Try to delete Underlying Storage by name if exists

            Args:
                name (str): the Underlying Storage name

            Returns:
                bool: True if deleted successfully

            """
            if self.verify_uls_exists(name):
                try:
                    # TODO: Check why bucket policy deletion fails on IBM COS
                    # when bucket have no policy set
                    if "aws" in name:
                        self.client.meta.client.delete_bucket_policy(Bucket=name)
                    self.client.Bucket(name).objects.all().delete()
                    self.client.Bucket(name).delete()
                    return True
                except ClientError:
                    logger.info(f"Deletion of Underlying Storage {name} failed.")
                    return False
            else:
                logger.warning(
                    f"Underlying Storage {name} does not exist, and was not deleted."
                )
                return True

        try:
            for deletion_result in TimeoutSampler(
                60, 5, _exec_uls_deletion, self, name
            ):
                if deletion_result:
                    logger.info("ULS deleted.")
                    break

        except TimeoutExpiredError:
            logger.error("Failed to delete ULS.")
            assert False

        # Todo: rename client to resource (or find an alternative)
        sample = TimeoutSampler(
            timeout=180, sleep=15, func=self.verify_uls_exists, uls_name=name
        )
        if not sample.wait_for_func_status(result=False):
            logger.error(
                f"Deletion of Underlying Storage {name} timed out. Unable to delete {name}"
            )
            logger.warning(
                f"AWS S3 bucket {name} still found after 3 minutes, and might require manual removal."
            )
        else:
            logger.info(f"Underlying Storage {name} deleted successfully.")

    def get_all_uls_names(self):
        """
        Returns a set containing all the bucket names that the client has access to

        """
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
                        "Principal": {"AWS": "*"},
                        "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
                        "Resource": [
                            f"arn:aws:s3:::{aws_bucket_name}/*",
                            f"arn:aws:s3:::{aws_bucket_name}",
                        ],
                    }
                ],
            }
            bucket_policy = json.dumps(bucket_policy)
            self.client.meta.client.put_bucket_policy(
                Bucket=aws_bucket_name, Policy=bucket_policy
            )
        else:
            self.client.meta.client.delete_bucket_policy(Bucket=aws_bucket_name)

    def create_s3_secret(self, secret_prefix):
        """
        Create a Kubernetes secret to allow NooBaa to create AWS-based backingstores

        """
        bs_secret_data = templating.load_yaml(constants.MCG_BACKINGSTORE_SECRET_YAML)
        secret_name_prefix = secret_prefix.lower()
        secret_name_prefix = secret_name_prefix.replace("_", "-")
        bs_secret_data["metadata"]["name"] = f"cldmgr-{secret_name_prefix}-secret"
        bs_secret_data["metadata"]["namespace"] = config.ENV_DATA["cluster_namespace"]
        bs_secret_data["data"][
            f"{secret_prefix}_ACCESS_KEY_ID"
        ] = base64.urlsafe_b64encode(self.access_key.encode("UTF-8")).decode("ascii")
        bs_secret_data["data"][
            f"{secret_prefix}_SECRET_ACCESS_KEY"
        ] = base64.urlsafe_b64encode(self.secret_key.encode("UTF-8")).decode("ascii")

        return create_resource(**bs_secret_data)


class GoogleClient(CloudClient):
    """
    Implementation of a Google Client using the Google API

    """

    def __init__(self, auth_dict, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cred_dict_string = base64.b64decode(
            auth_dict.get("CREDENTIALS_JSON_BASE64")
        ).decode("utf-8")
        cred_dict = json.loads(self.cred_dict_string)
        credentials = service_account.Credentials.from_service_account_info(cred_dict)

        with open(constants.GOOGLE_CREDS_JSON_PATH, "w") as cred_dump:
            cred_dump.write(self.cred_dict_string)

        self.secret = self.create_gcp_secret()

        try:
            self.client = GCPStorageClient(
                project=cred_dict["project_id"], credentials=credentials
            )
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
                bucket = GCPBucket(client=self.client, name=name)
                bucket.delete_blobs(bucket.list_blobs())
                bucket.delete()
                break
            except ClientError:  # TODO: Find relevant exception
                logger.info(
                    f"Deletion of Underlying Storage {name} failed. Retrying..."
                )
                sleep(3)

    def get_all_uls_names(self):
        """
        Returns a set containing all the bucket names that the client has access to

        """
        return {bucket.id for bucket in self.client.list_buckets()}

    def verify_uls_exists(self, uls_name):
        """
        Verifies whether a Underlying Storage with the given uls_name exists

        Args:
           uls_name (str): The Underlying Storage name to be verified

        Returns:
             bool: True if Underlying Storage exists, False otherwise

        """
        try:
            self.client.get_bucket(uls_name)
            return True
        except GoogleExceptions.NotFound:
            return False

    def create_gcp_secret(self):
        """
        Create a Kubernetes secret to allow NooBaa to create Google-based backingstores

        """
        bs_secret_data = templating.load_yaml(constants.MCG_BACKINGSTORE_SECRET_YAML)
        bs_secret_data["metadata"]["name"] = "cldmgr-gcp-secret"
        bs_secret_data["metadata"]["namespace"] = config.ENV_DATA["cluster_namespace"]
        bs_secret_data["data"][
            "GoogleServiceAccountPrivateKeyJson"
        ] = base64.urlsafe_b64encode(self.cred_dict_string.encode("UTF-8")).decode(
            "ascii"
        )

        return create_resource(**bs_secret_data)


class AzureClient(CloudClient):
    """
    Implementation of a Azure Client using the Azure API

    """

    def __init__(
        self, account_name=None, credential=None, auth_dict=None, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        if auth_dict:
            account_name = auth_dict.get("STORAGE_ACCOUNT_NAME")
            credential = auth_dict.get("STORAGE_ACCOUNT_KEY")
        if account_name and credential:
            self.account_name = account_name
            self.credential = credential
            self.secret = self.create_azure_secret()

            account_url = constants.AZURE_BLOB_ENDPOINT_TEMPLATE.format(account_name)
            self.blob_service_client = BlobServiceClient(
                account_url=account_url, credential=credential
            )

    def internal_create_uls(self, name, region):
        """
        Creates the Underlying Storage using the Azure API

        Args:
           name (str): The Underlying Storage name to be created

        """
        self.blob_service_client.get_container_client(name).create_container()

    def internal_delete_uls(self, name):
        """
        Deletes the Underlying Storage using the Azure API

        Args:
           name (str): The Underlying Storage name to be deleted

        """
        self.blob_service_client.get_container_client(name).delete_container()

    def get_all_uls_names(self):
        """
        Returns a set containing all the container names that the client has access to

        """
        return {
            container["name"]
            for container in self.blob_service_client.list_containers()
        }

    def verify_uls_exists(self, uls_name):
        """
        Verifies whether a Underlying Storage with the given uls_name exists

        Args:
           uls_name (str): The Underlying Storage name to be verified

        Returns:
             bool: True if Underlying Storage exists, False otherwise

        """
        try:
            self.blob_service_client.get_container_client(
                uls_name
            ).get_container_properties()
            return True
        except ResourceExistsError:
            return False

    def create_azure_secret(self):
        """
        Create a Kubernetes secret to allow NooBaa to create Azure-based backingstores

        """
        bs_secret_data = templating.load_yaml(constants.MCG_BACKINGSTORE_SECRET_YAML)
        bs_secret_data["metadata"]["name"] = "cldmgr-azure-secret"
        bs_secret_data["metadata"]["namespace"] = config.ENV_DATA["cluster_namespace"]
        bs_secret_data["data"]["AccountKey"] = base64.urlsafe_b64encode(
            self.credential.encode("UTF-8")
        ).decode("ascii")
        bs_secret_data["data"]["AccountName"] = base64.urlsafe_b64encode(
            self.account_name.encode("UTF-8")
        ).decode("ascii")

        return create_resource(**bs_secret_data)
