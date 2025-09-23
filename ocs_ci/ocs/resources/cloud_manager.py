import base64
import json
import logging
from abc import ABC, abstractmethod

import boto3
import google.api_core.exceptions as GoogleExceptions
from azure.core.exceptions import ResourceNotFoundError, AzureError
from azure.storage.blob import BlobServiceClient
from botocore.exceptions import ClientError, EndpointConnectionError
from google.auth.exceptions import DefaultCredentialsError
from google.cloud.storage import Client as GCPStorageClient
from google.cloud.storage.bucket import Bucket as GCPBucket
from google.oauth2 import service_account

from ocs_ci.framework import config
from ocs_ci.helpers.helpers import create_resource, create_unique_resource_name
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import delete_all_objects_in_batches
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    TimeoutExpiredError,
    ResourceWrongStatusException,
    ClusterNotInSTSModeException,
)
from ocs_ci.ocs.resources.rgw import RGW
from ocs_ci.utility import templating
from ocs_ci.utility.aws import update_config_from_s3
from ocs_ci.utility.utils import (
    TimeoutSampler,
    load_auth_config,
    get_role_arn_from_sub,
)

logger = logging.getLogger(name=__file__)


class CloudManager(ABC):
    """
    Class containing all client types

    """

    @config.run_with_provider_context_if_available
    def __init__(self, obc_obj=None):
        """
        Constructor for the CloudManager class

        Args:
            obc_obj (OBC): For RGW, we can pass the OBC object
                           of RGW bucket

        """
        cloud_map = {
            "AWS_STS": AwsSTSClient,
            "AWS": S3Client,
            "GCP": GoogleClient,
            "AZURE": AzureClient,
            "AZURE_WITH_LOGS": AzureWithLogsClient,
            "IBMCOS": S3Client,
            "RGW": S3Client,
        }
        try:
            logger.info(
                "Trying to load credentials from ocs-ci-data. "
                "This flow is only relevant when running under OCS-QE environments."
            )
            cred_dict = update_config_from_s3().get("AUTH")
        except (AttributeError, EndpointConnectionError):
            logger.warning(
                "Failed to load credentials from ocs-ci-data.\n"
                "Your local AWS credentials might be misconfigured.\n"
                "Trying to load credentials from local auth.yaml instead"
            )
            cred_dict = load_auth_config().get("AUTH", {})

        if not cred_dict:
            logger.warning(
                "Local auth.yaml not found, or failed to load. "
                "All cloud clients will be instantiated as None."
            )

        # Instantiate all needed cloud clients as None by default
        for cloud_name in constants.CLOUD_MNGR_PLATFORMS:
            setattr(self, f"{cloud_name.lower()}_client", None)

        else:
            # Override None clients with actual ones if found
            for cloud_name in cred_dict:
                if cloud_name in cloud_map:
                    # If all the values of the client are filled in auth.yaml,
                    # instantiate an actual client
                    if not any(
                        value is None for value in cred_dict[cloud_name].values()
                    ):
                        setattr(
                            self,
                            f"{cloud_name.lower()}_client",
                            cloud_map[cloud_name](auth_dict=cred_dict[cloud_name]),
                        )

        try:
            rgw_conn = RGW()
            if obc_obj:
                endpoint, access_key, secret_key = (
                    obc_obj.s3_external_endpoint,
                    obc_obj.access_key_id,
                    obc_obj.access_key,
                )
            else:
                endpoint, access_key, secret_key = rgw_conn.get_credentials()
            cred_dict["RGW"] = {
                "SECRET_PREFIX": "RGW",
                "DATA_PREFIX": "AWS",
                "ENDPOINT": endpoint,
                "S3_INTERNAL_ENDPOINT": rgw_conn.s3_internal_endpoint,
                "RGW_ACCESS_KEY_ID": access_key,
                "RGW_SECRET_ACCESS_KEY": secret_key,
            }
            setattr(self, "rgw_client", cloud_map["RGW"](auth_dict=cred_dict["RGW"]))
        except CommandFailed:
            setattr(self, "rgw_client", None)

        # set the client for STS enabled cluster
        try:
            role_arn = get_role_arn_from_sub()
            cred_dict["AWS"]["ROLE_ARN"] = role_arn
            setattr(
                self, "aws_sts_client", cloud_map["AWS_STS"](auth_dict=cred_dict["AWS"])
            )
        except ClusterNotInSTSModeException:
            setattr(self, "aws_sts_client", None)


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
        self.verify_uls_state(name, True)

    def delete_uls(self, name):
        """
        Super method that first logs the Underlying Storage deletion and then calls
        the appropriate implementation

        """
        if self.verify_uls_exists(name) is False:
            logger.warning(
                f"Underlying Storage {name} does not exist, hence it can't be deleted."
            )
            return

        logger.info(f"Deleting ULS: {name}")

        try:
            for deletion_result in TimeoutSampler(
                300, 5, self.internal_delete_uls, name
            ):
                if deletion_result:
                    logger.info("ULS deleted.")
                    break

        except TimeoutExpiredError:
            assert False, f"Failed to delete ULS {name}."

        self.verify_uls_state(name, False)

    def get_all_uls_names(self):
        pass

    def verify_uls_exists(self, uls_name):
        pass

    def verify_uls_state(self, uls_name, is_available):
        check_type = "Delete"
        if is_available:
            check_type = "Create"
        sample = TimeoutSampler(
            timeout=180, sleep=15, func=self.verify_uls_exists, uls_name=uls_name
        )
        if sample.wait_for_func_status(result=is_available):
            logger.info(
                f"Underlying Storage {uls_name} {check_type.lower()}d successfully."
            )
        else:
            if is_available:
                raise ResourceWrongStatusException(
                    f"{check_type[:-1]}ion of Underlying Storage {uls_name} timed out. "
                    f"Unable to {check_type.lower()} {uls_name}"
                )
            logger.warning(
                f"{uls_name} still found after 3 minutes, and might require manual removal."
            )

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

    @config.run_with_provider_context_if_available
    def __init__(
        self,
        auth_dict,
        verify=True,
        endpoint="https://s3.amazonaws.com",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.secret_prefix = auth_dict.get("SECRET_PREFIX", "AWS")
        self.data_prefix = auth_dict.get("DATA_PREFIX", "AWS")
        key_id = auth_dict.get(f"{self.secret_prefix}_ACCESS_KEY_ID")
        access_key = auth_dict.get(f"{self.secret_prefix}_SECRET_ACCESS_KEY")
        self.endpoint = auth_dict.get("ENDPOINT") or endpoint
        self.s3_internal_endpoint = auth_dict.get("S3_INTERNAL_ENDPOINT") or None
        self.region = auth_dict.get("REGION")
        self.access_key = key_id
        self.secret_key = access_key

        self.client = boto3.resource(
            "s3",
            verify=verify,
            endpoint_url=self.endpoint,
            region_name=config.ENV_DATA["region"],
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )
        self.secret = self.create_s3_secret(self.secret_prefix, self.data_prefix)

        self.nss_creds = {
            "access_key_id": self.access_key,
            "access_key": self.secret_key,
            "endpoint": endpoint,
            "region": self.region,
        }

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

        Returns:
            bool: True if deleted successfully

        """

        deletion_result = False

        try:
            # TODO: Check why bucket policy deletion fails on IBM COS
            # when bucket have no policy set
            if "aws" in name:
                self.client.meta.client.delete_bucket_policy(Bucket=name)
            delete_all_objects_in_batches(s3_resource=self.client, bucket_name=name)
            self.client.Bucket(name).delete()
            deletion_result = True

        except ClientError:
            logger.warning(f"Deletion of Underlying Storage {name} failed.")

        return deletion_result

        # Todo: rename client to resource (or find an alternative)

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

    def create_s3_secret(self, secret_prefix, data_prefix):
        """
        Create a Kubernetes secret to allow NooBaa to create AWS-based backingstores

        """
        bs_secret_data = templating.load_yaml(constants.MCG_BACKINGSTORE_SECRET_YAML)
        secret_name_prefix = secret_prefix.lower()
        secret_name_prefix = secret_name_prefix.replace("_", "-")
        bs_secret_data["metadata"]["name"] = create_unique_resource_name(
            f"cldmgr-{secret_name_prefix}", "secret"
        )
        bs_secret_data["metadata"]["namespace"] = config.ENV_DATA["cluster_namespace"]
        bs_secret_data["data"][f"{data_prefix}_ACCESS_KEY_ID"] = (
            base64.urlsafe_b64encode(self.access_key.encode("UTF-8")).decode("ascii")
        )
        bs_secret_data["data"][f"{data_prefix}_SECRET_ACCESS_KEY"] = (
            base64.urlsafe_b64encode(self.secret_key.encode("UTF-8")).decode("ascii")
        )

        return create_resource(**bs_secret_data)


class GoogleClient(CloudClient):
    """
    Implementation of a Google Client using the Google API

    """

    @config.run_with_provider_context_if_available
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

        Returns:
            bool: True if deleted successfully

        """

        deletion_result = False

        try:
            bucket = GCPBucket(client=self.client, name=name)
            blobs = self.client.list_blobs(bucket)
            bucket.delete_blobs(list(blobs))
            bucket.delete()
            deletion_result = True

        except GoogleExceptions.NotFound:
            logger.warning("Failed to delete some of the bucket blobs.")
            deletion_result = False

        return deletion_result

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
        bs_secret_data["metadata"]["name"] = create_unique_resource_name(
            "cldmgr-gcp", "secret"
        )
        bs_secret_data["metadata"]["namespace"] = config.ENV_DATA["cluster_namespace"]
        bs_secret_data["data"]["GoogleServiceAccountPrivateKeyJson"] = (
            base64.urlsafe_b64encode(self.cred_dict_string.encode("UTF-8")).decode(
                "ascii"
            )
        )

        return create_resource(**bs_secret_data)


class AzureClient(CloudClient):
    """
    Implementation of a Azure Client using the Azure API

    """

    @config.run_with_provider_context_if_available
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

        Returns:
            bool: True if deleted successfully

        """

        deletion_result = False

        try:
            self.blob_service_client.get_container_client(name).delete_container()
            deletion_result = True
        except AzureError:
            logger.warning(f"Failed to delete Azure uls {name}.")

        return deletion_result

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
        except ResourceNotFoundError:
            return False

    def create_azure_secret(self):
        """
        Create a Kubernetes secret to allow NooBaa to create Azure-based backingstores

        """
        bs_secret_data = templating.load_yaml(constants.MCG_BACKINGSTORE_SECRET_YAML)
        bs_secret_data["metadata"]["name"] = create_unique_resource_name(
            "cldmgr-azure", "secret"
        )
        bs_secret_data["metadata"]["namespace"] = config.ENV_DATA["cluster_namespace"]
        bs_secret_data["data"]["AccountKey"] = base64.urlsafe_b64encode(
            self.credential.encode("UTF-8")
        ).decode("ascii")
        bs_secret_data["data"]["AccountName"] = base64.urlsafe_b64encode(
            self.account_name.encode("UTF-8")
        ).decode("ascii")

        return create_resource(**bs_secret_data)


class AzureWithLogsClient(AzureClient):
    """
    Implementation of an Azure Client using the Azure API
    to an existing storage account with bucket logs enabled

    """

    @config.run_with_provider_context_if_available
    def __init__(
        self, account_name=None, credential=None, auth_dict=None, *args, **kwargs
    ):
        if auth_dict:
            self.tenant_id = auth_dict.get("TENANT_ID")
            self.app_id = auth_dict.get("APPLICATION_ID")
            self.app_secret = auth_dict.get("APPLICATION_SECRET")
            self.logs_analytics_workspace_id = auth_dict.get(
                "LOGS_ANALYTICS_WORKSPACE_ID"
            )
        super().__init__(
            account_name=account_name,
            credential=credential,
            auth_dict=auth_dict,
            *args,
            **kwargs,
        )

    def create_azure_secret(self):
        """
        Create a Kubernetes secret to allow NooBaa to create Azure-based backingstores

        Note that this method overides the parent method to include the
        additional fields that are needed for the bucket logs feature

        """
        bs_secret_data = templating.load_yaml(constants.MCG_BACKINGSTORE_SECRET_YAML)
        bs_secret_data["metadata"]["name"] = create_unique_resource_name(
            "cldmgr-azure-logs", "secret"
        )
        bs_secret_data["metadata"]["namespace"] = config.ENV_DATA["cluster_namespace"]
        bs_secret_data["data"]["AccountKey"] = base64.urlsafe_b64encode(
            self.credential.encode("UTF-8")
        ).decode("ascii")
        bs_secret_data["data"]["AccountName"] = base64.urlsafe_b64encode(
            self.account_name.encode("UTF-8")
        ).decode("ascii")

        # Note that the following encodings are in plain base64, and not urlsafe.
        # This is because the urlsafe encoding for this credentials might contain
        # characters that are not accepted when creating the secret.
        bs_secret_data["data"]["TenantID"] = base64.b64encode(
            self.tenant_id.encode("UTF-8")
        ).decode("ascii")
        bs_secret_data["data"]["ApplicationID"] = base64.b64encode(
            self.app_id.encode("UTF-8")
        ).decode("ascii")
        bs_secret_data["data"]["ApplicationSecret"] = base64.b64encode(
            self.app_secret.encode("UTF-8")
        ).decode("ascii")
        bs_secret_data["data"]["LogsAnalyticsWorkspaceID"] = base64.b64encode(
            self.logs_analytics_workspace_id.encode("UTF-8")
        ).decode("ascii")

        return create_resource(**bs_secret_data)


class AwsSTSClient(S3Client):
    def __init__(
        self,
        auth_dict,
        verify=True,
        endpoint="https://s3.amazonaws.com",
        *args,
        **kwargs,
    ):
        super().__init__(
            auth_dict=auth_dict, verify=verify, endpoint=endpoint, *args, **kwargs
        )
        self.role_arn = auth_dict["ROLE_ARN"]
