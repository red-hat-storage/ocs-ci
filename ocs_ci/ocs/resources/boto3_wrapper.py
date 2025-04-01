import logging

import boto3
from botocore.exceptions import ClientError


logger = logging.getLogger(name=__file__)


class BaseBoto3Wrapper:
    """
    A base class for wrapping the boto3 S3 client to allow for method-level adjustments
    """

    def __init__(
        self,
        verify=True,
        endpoint_url=None,
        access_key=None,
        secret_key=None,
        region_name=None,
        *args,
        **kwargs,
    ):
        self.verify = verify
        self.endpoint_url = endpoint_url
        self.access_key = access_key
        self.secret_key = secret_key
        self.region_name = region_name

        # Create both client and resource
        self._initialize_client_and_resource()
        self._wrap_methods()

    def _initialize_client_and_resource(self):
        """
        Initialize both client and resource interfaces
        """
        common_params = {
            "verify": self.verify,
            "endpoint_url": self.endpoint_url,
            "aws_access_key_id": self.access_key,
            "aws_secret_access_key": self.secret_key,
            "region_name": self.region_name,
        }

        self.boto3_client = boto3.client("s3", **common_params)
        self.boto3_resource = boto3.resource("s3", **common_params)
        self.buckets = self.boto3_resource.buckets
        self.meta = self.boto3_resource.meta

    def _wrap_methods(self):
        """
        Wraps callable methods with error handling and region adjustment
        """
        # Wrap client methods
        for method_name in dir(self.boto3_client):
            if callable(
                getattr(self.boto3_client, method_name)
            ) and not method_name.startswith("_"):
                wrapped_method = self._create_wrapper(self.boto3_client, method_name)
                setattr(self, method_name, wrapped_method)

    def _create_wrapper(self, obj, method_name):
        """
        A default wrapper function that does nothing.
        This is a placeholder and a demonstration of how to wrap methods.
        """
        method = getattr(obj, method_name)

        def wrapper(*args, **kwargs):
            return method(*args, **kwargs)

        return wrapper

    def Bucket(self, name):
        return self.boto3_resource.Bucket(name)


class Boto3WrapperForAWS(BaseBoto3Wrapper):
    """
    Wraps boto3 S3 client and resource to handle region-specific issues,
    such as the 301 Moved Permanently error.
    """

    def __init__(
        self,
        verify=True,
        endpoint_url=None,
        access_key=None,
        secret_key=None,
        region_name=None,
        *args,
        **kwargs,
    ):
        super().__init__(
            verify=verify,
            endpoint_url=endpoint_url,
            access_key=access_key,
            secret_key=secret_key,
            region_name=region_name,
            *args,
            **kwargs,
        )
        self.default_region = region_name
        self.default_endpoint = endpoint_url

    def _create_wrapper(self, obj, method_name):
        """
        Creates a wrapper function that handles the 301 error and updates the region
        if necessary.
        """
        method = getattr(obj, method_name)

        def wrapper(*args, **kwargs):
            try:
                return method(*args, **kwargs)
            except ClientError as e:
                response_md = e.response.get("ResponseMetadata", {})
                error_code = e.response.get("Error", {}).get("Code")

                if response_md.get("HTTPStatusCode") == 301:
                    region = response_md.get("HTTPHeaders", {}).get(
                        "x-amz-bucket-region"
                    )
                    # A specific region and endpoint are required
                    self.update_region_and_endpoint(region)

                elif error_code == "IllegalLocationConstraintException":
                    # The defaults are required
                    self.update_region_and_endpoint(None)

                else:
                    logger.error(f"ClientError: {e}")
                    raise

                # Common retry logic for both region change cases
                new_method = getattr(self.boto3_client, method_name)
                return new_method(*args, **kwargs)

        return wrapper

    def Bucket(self, name):
        try:
            return self.boto3_resource.Bucket(name)
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "301":
                region = e.response["ResponseMetadata"]["HTTPHeaders"][
                    "x-amz-bucket-region"
                ]
                logger.info(
                    f"Bucket is in {region} but client is using {self.endpoint_url} "
                )

                # Retry with the new resource
                self.update_region_and_endpoint(region)
                return self.boto3_resource.Bucket(name)

            else:
                logger.error(f"ClientError: {e}")
                raise

    def update_region_and_endpoint(self, region):
        """
        Update the region for the S3 client and resource.

        Args:
            region (str): The new region to set.
        """
        self.region_name = region if region else self.default_region
        self.endpoint_url = (
            f"https://s3.{region}.amazonaws.com" if region else self.default_endpoint
        )
        logger.info(f"Updating to region and endpoint: {region}, {self.endpoint_url}")
        self._initialize_client_and_resource()
        self._wrap_methods()
