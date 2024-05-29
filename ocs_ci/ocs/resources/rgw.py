import base64
import logging

import boto3

from ocs_ci.framework import config
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import constants
from ocs_ci.helpers.helpers import storagecluster_independent_check


logger = logging.getLogger(name=__file__)


class RGW(object):
    """
    Wrapper class for interaction with a cluster's RGW service
    """

    def __init__(self, namespace=None):
        self.namespace = (
            namespace if namespace else config.ENV_DATA["cluster_namespace"]
        )

        if storagecluster_independent_check():
            sc_name = constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_RGW
        else:
            sc_name = constants.DEFAULT_STORAGECLASS_RGW

        self.storageclass = OCP(
            kind="storageclass", namespace=namespace, resource_name=sc_name
        )
        self.s3_internal_endpoint = (
            self.storageclass.get().get("parameters").get("endpoint")
        )
        self.region = self.storageclass.get().get("parameters").get("region")
        self.s3_endpoint = None
        self.key_id = None
        self.secret_key = None
        self.s3_resource = None
        if config.ENV_DATA["platform"].lower() in constants.ON_PREM_PLATFORMS:
            self.s3_endpoint, self.key_id, self.secret_key = self.get_credentials()

            self.s3_resource = boto3.resource(
                "s3",
                endpoint_url=self.s3_endpoint,
                aws_access_key_id=self.key_id,
                aws_secret_access_key=self.secret_key,
            )
        else:
            logger.warning(
                f"Platform {config.ENV_DATA['platform']} doesn't support RGW"
            )

    def get_credentials(
        self,
        secret_name=constants.NOOBAA_OBJECTSTOREUSER_SECRET,
        access_key_field="AccessKey",
        secret_key_field="SecretKey",
    ):
        """
        Get Endpoint, Access key and Secret key from OCS secret. Endpoint is
        taken from rgw exposed service. Use rgw_endpoint fixture in test to get
        it exposed.

        Args:
            secret_name (str): Name of secret to be used
                for getting RGW credentials
            access_key_field (str): Name of a field of provided secret in
                which is stored access key credential
            secret_key_field (str): Name of a field of provided secret in
                which is stored secret key credential

        Returns:
            tuple: Endpoint, Access key, Secret key

        """
        secret_ocp_obj = OCP(kind=constants.SECRET, namespace=self.namespace)

        if storagecluster_independent_check():
            secret_name = constants.EXTERNAL_MODE_NOOBAA_OBJECTSTOREUSER_SECRET
            cos_ocp_obj = OCP(
                kind=constants.CEPHOBJECTSTORE,
                namespace=config.ENV_DATA["cluster_namespace"],
            )
            cephobjectstore = cos_ocp_obj.get(
                resource_name=constants.RGW_ROUTE_EXTERNAL_MODE
            )
            endpoint = cephobjectstore["status"]["endpoints"]["insecure"][0]
        else:
            route_ocp_obj = OCP(
                kind=constants.ROUTE, namespace=config.ENV_DATA["cluster_namespace"]
            )
            endpoint = route_ocp_obj.get(
                resource_name=constants.RGW_ROUTE_INTERNAL_MODE
            )
            endpoint = f"http://{endpoint['status']['ingress'][0]['host']}"

        creds_secret_obj = secret_ocp_obj.get(secret_name)
        access_key = base64.b64decode(
            creds_secret_obj.get("data").get(access_key_field)
        ).decode("utf-8")
        secret_key = base64.b64decode(
            creds_secret_obj.get("data").get(secret_key_field)
        ).decode("utf-8")
        return endpoint, access_key, secret_key

    def update_s3_creds(self, access_key, secret_key):
        """
        Set the S3 credentials and s3_resource stored in RGW object.

        Args:
            access_key (str): access key credential
            secret_key (str): secret key credential
        """
        self.key_id = access_key
        self.secret_key = secret_key
        self.s3_resource = boto3.resource(
            "s3",
            endpoint_url=self.s3_endpoint,
            aws_access_key_id=self.key_id,
            aws_secret_access_key=self.secret_key,
        )

    def s3_list_all_objects_in_bucket(self, bucketname):
        """
        Args:
            bucketname (str): Name of rgw bucket

        Returns:
            list: A list of all bucket objects

        """
        return {obj for obj in self.s3_resource.Bucket(bucketname).objects.all()}
