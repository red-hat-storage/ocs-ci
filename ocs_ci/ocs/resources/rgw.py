import base64
import logging

from ocs_ci.framework import config
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import constants
from ocs_ci.helpers.helpers import (
    create_resource,
    storagecluster_independent_check,
    wait_for_resource_state,
)
from ocs_ci.utility import templating

logger = logging.getLogger(__name__)


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
            f"https://{constants.RGW_SERVICE_INTERNAL_MODE}.{self.namespace}.svc:443"
        )
        self.region = self.storageclass.get().get("parameters").get("region")
        # Todo: Implement retrieval in cases where CephObjectStoreUser is available
        self.key_id = None
        self.secret_key = None
        self.s3_resource = None

    def get_credentials(self, secret_name=constants.NOOBAA_OBJECTSTOREUSER_SECRET):
        """
        Get Endpoint, Access key and Secret key from OCS secret. Endpoint is
        taken from rgw exposed service. Use rgw_endpoint fixture in test to get
        it exposed.

        Args:
            secret_name (str): Name of secret to be used
                for getting RGW credentials

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
            if config.EXTERNAL_MODE.get("rgw_secure"):
                endpoint = cephobjectstore["status"]["endpoints"]["secure"][0]
            else:
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
            creds_secret_obj.get("data").get("AccessKey")
        ).decode("utf-8")
        secret_key = base64.b64decode(
            creds_secret_obj.get("data").get("SecretKey")
        ).decode("utf-8")
        return endpoint, access_key, secret_key


def create_ec_cephobjectstore():
    """
    Create an erasure-coded CephObjectStore with a replicated metadata pool
    and an erasure-coded data pool, then create the corresponding bucket
    StorageClass.

    Erasure coding parameters are read from config.DEPLOYMENT:
        ec_data_chunks (int): number of data chunks, k value (default: 2)
        ec_coding_chunks (int): number of coding chunks, m value (default: 1)
        ec_failure_domain (str): failure domain (default: "host")
    """
    k = config.DEPLOYMENT.get("ec_data_chunks", 2)
    m = config.DEPLOYMENT.get("ec_coding_chunks", 1)
    fd = config.DEPLOYMENT.get("ec_failure_domain", "host")
    namespace = config.ENV_DATA["cluster_namespace"]
    os_name = constants.CEPHOBJECTSTORE_NAME_EC
    sc_name = constants.DEFAULT_STORAGECLASS_RGW_EC

    logger.info(
        f"Creating EC CephObjectStore '{os_name}' with k={k}, m={m}, "
        f"failureDomain={fd}"
    )

    os_data = templating.load_yaml(constants.EC_CEPHOBJECTSTORE_YAML)
    os_data["metadata"]["name"] = os_name
    os_data["metadata"]["namespace"] = namespace
    os_data["spec"]["dataPool"]["failureDomain"] = fd
    os_data["spec"]["dataPool"]["erasureCoded"]["dataChunks"] = k
    os_data["spec"]["dataPool"]["erasureCoded"]["codingChunks"] = m

    os_obj = create_resource(**os_data)
    logger.info(f"Waiting for EC CephObjectStore '{os_name}' to reach Ready phase")
    wait_for_resource_state(os_obj, constants.STATUS_READY, timeout=600)

    logger.info(f"Creating bucket StorageClass '{sc_name}' for EC CephObjectStore")

    sc_data = templating.load_yaml(constants.EC_STORAGECLASS_RGW_YAML)
    sc_data["metadata"]["name"] = sc_name
    sc_data["parameters"]["objectStoreName"] = os_name
    sc_data["parameters"]["objectStoreNamespace"] = namespace

    create_resource(**sc_data)
    logger.info(
        f"EC CephObjectStore '{os_name}' and StorageClass '{sc_name}' created successfully"
    )
