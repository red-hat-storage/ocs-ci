import base64
import json
import logging
import os
import re

import tempfile
from time import sleep
import time

import boto3
from botocore.client import ClientError

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import retrieve_verification_mode
from ocs_ci.ocs.constants import (
    DEFAULT_NOOBAA_BACKINGSTORE,
    DEFAULT_NOOBAA_BUCKETCLASS,
    STATUS_READY,
    NOOBAA_RESOURCE_NAME,
)
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    CredReqSecretNotFound,
    TimeoutExpiredError,
    UnsupportedPlatformError,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import (
    get_noobaa_pods,
    get_pods_having_label,
    Pod,
    wait_for_pods_to_be_running,
)
from ocs_ci.utility import templating, version
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import (
    get_attr_chain,
    exec_cmd,
    TimeoutSampler,
    mask_secrets,
    get_noobaa_cli_config,
)
from ocs_ci.helpers.helpers import retrieve_cli_binary, flatten_multilevel_dict
from ocs_ci.helpers.helpers import (
    create_unique_resource_name,
    create_resource,
    retrieve_default_ingress_crt,
    wait_for_resource_state,
)
import subprocess

logger = logging.getLogger(name=__file__)


class MCG:
    """
    Wrapper class for the Multi Cloud Gateway's S3 service
    """

    (
        s3_resource,
        s3_endpoint,
        s3_internal_endpoint,
        ocp_resource,
        mgmt_endpoint,
        region,
        access_key_id,
        access_key,
        namespace,
        noobaa_user,
        noobaa_password,
        noobaa_token,
        data_to_mask,
    ) = (None,) * 13

    def __init__(self, *args, **kwargs):
        """
        Constructor for the MCG class
        """
        with config.RunWithProviderConfigContextIfAvailable():
            self.namespace = config.ENV_DATA["cluster_namespace"]
            self.operator_pod = Pod(
                **get_pods_having_label(
                    constants.NOOBAA_OPERATOR_POD_LABEL, self.namespace
                )[0]
            )
            self.core_pod = Pod(
                **get_pods_having_label(
                    constants.NOOBAA_CORE_POD_LABEL, self.namespace
                )[0]
            )
            wait_for_resource_state(
                resource=self.operator_pod, state=constants.STATUS_RUNNING, timeout=300
            )

            # Determine which CLI to use based on version
            self.cli_path, self.command_prefix = get_noobaa_cli_config()
            ocs_version = version.get_semantic_ocs_version_from_config()

            # Initialize ODFCliRunner for OCS >= 4.20
            self.odf_cli_runner = None
            if ocs_version >= version.VERSION_4_20:
                from ocs_ci.helpers.odf_cli import ODFCliRunner

                self.odf_cli_runner = ODFCliRunner()

            if (
                not os.path.isfile(self.cli_path)
                or self.get_mcg_cli_version().minor != ocs_version.minor
            ):
                logger.info(
                    "The expected NooBaa CLI binary could not be found,"
                    " downloading the expected version"
                )
                if ocs_version >= version.VERSION_4_20:
                    retrieve_cli_binary(cli_type="odf")
                else:
                    retrieve_cli_binary(cli_type="mcg")

            """
            The certificate will be copied on each mcg_obj instantiation since
            the process is so light and quick, that the time required for the redundant
            copy is neglible in comparison to the time a hash comparison will take.
            """
            retrieve_default_ingress_crt()

            get_noobaa = OCP(kind="noobaa", namespace=self.namespace).get()

            self.s3_endpoint = (
                get_noobaa.get("items")[0]
                .get("status")
                .get("services")
                .get("serviceS3")
                .get("externalDNS")[0]
            )
            self.s3_internal_endpoint = self.determine_s3_endpoint()
            self.sts_endpoint = (
                get_noobaa.get("items")[0]
                .get("status")
                .get("services")
                .get("serviceSts")
                .get("externalDNS")[0]
            )
            self.sts_internal_endpoint = (
                get_noobaa.get("items")[0]
                .get("status")
                .get("services")
                .get("serviceSts")
                .get("internalDNS")[0]
            )
            self.mgmt_endpoint = (
                get_noobaa.get("items")[0]
                .get("status")
                .get("services")
                .get("serviceMgmt")
                .get("externalDNS")[0]
            ) + "/rpc"
            self.region = config.ENV_DATA["region"]

            noobaa_cr_services = (
                get_noobaa.get("items")[0].get("status").get("services")
            )
            self.data_to_mask = flatten_multilevel_dict(noobaa_cr_services)

            self.update_s3_creds()

            if config.ENV_DATA["platform"].lower() == "aws" and kwargs.get(
                "create_aws_creds"
            ):
                (
                    self.cred_req_obj,
                    self.aws_access_key_id,
                    self.aws_access_key,
                ) = self.request_aws_credentials()

                self.aws_s3_resource = boto3.resource(
                    "s3",
                    endpoint_url="https://s3.amazonaws.com",
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_access_key,
                )

    def retrieve_nb_token(self, timeout=300, sleep=30):
        """
        Try to retrieve a NB RPC token and decode its JSON

        """

        def internal_retrieval_logic():
            try:
                rpc_response = self.send_rpc_query(
                    "auth_api",
                    "create_auth",
                    params={
                        "role": "admin",
                        "system": "noobaa",
                        "email": self.noobaa_user,
                        "password": self.noobaa_password,
                    },
                )
                return rpc_response.json().get("reply").get("token")

            except json.JSONDecodeError:
                logger.warning(
                    "RPC did not respond with a JSON. Response: \n" + str(rpc_response)
                )
                logger.warning(
                    "Failed to retrieve token, NooBaa might be unhealthy. Retrying"
                )
                return None

        try:
            for token in TimeoutSampler(timeout, sleep, internal_retrieval_logic):
                if token:
                    return token
        except TimeoutExpiredError:
            logger.error(
                "NB RPC token was not retrieved successfully within the time limit."
            )
            assert False, (
                "NB RPC token was not retrieved successfully " "within the time limit."
            )

    def determine_s3_endpoint(self):
        """
        Get external mcg S3 endpoint if the cluster is in multicluster environment.
        Get internal endpoint otherwise.

        Returns:
            string: S3 endpoint URI

        """
        if config.multicluster:
            logger.warning(
                "Multicluster test run is executed. External S3 enpoint is used instead of internal."
            )
            return self.s3_endpoint
        else:
            get_noobaa = OCP(kind="noobaa", namespace=self.namespace).get()
            return (
                get_noobaa.get("items")[0]
                .get("status")
                .get("services")
                .get("serviceS3")
                .get("internalDNS")[0]
            )

    def s3_get_all_bucket_names(self):
        """
        Returns:
            set: A set of all bucket names

        """
        return {bucket.name for bucket in self.s3_resource.buckets.all()}

    def read_system(self):
        """
        Returns:
            dict: A dictionary with information about MCG resources

        """
        return self.send_rpc_query("system_api", "read_system", params={}).json()[
            "reply"
        ]

    def get_bucket_info(self, bucket_name):
        """
        Args:
            bucket_name (str): Name of searched bucket

        Returns:
            dict: Information about the bucket

        """
        logger.info(f"Requesting information about bucket {bucket_name}")
        for bucket in self.read_system().get("buckets"):
            if bucket["name"] == bucket_name:
                logger.debug(bucket)
                return bucket
        logger.warning(f"Bucket {bucket_name} was not found")
        return None

    def cli_get_all_bucket_names(self):
        """
        Returns:
            set: A set of all bucket names

        """
        obc_lst = self.exec_mcg_cmd("obc list").stdout.split("\n")[1:-1]
        # TODO assert the bucket passed the Pending state
        return {row.split()[1] for row in obc_lst}

    def cli_list_all_buckets(self) -> list[str]:
        """
        Returns:
            list[str]: A list of all bucket names

        """
        return self.exec_mcg_cmd("bucket list").stdout.strip().split("\n")[1:]

    def s3_list_all_objects_in_bucket(self, bucketname):
        """
        Returns:
            list: A list of all bucket objects
        """
        return {obj for obj in self.s3_resource.Bucket(bucketname).objects.all()}

    def s3_get_all_buckets(self):
        """
        Returns:
            list: A list of all s3.Bucket objects

        """
        return {bucket for bucket in self.s3_resource.buckets.all()}

    def s3_verify_bucket_exists(self, bucketname):
        """
        Verifies whether a bucket with the given bucketname exists
        Args:
            bucketname : The bucket name to be verified

        Returns:
              bool: True if bucket exists, False otherwise

        """
        try:
            self.s3_resource.meta.client.head_bucket(Bucket=bucketname)
            logger.info(f"{bucketname} exists")
            return True
        except ClientError:
            logger.info(f"{bucketname} does not exist")
            return False

    def oc_verify_bucket_exists(self, bucketname):
        """
        Verifies whether a bucket with the given bucketname exists
        Args:
            bucketname : The bucket name to be verified

        Returns:
              bool: True if bucket exists, False otherwise

        """
        try:
            OCP(namespace=self.namespace, kind="obc").get(bucketname)
            logger.info(f"{bucketname} exists")
            return True
        except CommandFailed as e:
            if "NotFound" in repr(e):
                logger.info(f"{bucketname} does not exist")
                return False
            raise

    def cli_verify_bucket_exists(self, bucketname):
        """
        Verifies whether a bucket with the given bucketname exists
        Args:
            bucketname : The bucket name to be verified

        Returns:
              bool: True if bucket exists, False otherwise

        """
        return bucketname in self.cli_get_all_bucket_names()

    def send_rpc_query(self, api, method, params=None):
        """
        Templates and sends an RPC query to the MCG mgmt endpoint

        Args:
            api: The name of the API to use
            method: The method to use inside the API
            params: A dictionary containing the command payload

        Returns:
            The server's response

        """

        masked_params = mask_secrets(str(params), self.data_to_mask)
        logger.info(
            f"Sending MCG RPC query via mcg-cli:\n{api} {method} {masked_params}"
        )

        cli_output = self.exec_mcg_cmd(
            f"api {api} {method} '{json.dumps(params)}' -ojson"
        )

        # This class is needed to add a json method to the response dict
        # which is needed to support existing usage
        class CLIResponseDict(dict):
            def json(self):
                return self

        return CLIResponseDict({"reply": json.loads(cli_output.stdout)})

    def check_data_reduction(self, bucketname, expected_reduction_in_bytes):
        """
        Checks whether the data reduction on the MCG server works properly
        Args:
            bucketname: An example bucket name that contains compressed/deduped data
            expected_reduction_in_bytes: amount of data that is supposed to be reduced after data
            compression and deduplication.

        Returns:
            bool: True if the data reduction mechanics work, False otherwise

        """

        def _retrieve_reduction_data():
            resp = self.send_rpc_query(
                "bucket_api", "read_bucket", params={"name": bucketname}
            )
            bucket_data = resp.json().get("reply").get("data").get("size")
            bucket_data_reduced = (
                resp.json().get("reply").get("data").get("size_reduced")
            )
            logger.info(
                "Overall bytes stored: "
                + str(bucket_data)
                + ". Reduced size: "
                + str(bucket_data_reduced)
            )

            return bucket_data, bucket_data_reduced

        try:
            for total_size, total_reduced in TimeoutSampler(
                300, 5, _retrieve_reduction_data
            ):
                if total_size - total_reduced > expected_reduction_in_bytes:
                    logger.info("Data reduced:" + str(total_size - total_reduced))
                    return True
                else:
                    logger.info(
                        "Data reduction is not yet sufficient. "
                        "Retrying in 5 seconds..."
                    )
        except TimeoutExpiredError:
            assert False, (
                "Data reduction is insufficient. "
                f"{total_size - total_reduced} bytes reduced out of {expected_reduction_in_bytes}."
            )

    def request_aws_credentials(self):
        """
        Uses a CredentialsRequest CR to create an AWS IAM that allows the program
        to interact with S3

        Returns:
            OCS: The CredentialsRequest resource
        """
        awscreds_data = templating.load_yaml(constants.MCG_AWS_CREDS_YAML)
        req_name = create_unique_resource_name("awscredreq", "credentialsrequests")
        awscreds_data["metadata"]["name"] = req_name
        awscreds_data["metadata"]["namespace"] = self.namespace
        awscreds_data["spec"]["secretRef"]["name"] = req_name
        awscreds_data["spec"]["secretRef"]["namespace"] = self.namespace

        creds_request = create_resource(**awscreds_data)
        sleep(5)

        secret_ocp_obj = OCP(kind="secret", namespace=self.namespace)
        try:
            cred_req_secret_dict = secret_ocp_obj.get(
                resource_name=creds_request.name, retry=5
            )
        except CommandFailed:
            logger.error("Failed to retrieve credentials request secret")
            raise CredReqSecretNotFound(
                "Please make sure that the cluster used is an AWS cluster, "
                "or that the `platform` var in your config is correct."
            )

        aws_access_key_id = base64.b64decode(
            cred_req_secret_dict.get("data").get("aws_access_key_id")
        ).decode("utf-8")

        aws_access_key = base64.b64decode(
            cred_req_secret_dict.get("data").get("aws_secret_access_key")
        ).decode("utf-8")

        def _check_aws_credentials():
            try:
                sts = boto3.client(
                    "sts",
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_access_key,
                )
                sts.get_caller_identity()

                return True

            except ClientError:
                logger.info("Credentials are still not active. Retrying...")
                return False

        try:
            for api_test_result in TimeoutSampler(120, 5, _check_aws_credentials):
                if api_test_result:
                    logger.info("AWS credentials created successfully.")
                    break

        except TimeoutExpiredError:
            logger.error("Failed to create credentials")
            assert False

        return creds_request, aws_access_key_id, aws_access_key

    def create_connection(self, cld_mgr, platform, conn_name=None):
        """
        Creates a new NooBaa connection to an AWS backend

        Args:
            cld_mgr (obj): A cloud manager instance
            platform (str): Platform to use for new connection
            conn_name (str): The connection name to be used
                If None provided then the name will be generated

        Returns:
            bool: False if the connection creation failed

        """
        if conn_name is None:
            conn_name = create_unique_resource_name(f"{platform}-connection", "mcgconn")

        if platform == constants.AWS_PLATFORM:
            params = {
                "auth_method": "AWS_V4",
                "endpoint": constants.MCG_NS_AWS_ENDPOINT.format(
                    constants.DEFAULT_AWS_REGION
                ),
                "endpoint_type": "AWS",
                "identity": get_attr_chain(cld_mgr, "aws_client.access_key"),
                "name": conn_name,
                "secret": get_attr_chain(cld_mgr, "aws_client.secret_key"),
            }
        elif platform == constants.AZURE_PLATFORM:
            params = {
                "endpoint": constants.MCG_NS_AZURE_ENDPOINT,
                "endpoint_type": "AZURE",
                "identity": get_attr_chain(cld_mgr, "azure_client.account_name"),
                "name": conn_name,
                "secret": get_attr_chain(cld_mgr, "azure_client.credential"),
            }
        elif platform == constants.RGW_PLATFORM:
            params = {
                "auth_method": "AWS_V4",
                "endpoint": get_attr_chain(cld_mgr, "rgw_client.endpoint"),
                "endpoint_type": "S3_COMPATIBLE",
                "identity": get_attr_chain(cld_mgr, "rgw_client.access_key"),
                "name": conn_name,
                "secret": get_attr_chain(cld_mgr, "rgw_client.secret_key"),
            }
        else:
            raise UnsupportedPlatformError(f"Unsupported Platform: {platform}")

        try:
            for resp in TimeoutSampler(
                30,
                3,
                self.send_rpc_query,
                "account_api",
                "add_external_connection",
                params,
            ):
                if "error" not in resp.text:
                    logger.info(f"Connection {conn_name} created successfully")
                    return True
                else:
                    logger.info(
                        f"{platform} IAM {conn_name} did not yet propagate: {resp.text}"
                    )
        except TimeoutExpiredError:
            logger.error(f"Could not create connection {conn_name}")
            assert False

    def create_namespace_resource(
        self, ns_resource_name, conn_name, region, cld_mgr, cloud_uls_factory, platform
    ):
        """
        Creates a new namespace resource

        Args:
            ns_resource_name (str): The name to be given to the new namespace resource
            conn_name (str): The external connection name to be used
            region (str): The region name to be used
            cld_mgr: A cloud manager instance
            cloud_uls_factory: The cloud uls factory
            platform (str): The platform resource name

        Returns:
            str: The name of the created target_bucket_name (cloud uls)
        """
        # Create the actual target bucket on AWS
        uls_dict = cloud_uls_factory({platform: [(1, region)]})
        target_bucket_name = list(uls_dict[platform])[0]

        # Create namespace resource
        result = self.send_rpc_query(
            "pool_api",
            "create_namespace_resource",
            {
                "name": ns_resource_name,
                "connection": conn_name,
                "target_bucket": target_bucket_name,
            },
        )
        logger.info(f"result from RPC call: {result}")
        return target_bucket_name

    def check_ns_resource_validity(
        self, ns_resource_name, target_bucket_name, endpoint
    ):
        """
        Check namespace resource validity

        Args:
            ns_resource_name (str): The name of the to be verified namespace resource
            target_bucket_name (str): The name of the expected target bucket (uls)
            endpoint: The expected endpoint path
        """
        # Retrieve the NooBaa system information
        system_state = self.read_system()

        # Retrieve the correct namespace resource info
        match_resource = [
            ns_resource
            for ns_resource in system_state.get("namespace_resources")
            if ns_resource.get("name") == ns_resource_name
        ]
        assert match_resource, f"The NS resource named {ns_resource_name} was not found"
        actual_target_bucket = match_resource[0].get("target_bucket")
        actual_endpoint = match_resource[0].get("endpoint")

        assert actual_target_bucket == target_bucket_name, (
            f"The NS resource named {ns_resource_name} got "
            f"wrong target bucket {actual_target_bucket} ≠ {target_bucket_name}"
        )
        assert actual_endpoint == endpoint, (
            f"The NS resource named {ns_resource_name} got wrong endpoint "
            f"{actual_endpoint} ≠ {endpoint}"
        )
        return True

    def delete_ns_connection(self, ns_connection_name):
        """
        Delete external connection

        Args:
            ns_connection_name (str): The name of the to be deleted external connection
        """
        self.send_rpc_query(
            "account_api",
            "delete_external_connection",
            {"connection_name": ns_connection_name},
        )

    def delete_ns_resource(self, ns_resource_name):
        """
        Delete namespace resource

        Args:
            ns_resource_name (str): The name of the to be deleted namespace resource
        """
        self.send_rpc_query(
            "pool_api", "delete_namespace_resource", {"name": ns_resource_name}
        )

    def oc_create_bucketclass(
        self,
        name,
        backingstores,
        placement_policy,
        namespace_policy,
        replication_policy,
    ):
        """
        Creates a new NooBaa bucket class using a template YAML
        Args:
            name (str): The name to be given to the bucket class
            backingstores (list): The backing stores to use as part of the policy
            placement_policy (str): The placement policy to be used - Mirror | Spread
            namespace_policy (dict): The namespace policy to be used
            replication_policy (dict): The replication policy dictionary

        Returns:
            OCS: The bucket class resource

        """
        bc_data = templating.load_yaml(constants.MCG_BUCKETCLASS_YAML)
        bc_data["metadata"]["name"] = name
        bc_data["metadata"]["namespace"] = self.namespace
        bc_data["spec"] = {}

        if (backingstores is not None) and (placement_policy is not None):
            bc_data["spec"]["placementPolicy"] = {"tiers": [{}]}
            tiers = bc_data["spec"]["placementPolicy"]["tiers"][0]
            tiers["backingStores"] = [
                backingstore.name for backingstore in backingstores
            ]
            tiers["placement"] = placement_policy

        # In cases of Single and Cache namespace policies, we use the
        # write_resource key to populate the relevant YAML field.
        # The right field name is still used.
        if namespace_policy:
            bc_data["spec"]["namespacePolicy"] = {}
            ns_policy_type = namespace_policy["type"]
            bc_data["spec"]["namespacePolicy"]["type"] = ns_policy_type

            if ns_policy_type == constants.NAMESPACE_POLICY_TYPE_SINGLE:
                bc_data["spec"]["namespacePolicy"]["single"] = {
                    "resource": namespace_policy["write_resource"]
                }

            elif ns_policy_type == constants.NAMESPACE_POLICY_TYPE_MULTI:
                bc_data["spec"]["namespacePolicy"]["multi"] = {
                    "writeResource": namespace_policy["write_resource"],
                    "readResources": namespace_policy["read_resources"],
                }

            elif ns_policy_type == constants.NAMESPACE_POLICY_TYPE_CACHE:
                bc_data["spec"]["placementPolicy"] = placement_policy
                bc_data["spec"]["namespacePolicy"]["cache"] = namespace_policy["cache"]

        if replication_policy:
            bc_data["spec"].setdefault(
                "replicationPolicy",
                (
                    json.dumps(replication_policy)
                    if version.get_semantic_ocs_version_from_config()
                    < version.VERSION_4_12
                    else json.dumps({"rules": replication_policy})
                ),
            )

        return create_resource(**bc_data)

    def cli_create_bucketclass_over_backingstores(
        self,
        name,
        backingstores,
        placement_policy,
        replication_policy=None,
    ):
        """
        Creates a new NooBaa bucket class using the noobaa cli over backingstores
        Args:
            name (str): The name to be given to the bucket class
            backingstores (list): The backing stores to use as part of the policy.
            placement_policy (str): The placement policy to be used - Mirror | Spread
            replication_policy (dict): The replication policy dictionary

        Returns:
            OCS: The bucket class resource

        """
        backingstore_name_list = [backingstore.name for backingstore in backingstores]
        backingstores = f" --backingstores {','.join(backingstore_name_list)}"
        placement_policy = f" --placement {placement_policy}"
        placement_type = (
            f"{constants.PLACEMENT_BUCKETCLASS} "
            if version.get_semantic_ocs_version_from_config() >= version.VERSION_4_7
            else ""
        )
        if (
            replication_policy is not None
            and version.get_semantic_ocs_version_from_config() >= version.VERSION_4_12
        ):
            replication_policy = {"rules": replication_policy}
        with tempfile.NamedTemporaryFile(
            delete=True, mode="wb", buffering=0
        ) as replication_policy_file:
            replication_policy_file.write(
                json.dumps(replication_policy).encode("utf-8")
            )
            replication_policy = (
                f" --replication-policy {replication_policy_file.name}"
                if replication_policy
                else ""
            )
            self.exec_mcg_cmd(
                f"bucketclass create {placement_type}{name}{backingstores}{placement_policy}{replication_policy}"
            )

    def cli_create_bucketclass_over_namespacestores(
        self,
        name,
        namespacestores,
        namespace_policy,
    ):
        """
        Creates a new NooBaa bucket class using the noobaa cli over namespace stores
        Args:
            name (str): The name to be given to the bucket class
            namespacestores (list): The namespaces stores to use as part of the policy
            namespace_policy (dict): The namespace policy to be used. The supported namespace policy types are
            Single, Cache and Multi. For Cache NSS, default backing store is used as the cache.
            In the case of Multi namespace policy type, first namespace store is used as the write resource.

        Returns:
            OCS: The bucket class resource

        """
        namestores_name_list = [
            namespacestore.name for namespacestore in namespacestores
        ]
        namestores_name_str = f"{','.join(namestores_name_list)}"

        namespace_policy_type = namespace_policy["type"].lower()
        cmd = f"bucketclass create namespace-bucketclass {namespace_policy_type} {name}"

        if namespace_policy_type == constants.NAMESPACE_POLICY_TYPE_SINGLE.lower():
            cmd += f" --resource={namestores_name_str}"
            self.exec_mcg_cmd(cmd)
        elif namespace_policy_type == constants.NAMESPACE_POLICY_TYPE_CACHE.lower():
            cmd += f" --hub-resource={namestores_name_str}"
            cmd += f" --backingstores={constants.DEFAULT_NOOBAA_BACKINGSTORE}"
            if "ttl" in namespace_policy:
                cmd += f" --ttl=={namespace_policy['ttl']}"
            self.exec_mcg_cmd(cmd)
        elif namespace_policy_type == constants.NAMESPACE_POLICY_TYPE_MULTI.lower():
            cmd += f" --read-resources='{namestores_name_str}'"
            cmd += f" --write-resource='{namespacestores[0].name}'"
            self.exec_mcg_cmd(cmd)
        else:
            raise NotImplementedError(
                f"Cli creating of bucketclass on namespacestore "
                f"with policy {namespace_policy_type} is not supported"
            )

    def check_if_mirroring_is_done(self, bucket_name, timeout=300):
        """
        Check whether all object chunks in a bucket
        are mirrored across all backing stores.

        Args:
            bucket_name: The name of the bucket that should be checked
            timeout: timeout in seconds to check if mirroring

        Raises:
            AssertionError: In case mirroring is not done in defined time.

        """

        def _get_mirroring_percentage():
            results = []
            obj_list = (
                self.send_rpc_query(
                    "object_api", "list_objects", params={"bucket": bucket_name}
                )
                .json()
                .get("reply")
                .get("objects")
            )

            for written_object in obj_list:
                object_chunks = (
                    self.send_rpc_query(
                        "object_api",
                        "read_object_mapping",
                        params={
                            "bucket": bucket_name,
                            "key": written_object.get("key"),
                            "obj_id": written_object.get("obj_id"),
                        },
                    )
                    .json()
                    .get("reply")
                    .get("chunks")
                )

                for object_chunk in object_chunks:
                    mirror_blocks = object_chunk.get("frags")[0].get("blocks")
                    mirror_nodes = [
                        mirror_blocks[i].get("block_md").get("node")
                        for i in range(len(mirror_blocks))
                    ]
                    if 2 <= len(mirror_blocks) == len(set(mirror_nodes)):
                        results.append(True)
                    else:
                        results.append(False)
            current_percentage = (results.count(True) / len(results)) * 100
            return current_percentage

        mirror_percentage = _get_mirroring_percentage()
        logger.info(f"{mirror_percentage}% mirroring is done.")
        previous_percentage = 0
        while mirror_percentage < 100:
            previous_percentage = mirror_percentage
            try:
                for mirror_percentage in TimeoutSampler(
                    timeout, 5, _get_mirroring_percentage
                ):
                    if previous_percentage == mirror_percentage:
                        logger.warning("The mirroring process is stuck.")
                    else:
                        break
            except TimeoutExpiredError:
                logger.error(
                    f"The mirroring process is stuck from last {timeout} seconds."
                )
                assert False
            mirror_percentage = _get_mirroring_percentage()
        logger.info("All objects mirrored successfully.")

    def check_backingstore_state(self, backingstore_name, desired_state, timeout=600):
        """
        Checks whether the backing store reached a specific state
        Args:
            backingstore_name (str): Name of the backing store to be checked
            desired_state (str): The desired state of the backing store
            timeout (int): Number of seconds for timeout which will be used
            in the checks used in this function.

        Returns:
            bool: Whether the backing store has reached the desired state

        """

        def _check_state():
            sysinfo = self.read_system()
            for pool in sysinfo.get("pools"):
                if pool.get("name") in backingstore_name:
                    current_state = pool.get("mode")
                    logger.info(
                        f"Current state of backingstore {backingstore_name} "
                        f"is {current_state}"
                    )
                    if current_state == desired_state:
                        return True
            return False

        try:
            for reached_state in TimeoutSampler(timeout, 10, _check_state):
                if reached_state:
                    logger.info(
                        f"BackingStore {backingstore_name} reached state "
                        f"{desired_state}."
                    )
                    return True
                else:
                    logger.info(
                        f"Waiting for BackingStore {backingstore_name} to "
                        f"reach state {desired_state}..."
                    )
        except TimeoutExpiredError:
            logger.error(
                f"The BackingStore did not reach the desired state "
                f"{desired_state} within the time limit."
            )
            raise

    def exec_mcg_cmd(
        self, cmd, namespace=None, use_yes=False, ignore_error=False, **kwargs
    ):
        """
        Executes a NooBaa CLI command through the appropriate CLI binary

        For OCS >= 4.20: Uses odf-cli noobaa <command> via ODFCliRunner
        For OCS < 4.20: Uses mcg-cli <command> directly

        Args:
            cmd (str): The command to run
            namespace (str): The namespace to run the command in
            use_yes (bool): If True, pipe 'yes' to the command
            ignore_error (bool): If True, don't raise exception on non-zero exit
            **kwargs: Additional arguments to pass to exec_cmd

        Returns:
            CompletedProcess: Result object with stdout and stderr as decoded strings

        """
        # Use ODFCliRunner for OCS >= 4.20
        if self.odf_cli_runner:
            # Pass namespace without -n prefix (run_noobaa will add it)
            ns = namespace if namespace else self.namespace

            # Mask sensitive data
            if self.data_to_mask:
                kwargs.setdefault("secrets", []).extend(self.data_to_mask)

            result = self.odf_cli_runner.run_noobaa(
                cmd,
                namespace=ns,
                use_yes=use_yes,
                ignore_error=ignore_error,
                **kwargs,
            )
            # Decode stdout/stderr if they're bytes
            if hasattr(result, "stdout") and isinstance(result.stdout, bytes):
                result.stdout = result.stdout.decode()
            if hasattr(result, "stderr") and isinstance(result.stderr, bytes):
                result.stderr = result.stderr.decode()
            return result

        # Original implementation for OCS < 4.20
        kubeconfig = config.RUN.get("kubeconfig")
        if kubeconfig:
            kubeconfig = f"--kubeconfig {kubeconfig} "

        namespace = f"-n {namespace}" if namespace else f"-n {self.namespace}"

        # Build the full command using stored CLI configuration
        if self.command_prefix:
            # For odf-cli: odf-cli noobaa <command>
            full_cmd = f"{self.cli_path} {self.command_prefix} {cmd} {namespace}"
        else:
            # For mcg-cli: mcg-cli <command>
            full_cmd = f"{self.cli_path} {cmd} {namespace}"

        # Mask sensitive data
        if self.data_to_mask:
            kwargs.setdefault("secrets", []).extend(self.data_to_mask)

        if use_yes:
            result = exec_cmd(
                [f"yes | {full_cmd}"],
                ignore_error=ignore_error,
                shell=True,
                **kwargs,
            )
        else:
            result = exec_cmd(
                full_cmd,
                ignore_error=ignore_error,
                **kwargs,
            )
        result.stdout = result.stdout.decode()
        result.stderr = result.stderr.decode()
        return result

    @property
    def status(self):
        """
        Expose the status check of NooBaa as a property
        """
        return self._status()

    @staticmethod
    @retry(
        exception_to_check=(CommandFailed, KeyError, subprocess.TimeoutExpired),
        tries=10,
        delay=6,
        backoff=1,
    )
    def _status():
        """
        Verify the status of NooBaa, and its default backing store and bucket class

        Returns:
            bool: return False if any of the above components of noobaa is not in READY state

        """
        # Get noobaa status
        namespace = config.ENV_DATA["cluster_namespace"]
        get_noobaa = OCP(kind="noobaa", namespace=namespace).get(
            resource_name=NOOBAA_RESOURCE_NAME
        )
        get_default_bs = OCP(kind="backingstore", namespace=namespace).get(
            resource_name=DEFAULT_NOOBAA_BACKINGSTORE
        )
        get_default_bc = OCP(kind="bucketclass", namespace=namespace).get(
            resource_name=DEFAULT_NOOBAA_BUCKETCLASS
        )
        return (
            get_noobaa["status"]["phase"]
            == get_default_bs["status"]["phase"]
            == get_default_bc["status"]["phase"]
            == STATUS_READY
        )

    @staticmethod
    def wait_for_ready_status(timeout=600):
        """
        Wait for NooBaa's resources to reach the 'Ready' status

        Args:
            timeout (int): Number of seconds to wait for the status

        Raises:
            TimeoutExpiredError: If the status is not reached within the timeout
        """
        starttime = time.time()
        nb_pods = [pod.name for pod in get_noobaa_pods()]
        wait_for_pods_to_be_running(
            namespace=config.ENV_DATA["cluster_namespace"],
            pod_names=nb_pods,
            timeout=timeout,
            sleep=10,
        )

        # The timeout is reduced by the time already spent waiting for the pods
        # time spent might get longer than the timeout due to overheads,
        # so we set a minimum timeout of 60 seconds
        time_spent = time.time() - starttime
        time_remaining = timeout - time_spent
        timeout = max(int(time_remaining), 60)

        try:
            for mcg_status_ready in TimeoutSampler(
                timeout=timeout, sleep=30, func=MCG._status
            ):
                if mcg_status_ready:
                    return
        except TimeoutExpiredError as e:
            raise TimeoutExpiredError(
                e, f"NooBaa health is not OK after {timeout} seconds"
            )

    def get_mcg_cli_version(self):
        """
        Get the NooBaa CLI version by parsing the output of the version command.

        Example output of the mcg-cli version command:
            INFO[0000] CLI version: 5.12.0
            INFO[0000] noobaa-image: noobaa/noobaa-core:master-20220913
            INFO[0000] operator-image: noobaa/noobaa-operator:5.12.0

        Example output of the odf-cli noobaa version command:
            CLI version: 5.20.0
            noobaa-image: noobaa/noobaa-core:master-20240101
            operator-image: noobaa/noobaa-operator:5.20.0

        Returns:
            semantic_version.base.Version: Object of semantic version.

        """

        # Execute version command using appropriate CLI
        cmd_result = self.exec_mcg_cmd("version")

        # Try stderr first (mcg-cli sends output to stderr)
        # Then try stdout (odf-cli might send to stdout)
        cmd_output = (
            cmd_result.stderr if cmd_result.stderr.strip() else cmd_result.stdout
        )

        # \s* captures any number of spaces
        # \S+ captures any number of non-space characters
        regular_expression = r"CLI version:\s*(\S+)"

        # group(1) is the first capturing group, which is the version string
        match = re.search(regular_expression, cmd_output, re.IGNORECASE)

        if not match:
            logger.warning(f"Could not parse CLI version from output: {cmd_output}")
            # Fallback to current OCS version if parsing fails
            return version.get_semantic_ocs_version_from_config()

        mcg_cli_version_str = match.group(1)

        return version.get_semantic_version(mcg_cli_version_str, only_major_minor=True)

    def reset_core_pod(self):
        """
        Delete the noobaa-core pod and wait for it to come up again

        """
        from ocs_ci.ocs.resources.pod import wait_for_pods_by_label_count

        self.core_pod.delete(wait=True)
        wait_for_pods_by_label_count(
            label=constants.NOOBAA_CORE_POD_LABEL, expected_count=1
        )
        self.core_pod = Pod(
            **get_pods_having_label(constants.NOOBAA_CORE_POD_LABEL, self.namespace)[0]
        )
        wait_for_resource_state(self.core_pod, constants.STATUS_RUNNING)

    def reset_endpoint_pods(self):
        """
        Delete the noobaa endpoint pod and wait for it to come up again

        """

        from ocs_ci.ocs.resources.pod import wait_for_pods_by_label_count

        nb_ep_dep_obj = OCP(
            kind="deployment",
            namespace=self.namespace,
            resource_name=constants.NOOBAA_ENDPOINT_DEPLOYMENT,
        )
        nb_ep_dep_obj.exec_oc_cmd(
            f"rollout restart deployment/{constants.NOOBAA_ENDPOINT_DEPLOYMENT}"
        )
        # Wait for the rollout to complete
        nb_ep_dep_obj.exec_oc_cmd(
            (
                f"rollout status deployment/{constants.NOOBAA_ENDPOINT_DEPLOYMENT}"
                " --timeout=120s"
            ),
            out_yaml_format=False,
        )
        expected_pod_count = nb_ep_dep_obj.get().get("spec").get("replicas")
        wait_for_pods_by_label_count(
            label=constants.NOOBAA_ENDPOINT_POD_LABEL,
            expected_count=expected_pod_count,
        )

        endpoint_pods = [
            Pod(**pod_data)
            for pod_data in get_pods_having_label(
                constants.NOOBAA_ENDPOINT_POD_LABEL, self.namespace
            )
        ]
        for pod in endpoint_pods:
            wait_for_resource_state(pod, constants.STATUS_RUNNING)

    def get_noobaa_admin_credentials_from_secret(self):
        """
        Get the NooBaa admin credentials from the OCP secret

        Returns:
            credentials_dict (dict): Dictionary containing the following keys:
                AWS_ACCESS_KEY_ID (str): NooBaa admin S3 access key ID
                AWS_SECRET_ACCESS_KEY (str): NooBaa admin S3 secret access key
                email (str): NooBaa admin user email
                password (str): NooBaa admin user password

        """

        get_noobaa = OCP(kind="noobaa", namespace=self.namespace).get()

        creds_secret_name = (
            get_noobaa.get("items")[0]
            .get("status")
            .get("accounts")
            .get("admin")
            .get("secretRef")
            .get("name")
        )

        secret_ocp_obj = OCP(kind="secret", namespace=self.namespace)
        creds_secret_obj = secret_ocp_obj.get(creds_secret_name)

        credentials_dict = {}

        credentials_dict["AWS_ACCESS_KEY_ID"] = base64.b64decode(
            creds_secret_obj.get("data").get("AWS_ACCESS_KEY_ID")
        ).decode("utf-8")

        credentials_dict["AWS_SECRET_ACCESS_KEY"] = base64.b64decode(
            creds_secret_obj.get("data").get("AWS_SECRET_ACCESS_KEY")
        ).decode("utf-8")

        credentials_dict["email"] = base64.b64decode(
            creds_secret_obj.get("data").get("email")
        ).decode("utf-8")

        credentials_dict["password"] = base64.b64decode(
            creds_secret_obj.get("data").get("password")
        ).decode("utf-8")

        return credentials_dict

    def update_s3_creds(self):
        """
        Set the S3 credentials of the NooBaa admin user from the
        noobaa-admin secret, and update the S3 resource and client

        """
        admin_credentials = self.get_noobaa_admin_credentials_from_secret()
        self.access_key_id = admin_credentials["AWS_ACCESS_KEY_ID"]
        self.access_key = admin_credentials["AWS_SECRET_ACCESS_KEY"]
        self.noobaa_user = admin_credentials["email"]
        self.noobaa_password = admin_credentials["password"]

        self.data_to_mask.extend(flatten_multilevel_dict(admin_credentials))
        self.noobaa_token = self.retrieve_nb_token()

        self.s3_resource = boto3.resource(
            "s3",
            verify=retrieve_verification_mode(),
            endpoint_url=self.s3_endpoint,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.access_key,
        )

        self.s3_client = self.s3_resource.meta.client

    def reset_admin_pw(self, new_password):
        """
        Reset the NooBaa admin password

        Args:
            new_password (str): New password to set for the NooBaa admin user

        """
        logger.info("Resetting the noobaa-admin password")

        cmd = "".join(
            (
                f"account passwd {self.noobaa_user}",
                f" --old-password {self.noobaa_password}",
                f" --new-password {new_password}",
                f" --retype-new-password {new_password}",
            )
        )

        self.exec_mcg_cmd(cmd)
        self.noobaa_password = new_password

        logger.info("Waiting a bit for the change to propogate through the system...")
        sleep(15)

    def get_admin_default_resource_name(self):
        """
        Get the default resource name of the admin account

        Returns:
            str: The default resource name

        """

        read_account_output = self.send_rpc_query(
            "account_api",
            "read_account",
            params={
                "email": self.noobaa_user,
            },
        )
        return read_account_output.json()["reply"]["default_resource"]

    def get_default_bc_backingstore_name(self):
        """
        Get the default backingstore name of the default bucketclass

        Returns:
            str: The default backingstore name

        """
        bucketclass_ocp_obj = OCP(
            kind=constants.BUCKETCLASS,
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=constants.DEFAULT_NOOBAA_BUCKETCLASS,
        )
        return (
            bucketclass_ocp_obj.get()
            .get("spec")
            .get("placementPolicy")
            .get("tiers")[0]
            .get("backingStores")[0]
        )

    def assign_sts_role(self, account_id, role_config):
        """
        Assign STS role to a Noobaa account

        Args:
            account_id (str): Name/email/id of the noobaa account
            role_config (dict): Role config consisting of role name, role policy etc

        """

        cmd = f"sts assign-role --email {account_id} --role_config '{str(role_config)}'"
        self.exec_mcg_cmd(
            cmd=cmd,
        )

    def remove_sts_role(self, account_id):
        """
        Remove STS role from a Noobaa account

        Args:
            account_id (str): Name/email/id of the noobaa account

        """
        cmd = f"sts remove-role --email {account_id}"
        self.exec_mcg_cmd(
            cmd=cmd,
        )
