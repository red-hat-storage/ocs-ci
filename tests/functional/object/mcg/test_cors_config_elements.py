import os
import logging
import pytest
import json
from random import choice
from time import sleep
from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    red_squad,
    mcg,
)
from ocs_ci.utility.utils import exec_cmd
from ocs_ci.ocs.bucket_utils import craft_s3_command
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


@mcg
@red_squad
class TestCorsConfig:
    """
    Test CORS config elements along with its operation against Bucket
    """

    def create_custom_cors_config(
        self,
        allowed_origins=["*"],
        allowed_methods=["*"],
        allowed_headers=["*"],
        expose_headers=["X-Request-ID"],
        max_age=300,
    ):
        """
        Creates custom CORS config for the bucket
        Args:
            allowed_origins (List): List of addresses
            allowed_methods (List): List of Methods
            allowed_headers (List): List of Allowed headers
            expose_headers (List): List of Exposed headers
            max_age (int): Max seconds
        Returns:
            Json config
        """
        cors_config = {
            "CORSRules": [
                {
                    "AllowedHeaders": allowed_headers,
                    "AllowedMethods": allowed_methods,
                    "AllowedOrigins": allowed_origins,
                    "ExposeHeaders": expose_headers,
                    "MaxAgeSeconds": max_age,
                }
            ]
        }
        return cors_config

    def exec_cors_command(
        self, operation, bucket_name, awscli_pod, mcg_obj, response=False, file=None
    ):
        """
        Generates CORS command with given parameter, execute it and returns response
        Args:
            operation (String): CORS operation name
            bucket_name (String): Name of the bucket
            awscli_pod (Obj): AWS CLI pod object
            mcg_obj (Obj): MCG object
            response (Bool) : Returns response if value is True
            file (string) : Path of CORS config file
        Returns:
            HTTP response if response is true else None
        """
        cors_cmd = f"{operation} --bucket {bucket_name} "
        if file is not None:
            cors_cmd = cors_cmd + f"--cors-configuration file://{file}"
        cors_op = awscli_pod.exec_cmd_on_pod(
            command=craft_s3_command(cors_cmd, mcg_obj, api=True),
            out_yaml_format=False,
            secrets=[
                mcg_obj.access_key_id,
                mcg_obj.access_key,
                mcg_obj.s3_internal_endpoint,
            ],
        )
        if response:
            return cors_op

    def exec_curl_command(self, endpoint, bucket_name, origin, method, **kwargs):
        """
        Generates curl command with given parameter, execute it and returns response
        Args:
            endpoint (String): host address
            bucket_name (String): Name of the bucket
            origin (String): FQDN address
            method (String): HTTP method name
            **kwargs (Dict): extra header info
        Returns:
            HTTP response
        """
        extra_header = ""
        for key, value in kwargs.items():
            extra_header = "-H '" + extra_header + key + ": " + value + "' "
        curl_command = (
            f"curl -k {endpoint}/{bucket_name} -X OPTIONS "
            f"-H 'Access-Control-Request-Method: {method}' "
            f"-H 'Origin: {origin}' "
            f"{extra_header} "
            f"-D -"
        )
        curl_output = exec_cmd(cmd=curl_command).stdout.decode()
        logger.info(curl_output)
        curl_output = curl_output.split()
        resp_code = curl_output[curl_output.index("HTTP/1.1") + 1]
        return int(resp_code)

    @pytest.mark.parametrize(
        argnames="bucketclass_dict",
        argvalues=[
            pytest.param(*[None], marks=[tier1, pytest.mark.polarion_id("OCS-1868")]),
            pytest.param(
                *[
                    {
                        "interface": "OC",
                        "backingstore_dict": {"aws": [(1, "eu-central-1")]},
                    },
                ],
            ),
            pytest.param(
                *[
                    {"interface": "OC", "backingstore_dict": {"azure": [(1, None)]}},
                ],
            ),
            pytest.param(
                *[{"interface": "OC", "backingstore_dict": {"gcp": [(1, None)]}}],
            ),
            pytest.param(
                *[
                    {"interface": "OC", "backingstore_dict": {"ibmcos": [(1, None)]}},
                ],
            ),
        ],
        ids=[
            "OBC-DEFAULT",
            "OBC-AWS",
            "OBC-AZURE",
            "OBC-GCP",
            "OBC-IBMCOS",
        ],
    )
    @tier1
    def test_basic_cors_operations(
        self, mcg_obj, awscli_pod, bucket_factory, bucketclass_dict
    ):
        """
        Test Basic CORS operation on bucket
            step #1: Create a bucket
            step #2: Get the bucket default CORS configuration
            step #3: delete the default CORS, by running delete-bucket-core
            step #4: Get the bucket default CORS configuration after deleting it
            step #5: set your own CORS configuration on the bucket using put-bucket-cors api
            step #6: Validate you get the correct bucket cors config
            step #7: Access bucket using supported origin
            step #8: Access bucket using non-supported origin
            step #9: Create multiple CORS config for single bucket
            step #10: Delete the assigned CORS config and try to access bucket from previously supported origin
        """
        # 1: Create a bucket
        bucket_name = bucket_factory(interface="OC", bucketclass=bucketclass_dict)[
            0
        ].name

        # 2: Get the bucket default CORS configuration
        get_bucket_cors_op = self.exec_cors_command(
            operation="get-bucket-cors",
            bucket_name=bucket_name,
            awscli_pod=awscli_pod,
            mcg_obj=mcg_obj,
            response=True,
        )
        logger.info(get_bucket_cors_op)

        # 3: delete the default CORS, by running delete-bucket-core
        self.exec_cors_command(
            operation="delete-bucket-cors",
            bucket_name=bucket_name,
            awscli_pod=awscli_pod,
            mcg_obj=mcg_obj,
        )

        # 4: Get the bucket default CORS configuration after deleting it
        get_bucket_cors_op = None
        try:
            get_bucket_cors_op = self.exec_cors_command(
                operation="get-bucket-cors",
                bucket_name=bucket_name,
                awscli_pod=awscli_pod,
                mcg_obj=mcg_obj,
                response=True,
            )
        except Exception as e:
            if "NoSuchCORSConfiguration" in str(e):
                logger.warning("Expected error")
                logger.warning(e)
            else:
                raise
        assert (
            get_bucket_cors_op is None
        ), f"Failed to delete default CORS config \n {get_bucket_cors_op}"
        logger.info("CORS config deleted successfully")

        # 5: set custom CORS configuration on the bucket using put-bucket-cors api
        cors_config = self.create_custom_cors_config(
            allowed_origins=["https://abc.com"],
            allowed_methods=["GET", "PUT"],
            allowed_headers=["*"],
            expose_headers=["ETag", "X-Request-ID"],
            max_age=30,
        )
        file_name = os.path.join("/tmp", "cors_config.json")
        with open(file_name, "w") as file:
            json.dump(cors_config, file, indent=4)
        copy_cors_cmd = (
            f"oc -n {awscli_pod.namespace} cp {file_name} {awscli_pod.name}:{file_name}"
        )
        exec_cmd(copy_cors_cmd)
        self.exec_cors_command(
            operation="put-bucket-cors",
            bucket_name=bucket_name,
            awscli_pod=awscli_pod,
            mcg_obj=mcg_obj,
            file=file_name,
        )

        # 6: Validate get-bucket-cors returns custom bucket cors config
        get_bucket_cors_op = self.exec_cors_command(
            operation="get-bucket-cors",
            bucket_name=bucket_name,
            awscli_pod=awscli_pod,
            mcg_obj=mcg_obj,
            response=True,
        )
        logger.info(get_bucket_cors_op)
        get_bucket_cors_dict = json.loads(get_bucket_cors_op)
        assert (
            cors_config == get_bucket_cors_dict
        ), "There is mismatch in uploaded CORS config and received CORS config"

        # 7: Access bucket using supported origin
        logger.info("Sleeping for 30 sec to refresh CORS rules")
        sleep(30)
        allowed_origin = choice(cors_config["CORSRules"][0]["AllowedOrigins"])
        http_method = choice(cors_config["CORSRules"][0]["AllowedMethods"])
        resp_expected_value = 200
        sample = TimeoutSampler(
            timeout=60,
            sleep=10,
            func=self.exec_curl_command,
            endpoint=mcg_obj.s3_endpoint,
            bucket_name=bucket_name,
            origin=allowed_origin,
            method=http_method,
        )
        sample.wait_for_func_value(resp_expected_value)
        # 8: Access bucket using non-supported origin
        incorrect_origin = (
            choice(cors_config["CORSRules"][0]["AllowedOrigins"]) + ".incorrect.origin"
        )
        http_method = choice(cors_config["CORSRules"][0]["AllowedMethods"])
        extra_header = {"Content-Type": "application/json"}
        resp_expected_value = 403
        sample = TimeoutSampler(
            timeout=60,
            sleep=10,
            func=self.exec_curl_command,
            endpoint=mcg_obj.s3_endpoint,
            bucket_name=bucket_name,
            origin=incorrect_origin,
            method=http_method,
            **extra_header,
        )
        sample.wait_for_func_value(resp_expected_value)
        # TODO: Implement step 9 and 10 as part of additional test scenarios(No part of happy path testing)
        # 9: Create multiple CORS config for single bucket
        # 10: Delete the assigned CORS config and try to access bucket from previously supported origin
