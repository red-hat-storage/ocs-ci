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
        get_bucket_cors = f"get-bucket-cors --bucket {bucket_name}"
        get_bucket_cors_op = awscli_pod.exec_cmd_on_pod(
            command=craft_s3_command(get_bucket_cors, mcg_obj, api=True),
            out_yaml_format=False,
            secrets=[
                mcg_obj.access_key_id,
                mcg_obj.access_key,
                mcg_obj.s3_internal_endpoint,
            ],
        )
        logger.info(get_bucket_cors_op)

        # 3: delete the default CORS, by running delete-bucket-core
        delete_bucket_cors = f"delete-bucket-cors --bucket {bucket_name}"
        awscli_pod.exec_cmd_on_pod(
            command=craft_s3_command(delete_bucket_cors, mcg_obj, api=True),
            out_yaml_format=False,
            secrets=[
                mcg_obj.access_key_id,
                mcg_obj.access_key,
                mcg_obj.s3_internal_endpoint,
            ],
        )

        # 4: Get the bucket default CORS configuration after deleting it
        get_bucket_cors_op = None
        try:
            get_bucket_cors = f"get-bucket-cors --bucket {bucket_name}"
            get_bucket_cors_op = awscli_pod.exec_cmd_on_pod(
                command=craft_s3_command(get_bucket_cors, mcg_obj, api=True),
                out_yaml_format=False,
                secrets=[
                    mcg_obj.access_key_id,
                    mcg_obj.access_key,
                    mcg_obj.s3_internal_endpoint,
                ],
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
        _ = exec_cmd(copy_cors_cmd)
        put_bucket_cors = f"put-bucket-cors --bucket {bucket_name} --cors-configuration file://{file_name}"
        awscli_pod.exec_cmd_on_pod(
            command=craft_s3_command(put_bucket_cors, mcg_obj, api=True),
            out_yaml_format=False,
            secrets=[
                mcg_obj.access_key_id,
                mcg_obj.access_key,
                mcg_obj.s3_internal_endpoint,
            ],
        )

        # 6: Validate get-bucket-cors returns custom bucket cors config
        get_bucket_cors = f"get-bucket-cors --bucket {bucket_name}"
        get_bucket_cors_op = awscli_pod.exec_cmd_on_pod(
            command=craft_s3_command(get_bucket_cors, mcg_obj, api=True),
            out_yaml_format=False,
            secrets=[
                mcg_obj.access_key_id,
                mcg_obj.access_key,
                mcg_obj.s3_internal_endpoint,
            ],
        )
        logger.info(get_bucket_cors_op)
        get_bucket_cors_dict = json.loads(get_bucket_cors_op)
        assert (
            cors_config == get_bucket_cors_dict
        ), "There is mismatch in uploaded CORS config and received CORS config"

        # 7: Access bucket using supported origin
        logger.info("Sleeping for 30 sec to refresh CORS rules")
        sleep(30)
        curl_command = (
            f"curl -k {mcg_obj.s3_endpoint}/{bucket_name} -X OPTIONS "
            f"-H 'Access-Control-Request-Method: {choice(cors_config['CORSRules'][0]['AllowedMethods'])}' "
            f"-H 'Origin: {choice(cors_config['CORSRules'][0]['AllowedOrigins'])}' "
            f"-H 'Content-Type: application/json' -D -"
        )
        curl_output = exec_cmd(cmd=curl_command).stdout.decode()
        logger.info(curl_output)
        curl_output = curl_output.split()
        resp_code = curl_output[curl_output.index("HTTP/1.1") + 1]
        resp_status = curl_output[curl_output.index("HTTP/1.1") + 2]
        assert (
            int(resp_code) == 200 and resp_status == "OK"
        ), f"Bucket is not accessible from Allowed Origin received response code {resp_code} and Status {resp_status}"

        # 8: Access bucket using non-supported origin
        curl_command = (
            f"curl -k {mcg_obj.s3_endpoint}/{bucket_name} -X OPTIONS "
            f"-H 'Access-Control-Request-Method: {choice(cors_config['CORSRules'][0]['AllowedMethods'])}' "
            f"-H 'Origin: {choice(cors_config['CORSRules'][0]['AllowedOrigins'])}aaa' "
            f"-H 'Content-Type: application/json' -D -"
        )
        curl_output = exec_cmd(cmd=curl_command).stdout.decode()
        logger.info(curl_output)
        curl_output = curl_output.split()
        resp_code = curl_output[curl_output.index("HTTP/1.1") + 1]
        resp_status = curl_output[curl_output.index("HTTP/1.1") + 2]
        assert (
            int(resp_code) == 403 and resp_status == "Forbidden"
        ), f"Bucket is accessible from Non Allowed Origin received response code {resp_code} and Status {resp_status}"
        # TODO: Implement step 9 and 10 as part of additional test scenarios(No part of happy path testing)
        # 9: Create multiple CORS config for single bucket
        # 10: Delete the assigned CORS config and try to access bucket from previously supported origin
