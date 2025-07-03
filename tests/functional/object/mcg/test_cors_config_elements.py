import os
import logging
import pytest
import json
import string
from time import sleep
from random import choice, choices, randint
from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    tier2,
    tier4a,
    red_squad,
    mcg,
    skipif_fips_enabled,
    on_prem_platform_required,
    pre_upgrade,
    post_upgrade,
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

    POSITIVE_RESPONSE = 200
    NEGATIVE_RESPONSE = 403
    allowed_methods_list = ["GET", "PUT", "POST", "DELETE", "HEAD"]
    expose_headers_list = [
        "ETag",
        "X-Custom-Header",
        "X-Total-Count",
        "X-Request-ID",
        "Content-Disposition",
        "Link",
    ]

    def generate_random_domain_addresses(self, num_of_addresses=1):
        """
        Generates Random domain addresses
        Args:
            num_of_addresses (int): Number of addresses to generate
        Return:
            List
        """
        domain_addresses = []
        for _ in range(num_of_addresses):
            domain_addresses.append(
                "https://"
                + "".join(choices(string.ascii_lowercase + string.digits, k=5))
                + ".com"
            )
        return domain_addresses

    def create_custom_cors_config(
        self, no_of_config=1, method_num=2, exp_header=2, origin_num=2
    ):
        """
        Creates custom CORS config for the bucket
        Args:
            no_of_config (int): Number of CORS config to generate
        Returns:
            Json config : Dictionary
        """
        cors_config = {"CORSRules": []}
        for _ in range(no_of_config):
            allowed_headers = ["*"]
            allowed_methods = choices(self.allowed_methods_list, k=method_num)
            expose_headers = choices(self.expose_headers_list, k=exp_header)
            allowed_origins = self.generate_random_domain_addresses(origin_num)
            max_age = randint(30, 300)
            conf = {
                "AllowedHeaders": allowed_headers,
                "AllowedMethods": allowed_methods,
                "AllowedOrigins": allowed_origins,
                "ExposeHeaders": expose_headers,
                "MaxAgeSeconds": max_age,
            }
            cors_config["CORSRules"].append(conf)
        return cors_config

    def apply_cors_on_bucket(
        self,
        cors_config,
        awscli_pod_session,
        bucket_name,
        mcg_obj_session,
        response=False,
    ):
        """
        Applies passed CORS config on bucket
        Args:
            cors_config (Dict): CORS config info
            awscli_pod_session (Obj): AWS CLI pod object
            bucket_name (String): Bucket name
            mcg_obj_session (Obj): MCG object
        Returns:
            None
        """
        file_name = os.path.join("/tmp", "cors_config.json")
        with open(file_name, "w") as file:
            json.dump(cors_config, file, indent=4)
        copy_cors_cmd = f"oc -n {awscli_pod_session.namespace} cp {file_name} {awscli_pod_session.name}:{file_name}"
        exec_cmd(copy_cors_cmd)
        resp = self.exec_cors_command(
            operation="put-bucket-cors",
            bucket_name=bucket_name,
            awscli_pod_session=awscli_pod_session,
            mcg_obj_session=mcg_obj_session,
            response=response,
            file=file_name,
        )
        return resp if response else None

    def exec_cors_command(
        self,
        operation,
        bucket_name,
        awscli_pod_session,
        mcg_obj_session,
        response=False,
        file=None,
    ):
        """
        Generates CORS command with given parameter, execute it and returns response
        Args:
            operation (String): CORS operation name
            bucket_name (String): Name of the bucket
            awscli_pod_session (Obj): AWS CLI pod object
            mcg_obj_session (Obj): MCG object
            response (Bool) : Returns response if value is True
            file (string) : Path of CORS config file
        Returns:
            HTTP response if response is true else None
        """
        cors_cmd = f"{operation} --bucket {bucket_name} "
        if file is not None:
            cors_cmd = cors_cmd + f"--cors-configuration file://{file}"
        cors_op = awscli_pod_session.exec_cmd_on_pod(
            command=craft_s3_command(cors_cmd, mcg_obj_session, api=True),
            out_yaml_format=False,
            secrets=[
                mcg_obj_session.access_key_id,
                mcg_obj_session.access_key,
                mcg_obj_session.s3_internal_endpoint,
            ],
        )
        if response:
            return cors_op

    def exec_curl_command(self, endpoint, bucket_name, origin, method, **kwargs):
        """
        Generates curl command with given parameter, execute it and returns response code
        Args:
            endpoint (String): host address
            bucket_name (String): Name of the bucket
            origin (String): FQDN address
            method (String): HTTP method name
            **kwargs (Dict): extra header info
        Returns:
            Response code (int)
        """
        extra_header = ""
        for key, value in kwargs.items():
            extra_header = extra_header + " -H '" + key + ": " + value + "' "
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
        resp_index = curl_output.index("HTTP/1.1") + 1
        # Discard first response code if script is using proxy info
        if (
            curl_output[resp_index + 1] == "Connection"
            and curl_output[resp_index + 2] == "established"
        ):
            resp_code = curl_output[resp_index + 4]
        else:
            resp_code = curl_output[resp_index]
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
                marks=[tier1],
            ),
            pytest.param(
                *[
                    {"interface": "OC", "backingstore_dict": {"azure": [(1, None)]}},
                ],
                marks=[tier1],
            ),
            pytest.param(
                *[{"interface": "OC", "backingstore_dict": {"gcp": [(1, None)]}}],
                marks=[tier2],
            ),
            pytest.param(
                *[
                    {"interface": "OC", "backingstore_dict": {"ibmcos": [(1, None)]}},
                ],
                marks=[tier2, skipif_fips_enabled],
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"aws": [(1, None)]},
                    },
                },
                marks=[tier1],
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"azure": [(1, None)]},
                    },
                },
                marks=[tier2],
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"rgw": [(1, None)]},
                    },
                },
                marks=[tier2, on_prem_platform_required],
            ),
            pytest.param(
                {
                    "interface": "CLI",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"gcp": [(1, None)]},
                    },
                },
                marks=[tier2],
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"ibmcos": [(1, None)]},
                    },
                },
                marks=[tier2, skipif_fips_enabled],
            ),
        ],
        ids=[
            "OBC-DEFAULT",
            "OBC-AWS",
            "OBC-AZURE",
            "OBC-GCP",
            "OBC-IBMCOS",
            "OBC-AWS-NSS",
            "OBC-Azure-NSS",
            "OBC-RGW-NSS",
            "OBC-GCP-NSS",
            "OBC-IBM-NSS",
        ],
    )
    def test_basic_cors_operations(
        self, mcg_obj_session, awscli_pod_session, bucket_factory, bucketclass_dict
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
            awscli_pod_session=awscli_pod_session,
            mcg_obj_session=mcg_obj_session,
            response=True,
        )
        logger.info(get_bucket_cors_op)

        # 3: delete the default CORS, by running delete-bucket-core
        self.exec_cors_command(
            operation="delete-bucket-cors",
            bucket_name=bucket_name,
            awscli_pod_session=awscli_pod_session,
            mcg_obj_session=mcg_obj_session,
        )

        # 4: Get the bucket default CORS configuration after deleting it
        get_bucket_cors_op = None
        try:
            get_bucket_cors_op = self.exec_cors_command(
                operation="get-bucket-cors",
                bucket_name=bucket_name,
                awscli_pod_session=awscli_pod_session,
                mcg_obj_session=mcg_obj_session,
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
        cors_config = self.create_custom_cors_config(no_of_config=1)
        self.apply_cors_on_bucket(
            cors_config, awscli_pod_session, bucket_name, mcg_obj_session
        )

        # 6: Validate get-bucket-cors returns custom bucket cors config
        get_bucket_cors_op = self.exec_cors_command(
            operation="get-bucket-cors",
            bucket_name=bucket_name,
            awscli_pod_session=awscli_pod_session,
            mcg_obj_session=mcg_obj_session,
            response=True,
        )
        logger.info(get_bucket_cors_op)
        get_bucket_cors_dict = json.loads(get_bucket_cors_op)
        assert (
            cors_config == get_bucket_cors_dict
        ), "There is mismatch in uploaded CORS config and received CORS config"

        # 7: Access bucket using supported origin
        allowed_origin = choice(cors_config["CORSRules"][0]["AllowedOrigins"])
        http_method = choice(cors_config["CORSRules"][0]["AllowedMethods"])
        sample = TimeoutSampler(
            timeout=120,
            sleep=10,
            func=self.exec_curl_command,
            endpoint=mcg_obj_session.s3_endpoint,
            bucket_name=bucket_name,
            origin=allowed_origin,
            method=http_method,
        )
        sample.wait_for_func_value(self.POSITIVE_RESPONSE)

        # 8: Access bucket using non-supported origin
        incorrect_origin = self.generate_random_domain_addresses()[0]
        http_method = choice(cors_config["CORSRules"][0]["AllowedMethods"])
        extra_header = {"Content-Type": "application/json"}
        sample = TimeoutSampler(
            timeout=120,
            sleep=10,
            func=self.exec_curl_command,
            endpoint=mcg_obj_session.s3_endpoint,
            bucket_name=bucket_name,
            origin=incorrect_origin,
            method=http_method,
            **extra_header,
        )
        sample.wait_for_func_value(self.NEGATIVE_RESPONSE)

        # 9: Create multiple CORS config for single bucket
        cors_config = self.create_custom_cors_config(no_of_config=3)
        self.apply_cors_on_bucket(
            cors_config, awscli_pod_session, bucket_name, mcg_obj_session
        )
        get_bucket_cors_op = self.exec_cors_command(
            operation="get-bucket-cors",
            bucket_name=bucket_name,
            awscli_pod_session=awscli_pod_session,
            mcg_obj_session=mcg_obj_session,
            response=True,
        )
        logger.info(get_bucket_cors_op)
        get_bucket_cors_dict = json.loads(get_bucket_cors_op)
        assert (
            cors_config == get_bucket_cors_dict
        ), "There is mismatch in uploaded CORS config and received CORS config"

        # 10: Delete the assigned CORS config and try to access bucket from previously supported origin
        self.exec_cors_command(
            operation="delete-bucket-cors",
            bucket_name=bucket_name,
            awscli_pod_session=awscli_pod_session,
            mcg_obj_session=mcg_obj_session,
        )
        allowed_origin = choice(cors_config["CORSRules"][0]["AllowedOrigins"])
        http_method = choice(cors_config["CORSRules"][0]["AllowedMethods"])
        sample = TimeoutSampler(
            timeout=120,
            sleep=10,
            func=self.exec_curl_command,
            endpoint=mcg_obj_session.s3_endpoint,
            bucket_name=bucket_name,
            origin=allowed_origin,
            method=http_method,
        )
        sample.wait_for_func_value(self.NEGATIVE_RESPONSE)

    @tier2
    def test_allowed_origin_cors_element(
        self, mcg_obj_session, awscli_pod_session, bucket_factory
    ):
        """
        Test AllowedOrigins element from CORS operation on bucket
            step #1: Create bucket and apply CORS config with one allowed origin address
            step #2: Perform Allowed request from allowed origin mentioned in CORS
            step #3: Modify the existing CORS config and add different and multiple origins in it
            step #4: Perform GET request from any allowed origin mentioned in step #3
            step #5: Add wildcard(*) character in existing CORS config
            step #6: Perform GET request from any origin
            step #7: Modify exisitng CORS and add wildcard character like ""http://*.abc.com""
            step #8: Perform GET request from any origin that has address like ""http://app.abc.com""
            step #9: Perform GET request from non origin address
        """
        # 1: Create bucket and apply CORS config with one allowed origin address
        bucket_name = bucket_factory(interface="OC")[0].name
        cors_config = self.create_custom_cors_config(no_of_config=1)
        self.apply_cors_on_bucket(
            cors_config, awscli_pod_session, bucket_name, mcg_obj_session
        )

        # 2: Perform Allowed request from allowed origin mentioned in CORS
        allowed_origin = choice(cors_config["CORSRules"][0]["AllowedOrigins"])
        http_method = choice(cors_config["CORSRules"][0]["AllowedMethods"])
        sample = TimeoutSampler(
            timeout=120,
            sleep=10,
            func=self.exec_curl_command,
            endpoint=mcg_obj_session.s3_endpoint,
            bucket_name=bucket_name,
            origin=allowed_origin,
            method=http_method,
        )
        sample.wait_for_func_value(self.POSITIVE_RESPONSE)

        # 3: Modify the existing CORS config and add different and multiple origins in it
        extra_origin = self.generate_random_domain_addresses()[0]
        cors_config["CORSRules"][0]["AllowedOrigins"].append(extra_origin)
        self.apply_cors_on_bucket(
            cors_config, awscli_pod_session, bucket_name, mcg_obj_session
        )

        # 4: Perform GET request from any allowed origin mentioned in step #3
        http_method = choice(cors_config["CORSRules"][0]["AllowedMethods"])
        sample = TimeoutSampler(
            timeout=120,
            sleep=10,
            func=self.exec_curl_command,
            endpoint=mcg_obj_session.s3_endpoint,
            bucket_name=bucket_name,
            origin=extra_origin,
            method=http_method,
        )
        sample.wait_for_func_value(self.POSITIVE_RESPONSE)

        # 5: Add wildcard(*) character in existing CORS config
        wildcard_origin = ["*"]
        cors_config["CORSRules"][0]["AllowedOrigins"] = wildcard_origin
        self.apply_cors_on_bucket(
            cors_config, awscli_pod_session, bucket_name, mcg_obj_session
        )

        # 6: Perform GET request from any origin
        random_origin = self.generate_random_domain_addresses()[0]
        http_method = choice(cors_config["CORSRules"][0]["AllowedMethods"])
        sample = TimeoutSampler(
            timeout=120,
            sleep=10,
            func=self.exec_curl_command,
            endpoint=mcg_obj_session.s3_endpoint,
            bucket_name=bucket_name,
            origin=random_origin,
            method=http_method,
        )
        sample.wait_for_func_value(self.POSITIVE_RESPONSE)
        # 7: Modify exisitng CORS and add wildcard character like ""http://*.abc.com""
        subdomain_origin = ["https://*.abc.com"]
        cors_config["CORSRules"][0]["AllowedOrigins"] = subdomain_origin
        self.apply_cors_on_bucket(
            cors_config, awscli_pod_session, bucket_name, mcg_obj_session
        )

        # 8: Perform GET request from any origin that has address like ""http://app.abc.com""
        http_method = choice(cors_config["CORSRules"][0]["AllowedMethods"])
        sample = TimeoutSampler(
            timeout=120,
            sleep=10,
            func=self.exec_curl_command,
            endpoint=mcg_obj_session.s3_endpoint,
            bucket_name=bucket_name,
            origin=random_origin,
            method=http_method,
        )
        sample.wait_for_func_value(self.POSITIVE_RESPONSE)
        # 9: Perform GET request from non origin address
        invalid_origin = self.generate_random_domain_addresses()[0]
        http_method = choice(cors_config["CORSRules"][0]["AllowedMethods"])
        sample = TimeoutSampler(
            timeout=120,
            sleep=10,
            func=self.exec_curl_command,
            endpoint=mcg_obj_session.s3_endpoint,
            bucket_name=bucket_name,
            origin=invalid_origin,
            method=http_method,
        )
        sample.wait_for_func_value(self.NEGATIVE_RESPONSE)

    @tier2
    def test_allowed_method_cors_element(
        self, mcg_obj_session, awscli_pod_session, bucket_factory
    ):
        """
        Test AllowedMethods element from CORS operation on bucket
            step #1: Create bucket and apply CORS config with one allowed HTTP method
            step #2: Perform allowd method request from allowed origin mentioned in CORS
            step #3: Perform non supported request from allowed origin mentioned in CORS
            step #4: Modify the existing CORS config and add multiple HTTP method in it(GET, POST)
            step #5: Perform GET and POST request from allowed origin
            step #6: Modify the existing CORS config and add non-supported HTTP method in it(PATCH)
            step #7: Modify the existing CORS config and add all suported HTTP method along with multiple origins in it
            step #8: Perform any request mentioned in allowed HTTP method from any supported origin mentioned on CORS
        """
        # 1: Create bucket and apply CORS config with one allowed HTTP method(GET)
        bucket_name = bucket_factory(interface="OC")[0].name
        cors_config = self.create_custom_cors_config(no_of_config=1, method_num=1)
        self.apply_cors_on_bucket(
            cors_config, awscli_pod_session, bucket_name, mcg_obj_session
        )

        # 2: Perform allowd method request from allowed origin mentioned in CORS
        allowed_origin = choice(cors_config["CORSRules"][0]["AllowedOrigins"])
        http_method = choice(cors_config["CORSRules"][0]["AllowedMethods"])
        sample = TimeoutSampler(
            timeout=120,
            sleep=10,
            func=self.exec_curl_command,
            endpoint=mcg_obj_session.s3_endpoint,
            bucket_name=bucket_name,
            origin=allowed_origin,
            method=http_method,
        )
        sample.wait_for_func_value(self.POSITIVE_RESPONSE)

        # 3: Perform non supported request from allowed origin mentioned in CORS
        non_supported_method = choice(self.allowed_methods_list)
        while True:
            if non_supported_method != http_method:
                break
            non_supported_method = choice(self.allowed_methods_list)
        sample = TimeoutSampler(
            timeout=120,
            sleep=10,
            func=self.exec_curl_command,
            endpoint=mcg_obj_session.s3_endpoint,
            bucket_name=bucket_name,
            origin=allowed_origin,
            method=non_supported_method,
        )
        sample.wait_for_func_value(self.NEGATIVE_RESPONSE)

        # 4: Modify the existing CORS config and add multiple HTTP method in it
        cors_config["CORSRules"][0]["AllowedMethods"].append(non_supported_method)
        self.apply_cors_on_bucket(
            cors_config, awscli_pod_session, bucket_name, mcg_obj_session
        )

        # 5: Perform multiple ops from allowed method request from allowed origin
        for i in cors_config["CORSRules"][0]["AllowedMethods"]:
            sample = TimeoutSampler(
                timeout=120,
                sleep=10,
                func=self.exec_curl_command,
                endpoint=mcg_obj_session.s3_endpoint,
                bucket_name=bucket_name,
                origin=allowed_origin,
                method=i,
            )
            sample.wait_for_func_value(self.POSITIVE_RESPONSE)

        # 6: Modify the existing CORS config and add non-supported HTTP method in it(PATCH)
        cors_config["CORSRules"][0]["AllowedMethods"].append("PATCH")
        try:
            resp = self.apply_cors_on_bucket(
                cors_config,
                awscli_pod_session,
                bucket_name,
                mcg_obj_session,
                response=True,
            )
            assert resp is None, "CORS is applying invalid HTTP method(PATCH)"
        except Exception as e:
            if "InvalidRequest" in str(e):
                logger.warning("Expected error")
                logger.warning(e)
            else:
                raise

        # 7: Modify the existing CORS config and add all suported HTTP method along with multiple origins in it
        cors_config["CORSRules"][0]["AllowedMethods"] = []
        for val in self.allowed_methods_list:
            cors_config["CORSRules"][0]["AllowedMethods"].append(val)
        cors_config["CORSRules"][0]["AllowedOrigins"] = ["*"]
        self.apply_cors_on_bucket(
            cors_config, awscli_pod_session, bucket_name, mcg_obj_session
        )

        # 8: Perform any request mentioned in allowed HTTP method from any supported origin mentioned on CORS
        for i in range(3):
            method = choice(self.allowed_methods_list)
            origin = self.generate_random_domain_addresses()[0]
            sample = TimeoutSampler(
                timeout=120,
                sleep=10,
                func=self.exec_curl_command,
                endpoint=mcg_obj_session.s3_endpoint,
                bucket_name=bucket_name,
                origin=origin,
                method=method,
            )
            sample.wait_for_func_value(self.POSITIVE_RESPONSE)

    @tier1
    def test_allowed_header_cors_element(
        self, mcg_obj_session, awscli_pod_session, bucket_factory
    ):
        """
        Test AllowedHeaders element from CORS operation on bucket
            step #1: Create bucket and apply CORS config with one allowed HTTP header(x-custom-header)
            step #2: Perform allowed header request from allowed origin mentioned in CORS
            step #3: Perform non supported request from allowed origin mentioned in CORS
            step #4: Modify the existing CORS config and add multiple HTTP hraders in it
                    (x-custom-header, x-other-header)
            step #5: Perform allowed header request from allowed origin
        """
        # 1: Create bucket and apply CORS config with one allowed HTTP header(Content-Type)
        bucket_name = bucket_factory(interface="OC")[0].name
        cors_config = self.create_custom_cors_config(
            method_num=1, exp_header=1, origin_num=1
        )
        cors_config["CORSRules"][0]["AllowedHeaders"] = ["x-custom-header"]
        self.apply_cors_on_bucket(
            cors_config, awscli_pod_session, bucket_name, mcg_obj_session
        )

        # 2: Perform allowed header request from allowed origin mentioned in CORS
        allowed_origin = choice(cors_config["CORSRules"][0]["AllowedOrigins"])
        http_method = choice(cors_config["CORSRules"][0]["AllowedMethods"])
        extra_header = {"Access-Control-Request-Headers": "X-Custom-Header"}
        sample = TimeoutSampler(
            timeout=120,
            sleep=10,
            func=self.exec_curl_command,
            endpoint=mcg_obj_session.s3_endpoint,
            bucket_name=bucket_name,
            origin=allowed_origin,
            method=http_method,
            **extra_header,
        )
        sample.wait_for_func_value(self.POSITIVE_RESPONSE)

        # 3: Perform non supported request from allowed origin mentioned in CORS
        extra_header = {"Access-Control-Request-Headers": "x-other-header"}
        sample = TimeoutSampler(
            timeout=120,
            sleep=10,
            func=self.exec_curl_command,
            endpoint=mcg_obj_session.s3_endpoint,
            bucket_name=bucket_name,
            origin=allowed_origin,
            method=http_method,
            **extra_header,
        )
        sample.wait_for_func_value(self.NEGATIVE_RESPONSE)

        # 4: Modify the existing CORS config and add multiple HTTP hraders in it(Content-Type, Content-MD5)
        cors_config["CORSRules"][0]["AllowedHeaders"].append("x-other-header")
        self.apply_cors_on_bucket(
            cors_config, awscli_pod_session, bucket_name, mcg_obj_session
        )

        # 5: Perform allowed header request from allowed origin
        sample = TimeoutSampler(
            timeout=120,
            sleep=10,
            func=self.exec_curl_command,
            endpoint=mcg_obj_session.s3_endpoint,
            bucket_name=bucket_name,
            origin=allowed_origin,
            method=http_method,
            **extra_header,
        )
        sample.wait_for_func_value(self.POSITIVE_RESPONSE)

    @tier2
    def test_expose_header_cors_element(
        self, mcg_obj_session, awscli_pod_session, bucket_factory
    ):
        """
        Test ExposeHeader element from CORS operation on bucket
            step #1: Create bucket and apply CORS config with only one ExposeHeader(x-amz-meta-custom-header)
            step #2: Perform GET request from allowed origin mentioned in CORS and validate exposed header is present
            step #3: Modify the existing CORS config and add multiple ExposeHeaders in it
                    (x-amz-meta-custom-header, x-amz-request-id)
            step #4: Perform GET request from allowed origin mentioned in CORS and validate exposed header is present
        """
        # 1: Create bucket and apply CORS config with only one ExposeHeader(x-amz-meta-custom-header)
        bucket_name = bucket_factory(interface="OC")[0].name
        cors_config = self.create_custom_cors_config(
            method_num=1, exp_header=1, origin_num=1
        )
        cors_config["CORSRules"][0]["ExposeHeaders"] = ["x-amz-meta-custom-header"]
        self.apply_cors_on_bucket(
            cors_config, awscli_pod_session, bucket_name, mcg_obj_session
        )

        # 2: Perform GET request from allowed origin mentioned in CORS and validate exposed header is present
        allowed_origin = choice(cors_config["CORSRules"][0]["AllowedOrigins"])
        http_method = choice(cors_config["CORSRules"][0]["AllowedMethods"])
        sleep(60)
        curl_command = (
            f"curl -k {mcg_obj_session.s3_endpoint}/{bucket_name} -X OPTIONS "
            f"-H 'Access-Control-Request-Method: {http_method}' "
            f"-H 'Origin: {allowed_origin}' "
            f"-D -"
        )
        curl_output = exec_cmd(cmd=curl_command).stdout.decode()
        logger.info(curl_output)
        for line in curl_output.strip().split("\n"):
            if "access-control-expose-headers" in line:
                op = line.split(":", 1)[1].strip()
                assert (
                    "x-amz-meta-custom-header" in op
                ), "Missing expected Expose Header element from the curl response"
                break
        else:
            assert False, "Missing Access-Control-Expose-Headers in the curl response"

        # 3: Modify the existing CORS config and add multiple ExposeHeaders in it
        cors_config["CORSRules"][0]["ExposeHeaders"].append("x-amz-request-id")
        provided_expose_header = cors_config["CORSRules"][0]["ExposeHeaders"]
        self.apply_cors_on_bucket(
            cors_config, awscli_pod_session, bucket_name, mcg_obj_session
        )
        # 4: Perform GET request from allowed origin mentioned in CORS and validate exposed headers are present
        sleep(60)
        curl_command = (
            f"curl -k {mcg_obj_session.s3_endpoint}/{bucket_name} -X OPTIONS "
            f"-H 'Access-Control-Request-Method: {http_method}' "
            f"-H 'Origin: {allowed_origin}' "
            f"-D -"
        )
        curl_output = exec_cmd(cmd=curl_command).stdout.decode()
        logger.info(curl_output)
        for line in curl_output.strip().split("\n"):
            if "access-control-expose-headers" in line:
                op = line.split(":", 1)[1].strip()
                expose_headers_list = [val.strip() for val in op.split(",")]
                assert sorted(expose_headers_list) == sorted(
                    provided_expose_header
                ), "Missing expected Expose Header element from the curl response"
                break
        else:
            assert False, "Missing Access-Control-Expose-Headers in the curl response"

    @tier4a
    def test_MaxAgeSeconds_and_AllowCredentials_element(
        self, mcg_obj_session, awscli_pod_session, bucket_factory
    ):
        """
        Test MaxAgeSeconds and AllowCredentials element from CORS operation on bucket
            On "AllowCredentials" element part, user is not allowed to set it to false
            and this parameter is invisible from user

            step #1: Create bucket and apply basic CORS config with MaxAgeSeconds element in it
            step #2: Modify MaxAgeSeconds parameter by adding 30 secs in it and validate the same
            step #3: Apply basic CORS config on bucket by adding "AllowCredentials" parameter to "False" value
        """
        # 1: Create bucket and apply basic CORS config with MaxAgeSeconds element in it
        bucket_name = bucket_factory(interface="OC")[0].name
        cors_config = self.create_custom_cors_config(no_of_config=1, method_num=1)
        self.apply_cors_on_bucket(
            cors_config, awscli_pod_session, bucket_name, mcg_obj_session
        )
        # 2: Modify MaxAgeSeconds parameter by adding 30 secs in it and validate the same
        new_sec = cors_config["CORSRules"][0].get("MaxAgeSeconds") + 30
        cors_config["CORSRules"][0]["MaxAgeSeconds"] = new_sec
        self.apply_cors_on_bucket(
            cors_config, awscli_pod_session, bucket_name, mcg_obj_session
        )
        get_bucket_cors_op = self.exec_cors_command(
            operation="get-bucket-cors",
            bucket_name=bucket_name,
            awscli_pod_session=awscli_pod_session,
            mcg_obj_session=mcg_obj_session,
            response=True,
        )
        logger.info(get_bucket_cors_op)
        get_bucket_cors_dict = json.loads(get_bucket_cors_op)
        assert (
            cors_config == get_bucket_cors_dict
        ), "There is mismatch in uploaded CORS config and received CORS config"

        # 3: Apply basic CORS config on bucket by adding "AllowCredentials" parameter to "False" value
        cors_config = self.create_custom_cors_config(no_of_config=1, method_num=1)
        cors_config["CORSRules"][0]["AllowCredentials"] = "False"
        try:
            resp = self.apply_cors_on_bucket(
                cors_config,
                awscli_pod_session,
                bucket_name,
                mcg_obj_session,
                response=True,
            )
            assert (
                resp is None
            ), "CORS is adding (non-supported) AllowCredentials element in it"
        except Exception as e:
            expected_err = 'Unknown parameter in CORSConfiguration.CORSRules[0]: "AllowCredentials"'
            if expected_err in str(e):
                logger.warning("Expected error")
                logger.info(
                    f"CORS config with AllowCredentials rule was rejected as expected: {e}"
                )
            else:
                raise

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
                marks=[skipif_fips_enabled],
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"aws": [(1, None)]},
                    },
                },
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"azure": [(1, None)]},
                    },
                },
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"rgw": [(1, None)]},
                    },
                },
                marks=[on_prem_platform_required],
            ),
            pytest.param(
                {
                    "interface": "CLI",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"gcp": [(1, None)]},
                    },
                },
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"ibmcos": [(1, None)]},
                    },
                },
                marks=[skipif_fips_enabled],
            ),
        ],
        ids=[
            "OBC-DEFAULT",
            "OBC-AWS",
            "OBC-AZURE",
            "OBC-GCP",
            "OBC-IBMCOS",
            "OBC-AWS-NSS",
            "OBC-Azure-NSS",
            "OBC-RGW-NSS",
            "OBC-GCP-NSS",
            "OBC-IBM-NSS",
        ],
    )
    @pre_upgrade
    def test_default_cors_pre_upgrade(
        self,
        request,
        bucket_factory_session,
        bucketclass_dict,
    ):
        """
        Create backingstore and namespacestore to validate CORS after upgrade
        """
        # 1: Create a bucket
        bucket_name = bucket_factory_session(
            interface="OC", bucketclass=bucketclass_dict
        )[0].name
        # Cache the bucket name for the post-upgrade test
        request.config.cache.set(request.node.callspec.id, bucket_name)

    @tier1
    @post_upgrade
    def test_default_cors_post_upgrade(
        self,
        request,
        mcg_obj_session,
        awscli_pod_session,
    ):
        """
        Verify CORS config is set to existing bucket post-upgrade
        """
        ids = [
            "OBC-DEFAULT",
            "OBC-AWS",
            "OBC-AZURE",
            "OBC-GCP",
            "OBC-IBMCOS",
            "OBC-AWS-NSS",
            "OBC-Azure-NSS",
            "OBC-RGW-NSS",
            "OBC-GCP-NSS",
            "OBC-IBM-NSS",
        ]
        for i in ids:
            # Retrieve the bucket name from the pre-upgrade test
            bucket_name = request.config.cache.get(i, None)
            # Verify the default CORS is set to buckets post-upgrade
            get_bucket_cors_op = self.exec_cors_command(
                operation="get-bucket-cors",
                bucket_name=bucket_name,
                awscli_pod_session=awscli_pod_session,
                mcg_obj_session=mcg_obj_session,
                response=True,
            )
            logger.info(get_bucket_cors_op)
