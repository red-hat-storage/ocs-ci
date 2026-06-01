"""
Tests for NooBaa denyHTTP feature (RHSTOR-8118).

Validates that setting spec.denyHTTP on the NooBaa CR disables HTTP access
to the S3 route while keeping HTTPS functional, ensuring compliance with
encrypted-only transport requirements.
"""

import logging

import boto3
import botocore.exceptions
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    mcg,
    red_squad,
    runs_on_provider,
    skipif_external_mode,
    skipif_ocs_version,
    tier2,
)
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.bucket_utils import retrieve_verification_mode
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)

RECONCILE_TIMEOUT = 180
RECONCILE_INTERVAL = 10


@mcg
@red_squad
@runs_on_provider
class TestDenyHTTP:
    """
    Test suite for the NooBaa denyHTTP feature that allows customers
    to disable HTTP access on the S3 route, forcing HTTPS-only usage.
    """

    @pytest.fixture()
    def revert_deny_http(self, request):
        """
        Teardown fixture that reverts spec.denyHTTP on the NooBaa CR
        back to false and verifies the S3 route is restored to its
        default insecureEdgeTerminationPolicy of Allow.
        """

        def finalizer():
            noobaa_obj = ocp.OCP(
                kind="noobaa",
                namespace=config.ENV_DATA["cluster_namespace"],
                resource_name=constants.NOOBAA_RESOURCE_NAME,
            )
            try:
                if noobaa_obj.data.get("spec", {}).get("denyHTTP"):
                    patch_param = (
                        '[{"op": "replace", "path": "/spec/denyHTTP", "value": false}]'
                    )
                    logger.info("Reverting denyHTTP to false on NooBaa CR")
                    noobaa_obj.patch(params=patch_param, format_type="json")
            except Exception:
                logger.warning(
                    "denyHTTP field not found on NooBaa CR, no revert needed"
                )

            nb_s3_route = ocp.OCP(
                kind=constants.ROUTE,
                namespace=config.ENV_DATA["cluster_namespace"],
                resource_name="s3",
            )
            for sample in TimeoutSampler(
                timeout=RECONCILE_TIMEOUT,
                sleep=RECONCILE_INTERVAL,
                func=self._get_insecure_policy,
                route_obj=nb_s3_route,
            ):
                if sample == "Allow":
                    logger.info(
                        "S3 route insecureEdgeTerminationPolicy reverted to Allow"
                    )
                    break

        request.addfinalizer(finalizer)

    @staticmethod
    def _get_insecure_policy(route_obj):
        """
        Reload route data and return the current insecureEdgeTerminationPolicy.

        Args:
            route_obj (OCP): OCP object for the S3 route.

        Returns:
            str: The current insecureEdgeTerminationPolicy value.
        """
        route_obj.reload_data()
        return route_obj.data["spec"]["tls"]["insecureEdgeTerminationPolicy"]

    @staticmethod
    def _get_s3_route_host():
        """
        Retrieve the hostname of the S3 route.

        Returns:
            str: The S3 route hostname.
        """
        nb_s3_route = ocp.OCP(
            kind=constants.ROUTE,
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name="s3",
        )
        return nb_s3_route.data["spec"]["host"]

    @staticmethod
    def _create_s3_client(endpoint_url, access_key_id, access_key):
        """
        Create a boto3 S3 client with the given endpoint URL and credentials.

        Args:
            endpoint_url (str): The S3 endpoint URL (http:// or https://).
            access_key_id (str): AWS access key ID.
            access_key (str): AWS secret access key.

        Returns:
            boto3.client: A boto3 S3 client.
        """
        s3_resource = boto3.resource(
            "s3",
            verify=retrieve_verification_mode(),
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=access_key,
        )
        return s3_resource.meta.client

    @staticmethod
    def _verify_s3_put_get(s3_client, bucket_name, object_key="test-deny-http-obj"):
        """
        Perform a put and get operation on a bucket to verify S3 access.

        Args:
            s3_client (boto3.client): A boto3 S3 client.
            bucket_name (str): Name of the bucket to access.
            object_key (str): Key for the test object.

        Returns:
            bool: True if put and get succeed.
        """
        test_data = "deny-http-test-data"
        s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=test_data)
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        retrieved_data = response["Body"].read().decode()
        assert (
            retrieved_data == test_data
        ), f"Data mismatch: expected '{test_data}', got '{retrieved_data}'"
        return True

    @staticmethod
    def _verify_curl_access(pod, url, should_succeed=True):
        """
        Verify HTTP/HTTPS access via curl from an in-cluster pod.

        Args:
            pod (Pod): Pod object to execute curl from.
            url (str): URL to curl.
            should_succeed (bool): Whether the request is expected to succeed.

        Returns:
            bool: True if the result matches the expectation.
        """
        cmd = f"curl -sk -o /dev/null -w '%{{http_code}}' --max-time 15 {url}"
        try:
            result = pod.exec_cmd_on_pod(
                command=cmd,
                out_yaml_format=False,
                timeout=30,
            )
            status_code = str(result).strip().strip("'")
            logger.info(f"Curl to {url} returned status code: {status_code}")
            if should_succeed:
                assert status_code not in (
                    "000",
                    "",
                ), f"Expected successful connection to {url}, got status {status_code}"
                return True
            else:
                return True
        except Exception as e:
            if should_succeed:
                raise AssertionError(
                    f"Expected successful curl to {url}, but got error: {e}"
                )
            logger.info(f"Curl to {url} failed as expected: {e}")
            return True

    @tier2
    @skipif_external_mode
    @skipif_ocs_version("<4.22")
    @pytest.mark.polarion_id("OCS-XXXX")
    def test_deny_http_noobaa(self, mcg_obj, bucket_factory, revert_deny_http):
        """
        Test the denyHTTP feature on the NooBaa CR.

        This test validates the happy path for RHSTOR-8118:

        1. Verify default state:
           - S3 route insecureEdgeTerminationPolicy is 'Allow'
           - HTTP access to a bucket succeeds
           - HTTPS access to a bucket succeeds

        2. Enable denyHTTP on NooBaa CR:
           - Patch spec.denyHTTP = true on the NooBaa CR
           - Verify S3 route insecureEdgeTerminationPolicy changes to 'None'
           - Verify HTTP access to the bucket fails
           - Verify HTTPS access to the bucket still succeeds
        """

        # --- Part 1: Verify default state (HTTP allowed) ---

        logger.info("Part 1: Verifying default state with HTTP allowed")

        nb_s3_route = ocp.OCP(
            kind=constants.ROUTE,
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name="s3",
        )
        current_policy = nb_s3_route.data["spec"]["tls"][
            "insecureEdgeTerminationPolicy"
        ]
        assert (
            current_policy == "Allow"
        ), f"Expected insecureEdgeTerminationPolicy to be 'Allow', got '{current_policy}'"
        logger.info(
            f"S3 route insecureEdgeTerminationPolicy is '{current_policy}' as expected"
        )

        bucket_name = bucket_factory()[0].name
        logger.info(f"Created test bucket: {bucket_name}")

        route_host = self._get_s3_route_host()
        http_endpoint = f"http://{route_host}:80"
        https_endpoint = mcg_obj.s3_endpoint

        logger.info(f"Testing HTTP access via {http_endpoint}")
        http_client = self._create_s3_client(
            http_endpoint, mcg_obj.access_key_id, mcg_obj.access_key
        )
        self._verify_s3_put_get(http_client, bucket_name, object_key="http-test-obj")
        logger.info("HTTP access succeeded as expected")

        logger.info(f"Testing HTTPS access via {https_endpoint}")
        https_client = self._create_s3_client(
            https_endpoint, mcg_obj.access_key_id, mcg_obj.access_key
        )
        self._verify_s3_put_get(https_client, bucket_name, object_key="https-test-obj")
        logger.info("HTTPS access succeeded as expected")

        logger.info("Verifying access via curl from NooBaa core pod")
        self._verify_curl_access(
            mcg_obj.core_pod, f"http://{route_host}:80", should_succeed=True
        )
        self._verify_curl_access(
            mcg_obj.core_pod, f"https://{route_host}:443", should_succeed=True
        )

        # --- Part 2: Enable denyHTTP and verify ---

        logger.info("Part 2: Enabling denyHTTP on the NooBaa CR")

        noobaa_obj = ocp.OCP(
            kind="noobaa",
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=constants.NOOBAA_RESOURCE_NAME,
        )
        patch_param = '[{"op": "add", "path": "/spec/denyHTTP", "value": true}]'
        noobaa_obj.patch(params=patch_param, format_type="json")
        logger.info("Patched NooBaa CR with spec.denyHTTP=true")

        logger.info("Waiting for S3 route to reconcile")
        for sample in TimeoutSampler(
            timeout=RECONCILE_TIMEOUT,
            sleep=RECONCILE_INTERVAL,
            func=self._get_insecure_policy,
            route_obj=nb_s3_route,
        ):
            if sample == "None":
                logger.info("S3 route insecureEdgeTerminationPolicy changed to 'None'")
                break

        updated_policy = nb_s3_route.data["spec"]["tls"][
            "insecureEdgeTerminationPolicy"
        ]
        assert (
            updated_policy == "None"
        ), f"Expected insecureEdgeTerminationPolicy to be 'None', got '{updated_policy}'"

        logger.info("Verifying HTTP access fails after denyHTTP is enabled")
        http_client_after = self._create_s3_client(
            http_endpoint, mcg_obj.access_key_id, mcg_obj.access_key
        )
        try:
            self._verify_s3_put_get(
                http_client_after, bucket_name, object_key="http-denied-obj"
            )
            raise AssertionError(
                "HTTP access succeeded unexpectedly after denyHTTP was enabled"
            )
        except (
            botocore.exceptions.ClientError,
            botocore.exceptions.EndpointConnectionError,
            botocore.exceptions.ConnectionClosedError,
            ConnectionError,
        ):
            logger.info("HTTP access failed as expected after denyHTTP was enabled")

        logger.info("Verifying HTTPS access still works after denyHTTP is enabled")
        https_client_after = self._create_s3_client(
            https_endpoint, mcg_obj.access_key_id, mcg_obj.access_key
        )
        self._verify_s3_put_get(
            https_client_after, bucket_name, object_key="https-after-deny-obj"
        )
        logger.info("HTTPS access succeeded as expected after denyHTTP was enabled")

        logger.info("Verifying access via curl after denyHTTP is enabled")
        self._verify_curl_access(
            mcg_obj.core_pod, f"http://{route_host}:80", should_succeed=False
        )
        self._verify_curl_access(
            mcg_obj.core_pod, f"https://{route_host}:443", should_succeed=True
        )

        logger.info("denyHTTP happy path test completed successfully")
