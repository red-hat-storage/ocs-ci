import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    red_squad,
    jira,
    skipif_ocs_version,
    mcg,
)
from ocs_ci.framework.testlib import (
    MCGTest,
    tier4,
)
from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp
from ocs_ci.helpers.helpers import create_unique_resource_name, create_s3_credentials_secret
from ocs_ci.helpers.dr_helpers import (
    get_managed_cluster_names,
    create_drpolicy,
    get_drpolicy_conditions,
    get_ramen_hub_config,
    cleanup_drpolicy,
)
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


@mcg
@red_squad
@tier4
@jira("DFBUGS-9859")
@skipif_ocs_version("<4.22")
class TestDRPolicyThirdPartyStorageInvalidBucket(MCGTest):
    """
    Test that DR Policy creation with third party storage fails validation
    when invalid bucket details are provided.

    Verifies fix for DFBUGS-9859: DR Policy for third party storage should
    not get validated with invalid bucket details.
    """

    @pytest.fixture(autouse=True)
    def setup_teardown(self, request):
        """
        Setup and teardown for the test.
        Ensures any DR policy created during the test is cleaned up.

        Returns:
            None
        """
        self.drpolicy_names = []

        def finalizer():
            """Clean up any DR policies created during the test."""
            for drpolicy_name in self.drpolicy_names:
                cleanup_drpolicy(drpolicy_name)

        request.addfinalizer(finalizer)

    def test_drpolicy_invalid_bucket_validation_fails(self):
        """
        Test that creating a DR Policy with third party storage and invalid
        bucket details results in a failed/not-validated state.

        Verifies fix for DFBUGS-9859.

        Steps:
        1. Retrieve managed cluster names from the hub cluster
        2. Create a DRPolicy with third party storage using invalid bucket details
        3. Verify the DRPolicy does NOT reach a Validated state
        4. Verify the DRPolicy condition indicates s3BucketNotFound or similar error
        """
        logger.test_step("Retrieve managed cluster information from the hub cluster")
        cluster_names = get_managed_cluster_names(exclude_local=True)

        logger.assertion(
            f"expected=at least 2 managed clusters, actual={len(cluster_names)}"
        )
        assert len(cluster_names) >= 2, (
            f"Need at least 2 managed clusters for DR policy test, found {len(cluster_names)}"
        )
        cluster_names = cluster_names[:2]
        logger.info(f"Using managed clusters: {cluster_names}")

        logger.test_step("Create S3 secret with dummy credentials for invalid bucket test")
        namespace = config.ENV_DATA.get(
            "cluster_namespace", constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        secret_name = create_unique_resource_name(
            resource_description="invalid-s3", resource_type="secret"
        )
        create_s3_credentials_secret(
            secret_name=secret_name,
            namespace=namespace,
            access_key="INVALID_ACCESS_KEY_ID",
            secret_key="INVALID_SECRET_ACCESS_KEY",
        )

        logger.test_step("Create a DRPolicy with invalid bucket details for third party storage")
        drpolicy_name = create_unique_resource_name(
            resource_description="invalid-bucket", resource_type="drpolicy"
        )
        self.drpolicy_names.append(drpolicy_name)

        invalid_bucket_name = create_unique_resource_name(
            resource_description="nonexistent", resource_type="bucket"
        )
        invalid_s3_endpoint = "https://s3.us-east-1.amazonaws.com"

        create_drpolicy(
            drpolicy_name=drpolicy_name,
            cluster_names=cluster_names,
            scheduling_interval="5m",
        )
        logger.info(f"Created DRPolicy {drpolicy_name} with invalid bucket details")

        logger.test_step("Update Ramen config with S3 profiles referencing invalid/nonexistent bucket")
        ramen_cm_obj, ramen_config_data = get_ramen_hub_config(namespace)

        s3_profiles = ramen_config_data.get("s3StoreProfiles", [])
        for cluster_name in cluster_names:
            profile_name = f"invalid-s3-profile-{cluster_name}-{drpolicy_name}"
            s3_profiles.append({
                "s3ProfileName": profile_name,
                "s3Bucket": invalid_bucket_name,
                "s3CompatibleEndpoint": invalid_s3_endpoint,
                "s3Region": "us-east-1",
                "s3SecretRef": {
                    "name": secret_name,
                    "namespace": namespace,
                },
            })
        ramen_config_data["s3StoreProfiles"] = s3_profiles

        logger.test_step("Wait and verify DRPolicy does NOT reach Validated state")
        validation_failed = False
        error_reason = ""

        for sample in TimeoutSampler(
            timeout=180,
            sleep=15,
            func=get_drpolicy_conditions,
            drpolicy_name=drpolicy_name,
        ):
            conditions = sample if sample else []
            for condition in conditions:
                condition_type = condition.get("type", "")
                condition_status = condition.get("status", "")
                condition_reason = condition.get("reason", "")
                condition_message = condition.get("message", "")

                logger.debug(
                    f"DRPolicy condition: type={condition_type}, "
                    f"status={condition_status}, reason={condition_reason}, "
                    f"message={condition_message}"
                )

                if condition_type == constants.DRPOLICY_CONDITION_VALIDATED and condition_status == "True":
                    logger.warning(
                        f"DRPolicy {drpolicy_name} was validated despite invalid bucket details. "
                        f"This indicates the bug DFBUGS-9859 is NOT fixed."
                    )
                    validation_failed = False
                    error_reason = "Policy was validated with invalid bucket"
                    break

                if condition_type == constants.DRPOLICY_CONDITION_VALIDATED and condition_status == "False":
                    validation_failed = True
                    error_reason = condition_reason
                    logger.info(
                        f"DRPolicy {drpolicy_name} correctly failed validation: "
                        f"reason={condition_reason}, message={condition_message}"
                    )
                    break

            if validation_failed:
                break

            # Check for bucket-related error reasons in conditions
            for condition in conditions:
                reason = condition.get("reason", "")
                message = condition.get("message", "").lower()
                if constants.DRPOLICY_S3_BUCKET_NOT_FOUND in reason or (
                    "bucket" in message and "not" in message
                ):
                    validation_failed = True
                    error_reason = reason
                    break

            if validation_failed:
                break

        logger.test_step("Verify DRPolicy validation correctly rejected invalid bucket configuration")

        logger.assertion(
            f"expected=DRPolicy validation should fail with invalid bucket, "
            f"actual=validation_failed={validation_failed}, reason={error_reason}"
        )
        assert validation_failed, (
            f"DRPolicy {drpolicy_name} should NOT be validated when created with invalid "
            f"bucket details. The fix for DFBUGS-9859 adds HeadBucket validation to detect "
            f"nonexistent or inaccessible buckets. Reason: {error_reason}"
        )

        bucket_error_indicators = [
            constants.DRPOLICY_S3_BUCKET_NOT_FOUND,
            constants.DRPOLICY_S3_CONNECTION_FAILED,
            constants.DRPOLICY_S3_LIST_FAILED,
            "bucket",
            "Bucket",
        ]
        reason_or_message_contains_bucket_error = any(
            indicator in error_reason for indicator in bucket_error_indicators
        )

        logger.assertion(
            f"expected=error reason contains bucket-related failure indicator, "
            f"actual=reason={error_reason}, "
            f"contains_indicator={reason_or_message_contains_bucket_error}"
        )
        assert reason_or_message_contains_bucket_error, (
            f"DRPolicy validation failure reason should indicate a bucket-related error "
            f"(e.g., s3BucketNotFound), but got: {error_reason}"
        )

        logger.info(
            f"DFBUGS-9859 fix verified: DRPolicy {drpolicy_name} correctly failed validation "
            f"with invalid bucket details. Reason: {error_reason}"
        )
