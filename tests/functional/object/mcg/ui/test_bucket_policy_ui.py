import json
import logging
import pytest
import time

from ocs_ci.framework.pytest_customization.marks import (
    black_squad,
    tier1,
    tier3,
    post_upgrade,
    polarion_id,
    red_squad,
    mcg,
)
from ocs_ci.ocs.ui.page_objects.buckets_tab import BucketsTab
from ocs_ci.ocs.ui.page_objects.bucket_tab_permissions import (
    PolicyConfig,
    PolicyType,
    BlockPublicAccessType,
)

logger = logging.getLogger(__name__)


@tier1
@black_squad
@red_squad
@mcg
@post_upgrade
class TestBucketPolicyUI:
    """
    Test class for bucket policy UI operations
    """

    @pytest.mark.parametrize(
        "policy_config",
        [
            pytest.param(
                {
                    "name": "AllowPublicReadAccess",
                    "method": "set_bucket_policy_ui",
                    "params": {},
                },
                marks=[tier1, polarion_id("OCS-7382")],
            ),
            pytest.param(
                {
                    "name": "AllowAccessToSpecificAccount",
                    "method": "set_bucket_policy_specific_account_ui",
                    "params": {"account_list": ["123456789012"]},
                },
                marks=[tier3, polarion_id("OCS-7383")],
            ),
            pytest.param(
                {
                    "name": "EnforceSecureTransportHTTPS",
                    "method": "set_bucket_policy_enforce_https_ui",
                    "params": {},
                },
                marks=[tier3, polarion_id("OCS-7384")],
            ),
            pytest.param(
                {
                    "name": "AllowReadWriteAccessToFolder",
                    "method": "set_bucket_policy_folder_access_ui",
                    "params": {
                        "folder_path": "documents",
                        "account_list": ["123456789012"],
                    },
                },
                marks=[tier3, polarion_id("OCS-7385")],
            ),
        ],
    )
    def test_set_bucket_policy_ui(self, setup_ui_class_factory, policy_config):
        """
        Test setting various bucket policies via UI.

        Args:
            policy_config (dict): Configuration containing policy name, method, and parameters

        Raises:
            pytest.skip: If no buckets are available for testing
        """
        setup_ui_class_factory()
        policy_name = policy_config["name"]
        method_name = policy_config["method"]
        params = policy_config["params"]

        logger.info(f"Starting test to set {policy_name} bucket policy")

        bucket_ui = BucketsTab()
        bucket_ui.navigate_buckets_page()

        account_dependent_policies = [
            "AllowAccessToSpecificAccount",
            "AllowReadWriteAccessToFolder",
        ]

        target_bucket_name = None

        if policy_name in account_dependent_policies:
            initial_buckets = set(bucket_ui.get_buckets_list())

            bucket_ui.create_bucket_ui(method="obc")

            bucket_ui.navigate_buckets_page()
            bucket_ui.page_has_loaded(sleep_time=2)

            for attempt in range(6):
                current_buckets = set(bucket_ui.get_buckets_list())
                new_buckets = current_buckets - initial_buckets

                obc_buckets = [
                    b for b in new_buckets if b.startswith("test-bucket-obc-")
                ]

                if obc_buckets:
                    target_bucket_name = obc_buckets[0]
                    break

                if attempt < 5:
                    time.sleep(5)

            if not target_bucket_name:
                all_buckets = bucket_ui.get_buckets_list()
                obc_buckets = [
                    b for b in all_buckets if b.startswith("test-bucket-obc-")
                ]

                if obc_buckets:
                    target_bucket_name = obc_buckets[0]
                else:
                    pytest.skip("No OBC bucket found for account-dependent policy test")
        else:
            _, target_bucket_name = bucket_ui.create_bucket_ui("s3", return_name=True)

            bucket_ui.navigate_buckets_page()
            bucket_ui.page_has_loaded(sleep_time=2)

        bucket_permissions_ui = bucket_ui.navigate_to_bucket_permissions(
            bucket_name=target_bucket_name
        )

        policy_method = getattr(bucket_permissions_ui, method_name)
        policy_method(bucket_name=target_bucket_name, **params)

        logger.info(f"Successfully completed {policy_name} bucket policy test")

    @polarion_id("OCS-6893")
    def test_delete_bucket_policy_ui(self, setup_ui_class_factory):
        """
        Test deleting bucket policy via UI.
        """
        setup_ui_class_factory()
        logger.info("Starting test to delete bucket policy")

        bucket_ui = BucketsTab()
        bucket_ui.navigate_buckets_page()

        _, bucket_name = bucket_ui.create_bucket_ui("s3", return_name=True)

        bucket_ui.navigate_buckets_page()
        bucket_ui.page_has_loaded(sleep_time=2)

        bucket_permissions_ui = bucket_ui.navigate_to_bucket_permissions(
            bucket_name=bucket_name
        )
        bucket_permissions_ui.set_bucket_policy_ui(bucket_name=bucket_name)

        bucket_ui = bucket_permissions_ui.navigate_back_to_buckets_list()
        bucket_ui.page_has_loaded(sleep_time=2)

        bucket_permissions_ui = bucket_ui.navigate_to_bucket_permissions(
            bucket_name=bucket_name
        )
        bucket_permissions_ui.delete_bucket_policy_ui(bucket_name=bucket_name)

        logger.info("Successfully completed delete bucket policy test")

    @polarion_id("OCS-6894")
    @pytest.mark.parametrize("policy_name", ["AllowPublicReadAccess"])
    def test_bucket_policy_workflow_steps(self, setup_ui_class_factory, policy_name):
        """
        Test individual steps of bucket policy workflow.

        Args:
            policy_name (str): Name of the policy to test

        Raises:
            pytest.skip: If no buckets are available for testing
        """
        setup_ui_class_factory()
        logger.info(f"Starting step-by-step test for policy: {policy_name}")

        bucket_ui = BucketsTab()
        bucket_ui.navigate_buckets_page()

        _, target_bucket_name = bucket_ui.create_bucket_ui("s3", return_name=True)

        bucket_ui.navigate_buckets_page()
        bucket_ui.page_has_loaded(sleep_time=2)

        bucket_permissions_ui = bucket_ui.navigate_to_bucket_permissions(
            bucket_name=target_bucket_name
        )

        bucket_permissions_ui.activate_policy_editor()

        config = PolicyConfig(target_bucket_name)
        policy_json = bucket_permissions_ui._build_bucket_policy(
            PolicyType.ALLOW_PUBLIC_READ, config
        )
        bucket_permissions_ui.set_policy_json_in_editor(policy_json)

        bucket_permissions_ui.apply_bucket_policy()

        logger.info(f"Successfully completed step-by-step test for {policy_name}")

    def test_bucket_public_access_with_policy(self, setup_ui_class_factory):
        """
        Tests the correct work of 'Block public access' tab
        The workflow is as following:
        1. Create a new bucket
        2. Create a new bucket policy with 'Allow All Access'
        3. Test the correct behaviour of 'Block public access' tab
        4. Delete the created bucket

        """

        setup_ui_class_factory()
        logger.info("Starting test ")

        bucket_ui = BucketsTab()
        bucket_ui.navigate_buckets_page()

        # Create the bucket
        _, target_bucket_name = bucket_ui.create_bucket_ui("s3", return_name=True)
        logger.info(f"Created bucket name: {target_bucket_name}")
        bucket_ui.navigate_buckets_page()
        bucket_ui.page_has_loaded(sleep_time=2)

        bucket_permissions_ui = bucket_ui.navigate_to_bucket_permissions(
            bucket_name=target_bucket_name
        )

        bucket_permissions_ui.activate_policy_editor()

        # Create, modify and set bucket policy
        config = PolicyConfig(target_bucket_name)
        policy_json = bucket_permissions_ui._build_bucket_policy(
            PolicyType.ALLOW_PUBLIC_READ, config
        )
        policy_dict = json.loads(policy_json)
        policy_dict["Statement"][0]["Action"][0] = "s3:*"

        policy_json = json.dumps(policy_dict)
        logger.info(f"Set policy json: {policy_json}")
        bucket_permissions_ui.set_policy_json_in_editor(policy_json)

        bucket_permissions_ui.apply_bucket_policy()

        # Test the Block public access tab
        bucket_permissions_ui.navigate_to_block_public_access_tab()
        bucket_permissions_ui.verify_block_public_access(
            BlockPublicAccessType.BLOCK_ALL
        )
        bucket_permissions_ui.verify_block_public_access(
            BlockPublicAccessType.BLOCK_NEW_POLICIES
        )
        bucket_permissions_ui.verify_block_public_access(
            BlockPublicAccessType.BLOCK_CROSS_ACCOUNT
        )

        bucket_ui.delete_bucket_ui(
            delete_via="three_dots", expect_fail=False, resource_name=target_bucket_name
        )

        logger.info("Successfully completed test")
