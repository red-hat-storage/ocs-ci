import logging
import pytest
import time

from ocs_ci.framework.pytest_customization.marks import (
    black_squad,
    tier1,
    post_upgrade,
    polarion_id,
)
from ocs_ci.ocs.ui.page_objects.buckets_tab import BucketsTab
from ocs_ci.ocs.ui.page_objects.bucket_tab_permissions import (
    BucketsTabPermissions,
    PolicyConfig,
    PolicyType,
)

logger = logging.getLogger(__name__)


@tier1
@black_squad
@post_upgrade
class TestBucketPolicyUI:
    """
    Test class for bucket policy UI operations
    """

    @polarion_id("OCS-6889")
    @pytest.mark.parametrize(
        "policy_config",
        [
            {
                "name": "AllowPublicReadAccess",
                "method": "set_bucket_policy_ui",
                "params": {},
            },
            {
                "name": "AllowAccessToSpecificAccount",
                "method": "set_bucket_policy_specific_account_ui",
                "params": {"account_list": ["123456789012"]},
            },
            {
                "name": "EnforceSecureTransportHTTPS",
                "method": "set_bucket_policy_enforce_https_ui",
                "params": {},
            },
            {
                "name": "AllowReadWriteAccessToFolder",
                "method": "set_bucket_policy_folder_access_ui",
                "params": {
                    "folder_path": "documents",
                    "account_list": ["123456789012"],
                },
            },
        ],
    )
    def test_set_bucket_policy_ui(self, setup_ui_class_factory, policy_config):
        """
        Test setting various bucket policies via UI.

        This test follows the workflow:
        1. Navigate to Object Storage
        2. Create OBC bucket if needed for account-specific policies
        3. Click on appropriate bucket
        4. Go to Permissions tab
        5. Click "Start from scratch" to activate policy editor
        6. Generate policy JSON programmatically
        7. Set the policy JSON in the code editor
        8. Apply the policy
        9. Confirm in modal

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

        # Check if this policy requires account ID (OBC bucket)
        account_dependent_policies = [
            "AllowAccessToSpecificAccount",
            "AllowReadWriteAccessToFolder",
        ]

        target_bucket_name = None

        if policy_name in account_dependent_policies:
            # Get current bucket list to identify new bucket after creation
            initial_buckets = set(bucket_ui.get_buckets_list())

            # Create OBC bucket for account-dependent policies
            bucket_ui.create_bucket_ui(method="obc")

            # Navigate back to buckets page and wait for new bucket to appear
            bucket_ui.navigate_buckets_page()
            bucket_ui.page_has_loaded(sleep_time=2)

            # Wait for new OBC bucket to appear (up to 30 seconds)
            target_bucket_name = None
            for attempt in range(6):  # 6 attempts * 5 seconds = 30 seconds max wait
                current_buckets = set(bucket_ui.get_buckets_list())
                new_buckets = current_buckets - initial_buckets

                # Look for newly created bucket with OBC pattern
                obc_buckets = [
                    b for b in new_buckets if b.startswith("test-bucket-obc-")
                ]

                if obc_buckets:
                    target_bucket_name = obc_buckets[0]  # Use first OBC bucket found
                    break

                if attempt < 5:  # Don't sleep on last attempt
                    time.sleep(5)

            if not target_bucket_name:
                # Fallback: try to find any bucket with OBC pattern in the current list
                all_buckets = bucket_ui.get_buckets_list()
                obc_buckets = [
                    b for b in all_buckets if b.startswith("test-bucket-obc-")
                ]

                if obc_buckets:
                    target_bucket_name = obc_buckets[0]
                else:
                    pytest.skip("No OBC bucket found for account-dependent policy test")
        else:
            # For policies that don't require account ID, use any existing bucket
            buckets = bucket_ui.get_buckets_list()
            if not buckets:
                pytest.skip("No buckets available for testing")

            target_bucket_name = buckets[0]

        bucket_permissions_ui = BucketsTabPermissions()

        policy_method = getattr(bucket_permissions_ui, method_name)
        policy_method(bucket_name=target_bucket_name, **params)

        logger.info(f"Successfully completed {policy_name} bucket policy test")

    @polarion_id("OCS-6893")
    def test_delete_bucket_policy_ui(self, setup_ui_class_factory):
        """
        Test deleting bucket policy via UI.

        This test uses the unified delete workflow that:
        1. Navigates to Object Storage and bucket permissions
        2. Verifies a policy exists before attempting deletion
        3. Activates policy editor and deletes the policy
        4. Handles confirmation dialog automatically

        Note: This test assumes a policy already exists from previous tests.

        Raises:
            pytest.skip: If no buckets are available for testing
        """
        setup_ui_class_factory()
        logger.info("Starting test to delete bucket policy")

        bucket_ui = BucketsTab()
        bucket_ui.navigate_buckets_page()

        buckets = bucket_ui.get_buckets_list()
        if not buckets:
            pytest.skip("No buckets available for testing")

        bucket_permissions_ui = BucketsTabPermissions()

        # Use unified delete workflow
        bucket_permissions_ui.delete_bucket_policy_ui(bucket_name=None)

        logger.info("Successfully completed delete bucket policy test")

    @polarion_id("OCS-6894")
    @pytest.mark.parametrize("policy_name", ["AllowPublicReadAccess"])
    def test_bucket_policy_workflow_steps(self, setup_ui_class_factory, policy_name):
        """
        Test individual steps of bucket policy workflow.

        This test breaks down the workflow into individual steps
        to verify each component works correctly.

        Args:
            policy_name (str): Name of the policy to test

        Raises:
            pytest.skip: If no buckets are available for testing
        """
        setup_ui_class_factory()
        logger.info(f"Starting step-by-step test for policy: {policy_name}")

        bucket_ui = BucketsTab()
        bucket_ui.navigate_buckets_page()

        buckets = bucket_ui.get_buckets_list()
        if not buckets:
            pytest.skip("No buckets available for testing")

        bucket_permissions_ui = BucketsTabPermissions()

        # Step 1: Navigate to bucket permissions
        bucket_permissions_ui.navigate_to_bucket_permissions(bucket_name=None)
        logger.info("✓ Step 1: Navigated to bucket permissions")

        # Step 2: Activate policy editor
        bucket_permissions_ui.activate_policy_editor()
        logger.info("✓ Step 2: Activated policy editor")

        # Step 3: Generate and set policy JSON manually for step-by-step verification
        config = PolicyConfig(buckets[0])
        policy_json = bucket_permissions_ui._build_bucket_policy(
            PolicyType.ALLOW_PUBLIC_READ, config
        )
        bucket_permissions_ui.set_policy_json_in_editor(policy_json)
        logger.info(f"✓ Step 3: Generated and set policy {policy_name}")

        # Step 4: Apply bucket policy
        bucket_permissions_ui.apply_bucket_policy()
        logger.info("✓ Step 4: Applied bucket policy")

        logger.info(f"Successfully completed step-by-step test for {policy_name}")
