import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import black_squad, tier1, post_upgrade
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
        2. Click on first bucket
        3. Go to Permissions tab
        4. Click "Start from scratch" to activate policy editor
        5. Generate policy JSON programmatically
        6. Set the policy JSON in the code editor
        7. Apply the policy
        8. Confirm in modal

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

        buckets = bucket_ui.get_buckets_list()
        if not buckets:
            pytest.skip("No buckets available for testing")

        bucket_permissions_ui = BucketsTabPermissions()

        policy_method = getattr(bucket_permissions_ui, method_name)
        policy_method(bucket_name=None, **params)

        logger.info(f"Successfully completed {policy_name} bucket policy test")

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
