import logging
import time
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier2,
    black_squad,
    ui,
)
from ocs_ci.ocs.ui.page_objects.bucket_lifecycle_ui import BucketLifecycleUI
from ocs_ci.ocs.ui.page_objects.buckets_tab import BucketsTab
from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.framework.logger_helper import log_step

logger = logging.getLogger(__name__)


@black_squad
class TestBucketLifecycleUI:
    """
    Test bucket lifecycle policy management via UI
    """

    created_buckets = []

    def _setup_bucket_and_navigate_to_lifecycle(self):
        """
        Helper method to create bucket via UI and navigate to lifecycle rules page.

        Returns:
            tuple: (BucketLifecycleUI, bucket_name) - UI object for lifecycle interaction and bucket name
        """
        lifecycle_ui = BucketLifecycleUI()
        bucket_ui = BucketsTab()

        bucket_ui.nav_object_storage_page()
        bucket_ui.nav_buckets_page()
        _, bucket_name = bucket_ui.create_bucket_ui_with_details("s3")
        log_step(f"Created bucket via UI: {bucket_name}")

        lifecycle_ui.do_click(lifecycle_ui.bucket_tab["management_tab"])

        self.created_buckets.append(bucket_name)
        logger.debug(
            f"Stored bucket name for later use. Total buckets: {len(self.created_buckets)}"
        )

        return lifecycle_ui, bucket_name

    def _validate_backend_rule(self, our_rule, rules_dict):
        """
        Validate that backend rule matches the expected configuration

        Args:
            our_rule (dict): Backend rule retrieved from S3 API to validate
            rules_dict (dict): Expected rule configuration dictionary
        """
        if "expiration" in rules_dict:
            assert "Expiration" in our_rule, "Expected Expiration in backend rule"
            assert our_rule["Expiration"]["Days"] == rules_dict["expiration"]["days"], (
                f"Expected expiration days {rules_dict['expiration']['days']}, "
                f"got {our_rule['Expiration']['Days']}"
            )

        if "noncurrent_version" in rules_dict:
            assert (
                "NoncurrentVersionExpiration" in our_rule
            ), "Expected NoncurrentVersionExpiration in backend rule"
            assert (
                our_rule["NoncurrentVersionExpiration"]["NoncurrentDays"]
                == rules_dict["noncurrent_version"]["days"]
            ), (
                f"Expected noncurrent days {rules_dict['noncurrent_version']['days']}, "
                f"got {our_rule['NoncurrentVersionExpiration']['NoncurrentDays']}"
            )
            if "preserve_versions" in rules_dict["noncurrent_version"]:
                assert (
                    our_rule["NoncurrentVersionExpiration"]["NewerNoncurrentVersions"]
                    == rules_dict["noncurrent_version"]["preserve_versions"]
                ), (
                    f"Expected preserve versions {rules_dict['noncurrent_version']['preserve_versions']}, "
                    f"got {our_rule['NoncurrentVersionExpiration']['NewerNoncurrentVersions']}"
                )

        if "expired_delete_markers" in rules_dict:
            assert (
                "Expiration" in our_rule
            ), "Expected Expiration section in backend rule for expired delete markers"
            assert (
                "ExpiredObjectDeleteMarker" in our_rule["Expiration"]
            ), "Expected ExpiredObjectDeleteMarker in Expiration section"
            assert (
                our_rule["Expiration"]["ExpiredObjectDeleteMarker"] is True
            ), "ExpiredObjectDeleteMarker should be True"

        if "incomplete_multipart" in rules_dict:
            assert (
                "AbortIncompleteMultipartUpload" in our_rule
            ), "Expected AbortIncompleteMultipartUpload in backend rule"
            assert (
                our_rule["AbortIncompleteMultipartUpload"]["DaysAfterInitiation"]
                == rules_dict["incomplete_multipart"]["days"]
            ), (
                f"Expected multipart days {rules_dict['incomplete_multipart']['days']}, "
                f"got {our_rule['AbortIncompleteMultipartUpload']['DaysAfterInitiation']}"
            )

        assert (
            our_rule["Status"] == "Enabled"
        ), f"Expected rule to be enabled, got {our_rule['Status']}"

    @ui
    @tier2
    @pytest.mark.parametrize(
        "rules_dict,description",
        [
            pytest.param(
                {"expiration": {"days": 30}},
                "single_expiration",
                marks=[pytest.mark.polarion_id("OCS-7394")],
                id="single_expiration",
            ),
            pytest.param(
                {"noncurrent_version": {"days": 7, "preserve_versions": 2}},
                "single_noncurrent",
                marks=[pytest.mark.polarion_id("OCS-7395")],
                id="single_noncurrent",
            ),
            pytest.param(
                {"expired_delete_markers": {}},
                "single_expired_markers",
                marks=[pytest.mark.polarion_id("OCS-7396")],
                id="single_expired_markers",
            ),
            pytest.param(
                {"incomplete_multipart": {"days": 5}},
                "single_multipart",
                marks=[pytest.mark.polarion_id("OCS-7397")],
                id="single_multipart",
            ),
        ],
    )
    def test_create_lifecycle_rule_with_multiple_actions(
        self, setup_ui_class_factory, mcg_obj, rules_dict, description
    ):
        """
        Test creation of lifecycle rule with multiple actions using new interface

        Args:
            setup_ui_class_factory: Pytest fixture for UI setup
            mcg_obj: MCG object fixture for backend validation
            rules_dict: Dictionary of rules to apply to the lifecycle policy
            description: Description of the rule combination being tested

        Steps:
        1. Create a bucket via UI
        2. Navigate to lifecycle rules page
        3. Create rule with multiple actions based on parameters
        4. Verify rule appears in UI list
        5. Verify the rule was processed as expected by the backend
        """
        setup_ui_class_factory()
        lifecycle_ui, bucket_name = self._setup_bucket_and_navigate_to_lifecycle()

        rule_name = create_unique_resource_name("rule", description)
        lifecycle_ui.create_lifecycle_rule(
            rule_name=rule_name,
            scope="whole_bucket",
            rules=rules_dict,
        )

        rules = lifecycle_ui.get_lifecycle_rules_list()
        assert rule_name in rules, f"Rule {rule_name} not found in rules list"
        log_step(f"Verify rule '{rule_name}' created with combination: {description}")

        backend_policy = lifecycle_ui.get_lifecycle_policy_from_backend(
            bucket_name, mcg_obj
        )
        assert backend_policy, "Failed to retrieve lifecycle policy from backend"

        backend_rules = backend_policy.get("Rules", [])
        our_rule = next((r for r in backend_rules if r["ID"] == rule_name), None)
        assert our_rule, f"Rule {rule_name} not found in backend policy"

        self._validate_backend_rule(our_rule, rules_dict)

        log_step(f"Validate rule '{rule_name}' in backend")

    @ui
    @tier2
    @pytest.mark.parametrize(
        "rules_dict,description,target_prefix",
        [
            pytest.param(
                {"expiration": {"days": 30}},
                "targeted_expiration",
                "logs",
                marks=[pytest.mark.polarion_id("OCS-7398")],
                id="targeted_expiration",
            ),
            pytest.param(
                {"noncurrent_version": {"days": 7, "preserve_versions": 2}},
                "targeted_noncurrent",
                "temp",
                marks=[pytest.mark.polarion_id("OCS-7399")],
                id="targeted_noncurrent",
            ),
            pytest.param(
                {"expired_delete_markers": {}},
                "targeted_expired_markers",
                "data",
                marks=[pytest.mark.polarion_id("OCS-7400")],
                id="targeted_expired_markers",
            ),
            pytest.param(
                {"incomplete_multipart": {"days": 5}},
                "targeted_multipart",
                "backups",
                marks=[pytest.mark.polarion_id("OCS-7401")],
                id="targeted_multipart",
            ),
        ],
    )
    def test_create_targeted_lifecycle_rule_with_multiple_actions(
        self, setup_ui_class_factory, mcg_obj, rules_dict, description, target_prefix
    ):
        """
        Test creation of targeted lifecycle rule with prefix filters.

        Args:
            setup_ui_class_factory: Pytest fixture for UI setup
            mcg_obj: MCG object fixture for backend validation
            rules_dict: Dictionary of rules to apply to the lifecycle policy
            description: Description of the rule combination being tested
            target_prefix: Prefix to target for lifecycle rule (e.g., "logs", "temp")

        Steps:
        1. Create a bucket via UI
        2. Navigate to lifecycle rules page
        3. Create targeted rule with prefix filter based on parameters
        4. Verify rule appears in list
        5. Verify rule configuration in backend with prefix filter
        """
        setup_ui_class_factory()
        lifecycle_ui, bucket_name = self._setup_bucket_and_navigate_to_lifecycle()

        # Create targeted rule with prefix filter using new API
        rule_name = create_unique_resource_name("rule", description)
        lifecycle_ui.create_lifecycle_rule(
            rule_name=rule_name,
            scope="targeted",
            rules=rules_dict,
            prefix=target_prefix,
        )

        rules = lifecycle_ui.get_lifecycle_rules_list()
        assert rule_name in rules, f"Rule {rule_name} not found in rules list"
        log_step(
            f"Verify targeted rule '{rule_name}' created with prefix '{target_prefix}' and combination: {description}"
        )

        backend_policy = lifecycle_ui.get_lifecycle_policy_from_backend(
            bucket_name, mcg_obj
        )
        assert backend_policy, "Failed to retrieve lifecycle policy from backend"

        backend_rules = backend_policy.get("Rules", [])
        our_rule = next((r for r in backend_rules if r["ID"] == rule_name), None)
        assert our_rule, f"Rule {rule_name} not found in backend policy"

        # Verify the rule has the correct prefix filter
        assert (
            "Filter" in our_rule
        ), "Expected Filter in backend rule for targeted policy"
        assert "Prefix" in our_rule["Filter"], "Expected Prefix in Filter section"
        assert (
            our_rule["Filter"]["Prefix"] == target_prefix
        ), f"Expected prefix {target_prefix}, got {our_rule['Filter']['Prefix']}"

        self._validate_backend_rule(our_rule, rules_dict)

        log_step(
            f"Validate targeted rule '{rule_name}' with prefix '{target_prefix}' in backend"
        )

    @ui
    @tier2
    @pytest.mark.polarion_id("OCS-6891")
    def test_edit_lifecycle_rule(self, setup_ui_class_factory, mcg_obj):
        """
        Test editing existing lifecycle rule

        Args:
            setup_ui_class_factory: Pytest fixture for UI setup
            mcg_obj: MCG object fixture for backend validation

        Steps:
        1. Create a new bucket via UI
        2. Navigate to lifecycle rules page
        3. Create a lifecycle rule with expiration 30 days
        4. Edit the rule to change expiration to 60 days
        5. Verify changes in UI list
        6. Verify changes in backend
        """
        setup_ui_class_factory()
        lifecycle_ui, bucket_name = self._setup_bucket_and_navigate_to_lifecycle()

        initial_rule_name = create_unique_resource_name("rule", "to-edit")
        initial_days = 30
        lifecycle_ui.create_lifecycle_rule(
            rule_name=initial_rule_name,
            scope="whole_bucket",
            rules={"expiration": {"days": initial_days}},
        )

        lifecycle_ui.do_click(lifecycle_ui.bucket_tab["management_tab"])
        time.sleep(2)

        rules = lifecycle_ui.get_lifecycle_rules_list()
        assert (
            initial_rule_name in rules
        ), f"Failed to create initial rule {initial_rule_name}"
        log_step(
            f"Verify initial rule '{initial_rule_name}' created with {initial_days} days"
        )

        new_days = 60
        lifecycle_ui.edit_lifecycle_rule(
            rule_name=initial_rule_name, new_rules={"expiration": {"days": new_days}}
        )

        updated_rules = lifecycle_ui.get_lifecycle_rules_list()
        assert (
            initial_rule_name in updated_rules
        ), f"Rule {initial_rule_name} not found after edit"

        backend_policy = lifecycle_ui.get_lifecycle_policy_from_backend(
            bucket_name, mcg_obj
        )
        assert backend_policy, "Failed to retrieve lifecycle policy from backend"

        backend_rules = backend_policy.get("Rules", [])
        our_rule = next(
            (r for r in backend_rules if r["ID"] == initial_rule_name), None
        )
        assert our_rule, f"Rule {initial_rule_name} not found in backend policy"

        assert "Expiration" in our_rule, "Expected Expiration in backend rule"
        assert (
            our_rule["Expiration"]["Days"] == new_days
        ), f"Expected expiration days {new_days}, got {our_rule['Expiration']['Days']}"

        log_step(
            f"Verify rule '{initial_rule_name}' edited from {initial_days} to {new_days} days"
        )

    @ui
    @tier2
    @pytest.mark.jira("DFBUGS-2960")
    @pytest.mark.polarion_id("OCS-6892")
    def test_delete_lifecycle_rule(self, setup_ui_class_factory, mcg_obj):
        """
        Test deletion of lifecycle rule

        Args:
            setup_ui_class_factory: Pytest fixture for UI setup
            mcg_obj: MCG object fixture for backend validation

        Steps:
        1. Navigate to an existing bucket's lifecycle rules page
        2. Check for existing rules, create one if none exist
        3. Delete the rule using kebab menu
        4. Verify rule is removed from UI list
        5. Verify rule is removed from backend
        """
        setup_ui_class_factory()
        lifecycle_ui = BucketLifecycleUI()

        if not self.created_buckets:
            log_step("No buckets from previous tests, creating a new one")
            lifecycle_ui, bucket_name = self._setup_bucket_and_navigate_to_lifecycle()
        else:
            # Validate list is not empty before accessing first element
            if len(self.created_buckets) == 0:
                log_step("Created buckets list is empty, creating a new bucket")
                lifecycle_ui, bucket_name = (
                    self._setup_bucket_and_navigate_to_lifecycle()
                )
            else:
                bucket_name = self.created_buckets[0]
                log_step(f"Using existing bucket from previous tests: {bucket_name}")
                lifecycle_ui.navigate_to_bucket_lifecycle(bucket_name)

        rules = lifecycle_ui.get_lifecycle_rules_list()

        if not rules:
            log_step("No existing rules found, creating a rule to delete")
            rule_to_delete = create_unique_resource_name("rule", "to-delete")
            lifecycle_ui.create_lifecycle_rule(
                rule_name=rule_to_delete,
                scope="whole_bucket",
                rules={"expiration": {"days": 30}},
            )

            lifecycle_ui.do_click(lifecycle_ui.bucket_tab["management_tab"])
            time.sleep(2)

            rules = lifecycle_ui.get_lifecycle_rules_list()
            assert rule_to_delete in rules, f"Failed to create rule {rule_to_delete}"
        else:
            rule_to_delete = rules[0]
            log_step(f"Found existing rule to delete: {rule_to_delete}")

        initial_rule_count = len(rules)
        logger.debug(f"Initial rule count: {initial_rule_count}")

        lifecycle_ui.delete_lifecycle_rule(rule_to_delete)
        time.sleep(5)  # Wait for backend processing and UI to update

        updated_rules = lifecycle_ui.get_lifecycle_rules_list()
        assert (
            rule_to_delete not in updated_rules
        ), f"Rule {rule_to_delete} still appears in UI after deletion"
        assert (
            len(updated_rules) == initial_rule_count - 1
        ), f"Expected {initial_rule_count - 1} rules after deletion, but found {len(updated_rules)}"

        backend_policy = lifecycle_ui.get_lifecycle_policy_from_backend(
            bucket_name, mcg_obj
        )
        if backend_policy and backend_policy.get("Rules"):
            backend_rule_ids = [rule["ID"] for rule in backend_policy["Rules"]]
            assert (
                rule_to_delete not in backend_rule_ids
            ), f"Rule {rule_to_delete} still exists in backend after deletion"

        log_step(f"Verify rule '{rule_to_delete}' deleted successfully")
