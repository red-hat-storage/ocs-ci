import logging
import time
from abc import ABC, abstractmethod

from botocore.exceptions import ClientError
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    WebDriverException,
)

from ocs_ci.ocs.ui.page_objects.confirm_dialog import ConfirmDialog
from ocs_ci.ocs.ui.page_objects.object_storage import ObjectStorage
from ocs_ci.ocs.ui.helpers_ui import format_locator

logger = logging.getLogger(__name__)


class LifecycleRuleInterface(ABC):
    """Base interface for lifecycle rule UI implementations"""

    def __init__(self, ui_instance):
        """
        Initialize with reference to UI instance

        Args:
            ui_instance: BucketLifecycleUI instance for accessing UI methods
        """
        self.ui = ui_instance

    @abstractmethod
    def apply(self, params: dict, edit_mode: bool = False) -> None:
        """
        Apply the rule with given parameters

        Args:
            params: Dictionary of parameters specific to this rule type
            edit_mode: True if editing existing rule, False if creating new
        """
        pass

    @abstractmethod
    def validate_params(self, params: dict) -> bool:
        """
        Validate parameters before applying

        Args:
            params: Dictionary of parameters to validate

        Returns:
            bool: True if parameters are valid
        """
        pass

    @abstractmethod
    def get_required_params(self) -> list:
        """
        Get list of required parameter names

        Returns:
            list: List of required parameter names
        """
        pass


class BucketLifecycleUI(ObjectStorage, ConfirmDialog):
    """
    A class for bucket lifecycle policy UI operations
    """

    def get_lifecycle_policy_from_backend(self, bucket_name, mcg_obj):
        """
        Get lifecycle policy from backend S3 API

        Args:
            bucket_name (str): Name of the bucket
            mcg_obj: MCG object with S3 client

        Returns:
            dict: The lifecycle configuration from backend
        """
        try:
            response = mcg_obj.s3_client.get_bucket_lifecycle_configuration(
                Bucket=bucket_name
            )
            logger.info(f"Retrieved lifecycle configuration: {response}")

            return response
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchLifecycleConfiguration":
                logger.info(
                    f"No lifecycle configuration found for bucket {bucket_name}"
                )
                return {"Rules": []}
            else:
                logger.error(f"Failed to get lifecycle configuration: {e}")
                return None

    def navigate_to_bucket_lifecycle(self, bucket_name):
        """
        Navigate to a specific bucket's lifecycle rules page

        Args:
            bucket_name (str): Name of the bucket
        """
        logger.info(f"Navigating to lifecycle rules for bucket: {bucket_name}")

        self.navigate_buckets_page()

        self.do_send_keys(self.generic_locators["search_resource_field"], bucket_name)

        self.do_click((f"//tr//a[contains(text(), '{bucket_name}')]", By.XPATH))

        self.do_click(self.bucket_tab["management_tab"])

        logger.info("Navigated to lifecycle rules page")

    def create_lifecycle_rule(
        self, rule_name: str, scope: str = "whole_bucket", rules: dict = None, **kwargs
    ):
        """
        Create a new lifecycle rule using the interface-based approach

        Args:
            rule_name (str): Name for the rule
            scope (str): 'whole_bucket' or 'targeted'
            rules (dict): Dictionary mapping rule types to their parameters, {"expiration": {"days": 30}}

        """
        if rules is None and "actions" in kwargs:
            rules = kwargs.pop("actions")

        if rules is None:
            rules = {}

        logger.info(f"Creating lifecycle rule: {rule_name} with rules: {rules}")

        self.do_click(self.bucket_tab["create_lifecycle_rule_button"])

        self.do_send_keys(self.bucket_tab["rule_name_input"], rule_name)

        if scope == "whole_bucket" or scope == "global":
            self.do_click(self.bucket_tab["rule_scope_global"])
        else:
            self.do_click(self.bucket_tab["rule_scope_targeted"])

            prefix = kwargs.get("prefix")
            if prefix:
                logger.info(f"Setting prefix filter: {prefix}")
                self.do_clear(self.bucket_tab["prefix_input"])
                self.do_send_keys(self.bucket_tab["prefix_input"], prefix)
                logger.info(f"Prefix filter '{prefix}' configured")

            min_size = kwargs.get("min_size")
            if min_size is not None:
                logger.info(f"Setting minimum object size: {min_size}")
                self.do_click(self.bucket_tab["min_object_size_checkbox"])
                self.do_clear(self.bucket_tab["min_object_size_input"])
                self.do_send_keys(
                    self.bucket_tab["min_object_size_input"], str(min_size)
                )

            max_size = kwargs.get("max_size")
            if max_size is not None:
                logger.info(f"Setting maximum object size: {max_size}")
                self.do_click(self.bucket_tab["max_object_size_checkbox"])
                self.do_clear(self.bucket_tab["max_object_size_input"])
                self.do_send_keys(
                    self.bucket_tab["max_object_size_input"], str(max_size)
                )

        for rule_type, params in rules.items():
            if rule_type in LIFECYCLE_RULE_REGISTRY:
                rule_class = LIFECYCLE_RULE_REGISTRY[rule_type]
                rule_instance = rule_class(self)

                if rule_instance.validate_params(params):
                    logger.info(f"Applying {rule_type} rule with params: {params}")
                    rule_instance.apply(params)
                else:
                    logger.error(f"Invalid parameters for {rule_type}: {params}")
                    raise ValueError(
                        f"Invalid parameters for {rule_type}. Required: {rule_instance.get_required_params()}"
                    )
            else:
                logger.warning(f"Unknown rule type: {rule_type}")

        self.scroll_into_view(self.bucket_tab["lifecycle_create_button"])

        self.do_click(self.bucket_tab["lifecycle_create_button"])
        time.sleep(3)

        self.do_click(self.bucket_tab["management_tab"])
        time.sleep(2)

        logger.info(f"Successfully created lifecycle rule: {rule_name}")

    def delete_lifecycle_rule(self, rule_name):
        """
        Delete a lifecycle rule

        Args:
            rule_name (str): Name of the rule to delete
        """
        logger.info(f"Deleting lifecycle rule: {rule_name}")

        try:
            kebab_locator = format_locator(
                self.bucket_tab["rule_kebab_menu"], rule_name
            )
            self.do_click(kebab_locator)

            self.do_click(self.bucket_tab["delete_rule_option"])

            self.dialog_confirm()

            logger.info(f"Successfully deleted lifecycle rule: {rule_name}")

        except (NoSuchElementException, TimeoutException, WebDriverException) as e:
            logger.error(f"Failed to delete lifecycle rule {rule_name}: {e}")
            raise
        except ImportError as e:
            logger.error(f"Failed to import format_locator: {e}")
            raise

    def edit_lifecycle_rule(self, rule_name, new_rules):
        """
        Edit an existing lifecycle rule

        Args:
            rule_name (str): Name of the rule to edit
            new_rules (dict): Dictionary of new rules to apply
        """
        logger.info(f"Editing lifecycle rule: {rule_name}")

        try:
            kebab_locator = format_locator(
                self.bucket_tab["rule_kebab_menu"], rule_name
            )
            self.do_click(kebab_locator)

            self.do_click(self.bucket_tab["edit_rule_option"])

            for rule_type, params in new_rules.items():
                if rule_type in LIFECYCLE_RULE_REGISTRY:
                    rule_class = LIFECYCLE_RULE_REGISTRY[rule_type]
                    rule_instance = rule_class(self)

                    if rule_instance.validate_params(params):
                        logger.info(
                            f"Applying updated {rule_type} rule with params: {params}"
                        )
                        rule_instance.apply(params, edit_mode=True)
                    else:
                        logger.error(f"Invalid parameters for {rule_type}: {params}")
                        raise ValueError(
                            f"Invalid parameters for {rule_type}. Required: {rule_instance.get_required_params()}"
                        )
                else:
                    logger.warning(f"Unknown rule type: {rule_type}")

            self.scroll_into_view(self.bucket_tab["lifecycle_save_button"])
            self.do_click(self.bucket_tab["lifecycle_save_button"])
            time.sleep(3)

            self.do_click(self.bucket_tab["management_tab"])
            time.sleep(2)

            logger.info(f"Successfully edited lifecycle rule: {rule_name}")

        except (NoSuchElementException, TimeoutException, WebDriverException) as e:
            logger.error(f"Failed to edit lifecycle rule {rule_name}: {e}")
            raise
        except ImportError as e:
            logger.error(f"Failed to import format_locator: {e}")
            raise
        except ValueError as e:
            logger.error(f"Invalid rule parameters: {e}")
            raise

    def get_lifecycle_rules_list(self):
        """
        Get list of all lifecycle rules for current bucket

        Returns:
            list: List of rule names
        """
        try:
            time.sleep(3)
            self.page_has_loaded()
            rule_elements = self.get_elements(self.bucket_tab["lifecycle_rules_list"])

            if not rule_elements:
                alternative_locators = [
                    "//table//tbody/tr",
                    "//table//tr[position()>1]",
                    "//div[contains(@class, 'rules')]//tr",
                    "//tbody/tr",
                ]

                for alt_locator in alternative_locators:
                    rule_elements = self.get_elements((alt_locator, By.XPATH))
                    if rule_elements:
                        logger.info(
                            f"Found rules using alternative locator: {alt_locator}"
                        )
                        break

            rule_names = []
            for rule in rule_elements:
                try:
                    name_element = None

                    try:
                        name_element = rule.find_element(
                            By.XPATH, ".//td[@data-label='Name']"
                        )
                    except NoSuchElementException:
                        pass

                    if not name_element:
                        try:
                            name_element = rule.find_element(By.XPATH, ".//td[1]")
                        except NoSuchElementException:
                            pass

                    if not name_element:
                        try:
                            name_element = rule.find_element(
                                By.XPATH,
                                ".//*[contains(text(), 'rule') or contains(text(), 'multipart')]",
                            )
                        except NoSuchElementException:
                            pass

                    if name_element and name_element.text.strip():
                        rule_names.append(name_element.text.strip())
                    else:
                        logger.warning(
                            f"Could not extract rule name from row: {rule.get_attribute('outerHTML')[:200]}"
                        )

                except (NoSuchElementException, AttributeError) as row_error:
                    logger.warning(f"Error processing rule row: {row_error}")

            logger.info(f"Found {len(rule_names)} lifecycle rules: {rule_names}")
            return rule_names

        except (NoSuchElementException, TimeoutException, WebDriverException) as e:
            logger.error(f"Failed to get lifecycle rules list: {e}")
            return []


class IncompleteMultipartRuleUI(LifecycleRuleInterface):
    """Implementation for incomplete multipart upload cleanup rule"""

    def apply(self, params: dict, edit_mode: bool = False) -> None:
        """Apply incomplete multipart upload cleanup rule"""
        # Always click the accordion to expand the section
        self.ui.do_click(self.ui.bucket_tab["incomplete_multipart_checkbox"])

        # Only click checkbox in CREATE mode - skip in edit mode as it's already enabled
        if not edit_mode:
            self.ui.do_click(self.ui.bucket_tab["incomplete_multipart_enable_checkbox"])
            time.sleep(2)  # Wait for the days input field to appear

            # Wait for the days input field to be available
            self.ui.page_has_loaded()

        days = params.get("days", 7)
        self.ui.do_clear(self.ui.bucket_tab["incomplete_multipart_days_input"])
        self.ui.do_send_keys(
            self.ui.bucket_tab["incomplete_multipart_days_input"], str(days)
        )

    def validate_params(self, params: dict) -> bool:
        """Validate parameters for incomplete multipart rule"""
        return "days" in params and isinstance(params["days"], (int, str))

    def get_required_params(self) -> list:
        """Get required parameters"""
        return ["days"]


class ExpirationRuleUI(LifecycleRuleInterface):
    """Implementation for object expiration rule"""

    def apply(self, params: dict, edit_mode: bool = False) -> None:
        """Apply object expiration rule"""
        # Always click Objects accordion to expand the section (1st click)
        self.ui.do_click(self.ui.bucket_tab["current_objects_accordion"])

        # Only click checkbox in CREATE mode (2nd click) - skip in edit mode as it's already enabled
        if not edit_mode:
            self.ui.do_click(self.ui.bucket_tab["expiration_delete_checkbox"])

        # Always update the days input value
        days = params.get("days")
        self.ui.do_clear(self.ui.bucket_tab["expiration_days_input"])
        self.ui.do_send_keys(self.ui.bucket_tab["expiration_days_input"], str(days))

    def validate_params(self, params: dict) -> bool:
        """Validate parameters for expiration rule"""
        return "days" in params and isinstance(params["days"], (int, str))

    def get_required_params(self) -> list:
        """Get required parameters"""
        return ["days"]


class NoncurrentVersionRuleUI(LifecycleRuleInterface):
    """Implementation for noncurrent version expiration rule"""

    def apply(self, params: dict, edit_mode: bool = False) -> None:
        """Apply noncurrent version expiration rule"""
        self.ui.do_click(self.ui.bucket_tab["noncurrent_objects_accordion"])

        # Only click checkbox in CREATE mode - skip in edit mode as it's already enabled
        if not edit_mode:
            self.ui.do_click(self.ui.bucket_tab["noncurrent_delete_checkbox"])

        days = params.get("days")
        self.ui.do_clear(self.ui.bucket_tab["noncurrent_days_input"])
        self.ui.do_send_keys(self.ui.bucket_tab["noncurrent_days_input"], str(days))

        if "preserve_versions" in params:
            preserve_versions = params["preserve_versions"]
            self.ui.do_clear(self.ui.bucket_tab["noncurrent_versions_input"])
            self.ui.do_send_keys(
                self.ui.bucket_tab["noncurrent_versions_input"], str(preserve_versions)
            )

    def validate_params(self, params: dict) -> bool:
        """Validate parameters for noncurrent version rule"""
        return "days" in params and isinstance(params["days"], (int, str))

    def get_required_params(self) -> list:
        """Get required parameters"""
        return ["days"]


class ExpiredDeleteMarkerRuleUI(LifecycleRuleInterface):
    """Implementation for expired delete marker cleanup rule"""

    def apply(self, params: dict, edit_mode: bool = False) -> None:
        """Apply expired delete marker cleanup rule"""
        self.ui.do_click(self.ui.bucket_tab["expired_markers_accordion"])

        # Only click checkbox in CREATE mode - skip in edit mode as it's already enabled
        if not edit_mode:
            self.ui.do_click(self.ui.bucket_tab["expired_markers_checkbox"])

    def validate_params(self, params: dict) -> bool:
        """Validate parameters for expired delete marker rule"""
        return True  # No parameters needed for this rule

    def get_required_params(self) -> list:
        """Get required parameters"""
        return []


LIFECYCLE_RULE_REGISTRY = {
    "abort_multipart": IncompleteMultipartRuleUI,
    "incomplete_multipart": IncompleteMultipartRuleUI,
    "expiration": ExpirationRuleUI,
    "noncurrent_version": NoncurrentVersionRuleUI,
    "expired_delete_markers": ExpiredDeleteMarkerRuleUI,
}
