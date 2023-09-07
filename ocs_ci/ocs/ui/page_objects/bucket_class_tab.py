import ipaddress
import random
import re
import string

from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.ui.base_ui import logger
from ocs_ci.ocs.ui.page_objects.data_foundation_tabs_common import (
    CreateResourceForm,
)
from ocs_ci.ocs.ui.page_objects.object_service import ObjectService
from ocs_ci.ocs.ui.page_objects.resource_list import ResourceList


class BucketClassTab(ObjectService, CreateResourceForm, ResourceList):
    def __init__(self):
        ObjectService.__init__(self)
        CreateResourceForm.__init__(self)
        self.rules = {
            constants.UI_INPUT_RULES_BUCKET_CLASS["rule1"]: self._check_3_63_char_rule,
            constants.UI_INPUT_RULES_BUCKET_CLASS[
                "rule2"
            ]: self._check_start_end_char_rule,
            constants.UI_INPUT_RULES_BUCKET_CLASS[
                "rule3"
            ]: self._check_only_lower_case_numbers_periods_hyphens_rule,
            constants.UI_INPUT_RULES_BUCKET_CLASS[
                "rule4"
            ]: self._check_no_ip_address_rule,
            constants.UI_INPUT_RULES_BUCKET_CLASS[
                "rule5"
            ]: self._check_bucketclass_name_not_used_before_rule,
        }
        self.name_input_loc = self.bucketclass["bucketclass_name"]

    def _check_3_63_char_rule(self, rule_exp) -> bool:
        """
        Check if the input text length between 3 and 63 characters only can be used.

        Args:
            rule_exp (str): the rule requested to be checked. rule_exp text should match the text from validation popup

        Returns:
            bool: True if the input text length not violated, False otherwise.
        """
        logger.info(f"checking the input rule '{rule_exp}'")
        numbers = re.findall(r"\d+", rule_exp)
        min_len, max_len = map(int, numbers)
        params_list = [
            (rule_exp, min_len - 1, self.status_error),
            (rule_exp, min_len, self.status_success),
            (rule_exp, min_len + 1, self.status_success),
            (rule_exp, max_len - 1, self.status_success),
            (rule_exp, max_len, self.status_success),
            (rule_exp, max_len + 1, self.status_error),
        ]

        return all(self._check_input_text_length(*params) for params in params_list)

    def _check_no_ip_address_rule(self, rule_exp) -> bool:
        """
        Check if the input does not contain a valid IPv4 address.

        This function generates a random IPv4 address and a random string that is not an IP address.

        Args:
            rule_exp (str): the rule requested to be checked. rule_exp text should match the text from validation popup

        Returns:
            bool: True if the rule is satisfied for the random string that is not an IP address, False otherwise.
        """

        def _generate_ipv4_address_str():
            octets = [random.randint(0, 255) for _ in range(4)]
            ipv4_address_str = ".".join(map(str, octets))
            ipv4_address = ipaddress.IPv4Address(ipv4_address_str)
            return str(ipv4_address)

        random_ip = str(_generate_ipv4_address_str())
        not_ip = "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(10)
        )

        params_list = [
            (rule_exp, random_ip, self.status_error),
            (rule_exp, not_ip, self.status_success),
        ]

        return all(self._check_rule_case(*params) for params in params_list)

    def _check_bucketclass_name_not_used_before_rule(self, rule_exp) -> bool:
        """
        Checks whether the existed bucket class name allowed to use again.

        This function executes an OpenShift command to retrieve the names of all existing bucket classes
        in all namespaces.
        It then checks whether the name of the existed bucket class would be allowed to use.

        Args:
            rule_exp (str): the rule requested to be checked. rule_exp text should match the text from validation popup

        Returns:
            bool: True if the bucket class name has not been used before, False otherwise.
        """
        existing_backingstore_names = str(
            OCP().exec_oc_cmd(
                "get bucketclass --all-namespaces -o custom-columns=':metadata.name'"
            )
        )
        return self._check_resource_name_not_exists_rule(
            existing_backingstore_names, rule_exp
        )

    def nav_to_bucket_class(self, bucket_class_name: str):
        """
        Navigate to bucket class with the given name.

        Args:
            bucket_class_name (str): bucket class name

        """
        logger.info(f"Navigate to bucket class {bucket_class_name}")
        self.nav_to_resource_via_name(bucket_class_name)

    def validate_bucket_class_ready(self) -> bool:
        """
        Validate the status of default bucket class is 'Ready'

        Returns:
            bool: True if the status of default bucket class is 'Ready', False otherwise.
        """
        logger.info("Verifying the status of default bucket class is 'Ready'")
        backingstore_status = self.get_element_text(
            self.validation_loc["backingstore-status"]
        )
        is_ready = backingstore_status == "Ready"
        if not is_ready:
            logger.warning(
                f"Status of default bucket class is {backingstore_status}, not 'Ready'"
            )
        return is_ready

    def nav_bucket_class_breadcrumb(self):
        """
        Navigate to bucket class breadcrumb

        """
        logger.info("Navigate to bucket class breadcrumb")
        self.do_click(
            (self.validation_loc["bucketclass-breadcrumb"]), enable_screenshot=True
        )
