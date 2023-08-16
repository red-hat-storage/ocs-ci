import random
import re
import string
import time
import pytest

import pandas as pd

from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.keys import Keys
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.ui.base_ui import logger, wait_for_element_to_be_visible
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator


class CreateResourceForm(PageNavigator):
    def __init__(self):
        self.status_error = "error status"
        self.status_indeterminate = "indeterminate status"
        self.status_success = "success status"
        self.result_col = ["rule", "check_func", "check_status"]
        self.test_results = pd.DataFrame(columns=self.result_col)
        super().__init__()

    def _report_failed(self, error_text):
        """
        Reports a failed test by logging an error message,
        taking a screenshot of the page and copying the DOM.

        Args:
            error_text (str): The error message to log.
        """
        logger.error(error_text)
        self.take_screenshot()
        self.copy_dom()

    def proceed_resource_creation(self):
        """
        Method to proceed to resource creation form, when Create button is visible
        """
        self.page_has_loaded()
        wait_for_element_to_be_visible(self.generic_locators["create_resource_button"])
        self.do_click(self.generic_locators["create_resource_button"])

    def check_error_messages(self):
        """
        Performs a series of checks to verify if the error messages for the input fields
        meet the expected requirements. It clicks on the "create resource" button and verifies
        the existence of all expected rules in the input field. It then checks the error messages
        for each input field based on the expected rules and raises a failure if the actual
        error message does not match the expected message.
        Finally, it navigates back to the previous page.
        """
        self.page_has_loaded()
        self._verify_input_requirements()
        self.navigate_backward()
        logger.info("all error improvements checks done")

    def _verify_input_requirements(self):
        """
        Verify that all input requirements are met.
        """
        rules_texts_ok = self._check_all_rules_exist(
            self.generic_locators["text_input_popup_rules"]
        )
        self.test_results.loc[len(self.test_results)] = [
            None,
            self._check_all_rules_exist.__name__,
            rules_texts_ok,
        ]

        for rule, func in self.rules.items():
            res = func(rule)
            self.test_results.loc[len(self.test_results)] = [rule, func.__name__, res]

        logger.info(
            "\n"
            + self.test_results.to_markdown(
                headers="keys", index=False, tablefmt="grid"
            )
        )

        if not self.test_results[self.result_col[2]].all():
            failed_cases = self.test_results[~self.test_results[self.result_col[2]]]
            pytest.fail(
                "Error message improvements check failed\n"
                f"{failed_cases.to_markdown(headers='keys', index=False, tablefmt='grid')}"
            )

    def _check_all_rules_exist(self, input_loc: tuple):
        """
        Clicks on the input validator icon, retrieves the rules from the input location,
        and checks whether they match the list of expected rules. Returns True if they match,
        False otherwise.

        Args:
            input_loc (tuple): The locator of the input field containing the rules.

        Returns:
            bool: True if the list of rules in the input field matches the expected list,
            False otherwise.
        """
        self.do_click(self.validation_loc["input_value_validator_icon"])
        rules_elements = self.get_elements(input_loc)
        rules_texts_statuses = [rule.text for rule in rules_elements]
        rules_texts = [rule.split("\n: ")[0] for rule in rules_texts_statuses]
        if sorted(rules_texts) != sorted(self.rules.keys()):
            self._report_failed(
                f"Rules are not identical to the list of expected rules\n"
                f"Expected: {self.rules.keys()}\n"
                f"Actual: {rules_texts}"
            )
            return False
        return True

    def _check_rule_case(self, rule: str, input_text: str, status_exp: str) -> bool:
        """
        Check if a rule case passes for a given input text and expected status.

        Args:
            rule (str): The expected rule to be checked.
            input_text (str): The input text to be tested.
            status_exp (str): The expected status for the rule.

        Returns:
            bool: True if the check passed, False otherwise.
        """
        logger.info(f"check input '{input_text}', rule '{rule}'")
        try:
            self._send_input_and_update_popup(input_text)
            check_pass = self._check_input_rule_and_status(rule, status_exp)
        except TimeoutException or NoSuchElementException as e:
            logger.error(f"Got exception on check rule '{rule}'\n{e}")
            check_pass = False
        finally:
            self._remove_text_from_input()

        return check_pass

    @retry(TimeoutException)
    def _remove_text_from_input(self) -> bool:
        """
        Remove all text from a specified input element.

        Returns:
            bool: True if the input element is successfully cleared, False otherwise.
        """
        wait_for_element_to_be_visible(self.name_input_loc, 30)
        elements = self.get_elements(self.name_input_loc)
        input_el = elements[0]
        input_len = len(str(input_el.get_attribute("value")))

        # timeout in seconds will be equal to a number of symbols to be removed, but not less than 30s
        timeout = input_len if input_len > 30 else 30
        timeout = time.time() + timeout
        if len(elements):
            while len(str(input_el.get_attribute("value"))) != 0:
                if time.time() < timeout:
                    # to remove text from the input independently where the caret is use both delete and backspace
                    input_el.send_keys(Keys.BACKSPACE, Keys.DELETE)
                    time.sleep(0.05)
                else:
                    raise TimeoutException("time to clear input os out")
        else:
            logger.error("test input locator not found")
            return False
        return True

    def _check_input_text_length(
        self, rule_exp: str, text_length: int, status_expected: str
    ) -> bool:
        """
        A method that checks the length of the input text based on a rule and expected status.

        Args:
            rule_exp (str): the expected rule to be applied to the input text.
            text_length (int): the number of characters of the input text to be generated and tested.
            status_expected (str): the expected status after applying the rule on the input text.
        Returns:
            check_pass (bool): a boolean value indicating whether the input text satisfies the expected rule and status.
        """
        random_text_input = "".join(
            random.choices(string.ascii_lowercase + string.digits, k=text_length)
        )
        self._send_input_and_update_popup(random_text_input)
        logger.info(
            f"rule '{rule_exp}'. "
            f"number of characters '{text_length}'. "
            f"status verify '{status_expected}'. "
            f"input '{random_text_input}. "
            f"check starts...'"
        )
        check_pass = self._check_input_rule_and_status(rule_exp, status_expected)

        self._remove_text_from_input()
        return check_pass

    def _send_input_and_update_popup(self, text_input: str):
        """
        Sends an input to the name input field, updates the validation popup, and reloads it.

        Args:
            text_input (str): The text input to send to the name input field.
        """
        try:
            self.do_send_keys(self.name_input_loc, text_input)
        except TimeoutException:
            logger.warning(
                "failed to send text to input. repeat send keys and update validation popup"
            )
            self._remove_text_from_input()
            self.do_send_keys(self.name_input_loc, text_input)
        # reload popup to process all input, but not a part
        self.do_click(self.validation_loc["input_value_validator_icon"])
        self.do_click(self.validation_loc["input_value_validator_icon"])

    def _check_input_rule_and_status(self, rule_exp, status_expected) -> bool:
        """
        Check the input rule and status against the expected values.

        Args:
            rule_exp (str): The expected input rule.
            status_expected (str): The expected status of the input rule.

        Returns:
            bool: True if the check passes, False otherwise.
        """
        check_pass = True

        def get_rule_actual():
            time_sleep = 2
            logger.debug(f"sleep {time_sleep} get browser render new popup")
            time.sleep(time_sleep)
            for _ in range(3):
                _rules_elements = self.get_elements(
                    self.generic_locators["text_input_popup_rules"]
                )
                logger.debug(f"sleep {time_sleep} get browser render new popup")
                time.sleep(time_sleep)
                if len(_rules_elements) > 0:
                    break
            else:
                logger.error("no rules found after 3 attempts")
            return [rule.text for rule in _rules_elements if rule_exp in rule.text]

        rule_actual = get_rule_actual()

        if len(rule_actual) > 1:
            self._report_failed(f"rule duplicated -> {rule_actual}'")
            check_pass = False
        elif len(rule_actual) < 1:
            self.page_has_loaded(retries=5, sleep_time=5)
            # reload popup to process all input one more time. May not appear if input is large - automation issue
            self.do_click(self.validation_loc["input_value_validator_icon"])
            if not len(get_rule_actual()):
                self.do_click(self.validation_loc["input_value_validator_icon"])
            rule_actual = get_rule_actual()
            if len(rule_actual) < 1:
                self._report_failed(f"rule not found -> {rule_actual}'")
                check_pass = False
        status_actual = rule_actual[0].split("\n: ")[1].replace(";", "")
        if status_expected not in status_actual:
            self._report_failed(
                f"status expected '{status_expected}'. status actual '{status_actual}'. check failed"
            )
            check_pass = False
        else:
            logger.info(
                f"status expected '{status_expected}'. status actual '{status_actual}'. check passed"
            )
        return check_pass

    def _check_start_end_char_rule(self, rule_exp) -> bool:
        """
        Check that the input field follows the rule that only alphanumeric lowercase characters are allowed and
        the first and last characters of the input field are also alphanumeric lowercase characters.

        Args:
            rule_exp (str): the rule requested to be checked. rule_exp text should match the text from validation popup

        Returns:
            bool: True if all the checks pass, False otherwise.
        """
        alphanumeric_lowercase = string.ascii_lowercase + string.digits
        params_list = [
            (
                rule_exp,
                random.choice(string.ascii_uppercase),
                random.choice(string.ascii_uppercase),
                self.status_error,
            ),
            (
                rule_exp,
                random.choice(string.ascii_uppercase),
                random.choice(alphanumeric_lowercase),
                self.status_error,
            ),
            (
                rule_exp,
                random.choice(alphanumeric_lowercase),
                random.choice(string.ascii_uppercase),
                self.status_error,
            ),
            (
                rule_exp,
                random.choice(string.punctuation),
                random.choice(alphanumeric_lowercase),
                self.status_error,
            ),
            (
                rule_exp,
                random.choice(alphanumeric_lowercase),
                random.choice(string.punctuation),
                self.status_error,
            ),
            (
                rule_exp,
                random.choice(alphanumeric_lowercase),
                random.choice(alphanumeric_lowercase),
                self.status_success,
            ),
        ]

        return all(self._check_start_end_char_case(*params) for params in params_list)

    def _check_start_end_char_case(
        self, rule: str, start_letter: str, end_letter: str, status_exp: str
    ) -> bool:
        """Checks that an input string with a specific start and end character meets a given input rule.

        Args:
            rule (str): The input rule to check.
            start_letter (str): The start character for the input string.
            end_letter (str): The end character for the input string.
            status_exp (str): The expected status of the input string, either 'success' or 'error'.

        Returns:
            bool: True if the input string meets the input rule and has the expected status, False otherwise.
        """
        random_name = "".join(
            random.choices(string.ascii_lowercase + string.digits, k=8)
        )
        text_input = start_letter + random_name + end_letter
        self._send_input_and_update_popup(text_input)
        check_pass = self._check_input_rule_and_status(rule, status_exp)
        self._remove_text_from_input()
        if not check_pass:
            logger.error(f"check failed with input '{text_input}'")
        else:
            logger.info(f"check passed with input '{text_input}'")
        return check_pass

    def _check_only_lower_case_numbers_periods_hyphens_rule(self, rule_exp) -> bool:
        """
        Check if only the input text containing lowercase letters, digits, periods,
        and hyphens allowed to use.

        Args:
            rule_exp (str): the rule requested to be checked. rule_exp text should match the text from validation popup

        Returns:
            bool: indicating whether all test cases passed.
        """
        allowed_chars = string.ascii_lowercase + string.digits + "-"

        random_name = "".join(random.choices(allowed_chars, k=10))
        random_name = "a" + random_name + "z"
        name_with_consecutive_period = random_name[:4] + ".." + random_name[6:]

        uppercase_letters = "".join(random.choices(string.ascii_uppercase, k=2))
        name_with_uppercase_letters = (
            random_name[:4] + uppercase_letters + random_name[6:]
        )

        name_with_no_ascii = random_name[:4] + "æå" + random_name[6:]

        params_list = [
            (rule_exp, name_with_consecutive_period, self.status_error),
            (rule_exp, name_with_uppercase_letters, self.status_error),
            (rule_exp, name_with_no_ascii, self.status_error),
            (rule_exp, random_name, self.status_success),
        ]

        return all(self._check_rule_case(*params) for params in params_list)

    def _check_max_length_backing_store_rule(self, rule_exp):
        """
        Check if the length of the backing store name is less than or equal to the maximum allowed length.

        Args:
            rule_exp (str): the rule requested to be checked. rule_exp text should match the text from validation popup

        Returns:
            bool: True if the rule was not violated, False otherwise.
        """

        logger.info(f"checking the input rule '{rule_exp}'")
        max_length_exp = int(re.search(r"\d+(\.\d+)?", rule_exp).group())
        params_list = [
            (rule_exp, max_length_exp - 1, self.status_success),
            (rule_exp, max_length_exp, self.status_success),
            (rule_exp, max_length_exp + 1, self.status_error),
        ]

        return all(self._check_input_text_length(*params) for params in params_list)

    def _check_resource_name_not_exists_rule(
        self, existing_resource_names: str, rule_exp: str
    ) -> bool:
        """
        Checks that an existing resource name cannot be used.

        Args:
            existing_resource_names (str): A string containing a list of existing resource names.
            rule_exp (str): A string representing a rule to be checked.

        Returns:
            bool: True if not allowed to use duplicated resource name, False otherwise.
        """
        name_exist = existing_resource_names.split()[0].strip()
        index_to_replace = random.randint(0, len(name_exist) - 1)
        char_to_replace = name_exist[index_to_replace]
        random_char = random.choice(
            string.ascii_lowercase.replace(char_to_replace, "") + string.digits
        )
        name_does_not_exist = (
            name_exist[:index_to_replace]
            + random_char
            + name_exist[index_to_replace + 1 :]
        )
        params_list = [
            (rule_exp, name_exist, self.status_error),
            (rule_exp, name_does_not_exist, self.status_success),
        ]
        return all(self._check_rule_case(*params) for params in params_list)


class DataFoundationTabBar(PageNavigator):
    def __init__(self):
        super().__init__()

    def nav_storage_systems_tab(self):
        """
        Navigate to Storage Systems tab. Accessible from any Data Foundation tabs
        """
        logger.info("Navigate to Data Foundation - Storage Systems")
        self.do_click(self.validation_loc["storage_systems"], enable_screenshot=True)
        self.page_has_loaded(retries=15, sleep_time=2)

        from ocs_ci.ocs.ui.page_objects.storage_system_tab import StorageSystemTab

        return StorageSystemTab()

    def nav_overview_tab(self):
        """
        Navigate to Overview tab. Accessible from any Data Foundation tabs
        """
        logger.info("Navigate to Data Foundation - Overview")
        # pay attention Overview loc will show twice if Home Page nav extended
        self.do_click(locator=self.page_nav["overview_page"])

        from ocs_ci.ocs.ui.page_objects.overview_tab import OverviewTab

        return OverviewTab()

    # noinspection PyUnreachableCode
    def nav_topology_tab(self):
        """
        Navigate to ODF Topology tab. Accessible from any Data Foundation tabs
        """
        self.do_click(self.validation_loc["topology_tab"])
        self.page_has_loaded()

        from ocs_ci.ocs.ui.page_objects.odf_topology_tab import TopologyTab

        return TopologyTab()


class DataFoundationDefaultTab(DataFoundationTabBar):
    """
    Default Foundation default Tab: TopologyTab | OverviewTab
    """

    def __init__(self):
        DataFoundationTabBar.__init__(self)
