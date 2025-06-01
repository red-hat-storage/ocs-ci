import logging
import random
import re
import string

from ocs_ci.ocs.exceptions import IncorrectUiOptionRequested
from ocs_ci.ocs.ui.base_ui import BaseUI


logger = logging.getLogger(__name__)


class EditLabelForm(BaseUI):
    def __init__(self):
        super().__init__()

    def check_edit_labels(self, block_pool_name: str = None):
        """
        Method to validate that warning message appears when input rule is violated
           Rule (visible on warning):
               Labels must start and end with an alphanumeric character,
               can consist of lower-case letters, numbers and non-consecutive dots (.),
               and hyphens (-), forward slash (/), underscore(_) and equal to (=)

           Error (visible on metadata.label rule violated):
               Error "Invalid value: <value>: name part must consist of alphanumeric characters, '-', '_' or '.',
               and must start and end with an alphanumeric character (e.g. 'MyName', or 'my.name', or '123-abc',
               regex used for validation is <regex> for field "metadata.labels".

           Args:
               block_pool_name (str): Name of the block pool. If not provided, the value will be retrieved from the
                   instance attribute `block_pool_name` if available. If neither `block_pool_name` argument nor
                   the instance attribute is provided, an `IncorrectUIOptionRequested` exception will be raised.

           Returns:
               bool: The result of the validation.

           Raises:
               IncorrectUIOptionRequested: If `block_pool_name` argument is not provided and the instance attribute
                   `block_pool_name` is not available.
        """

        if not block_pool_name and not hasattr(self, "block_pool_name"):
            raise IncorrectUiOptionRequested(
                "function require that Blocking Pool created and block_pool_name was passed either "
                "as argument to method verify_edit_labels or to BlockPools constructor"
            )
        elif block_pool_name:
            block_pool_name = block_pool_name
        elif hasattr(self, "block_pool_name"):
            block_pool_name = random.choice(self.block_pool_name)

        def create_random_valid_label():
            alphanumeric_chars = (
                string.ascii_lowercase + string.ascii_uppercase + string.digits
            )
            valid_chars = alphanumeric_chars + "./-_"
            first_char = random.choice(alphanumeric_chars)
            middle_chars = [
                random.choice(valid_chars) for _ in range(random.randint(0, 10))
            ]
            middle_chars = re.sub(r"\.{2,}", ".", "".join(middle_chars))
            last_char = random.choice(alphanumeric_chars)
            return first_char + "".join(middle_chars) + last_char

        valid_label = create_random_valid_label() + "=" + create_random_valid_label()

        def create_random_invalid_label():
            invalid_chars = string.punctuation.translate(str.maketrans("", "", "./-_"))
            invalid_label_list = [
                valid_label + random.choice(invalid_chars),
                random.choice(invalid_chars) + valid_label,
                valid_label + ".." + create_random_valid_label(),
            ]
            return random.choice(invalid_label_list)

        invalid_label = create_random_invalid_label()

        self.open_edit_label_of_block_pool(block_pool_name)

        if random.choice([True, False]):
            logger.info(
                f"send valid label '{valid_label}' to check edit Block Pool label warning message"
            )
            self.do_send_keys(self.bp_loc["edit_labels_of_pool_input"], valid_label)
            res = not self.wait_until_expected_text_is_found(
                self.bp_loc["invalid_label_name_note_edit_label_pool"],
                expected_text="Invalid label name",
                timeout=10,
            )
        else:
            logger.info(
                f"send invalid label '{invalid_label}' to check edit Block Pool label warning message"
            )
            self.do_send_keys(self.bp_loc["edit_labels_of_pool_input"], invalid_label)
            res = self.wait_until_expected_text_is_found(
                self.bp_loc["invalid_label_name_note_edit_label_pool"],
                expected_text="Invalid label name",
                timeout=10,
            )
        self.cancel_edit_label()

        # if the test is complex add result of this function to results dataframe
        if hasattr(self, "test_results"):
            self.test_results.loc[len(self.test_results)] = [
                "edit Block Pool label warnings",
                self.check_edit_labels.__name__,
                res,
            ]
        return res

    def open_edit_label_of_block_pool(self, block_pool_name):
        """
        Opens the edit label popup of a Block Pool specified by the given `block_pool_name`.

        Args:
            block_pool_name (str): The name of the block pool to open the edit label page for.
        """
        logger.info(f"Filtering pool page for {block_pool_name}")
        self.do_send_keys(
            self.generic_locators["search_resource_field"], block_pool_name
        )
        logger.info(f"Clicking on Actions for {block_pool_name}")
        self.do_click(self.bp_loc["actions_outside_pool"], enable_screenshot=True)
        logger.info("Clicking on Edit labels")
        self.do_click(self.bp_loc["edit_labels_of_pool"])

    def enter_label_and_save(self, label):
        """Enter the specified `label` in the edit label input field and save it.

        Args:
            label (str): The label to enter.
        """
        self.do_send_keys(self.bp_loc["edit_labels_of_pool_input"], label)
        self.save_edit_label()

    def cancel_edit_label(self):
        """
        Cancel the edit label operation.
        """
        self.do_click(self.bp_loc["cancel_edit_labels_of_pool"], enable_screenshot=True)

    def save_edit_label(self):
        """
        Save the changes made in the edit label operation.
        """
        self.do_click(self.bp_loc["edit_labels_of_pool_save"], enable_screenshot=True)
