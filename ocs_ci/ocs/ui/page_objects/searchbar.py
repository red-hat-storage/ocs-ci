from ocs_ci.ocs.exceptions import IncorrectUiOptionRequested
from ocs_ci.ocs.ui.base_ui import BaseUI, logger


class SearchBar(BaseUI):
    def select_search_by(self, search_by: str):
        """
        Select search by option

        Args:
            search_by (str): search by option Name | Label

        """
        if search_by not in ["name", "label"]:
            raise IncorrectUiOptionRequested(f"Invalid search by option {search_by}")
        current_option = self.get_element_text(
            self.generic_locators["searchbar_drop_down"]
        )
        if current_option != search_by:
            logger.info(f"Selecting search by option {search_by}")
            self.do_click(self.generic_locators["searchbar_drop_down"])
            self.do_click(self.generic_locators[f"searchbar_select_{search_by}"])
        else:
            logger.info(f"Search by option is already {search_by}")

    def search(self, search_text: str):
        """
        Search for text

        Args:
            search_text (str): Text to search

        """
        logger.info(f"Searching for {search_text}")
        self.do_send_keys(self.generic_locators["searchbar_input"], search_text)
