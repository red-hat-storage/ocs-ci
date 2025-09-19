"""
This module is a test suite for the Page Object Model (POM) framework. The POM framework is a design pattern that
creates an object repository for web UI elements. This allows for the separation of the test logic from the UI logic.
The POM framework is implemented in the OCS UI tests to improve the maintainability and readability of the tests.
"""

import logging

from ocs_ci.framework.pytest_customization.marks import libtest
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.utility.utils import human_to_bytes_ui

logger = logging.getLogger(__name__)


@libtest
def test_raw_capacity(setup_ui_session):
    """
    Test to verify the used capacity of the cluster can be parsed and compared to the available capacity
    """
    block_and_file = PageNavigator().nav_storage_cluster_default_page()
    block_and_file.validate_block_and_file_tab_active()

    used, available = block_and_file.get_raw_capacity_card_values()
    used_bytes = human_to_bytes_ui(used)
    logger.info(f"Used capacity: {used_bytes}")
    available_bytes = human_to_bytes_ui(available)
    logger.info(f"Available capacity: {available_bytes}")
    assert (
        used_bytes < available_bytes
    ), "Used capacity is not less than available capacity"
