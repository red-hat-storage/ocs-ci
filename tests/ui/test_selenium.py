"""
UI tests using selenium and chrome browser
"""

import logging

from ocs_ci.framework.testlib import ignore_leftovers, ManageTest, ui
from ocs_ci.ocs.ui_operator import choose_storage_cluster, get_status_label

logger = logging.getLogger(__name__)


@ui
@ignore_leftovers
class TestUI(ManageTest):
    """
    UI tests
    """

    def test_ui_storage_cluster_status(self, chrome_driver):
        logger.info(
            "Test checks that the status of storage cluster is Ready"
        )
        choose_storage_cluster(chrome_driver, "ocs-storagecluster")
        storage_cluster_status = get_status_label(chrome_driver)
        assert storage_cluster_status == "Ready"
