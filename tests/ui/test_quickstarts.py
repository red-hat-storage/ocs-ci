import logging
import pytest

from ocs_ci.ocs.ui.base_ui import PageNavigator
from ocs_ci.framework.testlib import (
    ui,
    skipif_ocs_version,
    tier2,
    skipif_ibm_cloud,
    ignore_data_rebalance,
)

logger = logging.getLogger(__name__)


@ignore_data_rebalance
class TestPvcUserInterface(object):
    """
    Test Quickstarts in the User Interface

    """

    @ui
    @skipif_ibm_cloud
    @tier2
    @skipif_ocs_version("<4.7")
    @pytest.mark.parametrize(
        argnames=["title"],
        argvalues=[
            pytest.param(
                *["Getting started with OpenShift Container Storage"],
                marks=pytest.mark.polarion_id("OCS-2458"),
            ),
            pytest.param(
                *["OpenShift Container Storage Configuration & Management"],
                marks=pytest.mark.polarion_id("OCS-2466"),
            ),
        ],
    )
    def test_quickstart_presense(self, setup_ui, title):
        """
        Check that the quickstart with the given title is present in the UI

        """
        quickstart_ui_obj = PageNavigator(setup_ui)
        quickstart_ui_obj.navigate_quickstarts_page()
        quickstart_ui_obj.take_screenshot()
        logger.info(f"Expected quickstart title: {title}")
        result = quickstart_ui_obj.check_element_text(element="h3", expected_text=title)
        logger.info(f"Actual result: {result}")
        assert quickstart_ui_obj.check_element_text(element="h3", expected_text=title)
