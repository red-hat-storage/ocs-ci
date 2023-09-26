import logging
import pytest

from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.framework.testlib import ui, skipif_ocs_version, tier2, skipif_ibm_cloud
from ocs_ci.framework.pytest_customization.marks import black_squad

logger = logging.getLogger(__name__)


class TestPvcUserInterface(object):
    """
    Test Quickstarts in the User Interface

    """

    @ui
    @skipif_ibm_cloud
    @tier2
    @skipif_ocs_version("<4.7")
    @black_squad
    @pytest.mark.parametrize(
        argnames=["title"],
        argvalues=[
            pytest.param(
                *["Getting started with OpenShift Data Foundation"],
                marks=pytest.mark.polarion_id("OCS-2458"),
            ),
            pytest.param(
                *["Configure and manage OpenShift Data Foundation"],
                marks=pytest.mark.polarion_id("OCS-2466"),
            ),
        ],
    )
    def test_quickstart_presence(self, setup_ui_class, title):
        """
        Check that the quickstart with the given title is present in the UI

        """
        quickstart_ui_obj = PageNavigator()
        quickstart_ui_obj.navigate_quickstarts_page()
        quickstart_ui_obj.take_screenshot()
        logger.info(f"Expected quickstart title: {title}")
        assert quickstart_ui_obj.check_element_text(
            element="p", expected_text=title
        ), f"Expected title '{title}' was not found"
