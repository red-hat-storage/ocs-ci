import logging

from ocs_ci.framework.testlib import (
    tier1,
    skipif_ui_not_support,
    skipif_ocs_version,
    ui,
)
from ocs_ci.framework.pytest_customization.marks import black_squad
from ocs_ci.ocs.ui.validation_ui import ValidationUI
from ocs_ci.utility import version

logger = logging.getLogger(__name__)


class TestUserInterfaceValidation(object):
    """
    Test User Interface Validation

    """

    @ui
    @tier1
    @black_squad
    @skipif_ui_not_support("validation")
    def test_dashboard_validation_ui(self, setup_ui):
        """
        Validate User Interface

        Args:
            setup_ui: login function on conftest file

        """
        validation_ui_obj = ValidationUI(setup_ui)
        ocs_version = version.get_semantic_ocs_version_from_config()
        if ocs_version >= version.VERSION_4_9:
            validation_ui_obj.odf_overview_ui()
        else:
            validation_ui_obj.verification_ui()

    @ui
    @tier1
    @skipif_ui_not_support("validation")
    @skipif_ocs_version("<4.9")
    @black_squad
    def test_odf_storagesystems_ui(self, setup_ui):
        """
        Validate User Interface for ODF Storage Systems Tab for ODF 4.9

        Args:
            setup_ui: login function on conftest file

        """
        validation_ui_obj = ValidationUI(setup_ui)
        validation_ui_obj.odf_storagesystems_ui()
