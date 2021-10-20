import logging

from ocs_ci.framework.testlib import tier1, skipif_ui_not_support
from ocs_ci.ocs.ui.validation_ui import ValidationUI

logger = logging.getLogger(__name__)


class TestUserInterfaceValidation(object):
    """
    Test User Interface Validation

    """

    @tier1
    @skipif_ui_not_support("validation")
    def test_validation_ui(self, setup_ui):
        """
        Validate User Interface

        Args:
            setup_ui: login function on conftest file

        """
        validation_ui_obj = ValidationUI(setup_ui)
        validation_ui_obj.verification_ui()

    @tier1
    def test_odf_overview_ui(self, setup_ui):
        """
        Validate User Interface for ODF Overview Tab for ODF 4.9

        Args:
            setup_ui: login function on conftest file

        """
        validation_ui_obj = ValidationUI(setup_ui)
        validation_ui_obj.odf_overview_ui()

    @tier1
    def test_odf_storagesystems_ui(self, setup_ui):
        """
        Validate User Interface for ODF Storage Systems Tab for ODF 4.9

        Args:
            setup_ui: login function on conftest file

        """
        validation_ui_obj = ValidationUI(setup_ui)
        validation_ui_obj.odf_storagesystems_ui()
