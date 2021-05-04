import logging

from ocs_ci.framework.testlib import tier1
from ocs_ci.ocs.ui.validation_ui import ValidationUI


logger = logging.getLogger(__name__)


class TestUserInterfaceValidation(object):
    """
    Test User Interface Validation

    """

    @tier1
    def test_validation_ui(self, setup_ui):
        """
        Validate User Interface

        """
        validation_ui_obj = ValidationUI(setup_ui)
        validation_ui_obj.verification_ui()
