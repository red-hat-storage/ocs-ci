import logging

from ocs_ci.ocs.ui.pvc_ui import PvcUI


logger = logging.getLogger(__name__)


class Test123(object):
    """
    Test PVC User Interface

    """

    def test_123_ui(
        self,
        setup_ui_class,
    ):
        """
        Test create, resize and delete pvc via UI

        """

        test = PvcUI()

        test.shrivaibavi_func()
