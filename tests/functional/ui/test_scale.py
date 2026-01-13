import logging

from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.framework.testlib import (
    ui,
    skipif_ocs_version,
    tier2,
    skipif_ibm_cloud_managed,
)
from ocs_ci.framework.pytest_customization.marks import black_squad

logger = logging.getLogger(__name__)


class TestScaleConnection(object):
    """
    Test connecting Scale cluster
    """

    @ui
    @skipif_ibm_cloud_managed
    @tier2
    @skipif_ocs_version("<4.20")
    @black_squad
    def test_add_delete_filesystem(self, setup_ui_class):
        scale_connect_obj = PageNavigator()
        external_systems = scale_connect_obj.nav_external_systems_page()
        external_systems.connect_scale_filesystem(
            scale_name="test-scale", filesystem_name="additional_filesystem_1"
        )
        external_systems.delete_scale_filesystem(
            scale_name="test-scale", filesystem_name="additional_filesystem_1"
        )
