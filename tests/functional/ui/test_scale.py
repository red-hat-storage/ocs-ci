import logging
import time

from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.framework.testlib import (
    ui,
    skipif_ocs_version,
    tier2,
    skipif_ibm_cloud_managed,
    polarion_id,
    fdf_required,
)
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import black_squad

logger = logging.getLogger(__name__)

SCALE_CONNECTION_NAME = "scale-cluster-1"
FILESYSTEM_1 = "fs1"
FILESYSTEM_2 = "fs2"


class TestScaleConnection(object):
    """
    Test connecting Scale cluster
    """

    @ui
    @skipif_ibm_cloud_managed
    @tier2
    @skipif_ocs_version("<4.20")
    @black_squad
    @fdf_required
    @polarion_id("OCS-7757")
    def test_connect_scale(self, setup_ui_class):
        """
        Test connecting Scale cluster as External system
        """
        scale_connect_obj = PageNavigator()
        external_systems = scale_connect_obj.nav_external_systems_page()
        external_systems.connect_scale(
            system_name=SCALE_CONNECTION_NAME,
            endpoint=config.ENV_DATA["scale_endpoint"],
            port="443",
            username=config.ENV_DATA["scale_username"],
            password=config.ENV_DATA["scale_password"],
            filesystem_name=FILESYSTEM_1,
        )
        assert external_systems.scale_present_on_page(SCALE_CONNECTION_NAME)
        # checking status temporarily disabled
        # until https://issues.redhat.com/browse/DFBUGS-4352 is fixed
        # assert external_systems.scale_status_ok(SCALE_CONNECTION_NAME)

    @ui
    @skipif_ibm_cloud_managed
    @tier2
    @skipif_ocs_version("<4.21")
    @black_squad
    @fdf_required
    @polarion_id("OCS-7758")
    def test_add_delete_filesystem(self, setup_ui_class):
        """
        Test connecting an additional filesystem when a scale cluster is connected
        and then deleting it
        """
        scale_connect_obj = PageNavigator()
        external_systems = scale_connect_obj.nav_external_systems_page()
        external_systems.connect_scale_filesystem(
            scale_name=SCALE_CONNECTION_NAME, filesystem_name=FILESYSTEM_2
        )
        external_systems.delete_scale_filesystem(
            scale_name=SCALE_CONNECTION_NAME, filesystem_name=FILESYSTEM_2
        )

    @ui
    @fdf_required
    @skipif_ibm_cloud_managed
    @tier2
    @skipif_ocs_version("<4.20")
    @black_squad
    @polarion_id("OCS-7759")
    def test_disconnect_scale(self, setup_ui_class):
        """
        Test that disconnecting scale removes it from External systems page
        """
        scale_connect_obj = PageNavigator()
        external_systems = scale_connect_obj.nav_external_systems_page()
        external_systems.disconnect_scale(
            scale_name=SCALE_CONNECTION_NAME,
        )
        time.sleep(10)
        assert not external_systems.scale_present_on_page(
            scale_name=SCALE_CONNECTION_NAME
        )
