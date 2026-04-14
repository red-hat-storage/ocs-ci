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
    runs_on_provider,
)
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import black_squad

logger = logging.getLogger(__name__)

SCALE_CONNECTION_NAME = "scale-cluster-1"
FILESYSTEM_1 = "fs1"
FILESYSTEM_2 = "fs2"


@fdf_required
@runs_on_provider
class TestScaleConnection(object):
    """
    Test connecting Scale cluster

    To be executed on FDF only
    """

    @ui
    @skipif_ibm_cloud_managed
    @tier2
    @skipif_ocs_version("<4.20")
    @black_squad
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
        external_systems.check_filesystem_details(
            scale_name=SCALE_CONNECTION_NAME,
            filesystem_name=FILESYSTEM_1,
            status="Connected",
        )

    @ui
    @skipif_ibm_cloud_managed
    @tier2
    @skipif_ocs_version("<4.22")
    @black_squad
    def test_scale_version(self, setup_ui_class):
        """
        Test that scale version on the dashboard is the same as in Scale cluster
        """
        scale_connect_obj = PageNavigator()
        external_systems = scale_connect_obj.nav_external_systems_page()
        scale_version_ui = external_systems.get_scale_version_from_dashboard()
        scale_version_cli = external_systems.get_scale_version_from_remotecluster()
        assert scale_version_ui == scale_version_cli(
            f"Scale version on the dashboard is {scale_version_cli} while in remotecluster it's {scale_version_cli}"
        )

    @ui
    @skipif_ibm_cloud_managed
    @tier2
    @skipif_ocs_version("<4.21")
    @black_squad
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
        external_systems.check_filesystem_details(
            scale_name=SCALE_CONNECTION_NAME,
            filesystem_name=FILESYSTEM_2,
            status="Connected",
        )
        # try to connect a filesystem with the same name, verify alert message
        alert_message = external_systems.connect_scale_filesystem(
            scale_name=SCALE_CONNECTION_NAME, filesystem_name=FILESYSTEM_2
        )
        external_systems.delete_scale_filesystem(
            scale_name=SCALE_CONNECTION_NAME, filesystem_name=FILESYSTEM_2
        )
        assert "already exists" in alert_message

    @ui
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
