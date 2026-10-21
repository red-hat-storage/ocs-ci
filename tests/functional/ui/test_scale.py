import logging
import time

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework.pytest_customization.marks import yellow_squad
from ocs_ci.framework.testlib import (
    cnsa_remote_mount,
    fdf_required,
    polarion_id,
    runs_on_provider,
    skipif_ibm_cloud_managed,
    skipif_ocs_version,
    tier2,
    ui,
)
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator

logger = logging.getLogger(__name__)

SCALE_CONNECTION_NAME = "scale-cluster-1"
FILESYSTEM_1 = "fs1"
FILESYSTEM_2 = "fs2"


@fdf_required
@runs_on_provider
@cnsa_remote_mount
@yellow_squad
class TestScaleConnection(object):
    """
    Test connecting Scale cluster via ODF External Systems UI page.
    Executed on FDF only.
    """

    @ui
    @skipif_ibm_cloud_managed
    @tier2
    @skipif_ocs_version("<4.20")
    @polarion_id("OCS-7757")
    def test_connect_scale(self, setup_ui_class):
        """
        Test connecting IBM Storage Scale cluster as an External system.
        """
        # Resolve target host/ip endpoint from AUTH configuration
        endpoint = config.AUTH.get("scale_gui_hosts")
        username = config.AUTH.get("scale_gui_user")
        password = config.AUTH.get("scale_gui_password")

        scale_connect_obj = PageNavigator()
        external_systems = scale_connect_obj.nav_external_systems_page()

        external_systems.connect_scale(
            system_name=SCALE_CONNECTION_NAME,
            endpoint=endpoint,
            port="443",
            username=username,
            password=password,
            filesystem_name=FILESYSTEM_2,
        )

        assert external_systems.scale_present_on_page(
            SCALE_CONNECTION_NAME
        ), f"Scale connection '{SCALE_CONNECTION_NAME}' was not found on the External Systems page."

    @ui
    @skipif_ibm_cloud_managed
    @tier2
    @skipif_ocs_version("<4.21")
    @polarion_id("OCS-7758")
    def test_add_filesystem(self, setup_ui_class):
        """
        Test connecting an additional filesystem when a Scale cluster is connected
        and subsequently deleting it.
        """
        scale_connect_obj = PageNavigator()
        external_systems = scale_connect_obj.nav_external_systems_page()
        external_systems.connect_scale_filesystem(
            scale_name=SCALE_CONNECTION_NAME, filesystem_name=FILESYSTEM_1
        )

    @ui
    @skipif_ibm_cloud_managed
    @tier2
    @skipif_ocs_version("<4.20")
    @polarion_id("OCS-7759")
    def test_disconnect_scale(self, setup_ui_class):
        """
        Test that disconnecting Scale removes it from the External Systems page.
        """
        scale_connect_obj = PageNavigator()
        external_systems = scale_connect_obj.nav_external_systems_page()
        external_systems.disconnect_scale(
            scale_name=SCALE_CONNECTION_NAME,
            filesystem_names=[FILESYSTEM_1, FILESYSTEM_2],
        )
        time.sleep(10)
        assert not external_systems.scale_present_on_page(
            scale_name=SCALE_CONNECTION_NAME
        ), f"Scale connection '{SCALE_CONNECTION_NAME}' is still present after disconnect."

        # Deleting the Scale cluster does not cascade to its Filesystem CRs, so
        # verify each associated filesystem was removed by disconnect_scale.
        fs_ocp = OCP(
            kind="filesystem.scale.spectrum.ibm.com",
            namespace=constants.IBM_STORAGE_SCALE_NAMESPACE,
        )
        for filesystem_name in [FILESYSTEM_1, FILESYSTEM_2]:
            fs_cr_name = f"{SCALE_CONNECTION_NAME}-{filesystem_name}"
            assert fs_ocp.check_resource_existence(
                should_exist=False, resource_name=fs_cr_name
            ), f"Filesystem '{fs_cr_name}' still exists after disconnect."
