from ocs_ci.ocs.ui.base_ui import logger
from ocs_ci.ocs.ui.views import locators, ODF_OPERATOR
from ocs_ci.utility.utils import get_ocp_version
from ocs_ci.framework import config
from ocs_ci.ocs.ocs_upgrade import OCSUpgrade
from ocs_ci.ocs.ui.deployment_ui import DeploymentUI
from ocs_ci.ocs.ui.validation_ui import ValidationUI
from ocs_ci.ocs.resources.storage_cluster import ocs_install_verification
from ocs_ci.framework.testlib import ManageTest, skipif_ocs_version, skipif_ocp_version


@skipif_ocp_version("<4.9")
@skipif_ocs_version("<4.8")
class TestOcsOdfUpgrade(ManageTest):

    ocp_version = get_ocp_version()
    dep_loc = locators[ocp_version]["deployment"]
    validation_loc = locators[ocp_version]["validation"]
    operator = ODF_OPERATOR

    def test_ocs_odf_upgrade(self, setup_ui):

        original_ocs_version = config.ENV_DATA.get("ocs_version")
        upgrade_in_current_source = config.UPGRADE.get(
            "upgrade_in_current_source", False
        )

        upgrade_odf = OCSUpgrade(
            namespace=config.ENV_DATA["cluster_namespace"],
            version_before_upgrade=original_ocs_version,
            ocs_registry_image=config.UPGRADE.get("upgrade_ocs_registry_image"),
            upgrade_in_current_source=upgrade_in_current_source,
        )
        val_obj = ValidationUI(setup_ui)
        pagenav_obj = ValidationUI(setup_ui)

        channel = upgrade_odf.set_upgrade_channel()
        csv_name_pre_upgrade = upgrade_odf.get_csv_name_pre_upgrade()

        ocs_registry_image = config.DEPLOYMENT.get("ocs_registry_image")
        logger.info(f"------------------------- {ocs_registry_image}")

        upgrade_odf.set_upgrade_images()

        dep_obj = DeploymentUI(setup_ui)
        dep_obj.operator = ODF_OPERATOR
        dep_obj.refresh_page()
        dep_obj.install_ocs_operator()

        logger.info("Click on Storage System under Provided APIs on Installed Operators Page")
        val_obj.do_click(self.validation_loc["storage-system-on-installed-operators"])
        logger.info("Checking presence of storagesystem on Operator details page")
        storage_systems_check = val_obj.check_element_text(
            expected_text="ocs-storagecluster-storagesystem"
        )
        assert storage_systems_check, (
            "Upgrade failure, Storage System wasn't created after odf-operator installation as part of OCS to ODF "
            "upgrade "
        )

        logger.info("Calling functions for other UI checks")
        pagenav_obj.odf_overview_ui()
        pagenav_obj.odf_storagesystems_ui()

        logger.info("Checking if upgrade completed")
        upgrade_odf.check_if_upgrade_completed(
            channel=channel, csv_name_pre_upgrade=csv_name_pre_upgrade
        )

        logger.info("Doing post upgrade verification")
        ocs_install_verification(
            timeout=600,
            skip_osd_distribution_check=True,
            ocs_registry_image=upgrade_odf.ocs_registry_image,
            post_upgrade_verification=True,
            version_before_upgrade=upgrade_odf.version_before_upgrade,
        )
