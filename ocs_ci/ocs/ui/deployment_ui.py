import logging
import time


from ocs_ci.ocs.ui.views import osd_sizes, OCS_OPERATOR, ODF_OPERATOR, LOCAL_STORAGE
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.utility import version
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.framework import config
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.node import get_worker_nodes
from ocs_ci.utility.deployment import get_ocp_ga_version
from ocs_ci.deployment.helpers.lso_helpers import (
    add_disk_for_vsphere_platform,
    create_optional_operators_catalogsource_non_ga,
)


logger = logging.getLogger(__name__)


class DeploymentUI(PageNavigator):
    """
    Deployment OCS/ODF Operator via User Interface

    """

    def __init__(self):
        super().__init__()

    def verify_disks_lso_attached(self, timeout=600, sleep=20):
        """
        Verify Disks Attached

        Args:
            timeout (int): Time in seconds to wait
            sleep (int): Sampling time in seconds

        """
        osd_size = config.ENV_DATA.get("device_size", defaults.DEVICE_SIZE)
        number_worker_nodes = get_worker_nodes()
        capacity = int(osd_size) * len(number_worker_nodes)
        if capacity >= 1024:
            capacity_str = str(capacity / 1024).rstrip("0").rstrip(".") + " TiB"
        else:
            capacity_str = str(capacity) + " GiB"
        logger.info(f"Waiting for {capacity_str}")
        sample = TimeoutSampler(
            timeout=timeout,
            sleep=sleep,
            func=self.check_element_text,
            expected_text=capacity_str,
            take_screenshot=True,
        )
        if not sample.wait_for_func_status(result=True):
            raise TimeoutExpiredError(f"Disks are not attached after {timeout} seconds")

    def install_ocs_operator(self):
        """
        Install OCS/ODF Opeartor
        """
        self.navigate_operatorhub_page()
        self.do_send_keys(self.dep_loc["search_operators"], text=self.operator_name)
        logger.info(f"Choose {self.operator_name} Version")
        if self.operator_name is OCS_OPERATOR:
            self.do_click(self.dep_loc["choose_ocs_version"], enable_screenshot=True)
        elif self.operator_name is ODF_OPERATOR:
            self.do_click(self.dep_loc["click_odf_operator"], enable_screenshot=True)
        logger.info(f"Click Install {self.operator_name}")
        self.do_click(
            self.dep_loc["click_install_ocs"], enable_screenshot=True, timeout=60
        )
        if self.operator_name is ODF_OPERATOR:
            self.do_click(self.dep_loc["enable_console_plugin"], enable_screenshot=True)
        self.do_click(self.dep_loc["click_install_ocs_page"], enable_screenshot=True)
        if self.operator_name is ODF_OPERATOR:
            try:
                self.navigate_installed_operators_page()
                self.do_click(locator=self.dep_loc["refresh_popup"], timeout=500)
            except Exception as e:
                logger.error(f"Refresh pop-up does not exist: {e}")
                self.refresh_page()
        self.verify_operator_succeeded(operator=self.operator_name)

    def refresh_popup(self, timeout=30):
        """
        Refresh PopUp
        """
        if self.check_element_text("Web console update is available"):
            logger.info("Web console update is available and Refresh web console")
            self.do_click(locator=self.dep_loc["refresh_popup"], timeout=timeout)

    def install_local_storage_operator(self):
        """
        Install local storage operator

        """
        if config.DEPLOYMENT.get("local_storage"):
            self.navigate_operatorhub_page()
            logger.info(f"Search {self.operator_name} Operator")
            self.do_send_keys(self.dep_loc["search_operators"], text="Local Storage")
            logger.info("Choose Local Storage Version")
            ocp_ga_version = get_ocp_ga_version(self.ocp_version_full)
            if ocp_ga_version:
                self.do_click(
                    self.dep_loc["choose_local_storage_version"], enable_screenshot=True
                )
            else:
                self.do_click(
                    self.dep_loc["choose_local_storage_version_non_ga"],
                    enable_screenshot=True,
                )

            logger.info("Click Install LSO")
            self.do_click(self.dep_loc["click_install_lso"], enable_screenshot=True)
            self.do_click(
                self.dep_loc["click_install_lso_page"], enable_screenshot=True
            )
            self.verify_operator_succeeded(operator=LOCAL_STORAGE, timeout_install=300)

    def install_storage_cluster(self):
        """
        Install StorageCluster/StorageSystem

        """
        if self.operator_name == ODF_OPERATOR:
            self.navigate_installed_operators_page()
            self.choose_expanded_mode(
                mode=True, locator=self.dep_loc["drop_down_projects"]
            )
            self.do_click(self.dep_loc["choose_all_projects"], enable_screenshot=True)
        else:
            self.search_operator_installed_operators_page(operator=self.operator_name)

        logger.info(f"Click on {self.operator_name} on 'Installed Operators' page")
        if self.operator_name == ODF_OPERATOR:
            logger.info("Click on Create StorageSystem")
            self.do_click(
                locator=self.dep_loc["odf_operator_installed"], enable_screenshot=True
            )
            time.sleep(5)
            self.do_click(
                locator=self.dep_loc["storage_system_tab"], enable_screenshot=True
            )
        elif self.operator_name == OCS_OPERATOR:
            logger.info("Click on Create StorageCluster")
            self.do_click(
                locator=self.dep_loc["ocs_operator_installed"], enable_screenshot=True
            )
            time.sleep(5)
            self.do_click(
                locator=self.dep_loc["storage_cluster_tab"], enable_screenshot=True
            )
            if self.check_element_text("404"):
                logger.info("Refresh storage cluster page")
                self.refresh_page()
        # WA for https://issues.redhat.com/browse/OCPBUGS-32223
        time.sleep(60)
        self.do_click(
            locator=self.dep_loc["create_storage_cluster"], enable_screenshot=True
        )
        # WA for https://issues.redhat.com/browse/OCPBUGS-32223
        time.sleep(30)
        if self.check_element_text("An error"):
            logger.info("Refresh storage system page if error occurred")
            self.refresh_page()
            time.sleep(30)
        if config.ENV_DATA.get("mcg_only_deployment", False):
            self.install_mcg_only_cluster()
        elif config.DEPLOYMENT.get("local_storage"):
            self.install_lso_cluster()
        else:
            self.install_internal_cluster()

    def install_mcg_only_cluster(self):
        """
        Install MCG ONLY cluster via UI

        """
        logger.info("Install MCG ONLY cluster via UI")
        if self.ocp_version == "4.9":
            self.do_click(self.dep_loc["advanced_deployment"])
        self.do_click(self.dep_loc["expand_advanced_mode"], enable_screenshot=True)
        if self.ocp_version == "4.9":
            self.do_click(self.dep_loc["mcg_only_option"], enable_screenshot=True)
        elif self.ocp_version in ("4.10", "4.11", "4.12"):
            self.do_click(self.dep_loc["mcg_only_option_4_10"], enable_screenshot=True)
        if config.DEPLOYMENT.get("local_storage"):
            self.install_lso_cluster()
        else:
            self.do_click(self.dep_loc["next"], enable_screenshot=True)
        self.do_click(self.dep_loc["next"], enable_screenshot=True)
        self.create_storage_cluster()

    def configure_in_transit_encryption(self):
        """
        Configure in_transit_encryption

        """
        if config.ENV_DATA.get("in_transit_encryption"):
            logger.info("Enable in-transit encryption")
            self.select_checkbox_status(
                status=True, locator=self.dep_loc["enable_in_transit_encryption"]
            )

    def install_lso_cluster(self):
        """
        Install LSO cluster via UI

        """
        logger.info("Click Internal - Attached Devices")
        if self.operator_name == ODF_OPERATOR:
            self.do_click(self.dep_loc["choose_lso_deployment"], enable_screenshot=True)
        else:
            self.do_click(
                self.dep_loc["internal-attached_devices"], enable_screenshot=True
            )
            logger.info("Click on All nodes")
            self.do_click(self.dep_loc["all_nodes_lso"], enable_screenshot=True)
        self.do_click(self.dep_loc["next"], enable_screenshot=True)

        logger.info(
            f"Configure Volume Set Name and Storage Class Name as {constants.LOCAL_BLOCK_RESOURCE}"
        )
        self.do_send_keys(
            locator=self.dep_loc["lv_name"],
            text=constants.LOCAL_BLOCK_RESOURCE,
            timeout=300,
        )
        self.take_screenshot()
        self.do_send_keys(
            locator=self.dep_loc["sc_name"], text=constants.LOCAL_BLOCK_RESOURCE
        )
        if self.operator_name == OCS_OPERATOR:
            logger.info("Select all nodes on 'Create Storage Class' step")
            self.do_click(
                locator=self.dep_loc["all_nodes_create_sc"], enable_screenshot=True
            )
        if config.ENV_DATA.get("platform") not in [
            constants.BAREMETAL_PLATFORM,
            constants.HCI_BAREMETAL,
        ]:
            self.take_screenshot()
            self.verify_disks_lso_attached()
            timeout_next = 60
        else:
            timeout_next = 600
        self.do_click(
            self.dep_loc["next"], enable_screenshot=True, timeout=timeout_next
        )

        logger.info("Confirm new storage class")
        self.do_click(self.dep_loc["yes"], enable_screenshot=True)
        if config.ENV_DATA.get("mcg_only_deployment", False):
            return

        sample = TimeoutSampler(
            timeout=600,
            sleep=10,
            func=self.check_element_text,
            expected_text="Memory",
        )
        if not sample.wait_for_func_status(result=True):
            raise TimeoutExpiredError("Nodes not found after 600 seconds")

        self.enable_taint_nodes()

        self.configure_performance()

        if self.operator_name == OCS_OPERATOR:
            logger.info(f"Select {constants.LOCAL_BLOCK_RESOURCE} storage class")
            self.choose_expanded_mode(
                mode=True, locator=self.dep_loc["storage_class_dropdown_lso"]
            )
            self.do_click(locator=self.dep_loc["localblock_sc"], enable_screenshot=True)
            timeout_next = 30
        else:
            timeout_next = 600
        self.do_click(
            self.dep_loc["next"], enable_screenshot=True, timeout=timeout_next
        )

        self.configure_encryption()

        self.configure_data_protection()

        self.create_storage_cluster()

    def install_internal_cluster(self):
        """
        Install Internal Cluster

        """
        logger.info("Click Internal")
        if self.operator_name == ODF_OPERATOR:
            self.do_click(
                locator=self.dep_loc["internal_mode_odf"], enable_screenshot=True
            )
        else:
            self.do_click(locator=self.dep_loc["internal_mode"], enable_screenshot=True)

        logger.info("Configure Storage Class (thin on vmware, gp2 on aws)")
        self.do_click(
            locator=self.dep_loc["storage_class_dropdown"], enable_screenshot=True
        )
        self.do_click(
            locator=self.dep_loc[self.storage_class],
            enable_screenshot=True,
            copy_dom=True,
        )

        if self.operator_name == ODF_OPERATOR:
            self.do_click(locator=self.dep_loc["next"], enable_screenshot=True)

        self.configure_osd_size()

        self.configure_performance()

        logger.info("Select all worker nodes")
        self.select_checkbox_status(status=True, locator=self.dep_loc["all_nodes"])

        self.enable_taint_nodes()

        if self.ocp_version == "4.6" and config.ENV_DATA.get("encryption_at_rest"):
            self.do_click(
                locator=self.dep_loc["enable_encryption"], enable_screenshot=True
            )

        if self.ocp_version_semantic >= version.VERSION_4_7:
            logger.info("Next on step 'Select capacity and nodes'")
            self.do_click(locator=self.dep_loc["next"], enable_screenshot=True)
            self.configure_in_transit_encryption()
            self.configure_encryption()

        self.configure_data_protection()

        self.create_storage_cluster()

    def configure_performance(self):
        """
        Configure performance mode

        """
        mode = config.ENV_DATA.get("performance_profile")
        if self.ocs_version_semantic >= version.VERSION_4_15 and mode in (
            "lean",
            "performance",
        ):
            self.do_click(
                locator=self.dep_loc["drop_down_performance"], enable_screenshot=True
            )
            if mode == "lean":
                self.do_click(locator=self.dep_loc["lean_mode"])
            elif mode == "performance":
                self.do_click(locator=self.dep_loc["performance_mode"])

    def create_storage_cluster(self):
        """
        Review and Create StorageCluster/StorageSystem

        """
        logger.info("Create storage cluster on 'Review and create' page")
        if self.operator_name is OCS_OPERATOR:
            self.do_click(
                locator=self.dep_loc["create_on_review"], enable_screenshot=True
            )
        elif self.operator_name is ODF_OPERATOR:
            self.do_click(
                locator=self.dep_loc["create_storage_system"], enable_screenshot=True
            )
        logger.info("Sleep 10 second after click on 'create storage cluster'")
        time.sleep(10)

    def configure_encryption(self):
        """
        Configure Encryption

        """
        if config.ENV_DATA.get("encryption_at_rest"):
            logger.info("Enable OSD Encryption")
            self.select_checkbox_status(
                status=True, locator=self.dep_loc["enable_encryption"]
            )

            logger.info("Cluster-wide encryption")
            self.select_checkbox_status(
                status=True, locator=self.dep_loc["wide_encryption"]
            )
        self.do_click(self.dep_loc["next"], enable_screenshot=True)

    def configure_data_protection(self):
        """
        Configure Data Protection

        """
        if (
            self.ocs_version_semantic >= version.VERSION_4_14
            and self.ocs_version_semantic <= version.VERSION_4_16
        ):
            self.do_click(self.dep_loc["next"], enable_screenshot=True)

    def enable_taint_nodes(self):
        """
        Enable taint Nodes

        """
        logger.info("Enable taint Nodes")
        if (
            self.ocp_version_semantic >= version.VERSION_4_10
            and config.DEPLOYMENT.get("ocs_operator_nodes_to_taint") > 0
        ):
            self.select_checkbox_status(
                status=True, locator=self.dep_loc["enable_taint_node"]
            )

    def configure_osd_size(self):
        """
        Configure OSD Size
        """
        device_size = str(config.ENV_DATA.get("device_size"))
        osd_size = device_size if device_size in osd_sizes else "512"
        logger.info(f"Configure OSD Capacity {osd_size}")
        if self.ocp_version_semantic >= version.VERSION_4_11:
            self.do_click(self.dep_loc["osd_size_dropdown"], enable_screenshot=True)
        else:
            self.choose_expanded_mode(
                mode=True, locator=self.dep_loc["osd_size_dropdown"]
            )
        self.do_click(locator=self.dep_loc[osd_size], enable_screenshot=True)

    def verify_operator_succeeded(
        self, operator=OCS_OPERATOR, timeout_install=300, sleep=20
    ):
        """
        Verify Operator Installation

        Args:
            operator (str): type of operator
            timeout_install (int): Time in seconds to wait
            sleep (int): Sampling time in seconds

        """
        self.search_operator_installed_operators_page(operator=operator)
        time.sleep(5)
        if operator == LOCAL_STORAGE:
            sample = TimeoutSampler(
                timeout=timeout_install,
                sleep=sleep,
                func=self.check_element_text,
                expected_text="Succeeded",
            )
        elif self.ocs_version_semantic > version.VERSION_4_15:
            sample = TimeoutSampler(
                timeout=timeout_install,
                sleep=sleep,
                func=self.check_number_occurrences_text,
                expected_text="Succeeded",
                number=2,
            )
        else:
            sample = TimeoutSampler(
                timeout=timeout_install,
                sleep=sleep,
                func=self.check_element_text,
                expected_text="Succeeded",
            )
        if not sample.wait_for_func_status(result=True):
            logger.error(
                f"{operator} Installation status is not Succeeded after {timeout_install} seconds"
            )
            self.take_screenshot()
            raise TimeoutExpiredError(
                f"{operator} Installation status is not Succeeded after {timeout_install} seconds"
            )
        self.take_screenshot()
        logger.info(f"{operator} operator installed Successfully.")

    def search_operator_installed_operators_page(self, operator=OCS_OPERATOR):
        """
        Search Operator on Installed Operators Page

        Args:
            operator (str): type of operator

        """
        self.navigate_operatorhub_page()
        self.navigate_installed_operators_page()
        logger.info(f"Search {operator} operator installed")
        if self.ocp_version_semantic >= version.VERSION_4_7:
            sample = TimeoutSampler(
                timeout=100,
                sleep=10,
                func=self.check_element_text,
                expected_text=operator,
            )
            if not sample.wait_for_func_status(result=True):
                logger.error(
                    f"The {operator} installation did not start after 100 seconds"
                )
                self.take_screenshot()
            self.do_send_keys(
                locator=self.dep_loc["search_operator_installed"],
                text=operator,
            )
        # https://bugzilla.redhat.com/show_bug.cgi?id=1899200
        elif self.ocp_version == "4.6":
            self.do_click(self.dep_loc["project_dropdown"], enable_screenshot=True)
            self.do_click(self.dep_loc[operator], enable_screenshot=True)
        elif self.ocp_version == "4.9" and operator != "Local Storage":
            self.choose_expanded_mode(
                mode=True, locator=self.dep_loc["drop_down_projects"]
            )
            default_projects_is_checked = self.driver.find_element_by_id(
                "no-label-switch-on"
            ).is_selected()
            if not default_projects_is_checked:
                logger.info("Show default projects")
                self.do_click(
                    self.dep_loc["enable_default_porjects"], enable_screenshot=True
                )
            self.do_click(
                self.dep_loc["choose_openshift-storage_project"], enable_screenshot=True
            )

    def install_ocs_ui(self):
        """
        Install OCS/ODF via UI

        """
        if config.DEPLOYMENT.get("local_storage"):
            create_optional_operators_catalogsource_non_ga()
            if config.ENV_DATA.get("platform") == constants.VSPHERE_PLATFORM:
                add_disk_for_vsphere_platform()
        self.install_local_storage_operator()
        self.install_ocs_operator()
        if not config.UPGRADE.get("ui_upgrade"):
            self.install_storage_cluster()
