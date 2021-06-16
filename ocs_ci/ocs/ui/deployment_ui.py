import logging
import tempfile
import time

from ocs_ci.ocs.ui.views import locators, osd_sizes
from ocs_ci.ocs.ui.base_ui import PageNavigator
from ocs_ci.utility.utils import get_ocp_version, TimeoutSampler, run_cmd
from ocs_ci.utility import templating
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.framework import config
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.node import get_worker_nodes
from ocs_ci.deployment.helpers.lso_helpers import add_disk_for_vsphere_platform
from ocs_ci.helpers.proxy import get_cluster_proxies


logger = logging.getLogger(__name__)


class DeploymentUI(PageNavigator):
    """
    Deployment OCS Operator via User Interface

    """

    def __init__(self, driver):
        super().__init__(driver)
        self.ocp_version = get_ocp_version()
        self.dep_loc = locators[self.ocp_version]["deployment"]

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
        sample = TimeoutSampler(
            timeout=timeout,
            sleep=sleep,
            func=self.check_element_text,
            expected_text=capacity_str,
        )
        if not sample.wait_for_func_status(result=True):
            logger.error(f" after {timeout} seconds")
            raise TimeoutExpiredError

    def create_catalog_source_yaml(self):
        """
        Create OLM YAML file

        """
        try:
            catalog_source_data = templating.load_yaml(constants.CATALOG_SOURCE_YAML)
            image = config.DEPLOYMENT.get(
                "ocs_registry_image", config.DEPLOYMENT["default_ocs_registry_image"]
            )
            catalog_source_data["spec"]["image"] = image
            catalog_source_manifest = tempfile.NamedTemporaryFile(
                mode="w+", prefix="catalog_source_manifest", delete=False
            )
            templating.dump_data_to_temp_yaml(
                catalog_source_data, catalog_source_manifest.name
            )
            run_cmd(f"oc create -f {catalog_source_manifest.name}", timeout=300)
            run_cmd(f"oc create -f {constants.OLM_YAML}", timeout=300)
            time.sleep(60)
        except Exception as e:
            logger.info(e)

    def install_ocs_operator(self):
        """
        Install OCS Opeartor

        """
        self.navigate_operatorhub_page()

        logger.info("Search OCS Operator")
        self.do_send_keys(
            self.dep_loc["search_operators"], text="OpenShift Container Storage"
        )

        logger.info("Choose OCS Version")
        self.do_click(self.dep_loc["choose_ocs_version"])

        logger.info("Click Install OCS")
        self.do_click(self.dep_loc["click_install_ocs"])
        self.do_click(self.dep_loc["click_install_ocs_page"])
        self.verify_operator_succeeded(operator="OpenShift Container Storage")

    def install_local_storage_operator(self):
        """
        Install local storage operator

        """
        if config.DEPLOYMENT.get("local_storage"):
            self.navigate_operatorhub_page()

            logger.info("Search OCS Operator")
            self.do_send_keys(self.dep_loc["search_operators"], text="Local Storage")
            logger.info("Choose Local Storage Version")
            self.do_click(self.dep_loc["choose_local_storage_version"])

            logger.info("Click Install LSO")
            self.do_click(self.dep_loc["click_install_lso"])
            self.do_click(self.dep_loc["click_install_lso_page"])
            self.verify_operator_succeeded(operator="Local Storage")

    def install_storage_cluster(self):
        """
        Install Storage Cluster

        """
        self.search_operator_installed_operators_page()

        logger.info("Click on ocs operator on Installed Operators")
        self.do_click(locator=self.dep_loc["ocs_operator_installed"])

        logger.info("Click on Storage Cluster")
        self.do_click(locator=self.dep_loc["storage_cluster_tab"])

        logger.info("Click on Create Storage Cluster")
        self.refresh_page()
        self.do_click(locator=self.dep_loc["create_storage_cluster"])

        if config.DEPLOYMENT.get("local_storage"):
            self.install_lso_cluster()
        else:
            self.install_internal_cluster()

    def install_lso_cluster(self):
        """
        Install LSO cluster via UI

        """
        logger.info("Click Internal - Attached Devices")
        self.do_click(self.dep_loc["internal-attached_devices"])

        logger.info("Click on All nodes")
        self.do_click(self.dep_loc["all_nodes_lso"])
        self.do_click(self.dep_loc["next"])

        logger.info(
            f"Configure Volume Set Name and Storage Class Name as {constants.LOCAL_BLOCK_RESOURCE}"
        )
        self.do_send_keys(
            locator=self.dep_loc["lv_name"], text=constants.LOCAL_BLOCK_RESOURCE
        )
        self.do_send_keys(
            locator=self.dep_loc["sc_name"], text=constants.LOCAL_BLOCK_RESOURCE
        )
        logger.info("Select all nodes on 'Create Storage Class' step")
        self.do_click(locator=self.dep_loc["all_nodes_create_sc"])
        self.verify_disks_lso_attached()
        self.do_click(self.dep_loc["next"])

        logger.info("Confirm new storage class")
        self.do_click(self.dep_loc["yes"])

        sample = TimeoutSampler(
            timeout=600,
            sleep=10,
            func=self.check_element_text,
            expected_text="Memory",
        )
        if not sample.wait_for_func_status(result=True):
            logger.error("Nodes not found after 600 seconds")
            raise TimeoutExpiredError

        logger.info(f"Select {constants.LOCAL_BLOCK_RESOURCE} storage class")
        self.choose_expanded_mode(
            mode=True, locator=self.dep_loc["storage_class_dropdown_lso"]
        )
        self.do_click(locator=self.dep_loc["localblock_sc"])
        self.do_click(self.dep_loc["next"])

        self.configure_encryption()

        self.create_storage_cluster()

    def install_internal_cluster(self):
        """
        Install Internal Cluster

        """
        logger.info("Click Internal")
        self.do_click(locator=self.dep_loc["internal_mode"])

        logger.info("Configure Storage Class (thin on vmware, gp2 on aws)")
        self.do_click(locator=self.dep_loc["storage_class_dropdown"])
        self.do_click(locator=self.dep_loc[self.storage_class])

        device_size = str(config.ENV_DATA.get("device_size"))
        osd_size = device_size if device_size in osd_sizes else "512"
        logger.info(f"Configure OSD Capacity {osd_size}")
        self.choose_expanded_mode(mode=True, locator=self.dep_loc["osd_size_dropdown"])
        self.do_click(locator=self.dep_loc[osd_size])

        logger.info("Select all worker nodes")
        self.select_checkbox_status(status=True, locator=self.dep_loc["all_nodes"])

        if self.ocp_version == "4.6" and config.ENV_DATA.get("encryption_at_rest"):
            self.do_click(locator=self.dep_loc["enable_encryption"])

        if self.ocp_version in ("4.7", "4.8"):
            logger.info("Next on step 'Select capacity and nodes'")
            self.do_click(locator=self.dep_loc["next"])
            self.configure_encryption()

        self.create_storage_cluster()

    def create_storage_cluster(self):
        """
        Review and Create storage cluster

        """
        logger.info("Create on Review and create page")
        self.do_click(locator=self.dep_loc["create_on_review"])
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
        self.do_click(self.dep_loc["next"])

    def verify_operator_succeeded(
        self, operator="OpenShift Container Storage", timeout_install=300, sleep=20
    ):
        """
        Verify Operator Installation

        Args:
            operator (str): type of operator
            timeout_install (int): Time in seconds to wait
            sleep (int): Sampling time in seconds

        """
        self.search_operator_installed_operators_page(operator=operator)
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
            raise TimeoutExpiredError

    def search_operator_installed_operators_page(
        self, operator="OpenShift Container Storage"
    ):
        """
        Search Operator on Installed Operators Page

        Args:
            operator (str): type of operator

        """
        self.navigate_operatorhub_page()
        self.navigate_installed_operators_page()
        logger.info(f"Search {operator} operator installed")

        if self.ocp_version in ("4.7", "4.8"):
            self.do_send_keys(
                locator=self.dep_loc["search_operator_installed"],
                text=operator,
            )
        # https://bugzilla.redhat.com/show_bug.cgi?id=1899200
        elif self.ocp_version == "4.6":
            self.do_click(self.dep_loc["project_dropdown"])
            self.do_click(self.dep_loc[operator])

    def install_ocs_ui(self):
        """
        Install OCS via UI

        """
        if config.DEPLOYMENT.get("local_storage"):
            add_disk_for_vsphere_platform()
        self.install_local_storage_operator()
        self.create_catalog_source_yaml()
        self.install_ocs_operator()
        self.install_storage_cluster()


def ui_deployment_conditions():
    """
    Conditions for installing the OCS operator via UI

    return:
        bool: True if support UI deployment, False otherwise
    """
    platform = config.ENV_DATA["platform"]
    ocp_version = get_ocp_version()
    is_arbiter = config.DEPLOYMENT.get("arbiter_deployment")
    is_lso = config.DEPLOYMENT.get("local_storage")
    is_external = config.DEPLOYMENT["external_mode"]
    is_disconnected = config.DEPLOYMENT.get("disconnected")
    is_kms = config.DEPLOYMENT.get("kms_deployment")
    http_proxy, https_proxy, no_proxy = get_cluster_proxies()
    is_proxy = True if http_proxy else False

    try:
        locators[ocp_version]["deployment"]
    except KeyError as e:
        logger.info(
            f"OCS deployment via UI doesn't supported on ocp version {ocp_version}"
        )
        logger.error(e)
        return False

    if platform not in (constants.AWS_PLATFORM, constants.VSPHERE_PLATFORM):
        logger.info(f"OCS deployment via UI doesn't supported on platform {platform}")
        return False
    elif is_external or is_disconnected or is_proxy or is_kms or is_arbiter:
        logger.info(
            "OCS deployment via UI doesn't supported on "
            "external/disconnected/proxy/kms/arbiter"
        )
        return False
    elif platform == constants.AWS_PLATFORM and is_lso is True:
        logger.info("OCS deployment via UI doesn't supported on AWS-LSO")
        return False
    elif ocp_version == "4.6" and is_lso is True:
        logger.info("OCS deployment via UI doesn't supported on LSO-OCP4.6")
        return False
    else:
        return True
