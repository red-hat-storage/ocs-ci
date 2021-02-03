import logging
import tempfile
import time

from ocs_ci.ocs.ui.views import locators
from ocs_ci.ocs.ui.base_ui import PageNavigator
from ocs_ci.utility.utils import get_ocp_version, TimeoutSampler, run_cmd
from ocs_ci.utility import templating
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.framework import config
from ocs_ci.ocs import constants


logger = logging.getLogger(__name__)


class DeploymentUI(PageNavigator):
    """
    Deployment OCS Operator via User Interface

    """

    def __init__(self, driver):
        super().__init__(driver)
        ocp_version = get_ocp_version()
        self.dep_loc = locators[ocp_version]["deployment"]
        self.mode = "internal"
        self.storage_class_type = "thin_sc"
        self.osd_size = "512"
        self.is_wide_encryption = False
        self.is_class_encryption = False
        self.is_use_kms = False

    @property
    def select_mode(self):
        return self.mode

    @select_mode.setter
    def select_mode(self, mode):
        if not isinstance(mode, str):
            raise ValueError("mode arg must be a string")
        self.mode = mode

    @property
    def select_storage_class(self):
        return self.storage_class_type

    @select_storage_class.setter
    def select_storage_class(self, storage_class):
        if not isinstance(storage_class, str):
            raise ValueError("storage class arg must be a string")
        self.storage_class = storage_class

    @property
    def select_osd_size(self):
        return self.osd_size

    @select_osd_size.setter
    def select_osd_size(self, osd_size):
        if not isinstance(osd_size, str):
            raise ValueError("osd size arg must be a string")
        self.osd_size = osd_size

    @property
    def wide_encryption(self):
        return self.is_wide_encryption

    @wide_encryption.setter
    def wide_encryption(self, is_wide_encryption):
        if not isinstance(is_wide_encryption, bool):
            raise ValueError("is_wide_encryption arg must be a bool")
        self.is_wide_encryption = is_wide_encryption

    @property
    def class_encryption(self):
        return self.is_class_encryption

    @class_encryption.setter
    def class_encryption(self, is_class_encryption):
        if not isinstance(is_class_encryption, bool):
            raise ValueError("is_class_encryption arg must be a bool")
        self.is_class_encryption = is_class_encryption

    @property
    def select_service_name(self):
        return self.service_name

    @select_service_name.setter
    def select_service_name(self, service_name):
        if not isinstance(service_name, str):
            raise ValueError("service_name arg must be a string")
        self.service_name = service_name

    @property
    def use_kms(self):
        return self.is_use_kms

    @use_kms.setter
    def use_kms(self, is_use_kms):
        if not isinstance(is_use_kms, bool):
            raise ValueError("is_use_kms arg must be a bool")
        self.is_use_kms = is_use_kms

    @property
    def select_kms_address(self):
        return self.kms_address

    @select_kms_address.setter
    def select_kms_address(self, kms_address):
        if not isinstance(kms_address, str):
            raise ValueError("kms_address arg must be a string")
        self.kms_address = kms_address

    @property
    def select_kms_address_port(self):
        return self.kms_address_port

    @select_kms_address_port.setter
    def select_kms_address_port(self, kms_address_port):
        if not isinstance(kms_address_port, str):
            raise ValueError("kms_address_port arg must be a string")
        self.kms_address_port = kms_address_port

    @property
    def select_kms_token(self):
        return self.kms_token

    @select_kms_token.setter
    def select_kms_token(self, kms_token):
        if not isinstance(kms_token, str):
            raise ValueError("kms_token arg must be a string")
        self.kms_token = kms_token

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

    def install_storage_cluster(self):
        """
        Install Storage Cluster

        """
        self.navigate_operatorhub_page()
        self.navigate_installed_operators_page()

        logger.info("Search OCS operator installed")
        self.do_send_keys(
            locator=self.dep_loc["search_ocs_installed"],
            text="OpenShift Container Storage",
        )

        logger.info("Click on ocs operator on Installed Operators")
        self.do_click(locator=self.dep_loc["ocs_operator_installed"])

        logger.info("Click on Storage Cluster")
        self.do_click(locator=self.dep_loc["storage_cluster_tab"])

        logger.info("Click on Create Storage Cluster")
        self.refresh_page()
        self.do_click(locator=self.dep_loc["create_storage_cluster"])

        if self.mode == "internal":
            self.install_internal_cluster()
        else:
            raise ValueError(f"Not Support on {self.mode}")

    def install_internal_cluster(self):
        """
        Install Internal Cluster

        """
        logger.info("Click Internal")
        self.do_click(locator=self.dep_loc["internal_mode"])

        logger.info("Configure Storage Class (thin on vmware, gp2 on aws)")
        self.do_click(locator=self.dep_loc["storage_class_dropdown"])
        self.do_click(locator=self.dep_loc[self.storage_class_type])

        logger.info(f"Configure OSD Capacity {self.osd_size}")
        self.choose_expanded_mode(mode=True, locator=self.dep_loc["osd_size_dropdown"])
        self.do_click(locator=self.dep_loc[self.osd_size])

        logger.info("Select all worker nodes")
        self.select_checkbox_status(status=True, locator=self.dep_loc["all_nodes"])

        logger.info("Next on step 'Select capacity and nodes'")
        self.do_click(locator=self.dep_loc["next_capacity"])

        self.configure_encryption()

        self.configure_kms()

        logger.info("Click on Next on configure page")
        self.do_click(locator=self.dep_loc["next_on_configure"])

        logger.info("Create on Review and create page")
        self.do_click(locator=self.dep_loc["create_on_review"])

        logger.info("Sleep 10 second after click on 'create storage cluster'")
        time.sleep(10)

    def configure_encryption(self):
        """
        Configure Encryption

        """
        if self.is_wide_encryption or self.is_class_encryption:
            logger.info("Enable Encryption")
            self.select_checkbox_status(
                status=True, locator=self.dep_loc["enable_encryption"]
            )

        if self.is_wide_encryption:
            logger.info("Cluster-wide encryption")
            self.select_checkbox_status(
                status=True, locator=self.dep_loc["wide_encryption"]
            )

        if self.is_class_encryption:
            logger.info("Storage class encryption")
            self.select_checkbox_status(
                status=True, locator=self.dep_loc["class_encryption"]
            )

    def configure_kms(self):
        """
        Configure KMS

        """
        if self.is_use_kms:
            logger.info(f"kms service name: {self.service_name}")
            self.do_send_keys(
                text=self.service_name, locator=self.dep_loc["kms_service_name"]
            )

            logger.info(f"kms address: {self.kms_address}")
            self.do_send_keys(
                text=self.kms_address, locator=self.dep_loc["kms_address"]
            )

            logger.info(f"kms address port: {self.kms_address_port}")
            self.do_send_keys(
                text=self.kms_address_port, locator=self.dep_loc["kms_address_port"]
            )

            logger.info(f"kms_token: {self.kms_token}")
            self.do_send_keys(text=self.kms_token, locator=self.dep_loc["kms_token"])

    def verify_ocs_installation(self, timeout_install=300, sleep=20):
        """
        Verify OCS Installation

        timeout_install (int): Time in seconds to wait
        sleep (int): Sampling time in seconds

        """
        self.navigate_installed_operators_page()

        self.do_send_keys(
            locator=self.dep_loc["search_ocs_install"],
            text="OpenShift Container Storage",
        )
        sample = TimeoutSampler(
            timeout=timeout_install,
            sleep=sleep,
            func=self.check_element_text,
            expected_text="Succeeded",
        )
        if not sample.wait_for_func_status(result=True):
            logger.error(
                f"OCS Installation status is not Succeeded after {timeout_install} seconds"
            )
            raise TimeoutExpiredError

    def install_ocs_ui(self):
        """
        Install OCS via UI

        """
        self.create_catalog_source_yaml()
        self.install_ocs_operator()
        self.verify_ocs_installation()
        self.install_storage_cluster()
