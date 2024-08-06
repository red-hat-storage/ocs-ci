import os
import logging
import time
import shlex
import subprocess

from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from ocs_ci.deployment import vmware
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import ACMClusterDeployException
from ocs_ci.ocs.ui.base_ui import BaseUI, SeleniumDriver
from ocs_ci.ocs.ui.helpers_ui import format_locator
from ocs_ci.ocs.ui.views import acm_ui_specific
from ocs_ci.utility.utils import (
    expose_ocp_version,
    run_cmd,
    get_running_acm_version,
)
from ocs_ci.ocs.constants import (
    ACM_CLUSTER_DESTROY_TIMEOUT,
    PLATFORM_XPATH_MAP,
    ACM_PLATOFRM_VSPHERE_CRED_PREFIX,
    VSPHERE_CA_FILE_PATH,
    DATA_DIR,
    ACM_OCP_RELEASE_IMG_URL_PREFIX,
    ACM_VSPHERE_NETWORK,
    ACM_CLUSTER_DEPLOY_TIMEOUT,
    ACM_CLUSTER_DEPLOYMENT_LABEL_KEY,
    ACM_CLUSTER_DEPLOYMENT_SECRET_TYPE_LABEL_KEY,
)
from ocs_ci.framework import config
from ocs_ci.utility.retry import retry
from ocs_ci.utility.ipam import IPAM


log = logging.getLogger(__name__)


class AcmPageNavigator(BaseUI):
    """
    ACM Page Navigator Class

    """

    def __init__(self):
        super().__init__()

    def navigate_welcome_page(self):
        """
        Navigate to ACM Welcome Page

        """
        log.info("Navigate into Home Page")
        self.choose_expanded_mode(mode=True, locator=self.acm_page_nav["Home"])
        self.do_click(locator=self.acm_page_nav["Welcome_page"])

    def navigate_overview_page(self):
        """
        Navigate to ACM Overview Page

        """
        log.info("Navigate into Overview Page")
        self.choose_expanded_mode(mode=True, locator=self.acm_page_nav["Home"])
        self.do_click(locator=self.acm_page_nav["Overview_page"])

    def navigate_clusters_page(self, timeout=120):
        """
        Navigate to ACM Clusters Page

        """
        log.info("Navigate into Clusters Page")
        self.check_element_presence(
            (
                self.acm_page_nav["Infrastructure"][1],
                self.acm_page_nav["Infrastructure"][0],
            ),
            timeout=timeout,
        )
        self.choose_expanded_mode(
            mode=True, locator=self.acm_page_nav["Infrastructure"]
        )
        self.do_click(
            locator=self.acm_page_nav["Clusters_page"],
            timeout=timeout,
            enable_screenshot=True,
            avoid_stale=True,
        )

    def navigate_bare_metal_assets_page(self):
        """
        Navigate to ACM Bare Metal Assets Page

        """
        log.info("Navigate into Bare Metal Assets Page")
        self.choose_expanded_mode(
            mode=True, locator=self.acm_page_nav["Infrastructure"]
        )
        self.do_click(locator=self.acm_page_nav["Bare_metal_assets_page"])

    def navigate_automation_page(self):
        """
        Navigate to ACM Automation Page

        """
        log.info("Navigate into Automation Page")
        self.choose_expanded_mode(
            mode=True, locator=self.acm_page_nav["Infrastructure"]
        )
        self.do_click(locator=self.acm_page_nav["Automation_page"])

    def navigate_infrastructure_env_page(self):
        """
        Navigate to ACM Infrastructure Environments Page

        """
        log.info("Navigate into Infrastructure Environments Page")
        self.choose_expanded_mode(
            mode=True, locator=self.acm_page_nav["Infrastructure"]
        )
        self.do_click(
            locator=self.acm_page_nav["Infrastructure_environments_page"],
            enable_screenshot=True,
        )

    def navigate_applications_page(self):
        """
        Navigate to ACM Applications Page

        """
        log.info("Navigate into Applications Page")
        self.do_click(
            locator=self.acm_page_nav["Applications"], timeout=120, avoid_stale=True
        )

    def navigate_governance_page(self):
        """
        Navigate to ACM Governance Page

        """
        log.info("Navigate into Governance Page")
        self.do_click(locator=self.acm_page_nav["Governance"])

    def navigate_credentials_page(self):
        """
        Navigate to ACM Credentials Page

        """
        log.info("Navigate into Governance Page")
        self.do_click(locator=self.acm_page_nav["Credentials"])

    def navigate_data_services(self):
        """
        Navigate to Data Services page on ACM UI, supported for ACM version 2.7 and above

        """
        log.info("Navigate to Data Policies page on ACM console")
        find_element = self.wait_until_expected_text_is_found(
            locator=self.acm_page_nav["data-services"],
            expected_text="Data Services",
            timeout=240,
        )
        if find_element:
            element = self.driver.find_element_by_xpath(
                "//button[normalize-space()='Data Services']"
            )
            if element.get_attribute("aria-expanded") == "false":
                self.do_click(locator=self.acm_page_nav["data-services"])
            data_policies = self.wait_until_expected_text_is_found(
                locator=self.acm_page_nav["data-policies"],
                expected_text="Data policies",
                timeout=240,
            )
            if data_policies:
                self.do_click(
                    locator=self.acm_page_nav["data-policies"], enable_screenshot=True
                )
        else:
            log.error("Couldn't navigate to data Services page on ACM UI")
            raise NoSuchElementException

    def navigate_from_ocp_to_acm_cluster_page(self):
        """
        For ACM version 2.7 and above we need to navigate from OCP
        console to ACM multicluster page

        """
        # There is a modal dialog box which appears as soon as we login
        # we need to click on close on that dialog box
        if self.check_element_presence(
            (
                self.acm_page_nav["modal_dialog_close_button"][1],
                self.acm_page_nav["modal_dialog_close_button"][0],
            ),
            timeout=200,
        ):
            self.do_click(self.acm_page_nav["modal_dialog_close_button"], timeout=300)

        if not self.check_element_presence(
            (
                self.acm_page_nav["click-local-cluster"][1],
                self.acm_page_nav["click-local-cluster"][0],
            ),
            timeout=300,
        ):
            log.error("local-cluster is not found, can not switch to ACM console")
            self.take_screenshot()
            raise NoSuchElementException
        self.do_click(self.acm_page_nav["click-local-cluster"])
        log.info("Successfully navigated to ACM console")
        self.take_screenshot()


class ACMOCPClusterDeployment(AcmPageNavigator):
    """
    Everything related to cluster creation through ACM goes here

    """

    def __init__(self, platform, cluster_conf):
        super().__init__()
        self.platform = platform
        self.cluster_conf = cluster_conf
        self.cluster_name = self.cluster_conf.ENV_DATA["cluster_name"]
        self.cluster_path = self.cluster_conf.ENV_DATA["cluster_path"]
        self.deploy_sync_mode = config.MULTICLUSTER.get("deploy_sync_mode", "async")
        self.deployment_status = None
        self.destroy_status = None
        self.cluster_deploy_timeout = self.cluster_conf.ENV_DATA.get(
            "cluster_deploy_timeout", ACM_CLUSTER_DEPLOY_TIMEOUT
        )
        self.cluster_destroy_timeout = self.cluster_conf.ENV_DATA.get(
            "cluster_destroy_timeout", ACM_CLUSTER_DESTROY_TIMEOUT
        )
        self.deployment_failed_reason = None
        self.destroy_failed_reason = None
        self.deployment_start_time = 0
        self.destroy_start_time = 0
        self.acm_version = "_".join(get_running_acm_version().split(".")[:2])
        print(self.acm_version)
        # Update acm version specific dictionary
        self.acm_page_nav.update(acm_ui_specific[f"acm_{self.acm_version}"])

    def create_cluster_prereq(self):
        raise NotImplementedError("Child class has to implement this method")

    def navigate_create_clusters_page(self):
        # Navigate to Clusters page which has 'Create Cluster'/
        # 'Import Cluster' buttons
        # Here we click on "Create Cluster" and we will be in create cluster page
        while True:
            self.navigate_clusters_page()
            log.info("Clicking on 'CreateCluster'")
            # Because of weird selenium behaviour we are checking
            # for CreateCluster button in 3 different ways
            # 1. CreateCluster button
            # 2. CreateCluster button with index xpath
            # 3. Checking url, which should end with 'create-cluster'
            if not self.check_element_presence(
                (By.XPATH, self.acm_page_nav["cc_create_cluster"][0]), timeout=60
            ):
                log.error("Create cluster button not found")
                raise ACMClusterDeployException("Can't continue with deployment")
            log.info("check 1:Found create cluster button")
            if not self.check_element_presence(
                (By.XPATH, self.acm_page_nav["cc_create_cluster_index_xpath"][0]),
                timeout=300,
            ):
                log.error("Create cluster button not found")
                raise ACMClusterDeployException("Can't continue with deployment")
            log.info("check 2:Found create cluster by index path")
            time.sleep(10)
            self.do_click(locator=self.acm_page_nav["cc_create_cluster"], timeout=100)
            time.sleep(20)
            if self.driver.current_url.endswith(
                self.acm_page_nav["cc_create_cluster_endswith_url"]
            ):
                break

    def click_next_button(self):
        self.do_click(self.acm_page_nav["cc_next_page_button"])

    def fill_multiple_textbox(self, key_val):
        """
        In a page if we want to fill multiple text boxes we can use
        this function which iteratively fills in values from the dictionary parameter

        key_val (dict): keys corresponds to the xpath of text box, value corresponds
            to the value to be filled in

        """
        for xpath, value in key_val.items():
            self.do_send_keys(locator=xpath, text=value)

    def click_platform_and_credentials(self):
        self.navigate_create_clusters_page()
        self.do_click(
            locator=self.acm_page_nav[PLATFORM_XPATH_MAP[self.platform]], timeout=100
        )
        self.do_click(
            locator=self.acm_page_nav["cc_infrastructure_provider_creds_dropdown"]
        )
        credential = format_locator(
            self.acm_page_nav["cc_infrastructure_provider_creds_select_creds"],
            self.platform_credential_name,
        )
        self.do_click(locator=credential)

    @retry(ACMClusterDeployException, tries=3, delay=10, backoff=1)
    def goto_cluster_details_page(self):
        self.navigate_clusters_page()
        locator = format_locator(self.acm_page_nav["cc_table_entry"], self.cluster_name)
        self.do_click(locator=locator)
        self.do_click(locator=self.acm_page_nav["cc_cluster_details_page"], timeout=100)
        self.choose_expanded_mode(True, self.acm_page_nav["cc_details_toggle_icon"])

    def get_deployment_status(self):
        self.goto_cluster_details_page()
        if self.acm_cluster_status_failed(timeout=2):
            self.deployment_status = "failed"
        elif self.acm_cluster_status_ready(timeout=2):
            self.deployment_status = "ready"
        elif self.acm_cluster_status_creating(timeout=2):
            self.deployment_status = "creating"
        else:
            self.deployment_status = "unknown"

        elapsed_time = int(time.time() - self.deployment_start_time)
        if elapsed_time > self.cluster_deploy_timeout:
            if self.deployment_status == "creating":
                self.deployment_status = "failed"
                self.deployment_failed_reason = "deploy_timeout"

    def wait_for_cluster_create(self):

        # Wait for status creating
        staus_check_timeout = 300
        while (
            not self.acm_cluster_status_ready(staus_check_timeout)
            and self.cluster_deploy_timeout >= 1
        ):
            self.cluster_deploy_timeout -= staus_check_timeout
            if self.acm_cluster_status_creating():
                log.info(f"Cluster {self.cluster_name} is in 'Creating' phase")
            else:
                self.acm_bailout_if_failed()
        if self.acm_cluster_status_ready():
            log.info(
                f"Cluster create successful, Cluster {self.cluster_name} is in 'Ready' state"
            )

    def acm_bailout_if_failed(self):
        if self.acm_cluster_status_failed():
            raise ACMClusterDeployException("Deployment is in 'FAILED' state")

    def acm_cluster_status_failed(self, timeout=5):
        return self.check_element_presence(
            (
                self.acm_page_nav["cc_cluster_status_page_status_failed"][1],
                self.acm_page_nav["cc_cluster_status_page_status_failed"][0],
            ),
            timeout=timeout,
        )

    def acm_cluster_status_ready(self, timeout=120):
        return self.check_element_presence(
            (
                self.acm_page_nav["cc_cluster_status_page_status_ready"][1],
                self.acm_page_nav["cc_cluster_status_page_status_ready"][0],
            ),
            timeout=timeout,
        )

    def acm_cluster_status_creating(self, timeout=120):
        return self.check_element_presence(
            (
                self.acm_page_nav["cc_cluster_status_page_status_creating"][1],
                self.acm_page_nav["cc_cluster_status_page_status_creating"][0],
            ),
            timeout=timeout,
        )

    def download_cluster_conf_files(self):
        """
        Download install-config and kubeconfig to cluster dir

        """
        if not os.path.exists(os.path.expanduser(f"{self.cluster_path}")):
            os.mkdir(os.path.expanduser(f"{self.cluster_path}"))

        # create auth dir inside cluster dir
        auth_dir = os.path.join(os.path.expanduser(f"{self.cluster_path}"), "auth")
        if not os.path.exists(auth_dir):
            os.mkdir(auth_dir)

        self.download_kubeconfig(auth_dir)

    def download_kubeconfig(self, authdir):
        get_kubeconf_secret_cmd = (
            f"oc get secret -o name -n {self.cluster_name} "
            f"-l {ACM_CLUSTER_DEPLOYMENT_LABEL_KEY}={self.cluster_name} "
            f"-l {ACM_CLUSTER_DEPLOYMENT_SECRET_TYPE_LABEL_KEY}=kubeconfig"
        )
        secret_name = run_cmd(get_kubeconf_secret_cmd)
        extract_cmd = (
            f"oc extract -n {self.cluster_name} "
            f"{secret_name} "
            f"--to={authdir} --confirm"
        )
        run_cmd(extract_cmd)
        if not os.path.exists(os.path.join(authdir, "kubeconfig")):
            raise ACMClusterDeployException("Could not find the kubeconfig")

    def create_cluster(self, cluster_config=None):
        """
        Create cluster using ACM UI

        Args:
            cluster_config (Config): framework.Config object of complete configuration required
                for deployment

        """
        raise NotImplementedError("Child class should implement this function")

    def destroy_cluster(self):
        """
        ACM UI based destroy cluster,
        select the cluster from UI, click on destroy

        """
        # Navigate to Clusters page
        self.navigate_clusters_page()
        # Click on cluster name from the table
        locator = format_locator(self.acm_page_nav["cc_table_entry"], self.cluster_name)
        self.do_click(locator=locator)
        # Click on 'Actions' dropdown
        action_dropdown = format_locator(
            self.acm_page_nav["cc_delete_cluster_action_dropdown"],
            f"{self.cluster_name}-actions",
        )
        self.do_click(action_dropdown, timeout=300)
        # From the 'Actions' dropdown click on 'Destroy Cluster'
        self.do_click(locator=self.acm_page_nav["cc_destroy_cluster"], timeout=300)
        # A confirmation window pops up
        # Fill cluster name in the confirmation window's text box
        self.do_click(
            self.acm_page_nav["cc_destroy_cluster_confirm_textbox"], timeout=300
        )
        self.do_send_keys(
            self.acm_page_nav["cc_destroy_cluster_confirm_textbox"], self.cluster_name
        )
        # Click on destroy button
        self.do_click(self.acm_page_nav["cc_destroy_button"], timeout=300)
        loc = format_locator(
            self.acm_page_nav["cc_cluster_being_destroyed_heading"], self.cluster_name
        )
        if not self.check_element_presence(locator=(By.XPATH, loc[0]), timeout=600):
            raise ACMClusterDeployException(
                "Something went wrong with destroy action "
                f"for the cluster {self.cluster_name}"
            )
        self.destroy_status = "Destroying"
        self.destroy_start_time = time.time()

    def get_destroy_status(self):
        """
        Return the current status of destroy operation

        """
        self.navigate_clusters_page(timeout=300)
        time.sleep(10)
        locator = format_locator(self.acm_page_nav["cc_table_entry"], self.cluster_name)
        if not self.check_element_presence((locator[1], locator[0]), timeout=300):
            # Cluster deletion has happened
            self.destroy_status = "Done"
        else:
            self.do_click(locator=locator, timeout=300)
            loc = format_locator(
                self.acm_page_nav["cc_cluster_being_destroyed_heading"],
                self.cluster_name,
            )
            if not self.check_element_presence(
                locator=(By.XPATH, loc[0]),
                timeout=300,
            ):
                log.error("Cluster destroy status msg missing")
                self.destroy_status = "Failed"
            else:
                elapsed_time = int(time.time() - self.destroy_start_time)
                if elapsed_time > self.cluster_destroy_timeout:
                    self.destroy_status = "Failed"
                    self.destroy_failed_reason = "timeout"

    def post_destroy_ops(self):
        """
        Post destroy ops should be implemented by child classes because
        post destroy ops are mostly specific to the platform

        """
        raise NotImplementedError(
            "Platform specific class has to implement post_destroy_ops"
        )


class ACMOCPPlatformVsphereIPI(ACMOCPClusterDeployment):
    """
    This class handles all behind the scene activities
    for cluster creation through ACM for vsphere platform

    """

    def __init__(self, cluster_conf=None):
        super().__init__(platform="vsphere", cluster_conf=cluster_conf)
        self.platform_credential_name = cluster_conf.ENV_DATA.get(
            "platform_credential_name",
            f"{ACM_PLATOFRM_VSPHERE_CRED_PREFIX}{self.cluster_name}",
        )
        # API VIP & Ingress IP
        self.ips = None
        self.nvips = 2
        self.vsphere_network = None

    def create_cluster_prereq(self, timeout=600):
        """
        Perform all prereqs before vsphere cluster creation from ACM

        Args:
            timeout (int): Timeout for any UI operations

        """
        # Create vsphre credentials
        # Click on 'Add credential' in 'Infrastructure provider' page
        self.navigate_create_clusters_page()
        self.refresh_page()
        hard_timeout = config.ENV_DATA.get("acm_ui_hard_deadline", 1200)
        remaining = hard_timeout
        while True:
            ret = self.check_element_presence(
                (By.XPATH, self.acm_page_nav[PLATFORM_XPATH_MAP[self.platform]][0]),
                timeout=300,
            )
            if ret:
                log.info("Found platform icon")
                break
            else:
                if remaining < 0:
                    raise TimeoutException("Timedout while waiting for platform icon")
                else:
                    remaining -= timeout
                    self.navigate_create_clusters_page()
                    self.refresh_page()

        self.do_click(
            locator=self.acm_page_nav[PLATFORM_XPATH_MAP[self.platform]], timeout=100
        )

        # "Basic vsphere credential info"
        # 1. credential name
        # 2. Namespace
        # 3. Base DNS domain
        self.do_click(locator=self.acm_page_nav["cc_provider_credentials"], timeout=100)
        parent_tab = self.driver.current_window_handle
        tabs = self.driver.window_handles
        self.driver.switch_to.window(tabs[1])
        self.do_click(locator=self.acm_page_nav["cc_provider_creds_vsphere"])

        basic_cred_dict = {
            self.acm_page_nav[
                "cc_provider_creds_vsphere_cred_name"
            ]: self.platform_credential_name,
            self.acm_page_nav[
                "cc_provider_creds_vsphere_base_dns"
            ]: f"{self.cluster_conf.ENV_DATA['base_domain']}",
        }
        self.fill_multiple_textbox(basic_cred_dict)
        # Credential Namespace is not a text box but a dropdown
        self.do_click(self.acm_page_nav["cc_provider_creds_vsphere_cred_namespace"])
        self.do_click(self.acm_page_nav["cc_provider_creds_default_namespace"])

        # click on 'Next' button at the bottom
        self.click_next_button()

        # Detailed VMWare credentials section
        # 1. vCenter server
        # 2. vCenter username
        # 3. vCenter password
        # 4. cVenter root CA certificate
        # 5. vSphere cluster name
        # 6. vSphere datacenter
        # 7. vSphere default  Datastore
        with open(VSPHERE_CA_FILE_PATH, "r") as fp:
            vsphere_ca = fp.read()
        vsphere_creds_dict = {
            self.acm_page_nav[
                "cc_provider_creds_vsphere_vcenter_server"
            ]: f"{self.cluster_conf.ENV_DATA['vsphere_server']}",
            self.acm_page_nav[
                "cc_provider_creds_vsphere_username"
            ]: f"{self.cluster_conf.ENV_DATA['vsphere_user']}",
            self.acm_page_nav[
                "cc_provider_creds_vsphere_password"
            ]: f"{self.cluster_conf.ENV_DATA['vsphere_password']}",
            self.acm_page_nav["cc_provider_creds_vsphere_rootca"]: f"{vsphere_ca}",
            self.acm_page_nav[
                "cc_provider_creds_vsphere_clustername"
            ]: f"{self.cluster_conf.ENV_DATA['vsphere_cluster']}",
            self.acm_page_nav[
                "cc_provider_creds_vsphere_dc"
            ]: f"{self.cluster_conf.ENV_DATA['vsphere_datacenter']}",
            self.acm_page_nav[
                "cc_provider_creds_vsphere_datastore"
            ]: f"{self.cluster_conf.ENV_DATA['vsphere_datastore']}",
        }
        self.fill_multiple_textbox(vsphere_creds_dict)
        self.click_next_button()
        # Skip 2 sections
        # "Configuration for disconnected installation" and "Proxy"
        for i in range(2):
            self.click_next_button()

        # Pull Secret and SSH
        # 1. Pull secret
        # 2. SSH Private key
        # 3. SSH Public key
        pull_secret_str = f"cat {os.path.join(DATA_DIR, 'pull-secret')}"
        jq_trimmed = "jq -c"
        json_out = subprocess.Popen(
            shlex.split(pull_secret_str), stdout=subprocess.PIPE
        )
        out = subprocess.Popen(
            shlex.split(jq_trimmed), stdin=json_out.stdout, stdout=subprocess.PIPE
        )
        pull_secret = out.communicate()[0].decode()

        ssh_pub_key_path = os.path.expanduser(self.cluster_conf.DEPLOYMENT["ssh_key"])
        ssh_priv_key_path = os.path.expanduser(
            self.cluster_conf.DEPLOYMENT["ssh_key_private"]
        )

        with open(ssh_pub_key_path, "r") as fp:
            ssh_pub_key = fp.read()

        with open(ssh_priv_key_path, "r") as fp:
            ssh_priv_key = fp.read()

        pull_secret_and_ssh = {
            self.acm_page_nav["cc_provider_creds_vsphere_pullsecret"]: f"{pull_secret}",
            self.acm_page_nav[
                "cc_provider_creds_vsphere_ssh_privkey"
            ]: f"{ssh_priv_key}",
            self.acm_page_nav["cc_provider_creds_vsphere_ssh_pubkey"]: f"{ssh_pub_key}",
        }
        self.fill_multiple_textbox(pull_secret_and_ssh)
        self.click_next_button()
        self.do_click(locator=self.acm_page_nav["cc_provider_creds_vsphere_add_button"])
        # Go to credentials tab
        self.do_click(locator=self.acm_page_nav["Credentials"])
        credential_table_entry = format_locator(
            self.acm_page_nav["cc_table_entry"], self.platform_credential_name
        )
        if not self.check_element_presence(
            (By.XPATH, credential_table_entry[0]), timeout=20
        ):
            raise ACMClusterDeployException("Could not create credentials for vsphere")
        else:
            log.info(
                f"vsphere credential successfully created {self.platform_credential_name}"
            )
        # Get the ips in prereq itself
        from ocs_ci.deployment import vmware

        # Switch context to cluster which we are about to create
        prev_ctx = config.cur_index
        config.switch_ctx(self.cluster_conf.MULTICLUSTER["multicluster_index"])
        self.ips = vmware.assign_ips(self.nvips)
        vmware.create_dns_records(self.ips)
        config.switch_ctx(prev_ctx)
        self.driver.close()
        self.driver.switch_to.window(parent_tab)
        self.driver.switch_to.default_content()

    def create_cluster(self):
        """
        This function navigates through following pages in the UI
        1. Cluster details
        2. Node poools
        3. Networks
        4. Proxy
        5. Automation
        6. Review

        Raises:
            ACMClusterDeployException: If deployment failed for the cluster

        """
        self.navigate_create_clusters_page()
        self.click_platform_and_credentials()
        self.click_next_button()
        self.fill_cluster_details_page()
        self.click_next_button()
        # For now we don't do anything in 'Node Pools' page
        self.click_next_button()
        self.fill_network_info()
        self.click_next_button()
        # Skip proxy, disconnected install  and Automation for now
        for i in range(3):
            self.click_next_button()
        # We are at Review page
        self.do_click(
            locator=self.acm_page_nav["cc_deployment_yaml_toggle_button"], timeout=120
        )
        # Edit pod network if required
        if self.cluster_conf.ENV_DATA.get("cluster_network_cidr"):
            self.do_click(locator=self.acm_page_nav["cc_install_config_tab"])
            time.sleep(2)
            self.add_different_pod_network()
        # Click on create
        self.do_click(locator=self.acm_page_nav["cc_create_button"])
        self.deployment_start_time = time.time()
        # We will be redirect to 'Details' page which has cluster deployment progress
        if self.deploy_sync_mode == "sync":
            try:
                self.wait_for_cluster_create()
            except ACMClusterDeployException:
                log.error(
                    f"Failed to create OCP cluster {self.cluster_conf.ENV_DATA['cluster_name']}"
                )
                raise
            # Download kubeconfig and install-config file
            self.download_cluster_conf_files()
        else:
            # Async mode of deployment, so just return to caller
            # we will just wait for status 'Creating' and then return
            if not self.acm_cluster_status_creating(timeout=600):
                raise ACMClusterDeployException(
                    f"Cluster {self.cluster_name} didn't reach 'Creating' phase"
                )
            self.deployment_status = "Creating"
            return

    def add_different_pod_network(self):
        """
        Edit online cluster yaml to add network info

        """

        def _reset():
            for device in actions.w3c_actions.devices:
                device.clear_actions()

        self.driver.execute_script(
            "return document.querySelector('div.yamlEditorContainer')"
        )
        actions = ActionChains(self.driver)
        yaml_first_line = "apiVersion: v1"
        for _ in range(0, 3):
            actions.send_keys(Keys.TAB).perform()
            time.sleep(1)
            _reset()
        for _ in range(len(yaml_first_line)):
            actions.send_keys(Keys.ARROW_RIGHT).perform()
            time.sleep(1)
            # Ugly code required,Otherwise every key sent will be in a
            # queue upon perform() all the keys in the queue will
            # be sent to yaml editor
            _reset()
        actions.send_keys(Keys.ENTER).perform()
        time.sleep(1)
        _reset()

        cluster_network = (
            f"networking:\n  clusterNetwork:\n  - cidr: "
            f"{self.cluster_conf.ENV_DATA['cluster_network_cidr']}\n"
            f"  hostPrefix: 23\n"
        )
        actions.send_keys(cluster_network).perform()
        _reset()
        for _ in range(0, 2):
            actions.send_keys(Keys.BACK_SPACE).perform()
            _reset()
        service_network = (
            f"serviceNetwork:\n  - {self.cluster_conf.ENV_DATA['service_network_cidr']}"
        )
        actions.send_keys(service_network).perform()

    def fill_network_info(self):
        """
        We need to fill following network info
        1. vSphere network name
        2. API VIP
        3. Ingress VIP
        """
        self.vsphere_network = self.cluster_conf.ENV_DATA.get(
            "vm_network", ACM_VSPHERE_NETWORK
        )
        self.do_click(self.acm_page_nav["cc_vsphere_network_name"])
        self.do_send_keys(
            self.acm_page_nav["cc_vsphere_network_name"], self.vsphere_network
        )
        # Chrome has a weird problem of trimming the whitespace
        # Suppose if network name is 'VM Network', when we put this text
        # in text box it automatically becomes 'VMNetwork', hence we need to take
        # care
        ele = self.driver.find_element(
            By.XPATH, self.acm_page_nav["cc_vsphere_network_name"][0]
        )
        remote_text = ele.get_property("value")
        if remote_text != self.vsphere_network:
            # Check if we have white space char
            # in network name
            try:
                index = self.vsphere_network.index(constants.SPACE)
                left_shift_offset = len(remote_text) - index
                self.do_send_keys(
                    self.acm_page_nav["cc_vsphere_network_name"],
                    f"{left_shift_offset * Keys.ARROW_LEFT}{constants.SPACE}",
                )
            except ValueError:
                raise ACMClusterDeployException(
                    "Weird browser behaviour, Not able to provide vsphere network info"
                )

        vsphere_network = {
            self.acm_page_nav["cc_api_vip"]: f"{self.ips[0]}",
            self.acm_page_nav["cc_ingress_vip"]: f"{self.ips[1]}",
        }
        self.fill_multiple_textbox(vsphere_network)

    def fill_cluster_details_page(self):
        """
        Fill in following details in "Cluster details" page
        1. Cluster name
        2. Base DNS domain
        3. Release image

        """
        release_img = self.get_ocp_release_img()
        cluster_details = {
            self.acm_page_nav[
                "cc_cluster_name"
            ]: f"{self.cluster_conf.ENV_DATA['cluster_name']}",
            self.acm_page_nav["cc_openshift_release_image"]: f"{release_img}",
        }
        self.fill_multiple_textbox(cluster_details)

    def get_ocp_release_img(self):
        vers = expose_ocp_version(self.cluster_conf.DEPLOYMENT["installer_version"])
        return f"{ACM_OCP_RELEASE_IMG_URL_PREFIX}:{vers}"

    def post_destroy_ops(self):
        """
        Post destroy ops includes
        1. Deleting DNS entries
        2. Freeing the ips assigned

        """
        prev_ctx = config.cur_index
        config.switch_ctx(self.cluster_conf.MULTICLUSTER["multicluster_index"])
        vmware.delete_dns_records()
        ipam = IPAM(appiapp="address")
        hosts = [
            f"{config.ENV_DATA.get('cluster_name')}-{i}" for i in range(self.nvips)
        ]
        ipam.release_ips(hosts)
        config.switch_ctx(prev_ctx)


class ACMOCPDeploymentFactory(object):
    def __init__(self):
        # All platform specific classes should have map here
        # For now point UPI to IPI deployment, because all DC credential
        # file have UPI entries
        self.platform_map = {
            "vsphereipi": ACMOCPPlatformVsphereIPI,
            "vsphereupi": ACMOCPPlatformVsphereIPI,
        }

    def get_platform_instance(self, cluster_config):
        """
        Args:
            cluster_config (dict): Cluster Config object
        """
        platform_deployment = (
            f"{cluster_config.ENV_DATA['platform']}"
            f"{cluster_config.ENV_DATA['deployment_type']}"
        )
        return self.platform_map[platform_deployment](SeleniumDriver(), cluster_config)
