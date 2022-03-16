import os
import logging
import time


from ocs_ci.ocs.exceptions import ACMClusterDeployException
from ocs_ci.ocs.ui.base_ui import BaseUI
from ocs_ci.ocs.ui.helpers_ui import format_locator
from ocs_ci.ocs.ui.views import locators
from ocs_ci.utility.utils import (
    get_ocp_version,
    get_running_cluster_id,
    expose_ocp_version,
    run_cmd,
)
from ocs_ci.ocs.constants import (
    PLATFORM_XPATH_MAP,
    ACM_PLATOFRM_VSPHERE_CRED_PREFIX,
    VSPHERE_CA_FILE_PATH,
    DATA_DIR,
    SSH_PRIV_KEY,
    SSH_PUB_KEY,
    ACM_OCP_RELEASE_IMG_URL_PREFIX,
    ACM_VSPHERE_NETWORK,
    ACM_CLUSTER_DEPLOY_TIMEOUT,
    ACM_CLUSTER_DEPLOYMENT_LABEL_KEY,
    ACM_CLUSTER_DEPLOYMENT_SECRET_TYPE_LABEL_KEY,
)
from ocs_ci.framework import config

log = logging.getLogger(__name__)


class AcmPageNavigator(BaseUI):
    """
    ACM Page Navigator Class

    """

    def __init__(self, driver):
        super().__init__(driver)
        self.ocp_version = get_ocp_version()
        self.acm_page_nav = locators[self.ocp_version]["acm_page"]

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

    def navigate_clusters_page(self):
        """
        Navigate to ACM Clusters Page

        """
        log.info("Navigate into Clusters Page")
        self.choose_expanded_mode(
            mode=True, locator=self.acm_page_nav["Infrastructure"]
        )
        self.do_click(locator=self.acm_page_nav["Clusters_page"])

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
        self.do_click(locator=self.acm_page_nav["Infrastructure_environments_page"])

    def navigate_applications_page(self):
        """
        Navigate to ACM Applications Page

        """
        log.info("Navigate into Applications Page")
        self.do_click(locator=self.acm_page_nav["Applications"])

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


class ACMOCPClusterDeployment(AcmPageNavigator):
    """
    Everything related to cluster creation through ACM goes here

    """

    def __init__(self, driver, deployment_platform, cluster_conf):
        super().__init__(driver)
        self.platform = deployment_platform
        self.cluster_conf = cluster_conf
        self.cluster_name = self.cluster_conf.ENV_DATA["cluster_name"]
        self.cluster_path = self.cluster_conf.ENV_DATA["cluster_path"]
        self.deploy_sync_mode = config.MULTICLUSTER.get("deploy_sync_mode", "async")
        self.deployment_status = None
        self.cluster_deploy_timeout = self.cluster_conf.ENV_DATA(
            "cluster_deploy_timeout", ACM_CLUSTER_DEPLOY_TIMEOUT
        )
        self.deployment_failed_reason = None
        self.deployment_start_time = 0

    def create_cluster_prereq(self):
        raise NotImplementedError("Child class has to implement this method")

    def navigate_create_clusters_page(self):
        # Navigate to Clusters page which has 'Create Cluster'/
        # 'Import Cluster' buttons
        # Here we click on "Create Cluster" and we will be in create cluster page
        self.navigate_clusters_page()
        self.do_click(locator=self.acm_page_nav["cc_create_cluster"])

    def click_next_button(self):
        self.do_click(self.acm_page_nav["cc_next_page_button"])

    def send_keys_multiple(self, key_val):
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
        self.do_click(locator=self.acm_page_nav[PLATFORM_XPATH_MAP[self.platform]])
        self.do_click(
            locator=self.acm_page_nav["cc_infrastructure_provider_creds_dropdown"]
        )
        credential = format_locator(
            self.acm_page_nav["cc_infrastructure_provider_creds_select_creds"],
            self.platform_credential_name,
        )
        self.do_click(locator=credential)

    def goto_cluster_details_page(self):
        self.navigate_clusters_page()
        locator = format_locator(self.acm_page_nav["cc_table_entry"], self.cluster_name)
        self.do_click(locator=locator)
        self.do_click(locator=self.acm_page_nav["cc_cluster_details_page"])

    def get_deployment_status(self):
        self.goto_cluster_details_page()
        if self.acm_cluster_status_creating():
            self.deployment_status = "creating"
        elif self.acm_cluster_status_ready():
            self.deployment_status = "ready"
        elif self.acm_cluster_status_failed():
            self.deployment_status = "failed"
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
        return

    def acm_cluster_status_failed(self, timeout=5):
        status_xpath = format_locator(
            (self.acm_page_nav["cc_cluster_status_page_status"], self.By.XPATH),
            "Failed",
        )
        return self.wait_until_expected_text_is_found(status_xpath, self.By.XPATH)

    def acm_cluster_status_ready(self, timeout=300):
        status_xpath = format_locator(
            (self.acm_page_nav["cc_cluster_status_page_status"], self.By.XPATH), "Ready"
        )
        return self.wait_until_expected_text_is_found(status_xpath, timeout=timeout)

    def acm_cluster_status_creating(self, timeout=300):
        status_xpath = self.acm_page_nav["cc_cluster_status_page_status_creating"]
        return self.check_element_presence(status_xpath, timeout)

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
            f"$(oc get secret -o name -n {self.cluster_name} "
            f"-l {ACM_CLUSTER_DEPLOYMENT_LABEL_KEY}={self.cluster_name} "
            f"-l {ACM_CLUSTER_DEPLOYMENT_SECRET_TYPE_LABEL_KEY}=kubeconfig)"
        )
        extract_cmd = (
            f"oc extract -n {self.cluster_name} "
            f"{get_kubeconf_secret_cmd} "
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


class ACMOCPPlatformVsphereIPI(ACMOCPClusterDeployment):
    """
    This class handles all behind the scene activities
    for cluster creation through ACM for vsphere platform

    """

    def __init__(self, driver, cluster_conf=None):
        super().__init__(driver=driver, platform="vsphere", cluster_conf=cluster_conf)
        self.platform_credential_name = cluster_conf.get(
            "platform_credential_name",
            f"{ACM_PLATOFRM_VSPHERE_CRED_PREFIX}{get_running_cluster_id()}",
        )
        # API VIP & Ingress IP
        self.ips = None
        self.vsphere_network = None

    def create_cluster_prereq(self):
        """
        Perform all prereqs before vsphere cluster creation from ACM

        """
        # Create vsphre credentials
        # Click on 'Add credential' in 'Infrastructure provider' page
        self.navigate_create_clusters_page()
        self.do_click(locator=self.acm_page_nav[PLATFORM_XPATH_MAP[self.platform]])

        # "Basic vsphere credential info"
        # 1. credential name
        # 2. Namespace
        # 3. Base DNS domain
        self.do_click(locator=self.acm_page_nav["cc_provider_credentials"], timeout=100)
        self.do_click(locator=self.acm_page_nav["cc_provider_creds_vsphere"])

        basic_cred_dict = {
            self.acm_page_nav[
                "cc_provider_creds_vsphere_cred_name"
            ]: self.platform_credential_name,
            self.acm_page_nav["cc_provider_creds_vsphere_cred_namespace"]: "default",
            self.acm_page_nav[
                "cc_provider_creds_vsphere_base_dns"
            ]: f"{self.cluster_conf.ENV_DATA['base_domain']}",
        }
        self.send_keys_multiple(basic_cred_dict)

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
        self.send_keys_multiple(vsphere_creds_dict)
        self.click_next_button()

        # Pull Secret and SSH
        # 1. Pull secret
        # 2. SSH Private key
        # 3. SSH Public key
        with open(os.path.join(DATA_DIR, "pull-secret"), "r") as fp:
            pull_secret = fp.read()

        with open(SSH_PUB_KEY, "r") as fp:
            ssh_pub_key = fp.read()

        with open(SSH_PRIV_KEY, "r") as fp:
            ssh_priv_key = fp.read()

        pull_secret_and_ssh = {
            self.acm_page_nav["cc_provider_creds_vsphere_pullsecret"]: f"{pull_secret}",
            self.acm_page_nav[
                "cc_provider_creds_vsphere_ssh_privkey"
            ]: f"{ssh_priv_key}",
            self.acm_page_nav["cc_provider_creds_vsphere_ssh_pubkey"]: f"{ssh_pub_key}",
        }
        self.send_keys_multiple(pull_secret_and_ssh)
        self.click_next_button()
        self.do_click(locator=self.acm_page_nav["cc_provider_creds_vsphere_add_button"])
        credential_table_entry = format_locator(
            self.acm_page_nav["cc_table_entry"], self.platform_credential_name
        )
        if not self.check_element_presence(credential_table_entry):
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
        self.ips = vmware.assign_ips(2)
        vmware.create_dns_records(self.ips)
        config.switch_ctx(prev_ctx)

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
        self.click_platform_and_credentials()
        self.click_next_button()
        self.fill_cluster_details_page()
        self.click_next_button()
        # For now we don't do anything in 'Node Pools' page
        self.click_next_button()
        self.fill_network_info()
        self.click_next_button()
        # Skip proxy for now
        self.click_next_button()
        # Skip Automation for now
        self.click_next_button()
        # We are at Review page
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
        vsphere_network = {
            self.acm_page_nav["cc_vsphere_network_name"]: self.vsphere_network,
            self.acm_page_nav["cc_api_vip"]: f"{self.ips[0]}",
            self.acm_page_nav["cc_ingress_vip"]: f"{self.ips[1]}",
        }
        self.send_keys_multiple(vsphere_network)

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
            self.acm_page_nav[
                "cc_base_dns_domain"
            ]: f"{self.cluster_conf.ENV_DATA['base_domain']}",
            self.acm_page_nav["cc_openshift_release_image"]: f"{release_img}",
        }
        self.send_keys_multiple(cluster_details)

    def get_ocp_release_img(self):
        vers = expose_ocp_version(self.cluster_conf.DEPLOYMENT["installer_version"])
        return f"{ACM_OCP_RELEASE_IMG_URL_PREFIX}{vers}"


class ACMOCPDeploymentFactory(object):
    def __init__(self):
        # All platform specific classes should have map here
        self.platform_map = {"vsphereipi": ACMOCPPlatformVsphereIPI}

    def get_platform_instance(self, driver, cluster_config):
        """
        Args:
            driver: selenium UI driver object
            cluster_config (dict): Cluster Config object
        """
        platform_deployment = (
            f"{cluster_config.ENV_DATA['platform']}"
            f"{cluster_config.ENV_DATA['deployment_type']}"
        )
        return self.platform_map[platform_deployment](driver, cluster_config)
