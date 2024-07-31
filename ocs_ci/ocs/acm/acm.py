import logging
import time
import os
import tempfile
import requests

from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    NoSuchElementException,
)
from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.ocs import constants
from ocs_ci.ocs.acm.acm_constants import (
    ACM_NAMESPACE,
    ACM_MANAGED_CLUSTERS,
    ACM_PAGE_TITLE,
    ACM_2_7_MULTICLUSTER_URL,
    ACM_PAGE_TITLE_2_7_ABOVE,
)
from ocs_ci.ocs.ocp import OCP, get_ocp_url
from ocs_ci.framework import config
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.ocs.ui.helpers_ui import format_locator
from ocs_ci.ocs.utils import get_non_acm_cluster_config, get_primary_cluster_config
from ocs_ci.utility.utils import (
    TimeoutSampler,
    get_ocp_version,
    get_running_acm_version,
    string_chunkify,
    run_cmd,
)
from ocs_ci.ocs.ui.acm_ui import AcmPageNavigator
from ocs_ci.ocs.ui.base_ui import (
    login_ui,
    SeleniumDriver,
)
from ocs_ci.utility.version import compare_versions
from ocs_ci.utility import version
from ocs_ci.ocs.exceptions import (
    ACMClusterImportException,
    UnexpectedDeploymentConfiguration,
)
from ocs_ci.utility import templating
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.helpers.helpers import create_project

log = logging.getLogger(__name__)


class AcmAddClusters(AcmPageNavigator):
    """
    ACM Page Navigator Class

    """

    def __init__(self):
        super().__init__()
        self.page_nav = self.acm_page_nav
        self.driver = SeleniumDriver()

    def import_cluster_ui(self, cluster_name, kubeconfig_location):
        """

        Args:
            cluster_name (str): cluster name to import
            kubeconfig_location (str): kubeconfig file location of imported cluster

        """
        # There is a modal dialog box which appears as soon as we login
        # we need to click on close on that dialog box
        try:
            if self.check_element_presence(
                (
                    self.acm_page_nav["modal_dialog_close_button"][1],
                    self.acm_page_nav["modal_dialog_close_button"][0],
                ),
                timeout=100,
            ):
                self.do_click(
                    self.acm_page_nav["modal_dialog_close_button"], timeout=100
                )
        except Exception as e:
            log.warning(f"Modal dialog not found: {e}")

        if not self.check_element_presence(
            (By.XPATH, self.acm_page_nav["Import_cluster"][0]), timeout=600
        ):
            raise ACMClusterImportException("Import button not found")
        self.do_click(self.acm_page_nav["Import_cluster"], timeout=1600)
        log.info("Clicked on Import cluster")
        self.wait_for_endswith_url("import", timeout=600)

        self.do_send_keys(
            self.page_nav["Import_cluster_enter_name"], text=f"{cluster_name}"
        )
        self.do_click(self.page_nav["Import_mode"])
        self.do_click(self.page_nav["choose_kubeconfig"])
        log.info(f"Copying Kubeconfig {kubeconfig_location}")
        kubeconfig_to_import = copy_kubeconfig(kubeconfig_location)
        for line in kubeconfig_to_import:
            if len(line) > 100:
                for chunk in string_chunkify(line, 100):
                    self.do_send_keys(self.page_nav["Kubeconfig_text"], text=f"{chunk}")
            else:
                self.do_send_keys(self.page_nav["Kubeconfig_text"], text=f"{line}")
            time.sleep(2)
        # With ACM2.6 there will be 1 more page
        # 1. Automation
        # So we have to click 'Next' button
        acm_version_str = ".".join(get_running_acm_version().split(".")[:2])
        if compare_versions(f"{acm_version_str} >= 2.6"):
            for i in range(2):
                self.do_click(locator=self.page_nav["cc_next_page_button"], timeout=10)
        log.info(f"Submitting import of {cluster_name}")
        self.do_click(self.page_nav["Submit_import"], timeout=600)

    def import_cluster(self, cluster_name, kubeconfig_location):
        """
        Import cluster using UI

        Args:
            cluster_name: (str): cluster name to import
            kubeconfig_location: (str): kubeconfig location
        Returns:
            None, but exits if sample object is not None using TimeoutSampler

        """

        self.import_cluster_ui(
            cluster_name=cluster_name, kubeconfig_location=kubeconfig_location
        )
        for sample in TimeoutSampler(
            timeout=450,
            sleep=60,
            func=validate_cluster_import,
            cluster_name=cluster_name,
        ):
            if sample:
                log.info(f"Cluster: {cluster_name} successfully imported")
                self.navigate_clusters_page()
                return
            else:
                log.error(f"import of cluster: {cluster_name} failed")

    def install_submariner_ui(self, globalnet=True):
        """
        Installs the Submariner on the ACM Hub cluster and expects 2 OCP clusters to be already imported
        on the Hub Cluster to create a link between them

        Args:
            globalnet (bool): Globalnet is set to True by default for ODF versions greater than or equal to 4.13

        """
        ocs_version = version.get_semantic_ocs_version_from_config()

        cluster_env = get_clusters_env()
        primary_index = get_primary_cluster_config().MULTICLUSTER["multicluster_index"]
        secondary_index = [
            s.MULTICLUSTER["multicluster_index"]
            for s in get_non_acm_cluster_config()
            if s.MULTICLUSTER["multicluster_index"] != primary_index
        ][0]
        # submariner catalogsource creation
        if config.ENV_DATA.get("submariner_release_type") == "unreleased":
            submariner_downstream_unreleased = templating.load_yaml(
                constants.SUBMARINER_DOWNSTREAM_UNRELEASED
            )
            # Update catalog source
            submariner_full_url = "".join(
                [
                    constants.SUBMARINER_DOWNSTREAM_UNRELEASED_BUILD_URL,
                    config.ENV_DATA["submariner_version"],
                ]
            )

            version_tag = config.ENV_DATA.get("submariner_unreleased_image", None)
            if version_tag is None:
                resp = requests.get(submariner_full_url, verify=False)
                raw_msg = resp.json()["raw_messages"]
                version_tag = raw_msg[0]["msg"]["pipeline"]["index_image"][
                    f"v{get_ocp_version()}"
                ].split(":")[1]
            submariner_downstream_unreleased["spec"]["image"] = ":".join(
                [constants.SUBMARINER_BREW_REPO, version_tag]
            )
            submariner_data_yaml = tempfile.NamedTemporaryFile(
                mode="w+", prefix="submariner_downstream_unreleased", delete=False
            )
            templating.dump_data_to_temp_yaml(
                submariner_downstream_unreleased, submariner_data_yaml.name
            )
            old_ctx = config.cur_index
            for cluster in get_non_acm_cluster_config():
                config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
                run_cmd(f"oc create -f {submariner_data_yaml.name}", timeout=300)
            config.switch_ctx(old_ctx)

        cluster_name_a = cluster_env.get(f"cluster_name_{primary_index}")
        cluster_name_b = cluster_env.get(f"cluster_name_{secondary_index}")
        self.navigate_clusters_page()
        self.page_has_loaded(retries=15, sleep_time=5)
        self.do_click(locator=self.acm_page_nav["Clusters_page"])
        log.info("Click on Cluster sets")
        self.do_click(self.page_nav["cluster-sets"])
        self.page_has_loaded(retries=15, sleep_time=5)
        log.info("Click on Create cluster set")
        self.do_click(self.page_nav["create-cluster-set"])
        global cluster_set_name
        cluster_set_name = create_unique_resource_name("submariner", "clusterset")
        log.info(f"Send Cluster set name '{cluster_set_name}'")
        self.do_send_keys(self.page_nav["cluster-set-name"], text=cluster_set_name)
        log.info("Click on Create")
        self.do_click(self.page_nav["click-create"], enable_screenshot=True)
        time.sleep(1)
        log.info("Click on Manage resource assignments")
        self.do_click(
            self.page_nav["click-manage-resource-assignments"], enable_screenshot=True
        )

        log.info(f"Search and select cluster '{cluster_name_a}'")
        self.do_send_keys(self.page_nav["search-cluster"], text=cluster_name_a)
        self.do_click(self.page_nav["select-first-checkbox"], enable_screenshot=True)
        log.info("Clear search by clicking on cross mark")
        self.do_click(self.page_nav["clear-search"])
        log.info(f"Search and select cluster '{cluster_name_b}'")
        self.do_send_keys(self.page_nav["search-cluster"], text=cluster_name_b)
        self.do_click(self.page_nav["select-first-checkbox"], enable_screenshot=True)
        log.info("Clear search by clicking on cross mark [2]")
        self.do_click(self.page_nav["clear-search"])
        log.info("Click on 'Review'")
        self.do_click(self.page_nav["review-btn"], enable_screenshot=True)
        log.info("Click on 'Save' to confirm the changes")
        self.do_click(self.page_nav["confirm-btn"], enable_screenshot=True)
        time.sleep(3)
        log.info("Click on 'Submariner add-ons' tab")
        self.do_click(self.page_nav["submariner-tab"])
        log.info("Click on 'Install Submariner add-ons' button")
        self.do_click(
            self.page_nav["install-submariner-btn"],
            enable_screenshot=True,
            avoid_stale=True,
        )
        log.info("Click on 'Target clusters'")
        self.do_click(self.page_nav["target-clusters"])
        log.info(f"Select 1st cluster which is {cluster_name_a}")
        self.do_click(
            format_locator(self.page_nav["cluster-name-selection"], cluster_name_a)
        )
        log.info(f"Select 2nd cluster which is {cluster_name_b}")
        self.do_click(
            format_locator(self.page_nav["cluster-name-selection"], cluster_name_b),
            enable_screenshot=True,
        )
        if ocs_version >= version.VERSION_4_13 and globalnet:
            log.info("Enabling globalnet")
            element = self.find_an_element_by_xpath("//input[@id='globalist-enable']")
            self.driver.execute_script("arguments[0].click();", element)
        else:
            log.error(
                "Globalnet is not supported with ODF version lower than 4.13 or it's disabled"
            )
        log.info("Click on Next button")
        self.do_click(self.page_nav["next-btn"])
        log.info("Click on 'Enable NAT-T' to uncheck it")
        self.do_click(self.page_nav["nat-t-checkbox"])
        log.info(
            "Increase the gateway count to 3 by clicking twice on the gateway count add button"
        )
        self.do_click(self.page_nav["gateway-count-btn"])
        self.do_click(self.page_nav["gateway-count-btn"])
        if config.ENV_DATA.get("submariner_release_type") == "unreleased":
            self.submariner_unreleased_downstream_info()
        log.info("Click on Next button")
        self.do_click(self.page_nav["next-btn"])
        log.info("Click on 'Enable NAT-T' to uncheck it [2]")
        self.do_click(self.page_nav["nat-t-checkbox"])
        log.info(
            "Increase the gateway count to 3 by clicking twice on the gateway count add button [2]"
        )
        self.do_click(self.page_nav["gateway-count-btn"])
        self.do_click(self.page_nav["gateway-count-btn"])
        if config.ENV_DATA.get("submariner_release_type") == "unreleased":
            self.submariner_unreleased_downstream_info()
        log.info("Click on Next button [2]")
        self.do_click(self.page_nav["next-btn"])
        if ocs_version >= version.VERSION_4_13 and globalnet:
            check_globalnet = self.get_element_text(self.page_nav["check-globalnet"])
            assert (
                check_globalnet == constants.GLOBALNET_STATUS
            ), "Globalnet was not enabled"
            log.info("Globalnet is enabled")
        self.take_screenshot()
        log.info("Click on 'Install'")
        self.do_click(self.page_nav["install-btn"])

    def submariner_unreleased_downstream_info(self):
        self.do_click(self.page_nav["submariner-custom-subscription"])
        self.do_clear(self.page_nav["submariner-custom-source"])
        self.do_send_keys(
            self.page_nav["submariner-custom-source"], "submariner-catalogsource"
        )
        submariner_unreleased_channel = (
            config.ENV_DATA["submariner_unreleased_channel"]
            if config.ENV_DATA["submariner_unreleased_channel"]
            else config.ENV_DATA["submariner_version"].rpartition(".")[0]
        )
        channel_name = "stable-" + submariner_unreleased_channel
        self.do_send_keys(
            self.page_nav["submariner-custom-channel"],
            channel_name,
        )

    def submariner_validation_ui(self):
        """
        This function validates submariner status on ACM console which connects 2 managed OCP clusters.
        This is a mandatory pre-check for Regional DR.

        """

        self.navigate_clusters_page()
        cluster_sets_page = self.wait_until_expected_text_is_found(
            locator=self.page_nav["cluster-sets"],
            expected_text="Cluster sets",
            timeout=120,
        )
        if cluster_sets_page:
            log.info("Click on Cluster sets")
            self.do_click(self.page_nav["cluster-sets"])
        else:
            log.error("Couldn't navigate to Cluster sets page")
            raise NoSuchElementException
        log.info("Click on the cluster set created")
        self.do_click(
            format_locator(self.page_nav["cluster-set-selection"], cluster_set_name)
        )
        log.info("Click on 'Submariner add-ons' tab")
        self.do_click(self.page_nav["submariner-tab"], enable_screenshot=True)
        log.info("Checking connection status of both the imported clusters")
        assert self.wait_until_expected_text_is_found(
            locator=self.page_nav["connection-status-1"],
            expected_text="Healthy",
            timeout=600,
        ), "Connection status 1 is unhealthy for Submariner"
        assert self.wait_until_expected_text_is_found(
            locator=self.page_nav["connection-status-2"],
            expected_text="Healthy",
            timeout=600,
        ), "Connection status 2 is unhealthy for Submariner"
        log.info("Checking agent status of both the imported clusters")
        assert self.wait_until_expected_text_is_found(
            locator=self.page_nav["agent-status-1"],
            expected_text="Healthy",
            timeout=600,
        ), "Agent status 1 is unhealthy for Submariner"
        assert self.wait_until_expected_text_is_found(
            locator=self.page_nav["agent-status-2"],
            expected_text="Healthy",
            timeout=600,
        ), "Agent status 2 is unhealthy for Submariner"
        log.info("Checking if nodes of both the imported clusters are labeled or not")
        assert self.wait_until_expected_text_is_found(
            locator=self.page_nav["node-label-1"],
            expected_text="Nodes labeled",
            timeout=600,
        ), "First gateway node label check did not pass for Submariner"
        assert self.wait_until_expected_text_is_found(
            locator=self.page_nav["node-label-2"],
            expected_text="Nodes labeled",
            timeout=600,
        ), "Second gateway node label check did not pass for Submariner"
        self.take_screenshot()
        log.info("Submariner is healthy, check passed")


def copy_kubeconfig(file=None, return_str=False):
    """

    Args:
        file: (str): kubeconfig file location
        return_str: (bool): if True return kubeconfig content as string
        else return list of lines of kubeconfig content

    Returns:
        list/str: kubeconfig content

    """

    try:
        with open(file, "r") as f:
            if return_str is True:
                txt = f.read()
            else:
                txt = f.readlines()
            return txt

    except FileNotFoundError as e:
        log.error(f"file {file} not found")
        raise e


def get_acm_url():
    """
    Gets ACM console url

    Returns:
        str: url of ACM console

    """
    mch_cmd = OCP(namespace=ACM_NAMESPACE)
    url = mch_cmd.exec_oc_cmd(
        "get route -ojsonpath='{.items[].spec.host}'", out_yaml_format=False
    )
    log.info(f"ACM console URL: {url}")

    return f"https://{url}"


def validate_page_title(title):
    """
    Validates Page HTML Title
    Args:
        title (str): required title
    """
    WebDriverWait(SeleniumDriver(), 60).until(ec.title_is(title))
    log.info(f"page title: {title}")


def login_to_acm():
    """
    Login to ACM console and validate by its title

    Returns:
        driver (Selenium WebDriver)

    """

    acm_version = ".".join(get_running_acm_version().split(".")[:2])
    if not acm_version:
        raise UnexpectedDeploymentConfiguration("ACM not found")
    cmp_str = f"{acm_version}>=2.7"
    if compare_versions(cmp_str):
        url = f"{get_ocp_url()}{ACM_2_7_MULTICLUSTER_URL}"
    else:
        url = get_acm_url()
    log.info(f"URL: {url}")
    driver = login_ui(url)
    page_nav = AcmPageNavigator()
    if not compare_versions(cmp_str):
        page_nav.navigate_from_ocp_to_acm_cluster_page()

    if compare_versions(cmp_str):
        page_title = ACM_PAGE_TITLE_2_7_ABOVE
    else:
        page_title = ACM_PAGE_TITLE
    validate_page_title(title=page_title)

    return driver


def verify_running_acm():
    """
    Detect ACM and its version on Cluster

    """
    mch_cmd = OCP(namespace=ACM_NAMESPACE)
    acm_status = mch_cmd.exec_oc_cmd(
        "get mch -o jsonpath='{.items[].status.phase}'", out_yaml_format=False
    )
    assert acm_status == "Running", f"ACM status is {acm_status}"
    acm_version = mch_cmd.exec_oc_cmd(
        "get mch -o jsonpath='{.items[].status.currentVersion}'", out_yaml_format=False
    )
    log.info(f"ACM Version Detected: {acm_version}")


def validate_cluster_import(cluster_name, switch_ctx=None):
    """
    Validate ACM status of managed cluster

    Args:
        cluster_name: (str): cluster name to validate
        switch_ctx (int): The cluster index by the cluster name

    Assert:
        All conditions of selected managed cluster should be "True", Failed otherwise

    Return:
        True, if not AssertionError
    """
    config.switch_ctx(switch_ctx) if switch_ctx else config.switch_ctx(0)
    oc_obj = OCP(kind=ACM_MANAGED_CLUSTERS)
    conditions = oc_obj.exec_oc_cmd(
        f"get managedclusters {cluster_name} -ojsonpath='{{.status.conditions}}'"
    )
    log.debug(conditions)

    for dict_status in conditions:
        log.info(f"Message: {dict_status.get('message')}")
        log.info(f"Status: {dict_status.get('status')}")
        assert (
            dict_status.get("status") == "True"
        ), f"Status is not True, but: {dict_status.get('status')}"

    # Return true if Assertion error was not raised:
    return True


def get_clusters_env():
    """
    Stores cluster's kubeconfig location and clusters name, in case of multi-cluster setup.
    Function will switch to context index zero before returning
    Returns:
        dict: with clusters names, clusters kubeconfig locations

    """
    clusters_env = {}
    for index in range(config.nclusters):
        config.switch_ctx(index=index)

        clusters_env[f"kubeconfig_location_c{index}"] = os.path.join(
            config.ENV_DATA["cluster_path"], config.RUN["kubeconfig_location"]
        )
        clusters_env[f"cluster_name_{index}"] = config.ENV_DATA["cluster_name"]

    config.switch_ctx(index=0)

    return clusters_env


def import_clusters_via_cli(clusters):
    """
    Import clusters via cli

    Args:
        clusters (list): list of tuples (cluster name, kubeconfig path)

    """
    for cluster in clusters:
        log.info("Importing clusters via CLI method")
        log.info(f"**** clustername={cluster[0]}")
        log.info(f"**** kubeconfig={cluster[1]}")
        create_project(cluster[0])

        log.info("Create and apply managed-cluster.yaml")
        managed_cluster = templating.load_yaml(
            "ocs_ci/templates/acm-deployment/managed-cluster.yaml"
        )
        managed_cluster["metadata"]["name"] = cluster[0]
        managed_cluster_obj = OCS(**managed_cluster)
        managed_cluster_obj.apply(**managed_cluster)

        log.info("Create and Apply the auto-import-secret.yaml")
        auto_import_secret = templating.load_yaml(
            "ocs_ci/templates/acm-deployment/auto-import-secret.yaml"
        )
        auto_import_secret["metadata"]["namespace"] = cluster[0]
        auto_import_secret["stringData"]["kubeconfig"] = cluster[1]
        auto_import_secret_obj = OCS(**auto_import_secret)
        auto_import_secret_obj.apply(**auto_import_secret)

        log.info("Wait managedcluster move to Available state")
        time.sleep(60)
        ocp_obj = OCP(kind=constants.ACM_MANAGEDCLUSTER)
        ocp_obj.wait_for_resource(
            timeout=1200,
            condition="True",
            column="AVAILABLE",
            resource_name=cluster[0],
        )
        ocp_obj.wait_for_resource(
            timeout=1200,
            condition="True",
            column="JOINED",
            resource_name=cluster[0],
        )

        log.info("Creating klusterlet addon configuration")
        klusterlet_config = templating.load_yaml(constants.ACM_HUB_KLUSTERLET_YAML)
        klusterlet_config["metadata"]["name"] = cluster[0]
        klusterlet_config["metadata"]["namespace"] = cluster[0]
        klusterlet_config_obj = OCS(**klusterlet_config)
        klusterlet_config_obj.create()

        log.info("Waiting for addon pods to be in running state")
        config.switch_to_cluster_by_name(cluster[0])
        wait_for_pods_to_be_running(
            namespace=constants.ACM_ADDONS_NAMESPACE, timeout=300, sleep=15
        )

        config.switch_acm_ctx()
        ocp_obj.wait_for_resource(
            timeout=1200,
            condition="true",
            column="HUB ACCEPTED",
            resource_name=cluster[0],
        )


def import_clusters_with_acm():
    """
    Run Procedure of: detecting acm, login to ACM console, import 2 clusters

    """
    # TODO: Import action should be dynamic per cluster count (Use config.nclusters loop)
    clusters_env = get_clusters_env()
    primary_index = get_primary_cluster_config().MULTICLUSTER["multicluster_index"]
    secondary_index = [
        s.MULTICLUSTER["multicluster_index"]
        for s in get_non_acm_cluster_config()
        if s.MULTICLUSTER["multicluster_index"] != primary_index
    ][0]
    log.info(clusters_env)
    kubeconfig_a = copy_kubeconfig(
        file=clusters_env.get(f"kubeconfig_location_c{primary_index}"), return_str=True
    )
    kubeconfig_b = copy_kubeconfig(
        file=clusters_env.get(f"kubeconfig_location_c{secondary_index}"),
        return_str=True,
    )
    cluster_name_a = clusters_env.get(f"cluster_name_{primary_index}")
    cluster_name_b = clusters_env.get(f"cluster_name_{secondary_index}")
    clusters = ((cluster_name_a, kubeconfig_a), (cluster_name_b, kubeconfig_b))
    verify_running_acm()
    if config.DEPLOYMENT.get("ui_acm_import"):
        login_to_acm()
        acm_nav = AcmAddClusters()
        acm_nav.import_cluster(
            cluster_name=cluster_name_a,
            kubeconfig_location=kubeconfig_a,
        )
    else:
        import_clusters_via_cli(clusters)
