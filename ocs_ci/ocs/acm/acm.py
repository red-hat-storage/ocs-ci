import logging
import time
import os

from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.ocs.acm.acm_constants import (
    ACM_NAMESPACE,
    ACM_MANAGED_CLUSTERS,
    ACM_PAGE_TITLE,
    ACM_2_7_MULTICLUSTER_URL,
    ACM_PAGE_TITLE_2_7_ABOVE,
)
from ocs_ci.ocs.ocp import OCP, get_ocp_url
from ocs_ci.framework import config
from ocs_ci.ocs.ui.helpers_ui import format_locator
from ocs_ci.utility.utils import (
    TimeoutSampler,
    get_running_acm_version,
    string_chunkify,
)
from ocs_ci.ocs.ui.acm_ui import AcmPageNavigator
from ocs_ci.ocs.ui.views import locators
from ocs_ci.ocs.ui.base_ui import login_ui
from ocs_ci.utility.version import compare_versions
from ocs_ci.ocs.exceptions import (
    ACMClusterImportException,
    UnexpectedDeploymentConfiguration,
)

log = logging.getLogger(__name__)


class AcmAddClusters(AcmPageNavigator):
    """
    ACM Page Navigator Class

    """

    def __init__(self, driver):
        super().__init__(driver)
        self.page_nav = locators[self.ocp_version]["acm_page"]

    def import_cluster_ui(self, cluster_name, kubeconfig_location):
        """

        Args:
            cluster_name (str): cluster name to import
            kubeconfig_location (str): kubeconfig file location of imported cluster

        """
        self.navigate_clusters_page()
        if not self.check_element_presence(
            (By.ID, self.acm_page_nav["Import_cluster"][0]), timeout=100
        ):
            raise ACMClusterImportException("Import button not found")
        self.do_click(self.acm_page_nav["Import_cluster"])
        log.info("Clicked on Import cluster")
        self.wait_for_endswith_url("import", timeout=300)

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
                return
            else:
                log.error(f"import of cluster: {cluster_name} failed")

    def install_submariner_ui(self):
        """
        Installs the Submariner on the ACM Hub cluster and expects 2 OCP clusters to be already imported
        on the Hub Cluster to create a link between them

        """
        cluster_env = get_clusters_env()
        cluster_name_a = cluster_env.get("cluster_name_1")
        cluster_name_b = cluster_env.get("cluster_name_2")
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
        self.do_click(self.page_nav["install-submariner-btn"], timeout=120)
        log.info("Click on 'Target clusters'")
        self.do_click(self.page_nav["target-clusters"])
        log.info(f"Select 1st cluster which is {cluster_name_a}")
        self.do_click(
            format_locator(
                locator=self.page_nav["cluster-name-selection"],
                string_to_insert=cluster_name_a,
            )
        )
        log.info(f"Select 2nd cluster which is {cluster_name_b}")
        self.do_click(
            format_locator(
                locator=self.page_nav["cluster-name-selection"],
                string_to_insert=cluster_name_b,
            ),
            enable_screenshot=True,
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
        log.info("Click on Next button")
        self.do_click(self.page_nav["next-btn"])
        log.info("Click on 'Enable NAT-T' to uncheck it [2]")
        self.do_click(self.page_nav["nat-t-checkbox"])
        log.info(
            "Increase the gateway count to 3 by clicking twice on the gateway count add button [2]"
        )
        self.do_click(self.page_nav["gateway-count-btn"])
        self.do_click(self.page_nav["gateway-count-btn"])
        log.info("Click on Next button [2]")
        self.do_click(self.page_nav["next-btn"])
        self.take_screenshot()
        log.info("Click on 'Install'")
        self.do_click(self.page_nav["install-btn"])

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
            format_locator(
                locator=self.page_nav["cluster-set-selection"],
                string_to_insert=cluster_set_name,
            )
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


def copy_kubeconfig(file):
    """

    Args:
        file: (str): kubeconfig file location

    Returns:
        list: with kubeconfig lines

    """

    try:
        with open(file, "r") as f:
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


def validate_page_title(driver, title):
    """
    Validates Page HTML Title
    Args:
        driver: driver (Selenium WebDriver)
        title (str): required title

    """
    WebDriverWait(driver, 60).until(ec.title_is(title))
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
    page_nav = AcmPageNavigator(driver)
    if not compare_versions(cmp_str):
        page_nav.navigate_from_ocp_to_acm_cluster_page()

    if compare_versions(cmp_str):
        page_title = ACM_PAGE_TITLE_2_7_ABOVE
    else:
        page_title = ACM_PAGE_TITLE
    validate_page_title(driver, title=page_title)

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


def validate_cluster_import(cluster_name):
    """
    Validate ACM status of managed cluster

    Args:
        cluster_name: (str): cluster name to validate

    Assert:
        All conditions of selected managed cluster should be "True", Failed otherwise

    Return:
        True, if not AssertionError
    """
    config.switch_ctx(0)
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


def import_clusters_with_acm():
    """
    Run Procedure of: detecting acm, login to ACM console, import 2 clusters

    """
    # TODO: Import action should be dynamic per cluster count (Use config.nclusters loop)
    clusters_env = get_clusters_env()
    log.info(clusters_env)
    kubeconfig_a = clusters_env.get("kubeconfig_location_c1")
    kubeconfig_b = clusters_env.get("kubeconfig_location_c2")
    cluster_name_a = clusters_env.get("cluster_name_1")
    cluster_name_b = clusters_env.get("cluster_name_2")
    verify_running_acm()
    driver = login_to_acm()
    acm_nav = AcmAddClusters(driver)
    acm_nav.import_cluster(
        cluster_name=cluster_name_a,
        kubeconfig_location=kubeconfig_a,
    )

    acm_nav.import_cluster(
        cluster_name=cluster_name_b,
        kubeconfig_location=kubeconfig_b,
    )
