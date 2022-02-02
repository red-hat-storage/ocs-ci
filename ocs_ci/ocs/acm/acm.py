import logging
import time
import os

from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec

from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.ocs.acm.acm_constants import (
    ACM_NAMESPACE,
    ACM_MANAGED_CLUSTERS,
    ACM_PAGE_TITLE,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework import config
from ocs_ci.ocs.ui.helpers_ui import format_locator
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.ui.acm_ui import AcmPageNavigator
from ocs_ci.ocs.ui.views import locators
from ocs_ci.ocs.ui.base_ui import login_ui

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
        self.do_click(self.page_nav["Import_cluster"])
        self.do_send_keys(
            self.page_nav["Import_cluster_enter_name"], text=f"{cluster_name}"
        )
        self.do_click(self.page_nav["Import_mode"])
        self.do_click(self.page_nav["choose_kubeconfig"])
        log.info(f"Coping Kubeconfig {kubeconfig_location}")
        kubeconfig_to_import = copy_kubeconfig(kubeconfig_location)
        for line in kubeconfig_to_import:
            self.do_send_keys(self.page_nav["Kubeconfig_text"], text=f"{line}")
            time.sleep(2)
        log.info(f"Submitting import of {cluster_name}")
        self.do_click(self.page_nav["Submit_import"])

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
        self.do_click(self.page_nav["install-submariner-btn"])
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
        Checks available status of imported clusters after submariner creation

        """

        self.navigate_clusters_page()
        self.page_has_loaded(retries=15, sleep_time=5)
        self.do_click(locator=self.acm_page_nav["Clusters_page"])
        log.info("Click on Cluster sets")
        self.do_click(self.page_nav["cluster-sets"])
        self.page_has_loaded(retries=15, sleep_time=5)
        log.info("Click on the cluster set created")
        self.do_click(
            format_locator(
                locator=self.page_nav["cluster-set-selection"],
                string_to_insert=cluster_set_name,
            )
        )
        log.info("Click on 'Submariner add-ons' tab")
        self.do_click(self.page_nav["submariner-tab"])
        log.info("Checking connection status of both the imported clusters")
        self.wait_until_expected_text_is_found(
            locator=self.page_nav["connection-status-1"],
            expected_text="Healthy",
            timeout=600,
        )
        self.wait_until_expected_text_is_found(
            locator=self.page_nav["connection-status-2"],
            expected_text="Healthy",
            timeout=600,
        )
        log.info("Checking agent status of both the imported clusters")
        self.wait_until_expected_text_is_found(
            locator=self.page_nav["agent-status-1"],
            expected_text="Healthy",
            timeout=600,
        )
        self.wait_until_expected_text_is_found(
            locator=self.page_nav["agent-status-2"],
            expected_text="Healthy",
            timeout=600,
        )
        log.info("Checking if nodes of both the imported clusters are labeled or not")
        self.wait_until_expected_text_is_found(
            locator=self.page_nav["node-label-1"],
            expected_text="Nodes labeled",
            timeout=600,
        )
        self.wait_until_expected_text_is_found(
            locator=self.page_nav["node-label-2"],
            expected_text="Nodes labeled",
            timeout=600,
        )
        self.take_screenshot()
        log.info("Submariner add-ons creation is successful")


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
    url = get_acm_url()
    log.info(f"URL: {url}, {type(url)}")
    driver = login_ui(url)
    validate_page_title(driver, title=ACM_PAGE_TITLE)

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
    Stores cluster's kubeconfig location and clusters name, in case of multi-cluster setup
        Returns after execution with cluster index zero as default context
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
    clusters_env = get_clusters_env()
    log.info(clusters_env)
    kubeconfig_a = clusters_env.get("kubeconfig_location_c1")
    kubeconfig_b = clusters_env.get("kubeconfig_location_c2")
    global cluster_name_a
    cluster_name_a = clusters_env.get("cluster_name_1")
    global cluster_name_b
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
