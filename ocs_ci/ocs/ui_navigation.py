"""
Functions for navigating between pages of OCP UI

"""

import logging
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException

from ocs_ci.utility.utils import get_kubeadmin_password, run_cmd

logger = logging.getLogger(__name__)


def click_element_by_id(driver, element_id):
    '''
    The function chooses the element by element's id and clicks on it

    Args:
        driver: webdriver instance
        element_id (str): the id of the element to be clicked

    '''
    element = driver.find_element_by_id(element_id)
    element.click()


def click_link(driver, link_text):
    '''
    The function clicks on the link with the given text

    Args:
        driver: webdriver instance
        link_text (str): the text of the link to be clicked

    '''
    element = driver.find_element_by_link_text(link_text)
    element.click()


def proceed_to_login_page(driver):
    '''
    Function for proceeding to OCP login page
    despite the browser's security warnings

    Args:
        driver: webdriver instance

    '''
    while not driver.find_elements_by_id('inputUsername'):
        click_element_by_id(driver, 'details-button')
        click_element_by_id(driver, 'proceed-link')


def ui_login(driver):
    '''
    Function logs in to OCP console

    Args:
        driver: webdriver instance

    '''
    console_url = run_cmd(
        "oc get consoles.config.openshift.io cluster -o"
        "jsonpath='{.status.consoleURL}'"
    )
    driver.get(console_url)
    proceed_to_login_page(driver)
    kubeadmin_login = driver.find_element_by_id('inputUsername')
    kubeadmin_login.clear()
    kubeadmin_login.send_keys('kubeadmin')
    kubeadmin_password = driver.find_element_by_id('inputPassword')
    kubeadmin_password.send_keys(get_kubeadmin_password())
    kubeadmin_password.send_keys(Keys.RETURN)
    logger.info("Logged in to OCP console")


def go_to_istalled_operators(driver):
    '''
    Function navigates to the list of installed operators

    Args:
        driver: webdriver instance (after login to OCP console)

    '''
    # "Operators" is either a link or a button depending on OCP version
    try:
        click_link(driver, "Operators")
    except NoSuchElementException:
        operators_button = driver.find_element_by_xpath(
            "//button[normalize-space(text())='Operators']"
        )
        operators_button.click()
    click_link(driver, "Installed Operators")
    logger.info("Moved to Installed Operators page")


def go_to_ocs_operator(driver):
    '''
    Function navigates to OCS operator page

    Args:
        driver: webdriver instance (after login to OCP console)

    '''
    go_to_istalled_operators(driver)
    ocs_operator_link = driver.find_element_by_css_selector(
        'a[data-test-operator-row="OpenShift Container Storage"]'
    )
    ocs_operator_link.click()
    logger.info("Moved to OCS Operator page")


def go_to_storage_clusters_list(driver):
    '''
    Function navigates to the list of storage clusters

    Args:
        driver: webdriver instance (after login to OCP console)

    '''
    go_to_ocs_operator(driver)
    storage_cluster_link = driver.find_element_by_css_selector(
        'a[data-test-id="horizontal-link-Storage Cluster"]'
    )
    storage_cluster_link.click()
    logger.info("Moved to the list of storage clusters")
