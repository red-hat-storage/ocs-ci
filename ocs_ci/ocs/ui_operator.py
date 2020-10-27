"""
Functions for interacting with the operator part of OCS/OCP UI:
OCS operator, storage cluster, backing store and bucket class operations
"""

import logging
import time

from ocs_ci.ocs.ui_navigation import (
    click_link, go_to_storage_clusters_list, click_element_by_id
)

logger = logging.getLogger(__name__)


def choose_storage_cluster(driver, storage_cluster_name):
    '''
    Function navigates to the storage cluster's details page

    Args:
        driver: webdriver instance (after login to OCP console)
        storage_cluster_name (str): name of the storage cluster

    '''
    go_to_storage_clusters_list(driver)
    click_link(driver, storage_cluster_name)
    logger.info(f"Storage cluster {storage_cluster_name} chosen")


def delete_storage_cluster(driver, storage_cluster_name):
    '''
    Function deletes the storage cluster

    Args:
        driver: webdriver instance (after login to OCP console)
        storage_cluster_name (str): the name of the storage cluster

    '''
    choose_storage_cluster(driver, storage_cluster_name)
    actions_button = driver.find_element_by_css_selector(
        'button[data-test-id="actions-menu-button"]'
    )
    actions_button.click()
    delete_button = driver.find_element_by_css_selector(
        'button[data-test-action="Delete OCS Cluster Service"]'
    )
    delete_button.click()
    if not driver.find_elements_by_id("confirm-action"):
        delete_button.click()
    click_element_by_id(driver, "confirm-action")
    time.sleep(25)
    logger.info(f"Storage cluster {storage_cluster_name} deleted")


def get_status_label(driver):
    '''
    Function returns the text from the status label

    Args:
        driver: webdriver instance (after navigation to the correct page)

    Returns:
        status_label.text (str): text of the status label

    '''
    status_label = driver.find_element_by_css_selector(
        'span[data-test="status-text"]'
    )
    return status_label.text
