import logging
import pytest

from selenium.webdriver.common.by import By
from ocs_ci.ocs.exceptions import TimeoutException
from ocs_ci.ocs.ui.base_ui import SeleniumDriver
from selenium.webdriver.support.ui import WebDriverWait
import time
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.action_chains import ActionChains


logger = logging.getLogger(__name__)


def grafana_resource_consumption_ui(test_duration, url, username, password):
    """
    This function logs in to grafana and captures resource consumption during performance test executions

    Args:
    test_duration (int): Test duration, for which the resource consumption needs to be captured

    """
    driver = SeleniumDriver()

    if test_duration == 0:
        logger.error("The test duration value cannont be zero")
    elif test_duration > 0 and test_duration < 301:
        last_duration = "Last 5 minutes"
    elif test_duration > 300 and test_duration < 901:
        last_duration = "Last 15 minutes"
    elif test_duration > 900 and test_duration < 1801:
        last_duration = "Last 30 minutes"
    elif test_duration > 1800 and test_duration < 3601:
        last_duration = "Last 1 hour"
    elif test_duration > 3600 and test_duration < 10801:
        last_duration = "Last 3 hour"
    elif test_duration > 10800 and test_duration < 21601:
        last_duration = "Last 6 hour"
    elif test_duration > 21600 and test_duration < 43201:
        last_duration = "Last 12 hour"
    elif test_duration > 43200 and test_duration < 86401:
        last_duration = "Last 24 hour"
    else:
        logger.error(f"The test duration value {test_duration} is invalid")
    logger.info(f"yurl{url}")
    try:
        driver.get(url)

        # Try grafana login prompt
        try:
            wait = WebDriverWait(driver, 30)
            logger.info(f"1{wait}")
            wait.until(ec.presence_of_element_located((By.NAME, "user")))
            logger.info(f"2{wait}")
            driver.find_element(By.NAME, "user").send_keys(username)
            logger.info(f"3{wait}")
            driver.find_element(By.NAME, "password").send_keys(password)
            logger.info(f"4{wait}")
            driver.find_element(By.CSS_SELECTOR, "button[type='submit']").click()
            logger.info(f"5{wait}")
        except TimeoutException:
            print("Can not find login page, May have already been logged in")
        '''
        expand_button = driver.find_element(By.XPATH, "//button[@aria-label='Expand folder PerfScale']")
        expand_button.click()
        time.sleep(3)

        perfscale = driver.find_element(By.LINK_TEXT, "PerfScale")
        perfscale.click()
        time.sleep(3)
        '''
        expand_button = wait.until(
            ec.element_to_be_clickable((By.XPATH, "//button[@aria-label='Expand folder PerfScale']"))
        )
        expand_button.click()

        # Step 2: Wait for the link to become visible and clickable
        perfscale_link = wait.until(
            ec.element_to_be_clickable((By.XPATH, "//a[text()='PerfScale']"))
        )
        perfscale_link.click()

        dashboard = wait.until(
            ec.element_to_be_clickable((By.LINK_TEXT, "ODF Performance Analysis"))
        )
        dashboard.click()

        # Sleeping for 3 seconds for dashboard to load
        time.sleep(3)

        # Based on the chosen time, we need to display the resource graphs
        time_range_selector = wait.until(
            ec.element_to_be_clickable(
                (By.CSS_SELECTOR, 'button[aria-label*="Time range selected"]')
            )
        )
        time_range_selector.click()
        logger.info(f"lkst{last_duration}")
        last_duration_option = wait.until(
            ec.element_to_be_clickable(
                (By.XPATH, f"//label[normalize-space()={last_duration}]")
            )
        )
        last_duration_option.click()
        logger.info(f"lksvt{last_duration}")

        print(f"Time range is updated to {last_duration}")

        # Capture network usage information
        network_selector = wait.until(
            ec.element_to_be_clickable(
                (By.XPATH, "//button[contains(., 'Networking')]")
            )
        )
        network_selector.click()
        time.sleep(2)

        # Go to total bandwidth graph section
        panel = wait.until(
            ec.presence_of_element_located(
                (
                    By.XPATH,
                    "//section[@data-testid='data-testid Panel header Total Bandwidth by Host']",
                )
            )
        )
        ActionChains(driver).move_to_element(panel).perform()
        time.sleep(2)

        # Click on view
        menu = wait.until(
            ec.element_to_be_clickable(
                (
                    By.XPATH,
                    "//button[@aria-label='Menu for panel with title Total Bandwidth by Host']",
                )
            )
        )
        menu.click()
        view = wait.until(
            ec.element_to_be_clickable((By.XPATH, "//span[text()='View']"))
        )
        view.click()

        # Take screenshot

        time.sleep(3)
        driver.save_screenshot("network_utilization.png")
        logger.info("Screenshot for network utilization is captured.")

        # Go back to previous menu
        previous = wait.until(
            ec.element_to_be_clickable(
                (
                    By.XPATH,
                    '//a[@data-testid="data-testid ODF Performance Analysis breadcrumb"]',
                )
            )
        )
        previous.click()

        # Capture Ceph CPU utilization graph

        ceph_cpu_selector = wait.until(
            ec.element_to_be_clickable(
                (By.XPATH, '//button[contains(.,"Ceph CPU / Memory Analysis")]')
            )
        )
        ceph_cpu_selector.click()
        time.sleep(3)
        ceph_cpu_xpath = f'//h2[text()="Ceph Daemon CPU Usage"]/ancestor::div[contains(@class, "react-grid-item")]'

        ceph_cpu = wait.until(
            ec.presence_of_element_located((By.XPATH, ceph_cpu_xpath))
        )
        ActionChains(driver).move_to_element(ceph_cpu).perform()

        menu_xpath = (
            f'//button[@aria-label="Menu for panel with title Ceph Daemon CPU Usage"]'
        )
        menu = wait.until(ec.element_to_be_clickable((By.XPATH, menu_xpath)))
        driver.execute_script("arguments[0].scrollIntoView(true);", menu)
        menu.click()

        view = wait.until(
            ec.element_to_be_clickable((By.XPATH, '//span[text()="View"]'))
        )
        view.click()

        time.sleep(2)
        driver.save_screenshot("Ceph_CPU.png")
        logger.info("Ceph CPU resource consumption is captured")

        # Go back to previous menu
        previous = wait.until(
            ec.element_to_be_clickable(
                (
                    By.XPATH,
                    '//a[@data-testid="data-testid ODF Performance Analysis breadcrumb"]',
                )
            )
        )
        previous.click()

        # Capture Ceph Memory utilization graph

        panel_xpath_2 = f'//h2[text()="Ceph Daemon Memory Utilisation"]/ancestor::div[contains(@class, "react-grid-item")]'

        panel_container_2 = wait.until(
            ec.presence_of_element_located((By.XPATH, panel_xpath_2))
        )
        ActionChains(driver).move_to_element(panel_container_2).perform()

        menu_xpath_2 = f'//button[@aria-label="Menu for panel with title Ceph Daemon Memory Utilisation"]'
        menu_button_2 = wait.until(ec.element_to_be_clickable((By.XPATH, menu_xpath_2)))
        driver.execute_script("arguments[0].scrollIntoView(true);", menu_button_2)
        menu_button_2.click()

        view_option = wait.until(
            ec.element_to_be_clickable((By.XPATH, '//span[text()="View"]'))
        )
        view_option.click()

        time.sleep(2)
        driver.save_screenshot("Ceph_memory.png")
        logger.info("Ceph Memory resource consumption is captured")

        # Go back to previous menu
        previous = wait.until(
            ec.element_to_be_clickable(
                (
                    By.XPATH,
                    '//a[@data-testid="data-testid ODF Performance Analysis breadcrumb"]',
                )
            )
        )
        previous.click()

    finally:
        driver.quit()
