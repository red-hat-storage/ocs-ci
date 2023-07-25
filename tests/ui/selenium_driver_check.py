import logging

import pytest

logger = logging.getLogger(__name__)


@pytest.mark.order1
@pytest.mark.parametrize("execution_number", range(2))
def test_check_driver_creation_login_factory(login_factory, execution_number):
    """
    Test that the driver is created correctly
    """
    logger.info(f"login_factory_PASS Execution number: {execution_number}")
    driver = login_factory("user", "password")
    driver.get("https://github.com")
    assert driver is not None


@pytest.mark.order2
@pytest.mark.parametrize("execution_number", range(2))
def test_check_driver_creation_login_factory_fail(login_factory, execution_number):
    """
    Test that the driver is created correctly
    """
    logger.info(f"login_factory_FAIL Execution number: {execution_number}")
    driver = login_factory("user", "password")
    driver.get("https://github.com")
    pytest.fail("Fail test_check_driver_creation_login_factory_fail intentionally")


@pytest.mark.order3
@pytest.mark.parametrize("execution_number", range(2))
def test_check_driver_creation_a(setup_ui_class, execution_number):
    """
    Test that the driver is created correctly
    """
    logger.info(f"Execution number: {execution_number}")
    driver = setup_ui_class
    assert driver is not None


@pytest.mark.order3
@pytest.mark.parametrize("execution_number", range(2))
def test_check_driver_creation_b(setup_ui_class, execution_number):
    """
    Test that the driver is created correctly
    """
    logger.info(f"Execution number: {execution_number}")
    driver = setup_ui_class
    assert driver is not None


@pytest.mark.order4
@pytest.mark.parametrize("execution_number", range(2))
def test_check_driver_creation_c(setup_ui_class, execution_number):
    """
    Test that the driver is created correctly
    """
    logger.info(f"Execution number: {execution_number}")
    driver = setup_ui_class
    driver.get("https://github.com")
    pytest.fail("Fail test_check_driver_creation_login_factory_fail intentionally")


@pytest.mark.order5
@pytest.mark.parametrize("execution_number", range(50))
def test_check_driver_creation_d(setup_ui_class, execution_number):
    """
    Test that the driver is created correctly
    """
    logger.info(f"Execution number: {execution_number}")
    driver = setup_ui_class
    assert driver is not None
