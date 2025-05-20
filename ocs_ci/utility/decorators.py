import logging

from ocs_ci.framework import config
from ocs_ci.helpers.odf_cli import odf_cli_setup_helper

# from ocs_ci.ocs.constants import

logger = logging.getLogger(__name__)


def switch_to_orig_index_at_last(func):
    """
    A decorator for switching to the original index after the function execution

    Args:
        func (function): The function we want to decorate

    """

    def inner(*args, **kwargs):
        orig_index = config.cur_index
        try:
            return func(*args, **kwargs)
        finally:
            if config.cur_index != orig_index:
                logger.info("Switching back to the original cluster")
                config.switch_ctx(orig_index)

    return inner


def switch_to_default_cluster_index_at_last(func):
    """
    A decorator for switching to the default cluster index after the function execution.
    This decorator will primarily be used in the 'teardown' and 'finalizer' methods when we want to make sure
    that the next test will start with the default cluster index.

    Args:
        func (function): The function we want to decorate

    """

    def inner(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        finally:
            default_cluster_index = config.ENV_DATA["default_cluster_context_index"]
            if config.cur_index != default_cluster_index:
                logger.info("Switching back to the default cluster index")
                config.switch_ctx(default_cluster_index)

    return inner


def switch_to_client_for_function(func, client_index=0):
    """
    A decorator for switching to the client cluster for the function execution.
    After the function execution, it switches back to the original index.

    Example of usage:
    Suppose we have the function definition 'def wait_for_storage_client_connected(timeout=180, sleep=10)'.
    Here are three examples of usage:
    1. switch_to_client_for_function(wait_for_storage_client_connected)(timeout=30)
    2. switch_to_client_for_function(wait_for_storage_client_connected, client_index=1)()
    2. switch_to_client_for_function(wait_for_storage_client_connected, client_index=1)(timeout=30, sleep=5)


    Args:
        func (function): The function we want to decorate
        client_index (int) : The client cluster index to switch. The default value is 0.

    """

    def inner(*args, **kwargs):
        orig_index = config.cur_index
        try:
            config.switch_to_consumer(client_index)
            return func(*args, **kwargs)
        finally:
            if config.cur_index != orig_index:
                logger.info("Switching back to the original cluster")
                config.switch_ctx(orig_index)

    return inner


def switch_to_provider_for_function(func):
    """
    A decorator for switching to the provider cluster for the function execution.
    After the function execution, it switches back to the original index.

    Args:
        func (function): The function we want to decorate

    """

    def inner(*args, **kwargs):
        orig_index = config.cur_index
        try:
            config.switch_to_provider()
            return func(*args, **kwargs)
        finally:
            if config.cur_index != orig_index:
                logger.info("Switching back to the original cluster")
                config.switch_ctx(orig_index)

    return inner


def safe_exec(func):
    """
    Decorator to safely execute a function and suppress any exceptions.

    If an exception is raised during the execution of the wrapped function,
    it is caught and logged as a warning. The function will return None in such cases.

    Useful for non-critical operations where failure should not interrupt the main flow.

    Returns:
        The result of the function if successful, or None if an exception occurred.
    """

    def wrapper(*args, **kwargs):
        result = None
        try:
            result = func(*args, **kwargs)
        except Exception as ex:
            logger.warning(ex)
        return result

    return wrapper


def enable_high_recovery(func):
    """
    Decorator to temporarily switch the Ceph recovery profile to 'high_recovery_ops'
    during the execution of the wrapped function, and revert it back afterward.

    This is useful when performing operations like OSD replacement or data rebalancing
    that benefit from faster Ceph recovery performance.

    If the ODF CLI runner or current profile cannot be determined, the function executes without change.

    The switch is always reverted, even if the function raises an exception.

    Returns:
        The result of the wrapped function.
    """

    def wrapper(*args, **kwargs):
        odf_cli_runner = safe_exec(odf_cli_setup_helper)()
        if not odf_cli_runner:
            logger.warning(
                "ODF CLI runner not available, proceeding without profile switch"
            )
            return func(*args, **kwargs)

        original_profile = safe_exec(odf_cli_runner.get_recovery_profile)()
        if not original_profile:
            logger.warning(
                "Failed to get current recovery profile, proceeding without profile switch"
            )
            return func(*args, **kwargs)

        logger.info("Setting recovery profile to 'high_recovery_ops'")
        safe_exec(odf_cli_runner.run_set_recovery_profile_high)()

        try:
            return func(*args, **kwargs)
        finally:
            logger.info(f"Switch to the original recovery profile '{original_profile}'")
            safe_exec(odf_cli_runner.run_set_recovery_profile)(original_profile)

    return wrapper


def enable_high_recovery_if_io_flag(func):
    """
    Decorator that applies 'enable_high_recovery' only if the 'io_in_bg' flag
    is set to True in the test configuration. Otherwise, the function is run as-is.

    Returns:
        The result of the wrapped function.
    """

    def wrapper(*args, **kwargs):
        if not config.RUN.get("io_in_bg"):
            logger.info(
                "The 'io_in_bg' param is not set. Proceeding with the original function..."
            )
            return func(*args, **kwargs)

        # Apply the real decorator 'enable_high_recovery'
        return enable_high_recovery(func)(*args, **kwargs)

    return wrapper
