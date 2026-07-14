import logging
import functools

from ocs_ci.framework import config
from ocs_ci.helpers.odf_cli import odf_cli_setup_helper
from ocs_ci.utility.retry import catch_exceptions

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


def enable_high_recovery(func):
    """
    Decorator to temporarily boost Ceph recovery performance during the execution
    of the wrapped function by applying both Ceph and mClock high-recovery profiles.

    This is useful for operations such as OSD replacement or data rebalancing
    where faster recovery is desired at the cost of reduced client I/O bandwidth.

    Behavior:
    - Sets the Ceph recovery profile to 'high_recovery_ops'
    - Sets the mClock profile to 'high_client_ops'
    - Restores both profiles to their original or default state after function execution

    If the ODF CLI runner is not available or the current profile cannot be determined,
    the wrapped function is executed without making any changes.

    Returns:
        The result of the wrapped function.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Import here to avoid circular loop
        from ocs_ci.deployment.helpers.odf_deployment_helpers import (
            set_ceph_mclock_high_client_recovery_profile,
            set_ceph_mclock_balanced_profile,
        )

        odf_cli_runner = catch_exceptions(Exception)(odf_cli_setup_helper)()
        if not odf_cli_runner:
            logger.warning(
                "ODF CLI runner not available, proceeding without profile switch"
            )
            return func(*args, **kwargs)

        original_profile = catch_exceptions(Exception)(
            odf_cli_runner.get_recovery_profile
        )()
        if not original_profile:
            logger.warning(
                "Failed to get current recovery profile, proceeding without profile switch"
            )
            return func(*args, **kwargs)

        logger.info("Setting recovery profile to 'high_recovery_ops'")
        catch_exceptions(Exception)(odf_cli_runner.run_set_recovery_profile_high)()

        logger.info("Setting mclock recovery profile to 'high_recovery_ops'")
        catch_exceptions(Exception)(set_ceph_mclock_high_client_recovery_profile)()

        try:
            return func(*args, **kwargs)
        finally:
            logger.info(f"Switch to the original recovery profile '{original_profile}'")
            catch_exceptions(Exception)(odf_cli_runner.run_set_recovery_profile)(
                original_profile
            )
            logger.info(
                "Setting mclock recovery profile to the default 'balanced' profile"
            )
            catch_exceptions(Exception)(set_ceph_mclock_balanced_profile)()

    return wrapper


def enable_high_recovery_during_rebalance_flag(func):
    """
    Decorator that applies 'enable_high_recovery' only if the
    'enable_high_recovery_during_rebalance' flag is set to True
    in the test configuration. Otherwise, the function runs as-is.

    Returns:
        The result of the wrapped function.

    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if not config.ENV_DATA.get("enable_high_recovery_during_rebalance"):
            logger.info(
                "The 'enable_high_recovery_during_rebalance' flag is not set. "
                "Proceeding with the original function..."
            )
            return func(*args, **kwargs)

        # Apply the real decorator 'enable_high_recovery'
        return enable_high_recovery(func)(*args, **kwargs)

    return wrapper
