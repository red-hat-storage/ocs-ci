import logging

from ocs_ci.framework import config


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
