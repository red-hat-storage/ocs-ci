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
            logger.info("Switching back to the original cluster")
            config.switch_ctx(orig_index)

    return inner
