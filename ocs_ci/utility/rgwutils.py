import logging

from ocs_ci.framework import config

log = logging.getLogger(__name__)


def get_rgw_count():
    """
    Get RGW Count

    RGW Count is 2 if it is an arbiter deployment, otherwise it is 1.

    Returns:
        int: RGW Count

    """
    if config.DEPLOYMENT.get("arbiter_deployment"):
        log.debug("RGW Count: 2")
        return 2
    else:
        log.debug("RGW Count: 1")
        return 1
